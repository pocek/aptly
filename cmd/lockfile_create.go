package cmd

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/aptly-dev/aptly/deb"
	"github.com/smira/commander"
	"github.com/smira/flag"
)

func generateEdsp(install_packages []string,
                  out_writer io.Writer, allPkgs []deb.Stanza) error {
	out := bufio.NewWriter(out_writer)
	defer out.Flush()

	fmt.Fprintf(out, "Request: %s\n", "EDSP 0.5")
	architectures := strings.Split(
		context.Flags().Lookup("architectures").Value.Get().(string), ",")
	if len(architectures) == 0 || architectures[0] == "" {
		architectures = []string{"amd64", "i386"}
	}
	// The apt solver seems to be very sensitive to the ordering here of these
	// fields.  Do not change.
	fmt.Fprintf(out, "Architectures: %s\n", strings.Join(architectures, " "))
	fmt.Fprintf(out, "Install: %s\n", strings.Join(install_packages, " "))
	fmt.Fprintf(out, "Architecture: %s\n\n", architectures[0])

	for _, pkg := range allPkgs {
		s := pkg.Copy()
		delete(s, "Recommends")
		delete(s, "Suggests")
		err := s.WriteTo(out, deb.FILETYPE_BINARY)
		if err != nil {
			return err
		}
		err = out.WriteByte('\n')
		if err != nil {
			return err
		}
	}
	return nil
}

func aptlyLockfileCreate(cmd *commander.Command, args []string) error {
	mirrors := strings.Split(
		context.Flags().Lookup("mirrors").Value.Get().(string), ",")
	if len(mirrors) == 1 && mirrors[0] == "" {
		return fmt.Errorf("No mirrors specified")
	}

	allPkgs := []deb.Stanza{};
	installPkgs := map[string]bool{}
	for _, pkgname := range args {
		installPkgs[pkgname] = true
	}

	for _, mirror := range mirrors {
		repo, err := update(cmd, mirror)
		if err != nil {
			return err
		}
		pl := repo.GetPackageList()
		pl.ForEach(func(pkg *deb.Package) error {
			pkg.PinPriority = 500
			s := pkg.Stanza()
			s["APT-Pin"] = strconv.Itoa(pkg.PinPriority)
			s["APT-Candidate"] = "yes"
			if s["Essential"] == "yes" {
				installPkgs[s["Package"]] = true
			}
			if s["Priority"] == "required" {
				installPkgs[s["Package"]] = true
			}
			allPkgs = append(allPkgs, s)
			return nil
		})
	}

	installPkgsList := []string{}
	for pkgname, _ := range installPkgs {
		installPkgsList = append(installPkgsList, pkgname)
	}
	sort.Strings(installPkgsList)

	sort.Slice(allPkgs, func (i, j int) bool {
		if allPkgs[i]["Package"] != allPkgs[j]["Package"] {
			return allPkgs[i]["Package"] < allPkgs[j]["Package"]
		}
		if allPkgs[i]["Version"] != allPkgs[j]["Version"] {
			return allPkgs[i]["Version"] < allPkgs[j]["Version"]
		}
		if allPkgs[i]["Architecture"] != allPkgs[j]["Architecture"] {
			return allPkgs[i]["Architecture"] < allPkgs[j]["Architecture"]
		}
		return false
	})

	{
		uniquePackages := []deb.Stanza{}
		lastKey := ""
		for _, x := range allPkgs {
			if x["SHA256"] != lastKey {
				x["APT-ID"] = strconv.Itoa(len(uniquePackages))
				uniquePackages = append(uniquePackages, x)
				lastKey = x["SHA256"]
			}
		}
		allPkgs = uniquePackages
	}

	if context.Flags().Lookup("print-edsp").Value.Get().(bool) {
		errors := make(chan error)
		go func() {errors <- generateEdsp(installPkgsList, os.Stdout, allPkgs)}()
		err := <-errors
		if err != nil {
			return fmt.Errorf("unable to create lockfile: %s", err)
		}
		return nil;
	} else {
		solver := context.Flags().Lookup("solver").Value.Get().(string)

		installedPkgs := []deb.Stanza{};
		var err error;
		if solver == "no-deps" {
			installedPkgs = solveNoDeps(installPkgs, allPkgs);
		} else {
		    installedPkgs, err = solveDepsEdsp(solver, installPkgsList, allPkgs);
			if err != nil {
				return fmt.Errorf("Deps solving failed: %s", err)
			}
		}

		sort.Slice(installedPkgs, func(i, j int) bool {
			return installedPkgs[i]["Package"] < installedPkgs[j]["Package"]
		})
		bufWriter := bufio.NewWriter(os.Stdout)
		defer bufWriter.Flush();

		for _, pkg := range installedPkgs {
			pkg.WriteTo(bufWriter, deb.FILETYPE_LOCKFILE)
			fmt.Fprintf(bufWriter, "\n")
		}

		return nil
	}
}

func solveDepsEdsp(solver string, installPkgsList []string, allPkgs []deb.Stanza) (installedPkgs []deb.Stanza, err error) {
	if !path.IsAbs(solver) {
		solver = path.Join("/usr/lib/apt/solvers", solver)
	}

	proc := exec.Command(solver)
	//proc := exec.Command("apt-cudf", "--solver", solver)

	edsp, err := proc.StdinPipe()
	if err != nil {
		return nil, err
	}

	out, err := proc.StdoutPipe()
	if err != nil {
		return nil, err
	}

	proc.Stderr = os.Stderr

	errors := make(chan error)
	go func() {
		err := generateEdsp(installPkgsList, edsp, allPkgs)
		closeerr := edsp.Close()
		if err != nil {
			errors <- err
		}
		if closeerr != nil {
			errors <- closeerr
		}
		close(errors)
	}()

	err = proc.Start()
	if err != nil {
		return nil, err
	}

	reader := deb.NewControlFileReader(out, false, false)

	for {
		stanza, err := reader.ReadStanza()
		if err != nil {
			return nil, err
		}
		if stanza == nil {
			break
		}
		if id, ok := stanza["Install"]; ok {
			iid, err := strconv.Atoi(id)
			if err != nil {
				return nil, fmt.Errorf(
					"Unexpected package id provided by solver %s.  Was expecting int got %s",
					solver, id)
			}
			pkg := allPkgs[iid]
			delete(pkg, "APT-ID")
			delete(pkg, "APT-Pin")
			delete(pkg, "APT-Candidate")
			installedPkgs = append(installedPkgs, pkg)
		} else if message, ok := stanza["Message"]; ok {
			log.Println(message)
		} else {
			log.Println("Unrecognised stanza returned by solver:", stanza)
		}
	}

	err = proc.Wait()
	if err != nil {
		return nil, fmt.Errorf("Solver process failed: %s", err)
	}

	err = <-errors
	if err != nil {
		return nil, fmt.Errorf("Failed generating EDSP: %s", err)
	}
	return installedPkgs, nil;
}

func solveNoDeps(installPkgs map[string]bool, allPkgs []deb.Stanza) []deb.Stanza {
	// This is used if you don't want to perform deps resolution, you just want
	// the listed packages.
	selectedPkgs := map[string]deb.Stanza{}
	for _, pkg := range allPkgs {
		if installPkgs[pkg["Package"]] {
			existing := selectedPkgs[pkg["Package"]]
			if existing == nil || deb.CompareVersions(existing["Version"], pkg["Version"]) < 0 {
				selectedPkgs[pkg["Package"]] = pkg
			}
		}
	}

	installedPkgs := []deb.Stanza{};
	for _, pkg := range selectedPkgs {
		installedPkgs = append(installedPkgs, pkg);
	}
	return installedPkgs
}

func makeCmdLockfile() *commander.Command {
	return &commander.Command{
		UsageLine: "lockfile",
		Short:     "create lockfiles",
		Subcommands: []*commander.Command{
			makeCmdLockfileCreate(),
		},
	}
}

func makeCmdLockfileCreate() *commander.Command {
	cmd := &commander.Command{
		Run:       aptlyLockfileCreate,
		UsageLine: "create -mirror xyz packages...",
		Short:     "create lockfile",
		Long: `Writes a lockfile to stdout`,
		Flag: *flag.NewFlagSet("aptly-mirror-update", flag.ExitOnError),
	}

	cmd.Flag.Bool("force", false, "force update mirror even if it is locked by another process")
	cmd.Flag.Bool("ignore-checksums", false, "ignore checksum mismatches while downloading package files and metadata")
	cmd.Flag.Bool("print-edsp", false, "write EDSP format to stdout")
	cmd.Flag.Int64("download-limit", 0, "limit download speed (kbytes/sec)")
	cmd.Flag.Int("max-tries", 1, "max download tries till process fails with download error")
	cmd.Flag.Var(&keyRingsFlag{}, "keyring", "gpg keyring to use when verifying Release file (could be specified multiple times)")
	cmd.Flag.String("architectures", "", "list of architectures comma separated")
	cmd.Flag.String("mirrors", "", "list of mirrors comma separated")
	cmd.Flag.String("solver", "apt", "CUDF solver to use for dependency resolution")

	return cmd
}

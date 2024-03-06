package cmd

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/aptly-dev/aptly/deb"
	"github.com/smira/commander"
	"github.com/smira/flag"
)

func aptlyLockfileCreate(cmd *commander.Command, args []string) error {
	mirrors := strings.Split(
		context.Flags().Lookup("mirrors").Value.Get().(string), ",")
	solver := context.Flags().Lookup("solver").Value.Get().(string)
	if len(mirrors) == 1 && mirrors[0] == "" {
		return fmt.Errorf("No mirrors specified")
	}

	if solver == "" {
		return fmt.Errorf("No solver specified")
	}

	repos := []*deb.RemoteRepo{}

	for _, mirror := range mirrors {
		repo, err := update(cmd, mirror)
		if err != nil {
			return err
		}
		repos = append(repos, repo)
	}

	aptReleaseFields := []string{"Version", "Origin", "Suite", "Codename", "Label"}
	aptReleaseFieldsShort := map[string]string{
		"Version": "v",
		"Origin": "o",
		"Suite": "a",
		"Codename": "n",
		"Label": "l",
	}

	var err error

	proc := exec.Command(solver)

	pipe, err := proc.StdinPipe()
	if err != nil {
		return err
	}

	proc.Stdout = os.Stdout
	proc.Stderr = os.Stderr

	err = proc.Start()
	if err != nil {
		return err
	}

	collection := context.CollectionFactory().RemoteRepoCollection()

	out := bufio.NewWriter(pipe)

	for idx, repo := range repos {
		pl := repo.GetPackageList()
		err = pl.ForEach(func(pkg *deb.Package) error {
			s := pkg.Stanza()
			s["X-Archive-Root"] = strings.TrimSuffix(repo.ArchiveRoot, "/")
			s["X-Mirror"] = repo.Name
			s["X-Component"] = pkg.Component
			s["Source-Version"] = pkg.GetField("$SourceVersion")
			s["Source"] = pkg.GetField("$Source")

			// Construct APT-Release field.
			// This is not fully correct, because one package may be present in
			// many releases. Can be improved later if needed.
			aptRelease := []string{}
			for _, field := range aptReleaseFields {
				val, ok := repo.Meta[field]
				if ok {
					aptRelease = append(aptRelease, fmt.Sprintf("%s=%s", aptReleaseFieldsShort[field], val))
				}
			}
			if pkg.Component != "" {
				aptRelease = append(aptRelease, fmt.Sprintf("c=%s", pkg.Component))
			}
			if len(aptRelease) > 0 {
				s["APT-Release"] = " " + strings.Join(aptRelease, ",")
			}

			err = s.WriteTo(out, deb.FILETYPE_LOCKFILE)
			if err != nil {
				return err
			}
			err = out.WriteByte('\n')
			if err != nil {
				return err
			}

			out.Flush()

			return nil
		})

		if err != nil {
			return err
		}

		err = collection.Drop(repo)
		repos[idx] = nil

		if err != nil {
			return err
		}
	}

	out.Flush()

	err = pipe.Close()
	if err != nil {
		return err
	}

	err = proc.Wait()
	if err != nil {
		return fmt.Errorf("Solver process failed: %s", err)
	}

	return nil
}


func makeCmdLockfile() *commander.Command {
	return &commander.Command{
		UsageLine: "lockfile",
		Short:     "update mirrors and output package indexes for further processing",
		Subcommands: []*commander.Command{
			makeCmdLockfileCreate(),
		},
	}
}

func makeCmdLockfileCreate() *commander.Command {
	cmd := &commander.Command{
		Run:       aptlyLockfileCreate,
		UsageLine: "create -mirror xyz",
		Short:     "update mirrors and output package indexes for further processing",
		Long:      `Feeds package lists into a solver/lockfile generator helper`,
		Flag:      *flag.NewFlagSet("aptly-mirror-update", flag.ExitOnError),
	}

	cmd.Flag.Bool("force", false, "force update mirror even if it is locked by another process")
	cmd.Flag.Bool("ignore-checksums", false, "ignore checksum mismatches while downloading package files and metadata")
	cmd.Flag.Int64("download-limit", 0, "limit download speed (kbytes/sec)")
	cmd.Flag.Int("max-tries", 1, "max download tries till process fails with download error")
	cmd.Flag.Var(&keyRingsFlag{}, "keyring", "gpg keyring to use when verifying Release file (could be specified multiple times)")
	cmd.Flag.String("mirrors", "", "list of mirrors comma separated")
	cmd.Flag.String("solver", "", "solver/lockfile generator helper to feed package lists to")

	return cmd
}

package cmd

import (
	"fmt"
	"strings"
	"sync"

	"github.com/aptly-dev/aptly/aptly"
	"github.com/aptly-dev/aptly/deb"
	"github.com/aptly-dev/aptly/query"
	"github.com/aptly-dev/aptly/utils"
	"github.com/smira/commander"
	"github.com/smira/flag"
)

func aptlyMirrorUpdate(cmd *commander.Command, args []string) error {
	if len(args) != 1 {
		cmd.Usage()
		return commander.ErrCommandError
	}

	repo, err := update(cmd, args[0])
	if err != nil {
		return err
	}

	return download(cmd, repo);
}

func update(cmd *commander.Command, name string) (*deb.RemoteRepo, error) {
	repo, err := context.CollectionFactory().RemoteRepoCollection().ByName(name)
	if err != nil {
		return nil, fmt.Errorf("unable to update: %s", err)
	}

	err = context.CollectionFactory().RemoteRepoCollection().LoadComplete(repo)
	if err != nil {
		return nil, fmt.Errorf("unable to update: %s", err)
	}

	force := context.Flags().Lookup("force").Value.Get().(bool)
	if !force {
		err = repo.CheckLock()
		if err != nil {
			return nil, fmt.Errorf("unable to update: %s", err)
		}
	}

	ignoreMismatch := context.Flags().Lookup("ignore-checksums").Value.Get().(bool)
	maxTries := context.Flags().Lookup("max-tries").Value.Get().(int)

	verifier, err := getVerifier(context.Flags())
	if err != nil {
		return nil, fmt.Errorf("unable to initialize GPG verifier: %s", err)
	}

	err = repo.Fetch(context.Downloader(), verifier)
	if err != nil {
		return nil, fmt.Errorf("unable to update: %s", err)
	}

	context.Progress().PrintfStdErr("Downloading & parsing package files...\n")
	err = repo.DownloadPackageIndexes(context.Progress(), context.Downloader(), verifier, context.CollectionFactory(), ignoreMismatch, maxTries)
	if err != nil {
		return nil, fmt.Errorf("unable to update: %s", err)
	}

	if repo.Filter != "" {
		context.Progress().PrintfStdErr("Applying filter...\n")
		var filterQuery deb.PackageQuery

		filterQuery, err = query.Parse(repo.Filter)
		if err != nil {
			return nil, fmt.Errorf("unable to update: %s", err)
		}

		var oldLen, newLen int
		oldLen, newLen, err = repo.ApplyFilter(context.DependencyOptions(), filterQuery, context.Progress())
		if err != nil {
			return nil, fmt.Errorf("unable to update: %s", err)
		}
		context.Progress().PrintfStdErr("Packages filtered: %d -> %d.\n", oldLen, newLen)
	}

	return repo, nil
}

func download(cmd *commander.Command, repo *deb.RemoteRepo) error {
	ignoreMismatch := context.Flags().Lookup("ignore-checksums").Value.Get().(bool)
	maxTries := context.Flags().Lookup("max-tries").Value.Get().(int)

	var (
		downloadSize int64
		queue        []deb.PackageDownloadTask
		err          error
	)

	skipExistingPackages := context.Flags().Lookup("skip-existing-packages").Value.Get().(bool)

	context.Progress().PrintfStdErr("Building download queue...\n")
	queue, downloadSize, err = repo.BuildDownloadQueue(context.PackagePool(), context.CollectionFactory().PackageCollection(),
		context.CollectionFactory().ChecksumCollection(), skipExistingPackages)

	if err != nil {
		return fmt.Errorf("unable to update: %s", err)
	}

	defer func() {
		// on any interruption, unlock the mirror
		err = context.ReOpenDatabase()
		if err == nil {
			repo.MarkAsIdle()
			context.CollectionFactory().RemoteRepoCollection().Update(repo)
		}
	}()

	repo.MarkAsUpdating()
	err = context.CollectionFactory().RemoteRepoCollection().Update(repo)
	if err != nil {
		return fmt.Errorf("unable to update: %s", err)
	}

	err = context.CloseDatabase()
	if err != nil {
		return fmt.Errorf("unable to update: %s", err)
	}

	context.GoContextHandleSignals()

	count := len(queue)
	context.Progress().PrintfStdErr("Download queue: %d items (%s)\n", count, utils.HumanBytes(downloadSize))

	// Download from the queue
	context.Progress().InitBar(downloadSize, true)

	downloadQueue := make(chan int)

	var (
		errors  []string
		errLock sync.Mutex
	)

	pushError := func(err error) {
		errLock.Lock()
		errors = append(errors, err.Error())
		errLock.Unlock()
	}

	go func() {
		for idx := range queue {
			select {
			case downloadQueue <- idx:
			case <-context.Done():
				return
			}
		}
		close(downloadQueue)
	}()

	var wg sync.WaitGroup

	for i := 0; i < context.Config().DownloadConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case idx, ok := <-downloadQueue:
					if !ok {
						return
					}

					task := &queue[idx]

					var e error

					// provision download location
					task.TempDownPath, e = context.PackagePool().(aptly.LocalPackagePool).GenerateTempPath(task.File.Filename)
					if e != nil {
						pushError(e)
						continue
					}

					// download file...
					e = context.Downloader().DownloadWithChecksum(
						context,
						repo.PackageURL(task.File.DownloadURL()).String(),
						task.TempDownPath,
						&task.File.Checksums,
						ignoreMismatch,
						maxTries)
					if e != nil {
						pushError(e)
						continue
					}

					task.Done = true
				case <-context.Done():
					return
				}
			}
		}()
	}

	// Wait for all download goroutines to finish
	wg.Wait()

	context.Progress().ShutdownBar()

	err = context.ReOpenDatabase()
	if err != nil {
		return fmt.Errorf("unable to update: %s", err)
	}

	// Import downloaded files
	context.Progress().InitBar(int64(len(queue)), false)

	for idx := range queue {
		context.Progress().AddBar(1)

		task := &queue[idx]

		if !task.Done {
			// download not finished yet
			continue
		}

		// and import it back to the pool
		task.File.PoolPath, err = context.PackagePool().Import(task.TempDownPath, task.File.Filename, &task.File.Checksums, true, context.CollectionFactory().ChecksumCollection())
		if err != nil {
			return fmt.Errorf("unable to import file: %s", err)
		}

		// update "attached" files if any
		for _, additionalTask := range task.Additional {
			additionalTask.File.PoolPath = task.File.PoolPath
			additionalTask.File.Checksums = task.File.Checksums
		}
	}

	context.Progress().ShutdownBar()

	select {
	case <-context.Done():
		return fmt.Errorf("unable to update: interrupted")
	default:
	}

	if len(errors) > 0 {
		return fmt.Errorf("unable to update: download errors:\n  %s", strings.Join(errors, "\n  "))
	}

	repo.FinalizeDownload(context.CollectionFactory(), context.Progress())
	err = context.CollectionFactory().RemoteRepoCollection().Update(repo)
	if err != nil {
		return fmt.Errorf("unable to update: %s", err)
	}

	context.Progress().PrintfStdErr("\nMirror `%s` has been successfully updated.\n", repo.Name)
	return err
}

func makeCmdMirrorUpdate() *commander.Command {
	cmd := &commander.Command{
		Run:       aptlyMirrorUpdate,
		UsageLine: "update <name>",
		Short:     "update mirror",
		Long: `
Updates remote mirror (downloads package files and meta information). When mirror is created,
this command should be run for the first time to fetch mirror contents. This command can be
run multiple times to get updated repository contents. If interrupted, command can be safely restarted.

Example:

  $ aptly mirror update wheezy-main
`,
		Flag: *flag.NewFlagSet("aptly-mirror-update", flag.ExitOnError),
	}

	cmd.Flag.Bool("force", false, "force update mirror even if it is locked by another process")
	cmd.Flag.Bool("ignore-checksums", false, "ignore checksum mismatches while downloading package files and metadata")
	cmd.Flag.Bool("ignore-signatures", false, "disable verification of Release file signatures")
	cmd.Flag.Bool("skip-existing-packages", false, "do not check file existence for packages listed in the internal database of the mirror")
	cmd.Flag.Int64("download-limit", 0, "limit download speed (kbytes/sec)")
	cmd.Flag.Int("max-tries", 1, "max download tries till process fails with download error")
	cmd.Flag.Var(&keyRingsFlag{}, "keyring", "gpg keyring to use when verifying Release file (could be specified multiple times)")

	return cmd
}

// Package aisfs - command-line mounting utility for aisfs.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/NVIDIA/aistore/cmd/aisfs/fs"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/jacobsa/daemonize"
	"github.com/jacobsa/fuse"
	"github.com/urfave/cli"
)

const (
	appName = "aisfs"
	version = "0.2"

	helpTemplate = `DESCRIPTION
	{{ .Name }} - {{ .Usage }}

VERSION:
	{{ .Version }}

USAGE:
	{{ .Name }} [OPTION...] BUCKET MOUNTPOINT

ARGUMENTS:
	BUCKET      bucket name
	MOUNTPOINT  empty directory for mounting the file system

OPTIONS: (must appear before arguments, see USAGE)
	{{- range .VisibleFlags }}
	{{.}}
	{{- end}}

HOW TO UNMOUNT:
	a) Execute command: $ fusermount -u MOUNTPOINT
	b) Execute command: $ umount MOUNTPOINT
			Note: running umount may require root privileges
	c) If aisfs run with --wait flag, press CTRL-C (SIGINT)
`
)

var build string

func init() {
	// Set custom app help template.
	cli.AppHelpTemplate = helpTemplate
}

func runDaemon(mountPath string, errorSink io.Writer) (err error) {
	var (
		executablePath string
		executableArgs []string
		executableEnv  []string
	)

	executablePath, err = os.Executable()
	if err != nil {
		return
	}

	// 1) Make sure that the daemon will wait for the file system to be unmounted.
	//
	// 2) Executable working directory might not be the same as
	//    the current working directory, so it's important to
	//    pass an absolute path of the mountpath directory.
	executableArgs = append([]string{"--wait"}, os.Args[1:len(os.Args)-1]...)
	executableArgs = append(executableArgs, mountPath)

	// Pass a PATH env var in order for daemon to find fusermount.
	executableEnv = []string{fmt.Sprintf("PATH=%s", os.Getenv("PATH"))}

	err = daemonize.Run(executablePath, executableArgs, executableEnv, errorSink)
	if err != nil {
		err = fmt.Errorf("failed to invoke a daemon: %v", err)
	}

	return
}

func dispatchSignalHandlers(mountPath string, mntCfg *fuse.MountConfig, serverCfg *fs.ServerConfig) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGHUP)
	go func() {
		for {
			s := <-signalCh
			switch s {
			case os.Interrupt:
				// If this executable is not run as a daemon, then allow Ctrl-C
				// to interrupt and unmount the file system.
				// Useful only if --wait flag was given by the user.

				err := fuse.Unmount(mountPath)
				if err == nil {
					return
				}
				mntCfg.ErrorLogger.Printf("failed to unmount upon SIGINT: %v", err)
			case syscall.SIGHUP:
				cfg, err := loadConfig(serverCfg.BucketName)
				if err != nil {
					mntCfg.ErrorLogger.Printf("failed to reload config upon SIGHUP: %v", err)
					break
				}
				cfg.writeTo(serverCfg)
				if mntCfg.DebugLogger != nil {
					mntCfg.DebugLogger.Printf("config successfully reloaded upon SIGHUP")
				}
			default:
				panic(s)
			}
		}
	}()
}

func newApp() *cli.App {
	return &cli.App{
		Name:      appName,
		Usage:     fmt.Sprintf("command-line mounting utility for %s", fs.Name),
		Version:   fmt.Sprintf("%s (build %s)", version, build),
		Writer:    os.Stdout,
		ErrWriter: os.Stderr,
		Action:    appMain,

		// Hide built-in 'help' subcommand (`aisfs help`).
		// This will also hide the global -h,--help flag.
		HideHelp: true,

		OnUsageError: func(_ *cli.Context, err error, _ bool) error {
			return incorrectUsageError(err)
		},

		Flags: []cli.Flag{

			cli.VersionFlag,

			// Add global -h,--help flag, otherwise it will remain hidden.
			cli.HelpFlag,

			cli.BoolFlag{
				Name:  "wait",
				Usage: "wait for file system to be unmounted",
			},

			cli.IntFlag{
				Name:  "uid",
				Value: -1,
				Usage: "uid of mount owner",
			},

			cli.IntFlag{
				Name:  "gid",
				Value: -1,
				Usage: "gid of mount owner",
			},

			cli.StringSliceFlag{
				Name:  "o",
				Usage: "additional mount options (see 'man 8 mount')",
			},
		},
	}
}

// App entry point.
func appMain(c *cli.Context) (err error) {
	var (
		flags     *flags
		cfg       *Config
		cluURL    string
		bucket    string
		mountDir  string
		mountPath string
		errorLog  *log.Logger
		debugLog  *log.Logger
		mountCfg  *fuse.MountConfig
		fsowner   *fs.Owner
		mfs       *fuse.MountedFileSystem
		server    fuse.Server
	)

	// If -h,--help flag is set, just show help message and return.
	if helpRequested(c) {
		cli.ShowAppHelp(c)
		return
	}

	if c.NArg() < 1 {
		return missingArgumentsError("BUCKET", "MOUNTPOINT")
	}
	if c.NArg() < 2 {
		return missingArgumentsError("MOUNTPOINT")
	}
	if c.NArg() > 2 {
		return incorrectUsageError(errors.New("too many arguments"))
	}

	flags = parseFlags(c)
	bucket = c.Args().Get(0)
	mountDir = c.Args().Get(1)

	mountPath, err = filepath.Abs(mountDir)
	if err != nil {
		return
	}

	// If waiting for the file system to be unmounted isn't requested,
	// run a daemon in the background that will mount the file system
	// and then IT will wait for the file system to be unmounted.
	if !flags.Wait {
		// Blocks until the daemon signals the outcome of mounting.
		return runDaemon(mountPath, c.App.ErrWriter)
	}

	// NOTE: The rest of the function should be viewed from two perspectives:
	// 1) Current process will execute it if --wait flag was given by the user.
	// 2) Daemon process will execute it if --wait flag was not given by the user.
	blocked := true
	// In case of 1) signal() does nothing.
	// In case of 2) signal() unblocks the calling process.
	sig := func() {
		if blocked {
			daemonize.SignalOutcome(err)
			blocked = false
		}
	}
	defer sig()

	fsowner, err = initOwner(flags)
	if err != nil {
		return
	}

	// Try to load existing config from file or use default one.
	cfg, err = loadConfig(bucket)
	if err != nil {
		return
	}

	// Validate and test cluster URL.
	cluURL, err = determineClusterURL(c, cfg, cmn.Bck{Name: bucket})
	if err != nil {
		return
	}

	errorLog, err = prepareLogFile(cfg.Log.ErrorFile, "ERROR: ", bucket)
	if err != nil {
		return
	}

	// If cfg.Log.DebugFile == "" no debug logging is performed.
	if cfg.Log.DebugFile != "" {
		debugLog, err = prepareLogFile(cfg.Log.DebugFile, "DEBUG: ", bucket)
		if err != nil {
			return
		}
	}

	// Useful message describing some fs params, printed only if --wait flag was given by the user.
	fmt.Fprintf(c.App.Writer, "Connecting to proxy at %q\nMounting bucket %q to %q\nuid %d\ngid %d\n",
		cluURL, bucket, mountPath, fsowner.UID, fsowner.GID)

	// Init a server configuration object.
	serverCfg := &fs.ServerConfig{
		MountPath:  mountPath,
		AISURL:     cluURL,
		BucketName: bucket,
		Owner:      fsowner,
	}
	cfg.writeTo(serverCfg)

	// Init a file system server.
	server, err = fs.NewAISFileSystemServer(serverCfg, errorLog)
	if err != nil {
		return
	}

	// Init a mount configuration object.
	mountCfg = &fuse.MountConfig{
		FSName:                  fs.Name,
		ErrorLogger:             errorLog,
		DebugLogger:             debugLog,
		DisableWritebackCaching: true,
		Options:                 flags.AdditionalMountOptions,
	}

	// Mount the file system.
	mfs, err = fuse.Mount(mountPath, server, mountCfg)
	if err != nil {
		return
	}

	// Signal the calling process that mounting was successful.
	sig()

	// Start signal dispatcher which catches different signals and reacts upon
	// receiving them.
	dispatchSignalHandlers(mountPath, mountCfg, serverCfg)

	// Wait for the file system to be unmounted.
	err = mfs.Join(context.Background())

	return
}

func main() {
	// Create and run a new CLI app.
	app := newApp()
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

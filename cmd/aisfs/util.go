// Package aisfs - command-line mounting utility for aisfs.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/aisfs/fs"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/containers"
	"github.com/urfave/cli"
)

const (
	timeStrFormat = "20060102150405"
)

////////////////
// URL HANDLING
////////////////

func determineClusterURL(c *cli.Context, cfg *Config, bck cmn.Bck) (clusterURL string, err error) {
	// Determine which cluster URL will be used
	clusterURL = cfg.Cluster.URL
	if clusterURL == "" {
		clusterURL = discoverClusterURL(c)
	}

	// Check if URL is malformed
	if _, err = url.Parse(clusterURL); err != nil {
		err = fmt.Errorf("malformed URL (%q): %v", clusterURL, err)
		return "", err
	}

	// Try to access the bucket, possibly catching an early error
	if ok := tryAccessBucket(clusterURL, bck); !ok {
		err = fmt.Errorf("no response from proxy at %q (bucket %q)", clusterURL, bck)
		return "", err
	}

	return
}

func discoverClusterURL(c *cli.Context) string {
	const (
		defaultAISURL       = "http://127.0.0.1:8080"
		defaultAISDockerURL = "http://172.50.0.2:8080"
		dockerErrMsgFmt     = "Failed to discover docker proxy URL: %v.\nUsing default %q.\n"
	)
	setURLMsg := fmt.Sprintf("Set URL with: export %s=`url`.", env.AIS.Endpoint)

	if envURL := os.Getenv(env.AIS.Endpoint); envURL != "" {
		return envURL
	}

	if containers.DockerRunning() {
		clustersIDs, err := containers.ClusterIDs()
		if err != nil {
			fmt.Fprintf(c.App.ErrWriter, dockerErrMsgFmt, err, defaultAISDockerURL)
			fmt.Fprintln(c.App.ErrWriter, setURLMsg)
			return defaultAISDockerURL
		}

		cos.AssertMsg(len(clustersIDs) > 0, "there should be at least one cluster running when docker is detected")
		proxyGateway, err := containers.ClusterProxyURL(clustersIDs[0])
		if err != nil {
			fmt.Fprintf(c.App.ErrWriter, dockerErrMsgFmt, err, defaultAISDockerURL)
			fmt.Fprintln(c.App.ErrWriter, setURLMsg)
			return defaultAISDockerURL
		}

		if len(clustersIDs) > 1 {
			fmt.Fprintf(c.App.ErrWriter, "Multiple docker clusters running. Connected to %d via %s.\n", clustersIDs[0], proxyGateway)
			fmt.Fprintln(c.App.ErrWriter, setURLMsg)
		}

		return "http://" + proxyGateway + ":8080"
	}

	return defaultAISURL
}

func tryAccessBucket(url string, bck cmn.Bck) bool {
	baseParams := api.BaseParams{
		Client: &http.Client{},
		URL:    url,
	}

	_, err := api.HeadBucket(baseParams, bck)
	return err == nil
}

//////////////////
// ERROR HANDLING
//////////////////

type errUsage struct {
	message string
}

func (e *errUsage) Error() string {
	return fmt.Sprintf("Incorrect usage of %s: %s\nRun '%s -h' for help", appName, e.message, appName)
}

func incorrectUsageError(err error) error {
	cos.Assert(err != nil)
	return &errUsage{
		message: err.Error(),
	}
}

func missingArgumentsError(missingArguments ...string) error {
	cos.Assert(len(missingArguments) > 0)
	return &errUsage{
		message: fmt.Sprintf("missing arguments: %s.", strings.Join(missingArguments, ", ")),
	}
}

///////////
// LOGGING
///////////

func buildFileNamePrefix(bucket string) string {
	return fmt.Sprintf("%s.%s.%s.*.ERROR.log", appName, bucket, time.Now().Format(timeStrFormat))
}

func updateSymlink(symlink, target string) (err error) {
	if _, err = os.Lstat(symlink); err == nil { // symlink already exists
		if err = os.Remove(symlink); err != nil {
			return fmt.Errorf("failed to unlink %q: %v", symlink, err)
		}
	}

	return os.Symlink(target, symlink)
}

func prepareLogFileTmpDir(prefix, bucket string) (*log.Logger, error) {
	var (
		tmpDir      = os.TempDir()
		symlinkName = fmt.Sprintf("aisfs.%s.ERROR.log", bucket)
		file        *os.File
		err         error
	)

	file, err = os.CreateTemp(tmpDir, buildFileNamePrefix(bucket))
	if err != nil {
		return nil, fmt.Errorf("failed to create log file in temp directory: %v", err)
	}

	if err = updateSymlink(filepath.Join(tmpDir, symlinkName), file.Name()); err != nil {
		os.Remove(file.Name())
		return nil, fmt.Errorf("failed to update symlink to latest log: %v", err)
	}

	return log.New(file, prefix, log.LstdFlags|log.Lmicroseconds|log.Lshortfile), nil
}

func prepareLogFile(fileName, prefix, bucket string) (*log.Logger, error) {
	if fileName == "" {
		return prepareLogFileTmpDir(prefix, bucket)
	}

	file, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}

	return log.New(file, prefix, log.LstdFlags|log.Lmicroseconds|log.Lshortfile), nil
}

///////////
// HELPERS
///////////

func globalFlagSet(c *cli.Context, flag cli.Flag) bool {
	flagName := strings.Split(flag.GetName(), ",")[0]
	return c.GlobalIsSet(flagName)
}

func helpRequested(c *cli.Context) bool {
	return globalFlagSet(c, cli.HelpFlag)
}

func splitOnFirst(str, sep string) (string, string) {
	split := strings.Index(str, sep)
	if split != -1 {
		return str[:split], str[split+1:]
	}
	return str, ""
}

func initOwner(flags *flags) (owner *fs.Owner, err error) {
	var (
		currentUser      *user.User
		userUID, userGID uint64
	)

	currentUser, err = user.Current()
	if err != nil {
		return
	}

	userUID, err = strconv.ParseUint(currentUser.Uid, 10, 32)
	if err != nil {
		return
	}

	userGID, err = strconv.ParseUint(currentUser.Gid, 10, 32)
	if err != nil {
		return
	}

	owner = &fs.Owner{
		UID: uint32(userUID),
		GID: uint32(userGID),
	}

	if flags.UID > 0 {
		owner.UID = uint32(flags.UID)
	}

	if flags.GID > 0 {
		owner.GID = uint32(flags.GID)
	}

	return owner, nil
}

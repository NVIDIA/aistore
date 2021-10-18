// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unsafe"
)

const GitHubHome = "https://github.com/NVIDIA/aistore"

const (
	configHomeEnvVar = "XDG_CONFIG_HOME"                // https://wiki.archlinux.org/index.php/XDG_Base_Directory
	GcsUA            = "gcloud-golang-storage/20151204" // from cloud.google.com/go/storage/storage.go (userAgent).
)

var (
	EnvVars = struct {
		Endpoint           string
		ShutdownMarkerPath string
		IsPrimary          string
		PrimaryID          string
		SkipVerifyCrt      string
		UseHTTPS           string
		NumTarget          string
		NumProxy           string
	}{
		Endpoint:           "AIS_ENDPOINT",
		IsPrimary:          "AIS_IS_PRIMARY",
		PrimaryID:          "AIS_PRIMARY_ID",
		SkipVerifyCrt:      "AIS_SKIP_VERIFY_CRT",
		UseHTTPS:           "AIS_USE_HTTPS",
		ShutdownMarkerPath: "AIS_SHUTDOWN_MARKER_PATH",

		// Env variables used for tests or CI
		NumTarget: "NUM_TARGET",
		NumProxy:  "NUM_PROXY",
	}

	bucketReg *regexp.Regexp
	nsReg     *regexp.Regexp

	thisNodeName string
)

func init() {
	bucketReg = regexp.MustCompile(`^[.a-zA-Z0-9_-]*$`)
	nsReg = regexp.MustCompile(`^[a-zA-Z0-9_-]*$`)
	GCO = &globalConfigOwner{}
	GCO.c.Store(unsafe.Pointer(&Config{}))
}

func SetNodeName(name string) { thisNodeName = name }

// WaitForFunc executes a function in goroutine and waits for it to finish.
// If the function runs longer than `timeLong` WaitForFunc notifies a user
// that the user should wait for the result
func WaitForFunc(f func() error, timeLong time.Duration) error {
	timer := time.NewTimer(timeLong)
	chDone := make(chan struct{}, 1)
	var err error
	go func() {
		err = f()
		chDone <- struct{}{}
	}()

loop:
	for {
		select {
		case <-timer.C:
			fmt.Println("Please wait, the operation may take some time...")
		case <-chDone:
			timer.Stop()
			break loop
		}
	}

	return err
}

// BytesHead returns first `length` bytes from the slice as a string
func BytesHead(b []byte, length ...int) string {
	maxLength := 16
	if len(length) != 0 {
		maxLength = length[0]
	}
	if len(b) < maxLength {
		return string(b)
	}
	return string(b[:maxLength]) + "..."
}

//////////////////////////////
// config: path, load, save //
//////////////////////////////

func homeDir() string {
	currentUser, err := user.Current()
	if err != nil {
		return os.Getenv("HOME")
	}
	return currentUser.HomeDir
}

func AppConfigPath(appName string) (configDir string) {
	// Determine the location of config directory
	if cfgHome := os.Getenv(configHomeEnvVar); cfgHome != "" {
		// $XDG_CONFIG_HOME/appName
		configDir = filepath.Join(cfgHome, appName)
	} else {
		configDir = filepath.Join(homeDir(), ".config", appName)
	}
	return
}

// ExpandPath replaces common abbreviations in file path (eg. `~` with absolute
// path to the current user home directory) and cleans the path.
func ExpandPath(path string) string {
	if path == "" || path[0] != '~' {
		return filepath.Clean(path)
	}
	if len(path) > 1 && path[1] != '/' {
		return filepath.Clean(path)
	}

	currentUser, err := user.Current()
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(filepath.Join(currentUser.HomeDir, path[1:]))
}

// PropToHeader converts a property full name to an HTTP header tag name
func PropToHeader(prop string) string {
	if strings.HasPrefix(prop, headerPrefix) {
		return prop
	}
	prop = strings.ReplaceAll(prop, ".", "-")
	prop = strings.ReplaceAll(prop, "_", "-")
	return headerPrefix + prop
}

// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
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

	thisNodeName string
)

func init() {
	GCO = &globalConfigOwner{}
	GCO.c.Store(unsafe.Pointer(&Config{}))
}

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

// alpha-numeric++ including letters, numbers, dashes (-), and underscores (_)
// period (.) is allowed conditionally except for '..'
func isAlphaPlus(s string, withPeriod bool) bool {
	for i, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		if c != '.' {
			return false
		}
		if !withPeriod || (i < len(s)-1 && s[i+1] == '.') {
			return false
		}
	}
	return true
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

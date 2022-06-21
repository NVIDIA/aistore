// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"os/user"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const GitHubHome = "https://github.com/NVIDIA/aistore"

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
	if strings.HasPrefix(prop, apc.HeaderPrefix) {
		return prop
	}
	if prop[0] == '.' || prop[0] == '_' {
		prop = prop[1:]
	}
	prop = strings.ReplaceAll(prop, ".", "-")
	prop = strings.ReplaceAll(prop, "_", "-")
	return apc.HeaderPrefix + prop
}

// promoted destination object's name
func PromotedObjDstName(objfqn, dirfqn, givenObjName string) (objName string, err error) {
	var baseName string
	givenObjName = strings.TrimRightFunc(givenObjName, func(r rune) bool {
		return r == filepath.Separator
	})
	// first, base name
	if dirfqn == "" {
		// dst = "given-name/(fqn base)" unless the given name itself
		// is a dir/name
		if strings.ContainsRune(givenObjName, filepath.Separator) {
			return givenObjName, nil
		}
		baseName = filepath.Base(objfqn)
	} else {
		baseName, err = filepath.Rel(dirfqn, objfqn)
		debug.AssertNoErr(err)
		if err != nil {
			return
		}
	}
	// destination name
	if givenObjName == "" {
		objName = baseName
	} else {
		objName = filepath.Join(givenObjName, baseName)
	}
	return
}

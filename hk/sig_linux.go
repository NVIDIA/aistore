// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"os"
	"path/filepath"
	"strconv"
)

// TODO: consider moving to `cos` and logging (`stats`) every 4h or so
func numOpenFiles() (int, error) {
	var (
		pid      = os.Getpid()
		proddir  = filepath.Join("/proc", strconv.Itoa(pid), "fd")
		dir, err = os.Open(proddir)
	)
	if err != nil {
		return 0, err
	}
	defer dir.Close()

	// read just the names
	names, e := dir.Readdirnames(0)
	if e != nil {
		return 0, e
	}
	return len(names), nil
}

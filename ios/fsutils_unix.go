// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"golang.org/x/sys/unix"
)

func getFSStats(path string) (fsStats unix.Statfs_t, err error) {
	if err = unix.Statfs(path, &fsStats); err != nil {
		nlog.Errorf("failed to statfs %q, err: %v", path, err)
	}
	return
}

// - on-disk size is sometimes referred to as "apparent size"
// - `withNonDirPrefix` is allowed to match nothing
// - TODO: carefully differentiate FATAL err-s: access perm-s, invalid command-line, executable missing
func executeDU(cmd *exec.Cmd, dirPath string, withNonDirPrefix bool, outputBlockSize uint64) (uint64, error) {
	out, err := cmd.CombinedOutput()
	if err != nil {
		switch {
		case len(out) == 0:
			return 0, fmt.Errorf("du %s: combined output empty, err: %v", dirPath, err)
		default:
			return 0, fmt.Errorf("failed to du %s: %v (%s)", dirPath, err, string(out))
		}
	}

	lines := strings.Split(string(out), "\n") // on Windows, use instead strings.FieldsFunc('\n' and '\r'), here and elsewhere
	if n := len(lines); n > 8 {
		lines = lines[n-8:]
	}
	// e.g.: "12345   total"
	for i := len(lines) - 1; i >= 0; i-- {
		s := lines[i]
		if strings.HasSuffix(s, "total") && s[0] > '0' && s[0] <= '9' {
			return uint64(_parseTotal(s)) * outputBlockSize, nil
		}
	}
	if !withNonDirPrefix {
		err = fmt.Errorf("failed to parse 'du %s': ...%v", dirPath, lines)
	}
	return 0, err
}

func _parseTotal(s string) (size int64) {
	var err error
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			size, err = strconv.ParseInt(s[:i], 10, 64)
			debug.AssertNoErr(err)
			break
		}
	}
	return
}

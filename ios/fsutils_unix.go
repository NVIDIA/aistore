// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			size, err = strconv.ParseInt(s[:i], 10, 64)
			debug.AssertNoErr(err)
			break
		}
	}
	return
}

func DirFileCount(dirPath string) (int, error) {
	cmd := fmt.Sprintf("find %s -type f | wc -l", dirPath)
	outputBytes, err := exec.Command("/bin/sh", "-c", cmd).Output()
	out := string(outputBytes)
	if err != nil || out == "" {
		return 0, fmt.Errorf("failed to count the number of files in %q: %v", dirPath, err)
	}
	out = strings.TrimSpace(out)
	return strconv.Atoi(out)
}

func DirSumFileSizes(dirPath string) (uint64, error) {
	cmd := fmt.Sprintf("find %s -type f | xargs wc -c | tail -1", dirPath)
	outputBytes, err := exec.Command("/bin/sh", "-c", cmd).Output()
	out := string(outputBytes)
	if err != nil || out == "" {
		return 0, fmt.Errorf("failed to correctly sum file sizes in %q: %v", dirPath, err)
	}
	i := strings.IndexByte(out, ' ')
	if i < 0 {
		debug.Assertf(out[0] == '0', "failed to sum file sizes in %q: [%s]", dirPath, out)
		return 0, nil
	}
	return strconv.ParseUint(out[:i], 10, 0)
}

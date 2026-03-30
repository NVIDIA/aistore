// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// best-effort auto-detect running in container
func detect() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}

	var (
		markers = [...]string{"docker", "containerd", "kubepods", "kube", "lxc", "libpod", "podman"}
		yes     bool
	)
	err := cos.ReadLines(rootProcess, func(line string) error {
		line = strings.ToLower(line)
		for _, s := range markers {
			if strings.Contains(line, s) {
				yes = true
				return io.EOF
			}
		}
		return nil
	})
	if err != nil && err != io.EOF {
		nlog.Warningln("failed to detect containerized runtime:", err)
	}
	return yes
}

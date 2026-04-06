// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	// /proc/self/cgroup — this process's cgroup membership (not PID 1's).
	// Using "self" is correct: in K8s with sidecar containers or cgroup
	// namespaces, PID 1 may be in a different cgroup than the AIS process.
	selfCgroup = proc + "self/cgroup"
)

// unified v2 system: /proc/self/cgroup contains a single line: "0::/<relative-path>";
// hybrid v1+v2:      multiple lines; look for the "0::" entry;
// pure v1 system:    "0::" line is absent - return false.
func initCgroupV2Paths() bool {
	var rel string
	err := cos.ReadLines(selfCgroup, func(line string) error {
		if after, ok := strings.CutPrefix(line, "0::"); ok {
			rel = after
			return io.EOF // stop
		}
		return nil
	})
	if err != nil || rel == "" {
		return false
	}

	base := contBase + rel
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}

	if _, err := os.Stat(base + cgV2CPUStat); err != nil {
		nlog.Warningf("cgroup v2: found %s entry %q but %s%s does not exist: %v",
			selfCgroup, rel, base, cgV2CPUStat, err)
		return false
	}

	// standard v2 knobs in the same cgroup directory
	contCPUV2Stat = base + cgV2CPUStat
	contCPUV2Max = base + cgV2CPUMax
	contMemV2Max = base + cgV2MemMax
	contMemV2Current = base + cgV2MemCur
	contMemV2Stat = base + cgV2MemStat

	return true
}

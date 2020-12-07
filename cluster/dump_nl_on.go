// +build debug

// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
)

var dnlOnce sync.Once

func initDumpNameLocks() {
	dnlOnce.Do(func() {
		go dumpNameLocks()
	})
}

func dumpNameLocks() {
	var lines []string
	for {
		time.Sleep(5 * time.Minute)
		a := "bucket"
		for i, nlocker := range []nameLocker{bckLocker, lomLocker} {
			if i > 0 {
				a = "LOM"
			}
			lines = append(lines, a+" name locks:")
			for _, nlc := range nlocker {
				nlc.mu.Lock()
				for uname, info := range nlc.m {
					if info.exclusive {
						lines = append(lines, "   "+uname+"(w)")
					} else {
						lines = append(lines, "   "+uname+"(r)")
					}
				}
				nlc.mu.Unlock()
			}
			if len(lines) > 1 {
				for _, l := range lines {
					debug.Errorln(l)
				}
			}
			lines = lines[:0]
		}
	}
}

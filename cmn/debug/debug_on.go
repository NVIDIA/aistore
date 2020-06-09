// +build debug

// Package provides debug utilities
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package debug

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const (
	Enabled = true
)

func init() {
	loadLogLevel()
}

// loadLogLevel sets debug verbosity for different packages based on
// environment variables. It is to help enable asserts that were originally
// used for testing/initial development and to set the verbosity of glog.
func loadLogLevel() {
	var (
		opts    []string
		modules = map[string]uint8{
			"ais":       glog.SmoduleAIS,
			"cluster":   glog.SmoduleCluster,
			"fs":        glog.SmoduleFS,
			"memsys":    glog.SmoduleMemsys,
			"mirror":    glog.SmoduleMirror,
			"reb":       glog.SmoduleReb,
			"transport": glog.SmoduleTransport,
		}
	)

	// Input will be in the format of AIS_DEBUG=transport=4,memsys=3 (same as GODEBUG).
	if val := os.Getenv("AIS_DEBUG"); val != "" {
		opts = strings.Split(val, ",")
	}

	for _, ele := range opts {
		pair := strings.Split(ele, "=")
		if len(pair) != 2 {
			glog.Fatalf("failed to get module=level element: %q", ele)
		}
		module, level := pair[0], pair[1]
		logModule, exists := modules[module]
		if !exists {
			glog.Fatalf("unknown module: %s", module)
		}
		logLvl, err := strconv.Atoi(level)
		if err != nil || logLvl <= 0 {
			glog.Fatalf("invalid verbosity level=%s, err: %s", level, err)
		}
		glog.SetV(logModule, glog.Level(logLvl))
	}
}

func Errorf(f string, a ...interface{}) {
	glog.ErrorDepth(1, fmt.Sprintf("[DEBUG] "+f, a...))
}

func Infof(f string, a ...interface{}) {
	glog.InfoDepth(1, fmt.Sprintf("[DEBUG] "+f, a...))
}

func Assert(cond bool, a ...interface{}) {
	if !cond {
		glog.Flush()
		if len(a) > 0 {
			panic("DEBUG PANIC: " + fmt.Sprint(a...))
		} else {
			panic("DEBUG PANIC")
		}
	}
}

func AssertFunc(f func() bool, a ...interface{}) { Assert(f(), a...) }

func AssertMsg(cond bool, msg string) {
	if !cond {
		glog.Flush()
		panic("DEBUG PANIC: " + msg)
	}
}

func AssertNoErr(err error) {
	if err != nil {
		glog.Flush()
		panic(err)
	}
}

func Assertf(cond bool, f string, a ...interface{}) { AssertMsg(cond, fmt.Sprintf(f, a...)) }

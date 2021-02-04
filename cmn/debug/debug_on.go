// +build debug

// Package provides debug utilities
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package debug

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const (
	MtxLocked = 1 << (iota + 1)
	MtxRLocked
)

func init() {
	loadLogLevel()
}

func fatalMsg(f string, v ...interface{}) {
	s := fmt.Sprintf(f, v...)
	if s == "" || s[len(s)-1] != '\n' {
		fmt.Fprintln(os.Stderr, s)
	} else {
		fmt.Fprint(os.Stderr, s)
	}
	os.Exit(1)
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
			"ec":        glog.SmoduleEC,
		}
	)

	// Input will be in the format of AIS_DEBUG=transport=4,memsys=3 (same as GODEBUG).
	if val := os.Getenv("AIS_DEBUG"); val != "" {
		opts = strings.Split(val, ",")
	}

	for _, ele := range opts {
		pair := strings.Split(ele, "=")
		if len(pair) != 2 {
			fatalMsg("failed to get module=level element: %q", ele)
		}
		module, level := pair[0], pair[1]
		logModule, exists := modules[module]
		if !exists {
			fatalMsg("unknown module: %s", module)
		}
		logLvl, err := strconv.Atoi(level)
		if err != nil || logLvl <= 0 {
			fatalMsg("invalid verbosity level=%s, err: %s", level, err)
		}
		glog.SetV(logModule, glog.Level(logLvl))
	}
}

func Enabled() bool { return true }

func Errorln(a ...interface{}) {
	if len(a) == 1 {
		glog.ErrorDepth(1, "[DEBUG] ", a[0])
		return
	}
	Errorf("%v", a)
}

func Errorf(f string, a ...interface{}) {
	glog.ErrorDepth(1, fmt.Sprintf("[DEBUG] "+f, a...))
}

func Infof(f string, a ...interface{}) {
	glog.InfoDepth(1, fmt.Sprintf("[DEBUG] "+f, a...))
}

func Func(f func()) { f() }

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

func AssertMutexLocked(m *sync.Mutex) {
	state := reflect.ValueOf(m).Elem().FieldByName("state")
	if state.Int()&1 == 1 {
		return
	}
	AssertMsg(false, "Mutex not locked")
}

func rwMtxLocked(m *sync.RWMutex) bool {
	state := reflect.ValueOf(m).Elem().FieldByName("w").FieldByName("state")
	return state.Int()&1 == 1
}

func rwMtxRLocked(m *sync.RWMutex) bool {
	const maxReaders = 1 << 30 // Taken from `sync/rwmutex.go`.
	rc := reflect.ValueOf(m).Elem().FieldByName("readerCount").Int()
	// NOTE: As it's generally true that `rc > 0` the problem arises when writer
	//  tries to lock the mutex. The writer announces it by manipulating `rc`
	//  (to be specific, decreases `rc` by `maxReaders`). Therefore, to check if
	//  there are any readers still holding lock we need to check both cases.
	return rc > 0 || (0 > rc && rc > -maxReaders)
}

func AssertRWMutex(m *sync.RWMutex, flag int) {
	var res bool
	if flag&MtxLocked != 0 {
		res = res || rwMtxLocked(m)
	}
	if flag&MtxRLocked != 0 {
		res = res || rwMtxRLocked(m)
	}
	AssertMsg(res, "RWMutex not (R)Locked")
}

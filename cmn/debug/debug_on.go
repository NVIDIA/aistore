//go:build debug

// Package provides debug utilities
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package debug

import (
	"bytes"
	"expvar"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

func ON() bool { return true }

func Infof(f string, a ...any) {
	nlog.InfoDepth(1, fmt.Sprintf("[DEBUG] "+f, a...))
}

func Func(f func()) { f() }

func _panic(a ...any) {
	msg := "DEBUG PANIC: "
	if len(a) > 0 {
		msg += fmt.Sprint(a...) + ": "
	}
	buffer := bytes.NewBuffer(make([]byte, 0, 1024))
	buffer.WriteString(msg)
	for i := 2; i < 9; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		// alternatively, the entire stack w/ full pathnames
		if !strings.Contains(file, "aistore") {
			break
		}
		f := filepath.Base(file)
		if l := len(f); l > 3 {
			f = f[:l-3]
		}
		if buffer.Len() > len(msg) {
			buffer.WriteString(" <- ")
		}
		fmt.Fprintf(buffer, "%s:%d", f, line)
	}
	if flag.Parsed() {
		nlog.Errorln(buffer.String())
		nlog.Flush(nlog.ActExit)
	} else {
		fmt.Fprintln(os.Stderr, buffer.String())
	}

	if !nlog.Stopping() { // no panicking when stopping
		panic(msg)
	}
}

func Assert(cond bool, a ...any) {
	if !cond {
		_panic(a...)
	}
}

func AssertFunc(f func() bool, a ...any) {
	if !f() {
		_panic(a...)
	}
}

func AssertNoErr(err error) {
	if err != nil {
		_panic(err)
	}
}

func Assertf(cond bool, f string, a ...any) {
	if !cond {
		msg := fmt.Sprintf(f, a...)
		_panic(msg)
	}
}

func AssertMutexLocked(m *sync.Mutex) {
	state := reflect.ValueOf(m).Elem().FieldByName("state")
	Assert(state.Int()&1 == 1, "Mutex not Locked")
}

func AssertRWMutexLocked(m *sync.RWMutex) {
	state := reflect.ValueOf(m).Elem().FieldByName("w").FieldByName("state")
	Assert(state.Int()&1 == 1, "RWMutex not Locked")
}

func AssertRWMutexRLocked(m *sync.RWMutex) {
	const maxReaders = 1 << 30 // Taken from `sync/rwmutex.go`.
	rc := reflect.ValueOf(m).Elem().FieldByName("readerCount").Int()
	// NOTE: As it's generally true that `rc > 0` the problem arises when writer
	//  tries to lock the mutex. The writer announces it by manipulating `rc`
	//  (to be specific, decreases `rc` by `maxReaders`). Therefore, to check if
	//  there are any readers still holding lock we need to check both cases.
	Assert(rc > 0 || (0 > rc && rc > -maxReaders), "RWMutex not RLocked")
}

func AssertNotPstr(a any) {
	if _, ok := a.(*string); ok {
		_panic(fmt.Errorf("invalid usage: %v (%T)", a, a))
	}
}

func FailTypeCast(a any) {
	val := fmt.Sprint(a)
	if len(val) < 32 {
		_panic(fmt.Errorf("unexpected type %s: (%T)", val, a))
	} else {
		_panic(fmt.Errorf("unexpected type (%T)", a))
	}
}

func Handlers() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"/debug/vars":               expvar.Handler().ServeHTTP,
		"/debug/pprof/":             pprof.Index,
		"/debug/pprof/cmdline":      pprof.Cmdline,
		"/debug/pprof/profile":      pprof.Profile,
		"/debug/pprof/symbol":       pprof.Symbol,
		"/debug/pprof/block":        pprof.Handler("block").ServeHTTP,
		"/debug/pprof/heap":         pprof.Handler("heap").ServeHTTP,
		"/debug/pprof/goroutine":    pprof.Handler("goroutine").ServeHTTP,
		"/debug/pprof/threadcreate": pprof.Handler("threadcreate").ServeHTTP,
	}
}

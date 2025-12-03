// Package nlog - aistore logger, provides buffering, timestamping, writing, and
// flushing/syncing/rotating
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	host = "unknown"
	// assorted filenames that we don't want to show up
	redactFnames = map[string]int{
		"target_stats": 0,
		"proxy_stats":  0,
		"common_stats": 0,
		"err":          0,
	}
	sevText = []string{sevInfo: "INFO", sevErr: "ERROR"}
)

var (
	// of `fixed` bufs
	pool = sync.Pool{
		New: func() any {
			return &fixed{buf: make([]byte, extraSize)}
		},
	}
)

var (
	nlogs [3]*nlog

	logDir  string
	arg0    string
	aisrole string
	title   string

	pid int

	onceInitFiles sync.Once

	stopping atomic.Bool // true when exiting
)

func init() {
	pid = os.Getpid()
	arg0 = filepath.Base(os.Args[0])
	if h, err := os.Hostname(); err == nil {
		host = _shortHost(h)
	}
}

func initFiles() {
	if logDir == "" {
		logDir = filepath.Join(os.TempDir(), "aislogs")
	}
	if err := fcreateAll(sevErr); err != nil {
		panic(fmt.Sprintf("FATAL: unable to create logs in %q: %v", logDir, err))
	}
}

func fcreateAll(sev severity) error {
	now := time.Now()
	for s := sev; s >= sevInfo && nlogs[s] == nil; s-- {
		if s == sevWarn {
			continue
		}
		nlog := newNlog(s)
		if err := nlog.rotate(now); err != nil {
			return err
		}
		nlogs[s] = nlog
	}
	return nil
}

func sname() (name string) {
	name = arg0
	if name == "aisnode" && aisrole != "" {
		name = "ais" + aisrole
	}
	return
}

func _shortHost(hostname string) string {
	if before, _, ok := strings.Cut(hostname, "."); ok {
		return before
	}
	if len(hostname) < 16 || strings.IndexByte(hostname, '-') < 0 {
		return hostname
	}
	// (e.g. "runner-r9rhlq8--project-4149-concurrent-0")
	parts := strings.Split(hostname, "-")
	if len(parts) < 2 {
		return hostname
	}
	if parts[1] != "" || len(parts) == 2 {
		return parts[0] + "-" + parts[1]
	}
	return parts[0] + "-" + parts[2]
}

func fcreate(tag string, t time.Time) (f *os.File, fname string, err error) {
	err = os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		return
	}
	name, link := logfname(tag, t)
	fname = filepath.Join(logDir, name)
	f, err = os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return
	}
	// re-symlink
	symlink := filepath.Join(logDir, link)
	os.Remove(symlink)
	os.Symlink(name, symlink)
	return
}

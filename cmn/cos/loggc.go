// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const gcLogs = "GC logs:"

// GCLogs keeps the total size of accumulated logs below maxtotal, removing the
// oldest files (except the current one) separately per log type.
func GCLogs(logdir string, maxtotal int64, verbose bool) {
	dentries, err := os.ReadDir(logdir)
	if err != nil {
		nlog.Errorln(gcLogs, "cannot read log dir", logdir, "err:", err)
		_ = CreateDir(logdir) // (local non-containerized + kill/restart under test)
		return
	}

	var (
		tot    int64
		n      = len(dentries)
		nn     = n - n>>2
		finfos = make([]iofs.FileInfo, 0, nn)
	)
	for i, logtype := range []string{".INFO.", ".ERROR."} {
		finfos, tot = sizeLogs(dentries, logtype, finfos)
		l := len(finfos)
		switch {
		case tot < maxtotal:
			if verbose {
				nlog.Infoln(gcLogs, "skipping:", logtype, "total:", tot, "max:", maxtotal)
			}
		case l > 1:
			go rmLogs(tot, maxtotal, logdir, logtype, finfos, verbose)
			if i == 0 {
				finfos = make([]iofs.FileInfo, 0, nn)
			}
		default:
			nlog.Warningln(gcLogs, "cannot cleanup a single large", logtype, "size:", tot, "configured max:", maxtotal)
			debug.Assert(l == 1)
			for _, finfo := range finfos {
				nlog.Warningln("\t>>>", gcLogs, filepath.Join(logdir, finfo.Name()))
			}
		}
	}
}

// e.g. name: ais.ip-10-0-2-19.root.log.INFO.20180404-031540.2249
// see also: nlog.InfoLogName, nlog.ErrLogName
func sizeLogs(dentries []os.DirEntry, logtype string, finfos []iofs.FileInfo) (_ []iofs.FileInfo, tot int64) {
	clear(finfos)
	finfos = finfos[:0]
	for _, dent := range dentries {
		if !dent.Type().IsRegular() {
			continue
		}
		if n := dent.Name(); !strings.Contains(n, logtype) {
			continue
		}
		if finfo, err := dent.Info(); err == nil {
			tot += finfo.Size()
			finfos = append(finfos, finfo)
		}
	}
	return finfos, tot
}

func rmLogs(tot, maxtotal int64, logdir, logtype string, finfos []iofs.FileInfo, verbose bool) {
	less := func(i, j int) bool {
		return finfos[i].ModTime().Before(finfos[j].ModTime())
	}
	l := len(finfos)
	if verbose {
		nlog.Infoln(gcLogs, logtype, "total:", tot, "max:", maxtotal, "num:", l)
	}
	sort.Slice(finfos, less)
	finfos = finfos[:l-1] // except the last, i.e. current

	for _, finfo := range finfos {
		fqn := filepath.Join(logdir, finfo.Name())
		if err := RemoveFile(fqn); err == nil {
			tot -= finfo.Size()
			if verbose {
				nlog.Infoln(gcLogs, "removed", fqn)
			}
			if tot < maxtotal {
				break
			}
		} else {
			nlog.Errorln(gcLogs, "failed to remove", fqn, "err:", err)
		}
	}
	nlog.Infoln(gcLogs, "done, new total:", tot)

	clear(finfos)
	finfos = finfos[:0]
}

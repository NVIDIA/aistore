// Package health is a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package health

import (
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
)

const (
	ival = 10 * time.Minute // TODO -- FIXME
)

// per mountpath: recent-or-running
type ror struct {
	last    int64
	running int64
}

var all sync.Map

func (f *FSHC) OnErr(mi *fs.Mountpath, fqn string) {
	var (
		r         *ror
		now       = mono.NanoTime()
		a, loaded = all.LoadOrStore(mi.Path, &ror{last: now, running: now})
	)
	if loaded {
		r = a.(*ror)
		debug.Assert(r != nil)
		prev := ratomic.LoadInt64(&r.last)
		if elapsed := time.Duration(now - prev); elapsed < ival {
			nlog.Infoln(mi.String()+":", "not running - only", elapsed.String(), "passed since the previous run")
			return
		}
		if ratomic.CompareAndSwapInt64(&r.running, 0, now) {
			nlog.Infoln(mi.String()+":", "already running, nothing to do")
			return
		}
	}
	go run(f, mi, r, fqn, now)
}

func run(f *FSHC, mi *fs.Mountpath, r *ror, fqn string, now int64) {
	f.run(mi, fqn)
	ratomic.StoreInt64(&r.last, now)
	ratomic.StoreInt64(&r.running, 0)
}

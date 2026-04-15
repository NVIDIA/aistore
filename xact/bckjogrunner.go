// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

// BckJogRunner extends BckJog with an internalized worker pool.
// Callers specify a single CbObj; everything else — dispatch, channel management,
// slab allocation, and media-aware worker tuning — is handled internally.
//
// Two-tier producer/consumer pipeline:
//   ┌──────────────────────────── BckJogRunner ────────────────────────────┐
//   │ ┌─ BckJog (embedded) ─┐                                              │
//   │ │  jogger[mpath-0] ─┐ │                                              │
//   │ │  jogger[mpath-1] ─┼────► workCh ──► worker-0  ─┐                   │
//   │ │       ...         │ │    (buffered) worker-1   ├──► CbObj(lom,buf) │
//   │ │  jogger[mpath-N] ─┘ │               worker-M  ─┘                   │
//   │ └─────────────────────┘                                              │
//   └──────────────────────────────────────────────────────────────────────┘
//
// Joggers (producers): one goroutine per mountpath; count = len(mountpaths), fixed.
// Workers (consumers): M goroutines; M is the only tunable dimension (NwpDflt = auto).
// NwpNone: no pool — CbObj is called inline by the jogger, parallelism = mountpath count.
//
// BckJogRunner uses _tuneNwpJogger (jogger-specific wrapper around TuneNumWorkers)
// which returns NwpNone — jogger-only mode — in two distinct cases:
//
//  1. Insufficient headroom: goroutine count, memory pressure, or CPU load are too high
//     to safely add workers. The runner falls back gracefully rather than making things worse.
//
//  2. numWorkers <= numMpaths: a pool smaller than or equal to the jogger count adds no
//     parallelism. Joggers already provide one concurrent callback per mountpath; inserting
//     a channel and fewer workers would only serialize and slow things down.
//
// In both cases the outcome is the same: NwpNone. Jogger calls the callback inline,
// no channel, no LIF/LOM reconstruction, no worker goroutines.

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
)

// nwpState manages BckJogRunner's worker pool.
type nwpState struct {
	workCh     chan core.LIF
	wg         sync.WaitGroup
	chanFull   cos.ChanFull
	numWorkers int
}

// BckJogRunnerOpts configures BckJogRunner initialization.
type BckJogRunnerOpts struct {
	// CbObj is called for each visited object.
	CbObj func(*core.LOM, []byte) error

	// WalkBck, when non-nil, overrides the traversal bucket.
	// Use when the xaction's logical bucket differs from the walk source
	// (e.g. copy-bucket registers the destination but walks the source).
	WalkBck *meta.Bck

	// Prefix filters visited objects by name prefix.
	Prefix string

	// RW must be true when the xaction modifies the local filesystem.
	RW bool

	// NumWorkers controls worker pool size:
	//   NwpNone (-1): no pool, objects processed inline
	//   NwpDflt (0):  auto-tune based on media type and current load
	//   > 0:          treat as a ceiling; still throttled under load
	NumWorkers int

	// Burst sets the work-channel lower bound (minimum capacity).
	// Zero means use cmn.XactBurstDflt. Callers with their own burst config
	// (e.g. TCB uses config.TCB.Burst) should pass it explicitly.
	Burst int
}

////////////////////
// BckJogRunner   //
////////////////////

// BckJogRunner extends BckJog with a managed worker pool.
// The caller provides a single CbObj callback; BckJogRunner owns dispatch,
// channel management, slab allocation, and worker lifecycle.
type BckJogRunner struct {
	BckJog
	nwp *nwpState                     // non-nil only when the pool is active.
	cb  func(*core.LOM, []byte) error // called for each visited object.
}

// Init initializes BckJogRunner from opts.
// If opts.NumWorkers != NwpNone, it auto-tunes worker count (media + load)
// and sets up the internal worker pool. Returns an error only if the system
// is under such extreme pressure that starting workers is unsafe.
func (r *BckJogRunner) Init(id, kind string, bck *meta.Bck, opts BckJogRunnerOpts, config *cmn.Config) error {
	r.cb = opts.CbObj

	// Resolve worker count first
	numWorkers := NwpNone
	if opts.NumWorkers != NwpNone {
		var err error
		numWorkers, err = _tuneNwpJogger(id /*xname not yet set*/, opts.NumWorkers, fs.NumAvail())
		if err != nil {
			return err
		}
	}

	// When a worker pool is active, joggers must NOT load the LOM — workers
	// reconstruct a fresh LOM from the LIF and load it themselves.
	doLoad := mpather.Load
	if numWorkers != NwpNone {
		doLoad = mpather.NoLoad // workers call lom.Load() after lif.LOM()
	}

	mpopts := &mpather.JgroupOpts{
		Parent:   r,
		CTs:      []string{fs.ObjCT}, // TODO: make this configurable in `BckJogRunnerOpts` when needed
		VisitObj: r.dispatch,
		Prefix:   opts.Prefix,
		DoLoad:   doLoad,
		RW:       opts.RW,
	}
	if opts.CbObj != nil {
		// per-jogger I/O buffer; workers allocate their own inside runWorker
		slab, err := core.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
		if err != nil {
			return fmt.Errorf("%s: get slab (size=%d): %w", id, memsys.MaxPageSlabSize, err)
		}
		mpopts.Slab = slab
	}
	walkBck := bck
	if opts.WalkBck != nil {
		walkBck = opts.WalkBck
	}
	mpopts.Bck.Copy(walkBck.Bucket())

	r.BckJog.Init(id, kind, bck, mpopts, config)

	if numWorkers == NwpNone {
		return nil
	}
	burst := opts.Burst
	if burst == 0 {
		burst = cmn.XactBurstDflt
	}
	chsize := cos.ClampInt(numWorkers*NwpBurstMult, burst, NwpBurstMax)
	r.nwp = &nwpState{
		workCh:     make(chan core.LIF, chsize),
		numWorkers: numWorkers,
	}
	return nil
}

// dispatch is BckJogRunner's internal VisitObj, wired at Init time.
// Routes each object inline when no pool is active, or into the work channel.
// The calling jogger frees the original LOM on return; workers reconstruct
// a fresh LOM via lif.LOM().
func (r *BckJogRunner) dispatch(lom *core.LOM, buf []byte) error {
	if r.nwp == nil {
		if r.cb != nil {
			return r.cb(lom, buf)
		}
		return nil
	}
	l, c := len(r.nwp.workCh), cap(r.nwp.workCh)
	r.nwp.chanFull.Check(l, c)
	select {
	case r.nwp.workCh <- lom.LIF():
	case <-r.ChanAbort():
		return r.AbortErr()
	}
	return nil
}

// Run launches workers (if any) then starts the mountpath joggers.
func (r *BckJogRunner) Run() {
	if r.nwp != nil {
		nlog.Infoln(r.Name(), "workers:", r.nwp.numWorkers)
		for range r.nwp.numWorkers {
			buf, slab := core.T.PageMM().Alloc()
			r.nwp.wg.Add(1)
			go r.runWorker(buf, slab)
		}
	}
	r.BckJog.Run()
}

func (r *BckJogRunner) runWorker(buf []byte, slab *memsys.Slab) {
	defer func() {
		slab.Free(buf)
		r.nwp.wg.Done()
	}()
	for {
		select {
		case lif, ok := <-r.nwp.workCh:
			if !ok {
				return
			}
			lom, err := lif.LOM()
			if err != nil {
				nlog.Warningln(r.Name(), lif.Cname(), err)
				r.Base.Abort(err)
				return
			}
			if err := lom.Load(false /*cache it*/, false); err != nil {
				core.FreeLOM(lom)
				continue // object may have been removed between dispatch and pickup
			}
			if err := r.cb(lom, buf); err != nil {
				r.AddErr(err, 0)
				if r.AbortErr() != nil {
					core.FreeLOM(lom)
					return
				}
			}
			core.FreeLOM(lom)
		case <-r.ChanAbort():
			return
		}
	}
}

// Wait waits for joggers to finish then drains the worker pool.
func (r *BckJogRunner) Wait() error {
	jogErr := r.BckJog.Wait()
	if r.nwp != nil {
		close(r.nwp.workCh)
		r.nwp.wg.Wait()
		if a := r.nwp.chanFull.Load(); a > 0 {
			nlog.Warningln(r.Name(), "work channel full (final)", a)
		}
	}
	return jogErr
}

// NumWorkers returns the configured worker count (0 if no pool).
func (r *BckJogRunner) NumWorkers() int {
	if r.nwp == nil {
		return 0
	}
	return r.nwp.numWorkers
}

// WorkChanFull returns the accumulated count of work-channel-full events.
func (r *BckJogRunner) WorkChanFull() int64 {
	if r.nwp == nil {
		return 0
	}
	return r.nwp.chanFull.Load()
}

// _tuneNwpJogger is the jogger-specific wrapper around TuneNumWorkers.
// After applying load- and media-aware tuning, it enforces the mountpath guard:
// a pool with numWorkers <= numMpaths adds no parallelism over jogger-inline dispatch.
func _tuneNwpJogger(xname string, numWorkers, numMpaths int) (int, error) {
	numWorkers, err := TuneNumWorkers(xname, numWorkers, numMpaths)
	if err != nil || numWorkers == NwpNone {
		return NwpNone, err
	}
	// pool only adds value when it exceeds jogger count
	if numWorkers <= numMpaths {
		return NwpNone, nil
	}
	return numWorkers, nil
}

// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// Rechunk transforms object storage format (monolithic <-> chunked).
// By default, rechunk operates only on in-cluster (cached) objects - it does not
// fetch objects from remote backends. Use `SyncRemote=true` to also update remote storage.
type (
	rechunkFactory struct {
		xreg.RenewBase
		xctn *xactRechunk
		kind string
	}
	xactRechunk struct {
		args   *apc.RechunkMsg
		chunks cmn.ChunksConf
		stats  struct {
			skipped   atomic.Int64 // objects not requiring or eligible for a rewrite
			matched   atomic.Int64 // chunked objects already matching the policy
			processed atomic.Int64 // objects rewritten to match the policy
			failed    atomic.Int64 // objects that failed processing
		}
		// TODO: migrate to xact.BckJogRunner to reduce boilerplate and gain auto-tuned worker pool
		xact.BckJog
	}
)

// interface guard
var (
	_ core.Xact      = (*xactRechunk)(nil)
	_ xreg.Renewable = (*rechunkFactory)(nil)
)

///////////////////
// rechunkFactory //
///////////////////

func (p *rechunkFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &rechunkFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind}
}

func (p *rechunkFactory) Start() error {
	p.xctn = newxactRechunk(p)
	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln("start rechunk", p.Bck.String(), "xid", p.UUID(), "args", p.xctn.CtlMsg())
	}
	return nil
}

func (p *rechunkFactory) Kind() string   { return p.kind }
func (p *rechunkFactory) Get() core.Xact { return p.xctn }

func (p *rechunkFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*rechunkFactory)

	if p.UUID() == prev.UUID() {
		return xreg.WprUse, nil
	}

	var (
		prevChunks = &prev.xctn.chunks
		currChunks = &p.Bck.Props.Chunks
	)

	// See docs/relnotes/5.1.md#chunking-upcoming: a changed chunks configuration aborts
	// the running rechunk so its replacement can converge to the new layout
	if prevChunks.EqualLayout(currChunks) {
		return xreg.WprUse, cmn.NewErrXactUsePrev(prevEntry.Get().String())
	}
	return xreg.WprAbort, nil
}

////////////////
// xactRechunk //
////////////////

func newxactRechunk(p *rechunkFactory) *xactRechunk {
	var (
		args      = p.Args.Custom.(*apc.RechunkMsg)
		r         = &xactRechunk{args: args, chunks: p.Bck.Props.Chunks}
		config    = cmn.GCO.Get()
		slab, err = core.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
		mpopts    = &mpather.JgroupOpts{
			Parent:   r,
			CTs:      []string{fs.ObjCT},
			VisitObj: r.do,
			Slab:     slab,
			Prefix:   args.Prefix,
			RW:       true,
		}
	)

	debug.AssertNoErr(err)
	mpopts.Bck.Copy(p.Bck.Bucket())

	r.BckJog.Init(p.UUID(), p.Kind(), p.Bck, mpopts, config)

	return r
}

func (r *xactRechunk) do(lom *core.LOM, _ []byte) error {
	// TODO: start with a read lock and upgrade only before rewriting; reload after a failed upgrade.
	lom.Lock(true)
	defer lom.Unlock(true)

	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return nil
		}
		r.stats.failed.Inc()
		r.AddErr(err, 0)
		return err
	}
	if lom.IsCopy() {
		r.stats.skipped.Inc()
		return nil
	}

	var (
		chunks    = &r.chunks
		size      = lom.Lsize()
		chunkSize = chunks.ChunkSizeFor(size)
	)
	if chunkSize == 0 {
		if !lom.IsChunked() {
			r.stats.skipped.Inc()
			r.ObjsAdd(1, size)
			return nil // Do nothing: monolithic objects stay monolithic
		}
		chunkSize = 0 // restore chunked objects to monolithic
	} else if lom.IsChunked() && !r.args.SyncRemote {
		matches, err := chunkLayoutMatches(lom, chunkSize)
		if err != nil {
			r.stats.failed.Inc()
			r.AddErr(err, 0)
			return err
		}
		if matches {
			r.stats.matched.Inc()
			r.ObjsAdd(1, size)
			return nil
		}
	}

	lh, err := lom.Open()
	if err != nil {
		r.stats.failed.Inc()
		r.AddErr(err, 0)
		return err
	}
	defer lh.Close()

	params := core.AllocPutParams()
	{
		// NOTE: if chunkSize > 0, will trigger the underlying `poi.chunk(chunkSize)`
		params.ChunkSize = chunkSize
		params.WorkTag = fs.WorkfilePut
		params.Xact = r
		params.Reader = lh
		params.Size = size
		params.OWT = cmn.OwtChunks
		params.Atime = lom.Atime()
		params.Locked = true                    // see `lom.Lock(true)` above
		params.SkipBackend = !r.args.SyncRemote // default: skip backend (local-only); if SyncRemote=true, also write to remote
	}

	err = core.T.PutObject(lom, params)
	core.FreePutParams(params)
	if err != nil {
		r.stats.failed.Inc()
		r.AddErr(err, 0)
		return err
	}

	r.stats.processed.Inc()
	r.ObjsAdd(1, size)

	return nil
}

func chunkLayoutMatches(lom *core.LOM, chunkSize int64) (bool, error) {
	u, err := core.NewUfest("", lom, true /*must-exist*/)
	if err != nil {
		return false, err
	}
	if err = u.LoadCompleted(lom); err != nil {
		return false, err
	}
	lastNum := u.Count()
	for num := 1; num < lastNum; num++ {
		chunk, err := u.GetChunk(num)
		if err != nil {
			return false, err
		}
		if chunk.Size() != chunkSize {
			return false, nil
		}
	}

	// last chunk
	last, err := u.GetChunk(lastNum)
	if err != nil {
		return false, err
	}
	expectedSize := lom.Lsize() - int64(lastNum-1)*chunkSize
	debug.Assert(expectedSize > 0)
	if expectedSize > chunkSize {
		return false, nil
	}
	return last.Size() == expectedSize, nil
}

func (r *xactRechunk) Run(wg *sync.WaitGroup) {
	wg.Done()
	r.BckJog.Run()
	errJog := r.BckJog.Wait()
	if errJog != nil && !r.IsAborted() {
		nlog.Warningln(r.Name(), errJog)
	}
	nlog.Infoln("finish rechunk", r.Name(), "xid", r.ID())
	r.Finish()
}

func (r *xactRechunk) Snap() *core.Snap { return r.Base.NewSnap(r) }

func (r *xactRechunk) CtlMsg() string {
	var sb cos.SB
	sb.Init(ctlMsgBufSize)

	chunks := &r.chunks
	sb.WriteString("objsize-limit:")
	sb.WriteString(cos.ToSizeIEC(int64(chunks.ObjSizeLimit), 0))
	sb.WriteString(", chunk-size:")
	sb.WriteString(cos.ToSizeIEC(int64(chunks.ChunkSize), 0))

	if r.args.SyncRemote {
		sb.WriteString(", sync-remote:true")
	}
	if r.args.Prefix != "" {
		sb.WriteString(", prefix:")
		sb.WriteString(r.args.Prefix)
	}
	if nv := r.NumVisits(); nv > 0 {
		sb.WriteString(", visited:")
		sb.WriteInt64(nv)
	}
	sb.WriteString(", skipped:")
	sb.WriteInt64(r.stats.skipped.Load())
	sb.WriteString(", matched:")
	sb.WriteInt64(r.stats.matched.Load())
	sb.WriteString(", processed:")
	sb.WriteInt64(r.stats.processed.Load())
	sb.WriteString(", failed:")
	sb.WriteInt64(r.stats.failed.Load())
	return sb.String()
}

// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
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
		xact.BckJog
		args *apc.RechunkMsg
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

func (p *rechunkFactory) Start() (err error) {
	p.xctn, err = newxactRechunk(p)
	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln("start rechunk", p.Bck.String(), "xid", p.UUID(), "args", p.xctn.CtlMsg())
	}
	return err
}

func (p *rechunkFactory) Kind() string   { return p.kind }
func (p *rechunkFactory) Get() core.Xact { return p.xctn }

func (p *rechunkFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*rechunkFactory)

	if p.UUID() == prev.UUID() {
		return xreg.WprUse, nil
	}

	var (
		prevArgs = prev.Args.Custom.(*apc.RechunkMsg)
		currArgs = p.Args.Custom.(*apc.RechunkMsg)
		xprev    = prevEntry.Get()
	)

	if prevArgs.ObjSizeLimit == currArgs.ObjSizeLimit && prevArgs.ChunkSize == currArgs.ChunkSize {
		// Same configuration - ignore the request and reuse the existing xaction
		return xreg.WprUse, cmn.NewErrXactUsePrev(xprev.String())
	}

	// Different configuration - fail with recommendation to abort the running one
	return wpr, cmn.NewErrFailedTo(
		core.T,
		"start new rechunk",
		xprev.String(),
		fmt.Errorf("rechunk with different objsize_limit (%s vs %s) or chunk_size (%s vs %s) is already running; to override, first stop the running one %q",
			cos.ToSizeIEC(prevArgs.ObjSizeLimit, 0),
			cos.ToSizeIEC(currArgs.ObjSizeLimit, 0),
			cos.ToSizeIEC(prevArgs.ChunkSize, 0),
			cos.ToSizeIEC(currArgs.ChunkSize, 0),
			xprev.ID()),
	)
}

////////////////
// xactRechunk //
////////////////

func newxactRechunk(p *rechunkFactory) (*xactRechunk, error) {
	var (
		args      = p.Args.Custom.(*apc.RechunkMsg)
		r         = &xactRechunk{args: args}
		config    = cmn.GCO.Get()
		slab, err = core.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
		mpopts    = &mpather.JgroupOpts{
			Parent:   r,
			CTs:      []string{fs.ObjCT},
			VisitObj: r.do,
			Slab:     slab,
			Prefix:   args.Prefix,
			DoLoad:   mpather.Load,
			RW:       true,
		}
	)

	if args.ChunkSize <= 0 {
		return nil, cmn.NewErrFailedTo(core.T, "newxactRechunk", "chunk size is not set", nil)
	}

	debug.AssertNoErr(err)
	mpopts.Bck.Copy(p.Bck.Bucket())

	r.BckJog.Init(p.UUID(), p.Kind(), p.Bck, mpopts, config)

	return r, nil
}

// | Object Size       | Was Chunked? | Action                 |
// |-------------------|--------------|------------------------|
// | < objSizeLimit    |     Yes      | Restore monolithic     |
// | < objSizeLimit    |     No       | No-op                  |
// | >= objSizeLimit   |     Yes      | Re-chunk               |
// | >= objSizeLimit   |     No       | Re-chunk               |
func (r *xactRechunk) do(lom *core.LOM, _ []byte) error {
	var (
		size      = lom.Lsize()
		chunkSize = r.args.ChunkSize
	)
	if size < r.args.ObjSizeLimit || r.args.ObjSizeLimit == 0 {
		if !lom.IsChunked() {
			// Track skipped object stats (no-op case)
			r.ObjsAdd(1, size)
			return nil // Do nothing: monolithic objects stay monolithic
		}
		chunkSize = 0 // restore chunked objects to monolithic
	}

	lom.Lock(true)
	defer lom.Unlock(true)

	lh, err := lom.Open()
	if err != nil {
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
		// NOTE: if chunkSize == 0 (chunk disabled) but size > config.MaxMonolithicSize,
		// it will trigger the underlying `poi.chunk(config.MaxMonolithicSize)`
		params.Size = size
		params.OWT = cmn.OwtChunks
		params.Atime = lom.Atime()
		params.Locked = true                    // see `lom.Lock(true)` above
		params.SkipBackend = !r.args.SyncRemote // default: skip backend (local-only); if SyncRemote=true, also write to remote
	}

	err = core.T.PutObject(lom, params)
	core.FreePutParams(params)
	if err != nil {
		r.AddErr(err, 0)
		return err
	}

	r.ObjsAdd(1, size) // Track processed object stats

	return nil
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
	sb.Init(128)

	sb.WriteString("objsize-limit:")
	sb.WriteString(cos.ToSizeIEC(r.args.ObjSizeLimit, 0))
	sb.WriteString(", chunk-size:")
	sb.WriteString(cos.ToSizeIEC(r.args.ChunkSize, 0))

	if r.args.SyncRemote {
		sb.WriteString(", sync-remote:true")
	}
	if r.args.Prefix != "" {
		sb.WriteString(", prefix:")
		sb.WriteString(r.args.Prefix)
	}
	if nv := r.NumVisits(); nv > 0 {
		sb.WriteString(", visited:")
		sb.WriteString(strconv.FormatInt(nv, 10))
	}
	return sb.String()
}

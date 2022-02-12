// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// XactDirPromote copies a bucket locally within the same cluster

type (
	proFactory struct {
		xreg.RenewBase
		xctn *XactDirPromote
		args *xreg.DirPromoteArgs
	}
	XactDirPromote struct {
		xact.BckJog
		dir         string
		params      *cmn.ActValPromote
		smap        *cluster.Smap
		isFileShare bool
	}
)

// interface guard
var (
	_ cluster.Xact   = (*XactDirPromote)(nil)
	_ xreg.Renewable = (*proFactory)(nil)
)

////////////////
// proFactory //
////////////////

func (*proFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	c := args.Custom.(*xreg.DirPromoteArgs)
	p := &proFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, args: c}
	return p
}

func (p *proFactory) Start() error {
	xctn := &XactDirPromote{dir: p.args.Dir, params: p.args.Params, isFileShare: p.args.IsFileShare}
	xctn.BckJog.Init(p.Args.UUID /*global xID*/, cmn.ActPromote, p.Bck, &mpather.JoggerGroupOpts{T: p.T})
	p.xctn = xctn
	return nil
}

func (*proFactory) Kind() string        { return cmn.ActPromote }
func (p *proFactory) Get() cluster.Xact { return p.xctn }

func (*proFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

////////////////////
// XactDirPromote //
////////////////////

func (r *XactDirPromote) Run(wg *sync.WaitGroup) {
	wg.Done()
	glog.Infoln(r.Name(), r.dir, "=>", r.Bck())
	r.smap = r.Target().Sowner().Get()
	var (
		err  error
		opts = &fs.WalkOpts{Dir: r.dir, Callback: r.walk, Sorted: false}
	)
	if r.params.Recursive {
		err = fs.Walk(opts) // godirwalk
	} else {
		err = fs.WalkDir(r.dir, r.walk) // Go filepath.WalkDir
	}
	r.Finish(err)
}

func (r *XactDirPromote) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	debug.Assert(filepath.IsAbs(fqn))
	bck := r.Bck()

	// promote
	objName, err := cmn.PromotedObjDstName(fqn, r.dir, r.params.ObjName)
	if err != nil {
		return err
	}
	// file share => promote only the part that lands locally
	if r.isFileShare {
		si, err := cluster.HrwTarget(bck.MakeUname(objName), r.smap)
		if err != nil {
			return err
		}
		if si.ID() != r.Target().SID() {
			return nil
		}
	}
	params := cluster.PromoteParams{
		SrcFQN:    fqn,
		Bck:       bck,
		ObjName:   objName,
		Overwrite: r.params.Overwrite,
		KeepSrc:   r.params.KeepSrc,
	}
	lom, err := r.Target().Promote(params)
	if err != nil {
		if finfo, ers := os.Stat(fqn); ers == nil {
			if !finfo.Mode().IsRegular() {
				glog.Warningf("%v (mode=%#x)", err, finfo.Mode()) // symbolic link, etc.
			}
		} else {
			glog.Error(err)
		}
	} else if lom != nil { // locally placed (PromoteFile returns nil when sending remotely)
		r.ObjsAdd(1, lom.SizeBytes())
		cluster.FreeLOM(lom)
	}
	return nil
}

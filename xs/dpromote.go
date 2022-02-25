// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
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
		args *cluster.PromoteArgs
	}
	XactDirPromote struct {
		xact.BckJog
		dir  string
		args *cluster.PromoteArgs
		smap *cluster.Smap
		// set separately in the commit phase prior to Run
		confirmedFileShare bool
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
	c := args.Custom.(*cluster.PromoteArgs)
	p := &proFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, args: c}
	return p
}

func (p *proFactory) Start() error {
	xctn := &XactDirPromote{dir: p.args.SrcFQN, args: p.args}
	xctn.BckJog.Init(p.Args.UUID /*global xID*/, apc.ActPromote, p.Bck, &mpather.JoggerGroupOpts{T: p.T})
	p.xctn = xctn
	return nil
}

func (*proFactory) Kind() string        { return apc.ActPromote }
func (p *proFactory) Get() cluster.Xact { return p.xctn }

func (*proFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

////////////////////
// XactDirPromote //
////////////////////

func (r *XactDirPromote) SetFileShare(v bool) { r.confirmedFileShare = v } // is called before Run()

func (r *XactDirPromote) Run(wg *sync.WaitGroup) {
	wg.Done()
	glog.Infoln(r.Name(), r.dir, "=>", r.Bck())
	r.smap = r.Target().Sowner().Get()
	var (
		err  error
		opts = &fs.WalkOpts{Dir: r.dir, Callback: r.walk, Sorted: false}
	)
	if r.args.Recursive {
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
	objName, err := cmn.PromotedObjDstName(fqn, r.dir, r.args.ObjName)
	if err != nil {
		return err
	}
	// file share == true: promote only the part of the namespace that "lands" locally
	if r.confirmedFileShare {
		si, err := cluster.HrwTarget(bck.MakeUname(objName), r.smap)
		if err != nil {
			return err
		}
		if si.ID() != r.Target().SID() {
			return nil
		}
	}
	params := cluster.PromoteParams{
		Bck:  bck,
		Xact: r,
		PromoteArgs: cluster.PromoteArgs{
			SrcFQN:       fqn,
			ObjName:      objName,
			OverwriteDst: r.args.OverwriteDst,
			DeleteSrc:    r.args.DeleteSrc,
		},
	}
	// TODO: options to ignore specific error types, limited number of errors,
	// all errors... (archive)
	_, err = r.Target().Promote(params)
	if cmn.IsNotExist(err) {
		err = nil
	}
	return err
}

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
	"github.com/NVIDIA/aistore/cmn/cos"
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
		xctn   *XactDirPromote
		dir    string
		params *cmn.ActValPromote
	}
	XactDirPromote struct {
		xact.BckJog
		dir    string
		params *cmn.ActValPromote
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
	p := &proFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, dir: c.Dir, params: c.Params}
	return p
}

func (p *proFactory) Start() error {
	xctn := NewXactDirPromote(p.dir, p.Bck, p.T, p.params)
	go xctn.Run(nil)
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

func NewXactDirPromote(dir string, bck *cluster.Bck, t cluster.Target, params *cmn.ActValPromote) (r *XactDirPromote) {
	r = &XactDirPromote{dir: dir, params: params}
	r.BckJog.Init(cos.GenUUID(), cmn.ActPromote, bck, &mpather.JoggerGroupOpts{T: t})
	return
}

func (r *XactDirPromote) Run(*sync.WaitGroup) {
	var err error
	glog.Infoln(r.Name(), r.dir, "=>", r.Bck())
	opts := &fs.WalkOpts{Dir: r.dir, Callback: r.walk, Sorted: false}
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
	if err := bck.Init(r.Target().Bowner()); err != nil {
		return err
	}

	// promote
	objName, err := cmn.PromotedObjDstName(fqn, r.dir, r.params.ObjName)
	if err != nil {
		return err
	}
	params := cluster.PromoteFileParams{
		SrcFQN:    fqn,
		Bck:       bck,
		ObjName:   objName,
		Overwrite: r.params.Overwrite,
		KeepOrig:  r.params.KeepOrig,
	}
	lom, err := r.Target().PromoteFile(params)
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

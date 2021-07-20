// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

// XactDirPromote copies a bucket locally within the same cluster

type (
	proFactory struct {
		xreg.BaseEntry
		xact   *XactDirPromote
		t      cluster.Target
		dir    string
		params *cmn.ActValPromote
	}
	XactDirPromote struct {
		xaction.XactBckJog
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
	p := &proFactory{t: args.T, dir: c.Dir, params: c.Params}
	p.Bck = bck
	return p
}

func (p *proFactory) Start() error {
	xact := NewXactDirPromote(p.dir, p.Bck, p.t, p.params)
	go xact.Run()
	p.xact = xact
	return nil
}

func (*proFactory) Kind() string        { return cmn.ActPromote }
func (p *proFactory) Get() cluster.Xact { return p.xact }

////////////////////
// XactDirPromote //
////////////////////

func NewXactDirPromote(dir string, bck *cluster.Bck, t cluster.Target, params *cmn.ActValPromote) (r *XactDirPromote) {
	r = &XactDirPromote{dir: dir, params: params}
	r.XactBckJog.Init(cos.GenUUID(), cmn.ActPromote, bck, &mpather.JoggerGroupOpts{T: t})
	return
}

func (r *XactDirPromote) Run() {
	glog.Infoln(r.String(), r.dir, "=>", r.Bck())
	opts := &fs.Options{
		Dir:      r.dir,
		Callback: r.walk,
		Sorted:   false,
	}
	err := fs.Walk(opts)
	r.Finish(err)
}

func (r *XactDirPromote) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	if !r.params.Recursive {
		fname, err := filepath.Rel(r.dir, fqn)
		cos.AssertNoErr(err)
		if strings.ContainsRune(fname, filepath.Separator) {
			return nil
		}
	}
	// NOTE: destination objName is:
	// r.params.ObjName + filepath.Base(fqn) if promoting single file
	// r.params.ObjName + strings.TrimPrefix(fileFqn, dirFqn) if promoting the whole directory
	debug.Assert(filepath.IsAbs(fqn))

	bck := r.Bck()
	if err := bck.Init(r.Target().Bowner()); err != nil {
		return err
	}
	objName := r.params.ObjName
	if objName != "" && objName[len(objName)-1] != os.PathSeparator {
		objName += string(os.PathSeparator)
	}
	objName += strings.TrimPrefix(strings.TrimPrefix(fqn, r.dir), string(filepath.Separator))
	objName = strings.Trim(objName, string(filepath.Separator))

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
	} else if lom != nil { // nil when (placement = different target)
		r.ObjectsInc()
		r.BytesAdd(lom.SizeBytes())
		cluster.FreeLOM(lom)
	}
	return nil
}

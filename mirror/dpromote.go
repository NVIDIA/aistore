// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

// XactDirPromote copies a bucket locally within the same cluster

type (
	XactDirPromote struct {
		xactBckBase
		dir    string
		params *cmn.ActValPromote
	}
)

//
// public methods
//

func NewXactDirPromote(id, dir string, bck cmn.Bck, t cluster.Target, params *cmn.ActValPromote) *XactDirPromote {
	return &XactDirPromote{
		xactBckBase: *newXactBckBase(id, cmn.ActPromote, bck, t),
		dir:         dir,
		params:      params,
	}
}

func (r *XactDirPromote) Run() (err error) {
	glog.Infoln(r.String(), r.dir, "=>", r.Bck())
	opts := &fs.Options{
		Dir:      r.dir,
		Callback: r.walk,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		glog.Errorln(err)
	}
	return
}

func (r *XactDirPromote) Description() string {
	return "promote file|directory"
}

func (r *XactDirPromote) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	if !r.params.Recurs {
		fname, err := filepath.Rel(r.dir, fqn)
		cmn.AssertNoErr(err)
		if strings.ContainsRune(fname, filepath.Separator) {
			return nil
		}
	}
	// NOTE: destination objname is:
	// r.params.ObjName + filepath.Base(fqn) if promoting single file
	// r.params.ObjName + strings.TrimPrefix(fileFqn, dirFqn) if promoting the whole directory
	cmn.Assert(filepath.IsAbs(fqn))

	bck := cluster.NewBckEmbed(r.Bck())
	if err := bck.Init(r.t.GetBowner(), r.t.Snode()); err != nil {
		return err
	}
	objName := r.params.ObjName + strings.TrimPrefix(strings.TrimPrefix(fqn, r.dir), string(filepath.Separator))
	objName = strings.Trim(objName, string(filepath.Separator))
	err := r.Target().PromoteFile(fqn, bck, objName, r.params.Overwrite, true /*safe*/, r.params.Verbose)
	if err != nil {
		if finfo, ers := os.Stat(fqn); ers == nil {
			if finfo.Mode().IsRegular() {
				glog.Error(err)
			} // else symbolic link, etc.
		} else if !os.IsNotExist(ers) {
			glog.Error(err)
		}
	}
	return nil
}

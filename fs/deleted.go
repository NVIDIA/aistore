// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

// TODO: undelete (feature)

const deletedRoot = ".$deleted"

func (mi *MountpathInfo) DeletedRoot() string {
	return filepath.Join(mi.Path, deletedRoot)
}

func (mi *MountpathInfo) TempDir(dir string) string {
	return filepath.Join(mi.Path, deletedRoot, dir)
}

func (mi *MountpathInfo) RemoveDeleted(who string) (rerr error) {
	delroot := mi.DeletedRoot()
	dentries, err := os.ReadDir(delroot)
	if err != nil {
		if os.IsNotExist(err) {
			cos.CreateDir(delroot)
			err = nil
		}
		return err
	}
	for _, dent := range dentries {
		fqn := filepath.Join(delroot, dent.Name())
		if !dent.IsDir() {
			err := fmt.Errorf("%s: unexpected non-directory item %q in 'deleted'", who, fqn)
			debug.AssertNoErr(err)
			glog.Error(err)
			continue
		}
		if err = os.RemoveAll(fqn); err == nil {
			continue
		}
		if !os.IsNotExist(err) {
			glog.Errorf("%s: failed to remove %q from 'deleted', err %v", who, fqn, err)
			if rerr == nil {
				rerr = err
			}
		}
	}
	return
}

// MoveToDeleted removes directory in steps:
// 1. Synchronously gets temporary directory name
// 2. Synchronously renames old folder to temporary directory
func (mi *MountpathInfo) MoveToDeleted(dir string) (err error) {
	var base, tmpBase, tmpDst string
	err = cos.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	cs := GetCapStatus()
	if cs.Err != nil {
		goto rm // not moving - removing
	}
	base = filepath.Base(dir)
	tmpBase = mi.TempDir(base)
	err = cos.CreateDir(tmpBase)
	if err != nil {
		if cos.IsErrOOS(err) {
			cs.OOS = true
		}
		goto rm
	}
	tmpDst = filepath.Join(tmpBase, strconv.FormatInt(mono.NanoTime(), 10))
	if err = os.Rename(dir, tmpDst); err == nil {
		return
	}
	if cos.IsErrOOS(err) {
		cs.OOS = true
	}
rm:
	// not placing in 'deleted' - removing right away
	errRm := os.RemoveAll(dir)
	if errRm != nil && !os.IsNotExist(errRm) && err == nil {
		err = errRm
	}
	if cs.OOS {
		glog.Errorf("%s %s: OOS (%v)", mi, cs, err)
	}
	return err
}

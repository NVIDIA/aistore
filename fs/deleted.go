// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

// TODO: undelete (feature)

const deletedRoot = ".$deleted"

func (mi *MountpathInfo) DeletedDir() string {
	return filepath.Join(mi.Path, deletedRoot)
}

func (mi *MountpathInfo) TempDir(dir string) string {
	return filepath.Join(mi.Path, deletedRoot, dir)
}

func (mi *MountpathInfo) RemoveDeleted(who string) (rerr error) {
	deletedDir := mi.DeletedDir()
	dentries, err := os.ReadDir(deletedDir)
	if err != nil {
		if os.IsNotExist(err) {
			cos.CreateDir(deletedDir)
			err = nil
		}
		return err
	}
	for _, dent := range dentries {
		fqn := filepath.Join(deletedDir, dent.Name())
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
func (mi *MountpathInfo) MoveToDeleted(dir string) error {
	if err := Access(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		glog.Errorf("%s: %v", mi, err)
	}
	cs := GetCapStatus()
	if cs.Err != nil {
		if err := os.RemoveAll(dir); err != nil {
			glog.Errorf("FATAL: %s %s: failed to remove, err: %v", mi, cs, err)
			return err
		}
		glog.Errorf("%s %s: removed %q instead of placing it in 'deleted'", mi, cs, dir)
		return nil
	}
tmpExists:
	var (
		tmpDir     string
		deletedDir = mi.DeletedDir()
	)
	err := cos.CreateDir(deletedDir)
	if err != nil {
		if cos.IsErrOOS(err) {
			goto oos
		}
		return err
	}
	tmpDir = filepath.Join(deletedDir, fmt.Sprintf("$dir-%d", mono.NanoTime()))
	if err = os.Rename(dir, tmpDir); err == nil {
		return nil
	}
	if os.IsExist(err) {
		glog.Warningf("%q already exists in 'deleted'", tmpDir) // should never happen
		goto tmpExists
	}
	if os.IsNotExist(err) {
		return nil // consider benign
	}
	if !cos.IsErrOOS(err) {
		return err
	}
oos:
	glog.Errorf("%s %s: OOS (err=%v) - not recycling via 'deleted', removing %q right away", mi, cs, err, dir)
	if nested := os.RemoveAll(dir); nested != nil {
		glog.Errorf("FATAL: %s (%v, %v)", mi, err, nested)
	}
	return err
}

// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// TODO: undelete (feature)

const (
	deletedRoot = ".$deleted"
	desleep     = cos.PollSleepMedium
	deretries   = 3
)

func (mi *Mountpath) DeletedRoot() string {
	return filepath.Join(mi.Path, deletedRoot)
}

func (mi *Mountpath) TempDir(dir string) string {
	return filepath.Join(mi.Path, deletedRoot, dir)
}

func (mi *Mountpath) RemoveDeleted(who string) (rerr error) {
	delroot := mi.DeletedRoot()
	dentries, err := os.ReadDir(delroot)
	if err != nil {
		if cos.IsNotExist(err) {
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
			nlog.Errorln(err)
			continue
		}
		if err = os.RemoveAll(fqn); err == nil {
			continue
		}
		if !cos.IsNotExist(err) {
			nlog.Errorf("%s: failed to remove %q from 'deleted', err %v", who, fqn, err)
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
func (mi *Mountpath) MoveToDeleted(dir string) (err error) {
	if errN := cos.Stat(dir); errN != nil {
		if cos.IsNotExist(errN) {
			errN = nil
		}
		return errN
	}

	var (
		base, tmpBase, tmpDst string
		cs                    = Cap()
		errCap, oos           = cs.Err(), cs.IsOOS()
	)
	if errCap != nil {
		goto rm // not moving - removing
	}

	base = filepath.Base(dir)
	tmpBase = mi.TempDir(base)
	err = cos.CreateDir(tmpBase)
	if err != nil {
		if cos.IsErrOOS(err) {
			oos = true
		}
		goto rm
	}

	tmpDst = filepath.Join(tmpBase, strconv.FormatInt(mono.NanoTime(), 10))
	if err = os.Rename(dir, tmpDst); err == nil {
		return nil // ok
	}

	if cos.IsErrOOS(err) {
		oos = true
	}
rm:
	// not placing in 'deleted' - removing right away
	errRm := RemoveAll(dir)
	if err == nil {
		err = errRm
	}
	if oos {
		nlog.Errorf("%s %s: OOS (%v)", mi, cs.String(), err)
	}
	return err
}

func (mi *Mountpath) clearMDs(inclBMD bool) (rerr error) {
	for _, mdfd := range mdFilesDirs {
		if !inclBMD && mdfd == fname.Bmd {
			continue
		}
		fpath := filepath.Join(mi.Path, mdfd)
		if err := RemoveAll(fpath); err != nil {
			nlog.Errorln(err)
			rerr = err
		}
	}
	return rerr
}

//
// decommission
//

func Decommission(mdOnly bool) {
	var (
		avail, disabled = Get()
		allmpi          = []MPI{avail, disabled}
	)
	for i := range deretries { // retry
		if mdOnly {
			if err := demd(allmpi); err == nil {
				return
			}
		} else {
			if err := deworld(allmpi); err == nil {
				return
			}
		}
		if i < deretries-1 {
			nlog.Errorln("decommission: retrying cleanup...")
			time.Sleep(desleep)
		}
	}
}

func demd(allmpi []MPI) (rerr error) {
	for _, mpi := range allmpi {
		for _, mi := range mpi {
			// NOTE: BMD goes with data (ie., no data - no BMD)
			if err := mi.clearMDs(false /*include BMD*/); err != nil {
				rerr = err
			}
			// node ID (SID)
			if err := removeXattr(mi.Path, nodeXattrID); err != nil {
				debug.AssertNoErr(err)
				rerr = err
			}
		}
	}
	return rerr
}

// the entire content including user data, MDs, and daemon ID
func deworld(allmpi []MPI) (rerr error) {
	for _, mpi := range allmpi {
		for _, mi := range mpi {
			if err := os.RemoveAll(mi.Path); err != nil {
				debug.Assert(!cos.IsNotExist(err))
				// retry ENOTEMPTY in place
				if errors.Is(err, syscall.ENOTEMPTY) {
					time.Sleep(desleep)
					err = os.RemoveAll(mi.Path)
				}
				if err != nil {
					nlog.Errorln(err)
					rerr = err
				}
			}
		}
	}
	return rerr
}

// retrying ENOTEMPTY - "directory not empty" race vs. new writes
func RemoveAll(dir string) (err error) {
	for i := range deretries {
		err = os.RemoveAll(dir)
		if err == nil {
			break
		}
		debug.Assert(!cos.IsNotExist(err), err)
		nlog.ErrorDepth(1, err)
		if !errors.Is(err, syscall.ENOTEMPTY) {
			break
		}
		if i < deretries-1 {
			time.Sleep(desleep)
		}
	}
	return err
}

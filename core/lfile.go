// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	_openFlags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	_apndFlags = os.O_APPEND | os.O_WRONLY
)

//
// open
//

func (lom *LOM) Open() (cos.LomReader, error) {
	return os.Open(lom.FQN)
}

//
// create
//

func (lom *LOM) Create() (cos.LomWriter, error) {
	debug.Assert(lom.isLockedExcl(), lom.Cname()) // caller must wlock
	return lom._cf(lom.FQN)
}

func (lom *LOM) CreateWork(wfqn string) (cos.LomWriter, error) { return lom._cf(wfqn) } // -> lom
func (lom *LOM) CreatePart(wfqn string) (*os.File, error)      { return lom._cf(wfqn) } // TODO: differentiate
func (lom *LOM) CreateSlice(wfqn string) (*os.File, error)     { return lom._cf(wfqn) } // TODO: ditto

func (lom *LOM) _cf(fqn string) (fh *os.File, err error) {
	fh, err = os.OpenFile(fqn, _openFlags, cos.PermRWR)
	if err == nil || !os.IsNotExist(err) {
		return fh, err
	}

	// slow path: create sub-directories
	bdir := lom.mi.MakePathBck(lom.Bucket())
	if err = cos.Stat(bdir); err != nil {
		var (
			err        = fmt.Errorf("%s (bdir %s): %w", lom, bdir, err)
			bmd        = T.Bowner().Get()
			_, present = bmd.Get(&lom.bck)
		)
		if present {
			err = fmt.Errorf("%w [%v]", syscall.ENOTDIR, err)
		}
		return nil, err
	}
	fdir := filepath.Dir(fqn)
	if err = cos.CreateDir(fdir); err != nil {
		return nil, err
	}
	return os.OpenFile(fqn, _openFlags, cos.PermRWR)
}

// append
func (*LOM) AppendWork(wfqn string) (fh cos.LomWriter, err error) {
	fh, err = os.OpenFile(wfqn, _apndFlags, cos.PermRWR)
	return fh, err
}

//
// remove
//

func (lom *LOM) RemoveMain() (err error) {
	err = cos.RemoveFile(lom.FQN)
	if os.IsNotExist(err) {
		err = nil
	}
	return err
}

func (lom *LOM) RemoveObj(force ...bool) (err error) {
	debug.AssertFunc(func() bool {
		if lom.isLockedExcl() {
			return true
		}
		// NOTE: making "rlock" exception to be able to forcefully rm corrupted object in the GET path
		return len(force) > 0 && force[0] && lom.isLockedRW()
	})
	lom.Uncache()
	err = lom.RemoveMain()
	for copyFQN := range lom.md.copies {
		if erc := cos.RemoveFile(copyFQN); erc != nil && !os.IsNotExist(erc) {
			err = erc
		}
	}
	lom.md.lid = 0
	return err
}

//
// rename
//

func (lom *LOM) RenameMainTo(wfqn string) error {
	return cos.Rename(lom.FQN, wfqn)
}

func (lom *LOM) RenameToMain(wfqn string) error {
	return cos.Rename(wfqn, lom.FQN)
}

func (lom *LOM) RenameFinalize(wfqn string) error {
	bdir := lom.mi.MakePathBck(lom.Bucket())
	if err := cos.Stat(bdir); err != nil {
		return fmt.Errorf("%s(bdir: %s): %w", lom, bdir, err)
	}
	if err := lom.RenameToMain(wfqn); err != nil {
		return cmn.NewErrFailedTo(T, "finalize", lom.Cname(), err)
	}
	return nil
}

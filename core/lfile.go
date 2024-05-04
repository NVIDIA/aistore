// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

func (lom *LOM) OpenFile() (*os.File, error) {
	return os.Open(lom.FQN)
}

// (compare with cos.CreateFile)
func (lom *LOM) CreateFile(fqn string) (fh *os.File, err error) {
	fh, err = os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, cos.PermRWR)
	if err == nil || !os.IsNotExist(err) {
		return
	}
	return lom._cf(fqn, os.O_WRONLY)
}

// slow path
func (lom *LOM) _cf(fqn string, mode int) (fh *os.File, err error) {
	bdir := lom.mi.MakePathBck(lom.Bucket())
	if err = cos.Stat(bdir); err != nil {
		return nil, fmt.Errorf("%s (bdir %s): %w", lom, bdir, err)
	}
	fdir := filepath.Dir(fqn)
	if err = cos.CreateDir(fdir); err != nil {
		return
	}
	fh, err = os.OpenFile(fqn, os.O_CREATE|mode|os.O_TRUNC, cos.PermRWR)
	return
}

func (lom *LOM) CreateFileRW(fqn string) (fh *os.File, err error) {
	fh, err = os.OpenFile(fqn, os.O_CREATE|os.O_RDWR|os.O_TRUNC, cos.PermRWR)
	if err == nil || !os.IsNotExist(err) {
		return
	}
	return lom._cf(fqn, os.O_RDWR)
}

func (lom *LOM) Remove(force ...bool) (err error) {
	// making "rlock" exception to be able to (forcefully) remove corrupted obj in the GET path
	debug.AssertFunc(func() bool {
		rc, exclusive := lom.IsLocked()
		return exclusive || (len(force) > 0 && force[0] && rc > 0)
	})
	lom.Uncache()
	err = cos.RemoveFile(lom.FQN)
	if os.IsNotExist(err) {
		err = nil
	}
	for copyFQN := range lom.md.copies {
		if erc := cos.RemoveFile(copyFQN); erc != nil && !os.IsNotExist(erc) {
			err = erc
		}
	}
	lom.md.bckID = 0
	return err
}

// (compare with cos.Rename)
func (lom *LOM) RenameFrom(workfqn string) error {
	bdir := lom.mi.MakePathBck(lom.Bucket())
	if err := cos.Stat(bdir); err != nil {
		return fmt.Errorf("%s(bdir: %s): %w", lom, bdir, err)
	}
	if err := cos.Rename(workfqn, lom.FQN); err != nil {
		return cmn.NewErrFailedTo(T, "finalize", lom, err)
	}
	return nil
}

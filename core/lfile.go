// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"os"
	"path/filepath"
	rdebug "runtime/debug"
	"syscall"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	_openFlags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	_apndFlags = os.O_APPEND | os.O_WRONLY
)

type errBdir struct {
	err   error
	cname string
}

func (e *errBdir) Error() string {
	if os.IsNotExist(e.err) {
		return e.cname + ": missing bdir (bucket exists?)"
	}
	return fmt.Sprintf("%s: missing bdir [%v]", e.cname, e.err)
}

//
// open
//

// open read-only, return os.File
func (lom *LOM) OpenFile() (fh *os.File, _ error) {
	reader, err := lom.Open()
	if err != nil {
		return nil, err
	}
	fh = reader.(*os.File)
	return fh, nil
}

// same as above but return reader
func (lom *LOM) Open() (fh cos.LomReader, err error) {
	fh, err = os.Open(lom.FQN)
	switch {
	case err == nil:
		return fh, nil
	case os.IsNotExist(err):
		if e := lom._checkBdir(); e != nil {
			err = e
		}
		return nil, err
	default:
		// DEBUG
		if cos.IsErrFntl(err) {
			nlog.Errorln(">>>", err)
			rdebug.PrintStack()
		}
		return nil, err
	}
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
func (lom *LOM) CreateSlice(wfqn string) (*os.File, error)     { return lom._cf(wfqn) } // --/--

func (lom *LOM) _cf(fqn string) (fh *os.File, err error) {
	fh, err = os.OpenFile(fqn, _openFlags, cos.PermRWR)
	if err == nil {
		return fh, nil
	}

	switch {
	case cos.IsErrFntl(err):
		// - when creating LOM: fixup fntl in place
		// - otherwise, return fntl error (with an implied requirement that caller must handle it)
		if fqn != lom.FQN {
			return nil, err
		}
		var (
			short = lom.ShortenFntl()
			saved = lom.PushFntl(short)
		)
		fh, err = os.OpenFile(short[0], _openFlags, cos.PermRWR)
		if err == nil {
			lom.md.lid = lom.md.lid.setlmfl(lmflFntl)
			lom.SetCustomKey(cmn.OrigFntl, saved[0])
		} else {
			debug.Assert(!cos.IsErrFntl(err))
			lom.PopFntl(saved)
		}
		return fh, err
	case !os.IsNotExist(err):
		T.FSHC(err, lom.Mountpath(), "")
		return nil, err
	}

	// slow path: create sub-directories
	if err = lom._checkBdir(); err != nil {
		return nil, err
	}
	fdir := filepath.Dir(fqn)
	if err = cos.CreateDir(fdir); err != nil {
		return nil, err
	}
	return os.OpenFile(fqn, _openFlags, cos.PermRWR)
}

func (lom *LOM) _checkBdir() (err error) {
	bdir := lom.mi.MakePathBck(lom.Bucket())
	if err = cos.Stat(bdir); err == nil {
		return nil
	}
	err = &errBdir{cname: lom.Cname(), err: err}
	bmd := T.Bowner().Get()
	if _, present := bmd.Get(&lom.bck); present {
		err = fmt.Errorf("%w [%v]", syscall.ENOTDIR, err)
	}
	return err
}

// append
func (*LOM) AppendWork(wfqn string) (fh cos.LomWriter, err error) {
	fh, err = os.OpenFile(wfqn, _apndFlags, cos.PermRWR)
	return fh, err
}

//
// remove
//

func (lom *LOM) RemoveMain() error {
	return cos.RemoveFile(lom.FQN)
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
		if erc := cos.RemoveFile(copyFQN); erc != nil && !os.IsNotExist(erc) && err == nil {
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
		return &errBdir{cname: lom.Cname(), err: err}
	}
	err := lom.RenameToMain(wfqn)
	if err == nil {
		return nil
	}
	if cos.IsErrMv(err) {
		return err
	}
	T.FSHC(err, lom.Mountpath(), wfqn)
	return cmn.NewErrFailedTo(T, "finalize", lom.Cname(), err)
}

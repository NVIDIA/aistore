// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
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

// open read-only, return a reader
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

	// case cos.IsErrFntl(err)

	default:
		return nil, err
	}
}

//
// create
//

func (lom *LOM) Create() (cos.LomWriter, error) {
	debug.Assert(lom.IsLocked() == apc.LockWrite, "must be wlocked: ", lom.Cname())
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
	if err := lom._checkBdir(); err != nil {
		return nil, err
	}
	fdir := filepath.Dir(fqn)
	if err := cos.CreateDir(fdir); err != nil {
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
		nlog.Warningln(err, "(", bdir, bmd.String(), ")")
	} else if cmn.Rom.FastV(4, cos.SmoduleCore) {
		nlog.Warningln(err, "- benign: bucket does not exist (", bdir, bmd.String(), ")")
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
	lom.UncacheDel()

	debug.AssertFunc(func() bool {
		locked := lom.IsLocked()
		if locked == apc.LockWrite {
			return true
		}
		// NOTE: making "rlock" exception to be able to forcefully rm corrupted object in the GET path
		return len(force) > 0 && force[0] && locked == apc.LockRead
	})
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
		e := &errBdir{cname: lom.Cname(), err: err}
		nlog.Warningln(e, "(", bdir, ")")
		return e
	}
	err := lom.RenameToMain(wfqn)
	switch {
	case err == nil:
		return nil
	case cos.IsErrMv(err):
		return err
	case cos.IsErrFntl(err):
		// - when finalizing LOM: fixup fntl in place
		var (
			short = lom.ShortenFntl()
			saved = lom.PushFntl(short)
		)
		err = lom.RenameToMain(wfqn)
		if err == nil {
			lom.md.lid = lom.md.lid.setlmfl(lmflFntl)
			lom.SetCustomKey(cmn.OrigFntl, saved[0])
		} else {
			debug.Assert(!cos.IsErrFntl(err))
			lom.PopFntl(saved)
		}
		return err
	default:
		T.FSHC(err, lom.Mountpath(), wfqn)
		return cmn.NewErrFailedTo(T, "finalize", lom.Cname(), err)
	}
}

// extract a single file from a (.tar, .tgz or .tar.gz, .zip, .tar.lz4) shard
// uses the provided `mime` or lom.ObjName to detect formatting (empty = auto-detect)
func (lom *LOM) NewArchpathReader(lmfh cos.LomReader, archpath, mime string) (csl cos.ReadCloseSizer, err error) {
	debug.Assert(archpath != "")
	mime, err = archive.MimeFile(lmfh, T.ByteMM(), mime, lom.ObjName)
	if err != nil {
		return nil, err
	}

	var ar archive.Reader
	ar, err = archive.NewReader(mime, lmfh, lom.Lsize())
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", lom.Cname(), err)
	}

	csl, err = ar.ReadOne(archpath)
	if err != nil {
		return nil, err
	}
	if csl == nil {
		return nil, cos.NewErrNotFound(T, archpath+" in "+lom.Cname())
	}
	return csl, nil
}

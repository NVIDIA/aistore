// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
)

const (
	_openFlags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	_apndFlags = os.O_APPEND | os.O_WRONLY
)

/////////////
// errBdir //
/////////////

type errBdir struct {
	err   error
	cname string
}

func (e *errBdir) Error() string {
	if cos.IsNotExist(e.err) {
		return e.cname + ": missing bdir (bucket exists?)"
	}
	return fmt.Sprintf("%s: missing bdir [%v]", e.cname, e.err)
}

///////////////
// LomHandle //
///////////////

type (
	LomHandle struct {
		cos.LomReader
		lom *LOM
	}
)

// interface guard
var (
	_ cos.LomReaderOpener = (*LomHandle)(nil)
)

func (lom *LOM) NewHandle(loaded bool) (*LomHandle, error) {
	debug.Assert(lom.IsLocked() > apc.LockNone, lom.Cname(), " is not locked")
	if !loaded {
		if err := lom.Load(false /*cache it*/, true); err != nil {
			return nil, err
		}
	}
	fh, err := lom.Open()
	if err != nil {
		return nil, err
	}
	return &LomHandle{LomReader: fh, lom: lom}, nil
}

func (lh *LomHandle) Open() (cos.ReadOpenCloser, error) { return lh.lom.NewHandle(true) }

//
// LOM (open, close, remove) -------------------------------
//

var _ io.ReaderAt = (*os.File)(nil) // sanity (whereby UfestReader is guarded elsewhere)

// open read-only, return a reader
// see also: lom.NewDeferROC()
// see also: lom.GetROC()
func (lom *LOM) Open() (lh cos.LomReader, err error) {
	debug.Assert(lom.IsLocked() > apc.LockNone, lom.Cname(), " is not locked")
	if lom.IsChunked() {
		lh, err = lom.NewUfestReader()
	} else {
		lh, err = os.Open(lom.FQN)
	}
	switch {
	case err == nil:
		return lh, nil
	case cos.IsNotExist(err):
		if e := lom._checkBdir(); e != nil {
			err = e
		}
		return nil, err
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
func (lom *LOM) CreatePart(wfqn string) (*os.File, error)      { return lom._cf(wfqn) } // TODO -- FIXME: niy
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
			lom.setlmfl(lmflFntl)
			lom.SetCustomKey(cmn.OrigFntl, saved[0])
		} else {
			debug.Assert(!cos.IsErrFntl(err))
			lom.PopFntl(saved)
			lom.DelCustomKey(cmn.OrigFntl)
		}
		return fh, err
	case !cos.IsNotExist(err):
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
	// TODO -- FIXME: review datapath that calls directly - u.removeCompleted()
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
		if erc := cos.RemoveFile(copyFQN); erc != nil && !cos.IsNotExist(erc) && err == nil {
			err = erc
		}
	}
	if lom.IsChunked() {
		u, e := NewUfest("", lom, true /*must-exist*/)
		debug.AssertNoErr(e)
		errN := u.removeCompleted(false /*except first*/)
		if err == nil {
			err = errN
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
	err := cos.Rename(wfqn, lom.FQN)
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOTDIR) && lom.Bck().IsRemote() {
		debug.Assert(cos.IsErrMv(err), err) // cos.Rename() returned cos.ErrMv of the type 2
		fdir := filepath.Dir(lom.FQN)
		found, errV := lom._enotdir(fdir, err)
		if errV == nil {
			// recovered - retry just once
			err = cos.Rename(wfqn, lom.FQN)
			if err == nil {
				nlog.Infoln("self heal ok")
			} else {
				nlog.Warningln("self heal fail:", err)
			}
			return err
		}
		if found && !cos.IsNotExist(errV) {
			T.FSHC(errV, lom.Mountpath(), "")
		}
	}
	return err
}

// recover from ENOTDIR (above)
// TODO [observability]: add stats counter
func (lom *LOM) _enotdir(path string, e error) (bool, error) {
	const (
		prefix = ".enotdir."
	)
	orig := path
	bdir := lom.mi.MakePathBck(lom.Bucket())
	for strings.HasPrefix(path, bdir) {
		if finfo, err := os.Stat(path); err == nil && !finfo.IsDir() {
			// the culprit
			if !finfo.Mode().IsRegular() {
				nlog.Errorf("unexpected filesystem entity in the %q path: %s", orig, finfo.Mode().String())
				return true, os.Remove(path)
			}
			// move it to trash
			nlog.Warningln("self-heal: moving", path, "to trash [", lom.Cname(), e, "]")

			flat := prefix + strings.ReplaceAll(path, "/", "_")
			name := filepath.Join(prefix+strconv.FormatInt(mono.NanoTime(), 32), flat)
			fqn := lom.mi.MakePathFQN(lom.Bucket(), fs.ObjCT, name)
			if err := cos.Rename(path, fqn); err != nil {
				nlog.Warningln("failed to rename:", err)
				return true, err
			}
			if err := lom.mi.MoveToDeleted(filepath.Dir(fqn)); err != nil {
				nlog.Warningln("failed to move-del:", err)
				return true, err
			}
		}
		path = filepath.Dir(path)
	}
	return false, nil
}

func (lom *LOM) RenameFinalize(wfqn string) error {
	bdir := lom.mi.MakePathBck(lom.Bucket())
	if err := cos.Stat(bdir); err != nil {
		e := &errBdir{cname: lom.Cname(), err: err}
		nlog.Warningln(e, "(", bdir, ")")
		return e
	}

	// (handle proactively)
	// note that generated work/chunk/etc. names are handled in fs.CSM.Gen();
	// finalize is the last place that can still hit ENAMETOOLONG
	var saved []string
	if fs.IsFntl(lom.ObjName) {
		short := lom.ShortenFntl()
		saved = lom.PushFntl(short)
	}

	err := lom.RenameToMain(wfqn)
	if err == nil {
		if len(saved) > 0 {
			if _, ok := lom.GetCustomKey(cmn.OrigFntl); !ok {
				lom.SetCustomKey(cmn.OrigFntl, saved[0])
			}
			lom.setlmfl(lmflFntl)
		}
		return nil
	}
	debug.Assert(!cos.IsErrFntl(err))
	if len(saved) > 0 {
		lom.PopFntl(saved) // undo
	}
	if !cos.IsErrMv(err) {
		T.FSHC(err, lom.Mountpath(), wfqn)
		err = cmn.NewErrFailedTo(T, "finalize", lom.Cname(), err)
	}
	return err
}

//
// archive // TODO -- FIXME: test with chunked objects
//

// extract a single file from a (.tar, .tgz or .tar.gz, .zip, .tar.lz4) shard
// uses the provided `mime` or lom.ObjName to detect formatting (empty = auto-detect)
func (lom *LOM) NewArchpathReader(lh cos.LomReader, archpath, mime string) (csl cos.ReadCloseSizer, err error) {
	debug.Assert(archpath != "")
	if err := cos.ValidateArchpath(archpath); err != nil {
		return nil, err
	}
	mime, err = archive.MimeFile(lh, T.ByteMM(), mime, lom.ObjName)
	if err != nil {
		return nil, err
	}

	var ar archive.Reader
	ar, err = archive.NewReader(mime, lh, lom.Lsize())
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

//
// other FQN access
//

func (lom *LOM) GetXattr(buf []byte) ([]byte, error)      { return fs.GetXattrBuf(lom.FQN, xattrLOM, buf) }
func (lom *LOM) GetXattrN(name string) ([]byte, error)    { return fs.GetXattr(lom.FQN, name) }
func (lom *LOM) SetXattr(data []byte) error               { return fs.SetXattr(lom.FQN, xattrLOM, data) }
func (lom *LOM) SetXattrN(name string, data []byte) error { return fs.SetXattr(lom.FQN, name, data) }

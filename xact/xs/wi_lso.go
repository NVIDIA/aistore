// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

// common context and helper methods for object listing

type (
	lomVisitedCb func(lom *core.LOM)

	// context used to `list` objects in local filesystems
	walkInfo struct {
		smap         *meta.Smap
		msg          *apc.LsoMsg
		lomVisitedCb lomVisitedCb
		markerDir    string
		wanted       cos.BitFlags
		custom       cos.StrKVs
	}
)

func noopCb(*core.LOM) {}

func isOK(status uint16) bool { return status == apc.LocOK }

// TODO: `msg.StartAfter`
func newWalkInfo(msg *apc.LsoMsg, lomVisitedCb lomVisitedCb) (wi *walkInfo) {
	wi = &walkInfo{
		smap:         core.T.Sowner().Get(),
		lomVisitedCb: lomVisitedCb,
		msg:          msg,
		wanted:       wanted(msg),
	}
	if msg.ContinuationToken != "" { // marker is always a filename
		wi.markerDir = filepath.Dir(msg.ContinuationToken)
		if wi.markerDir == "." {
			wi.markerDir = ""
		}
	}
	if msg.IsFlagSet(apc.LsVerChanged) {
		wi.custom = make(cos.StrKVs)
	}
	return
}

func (wi *walkInfo) lsmsg() *apc.LsoMsg { return wi.msg }

func (wi *walkInfo) processDir(fqn string) error {
	ct, err := core.NewCTFromFQN(fqn, nil)
	if err != nil {
		return nil
	}

	if wi.msg.Prefix != "" && !cmn.DirHasOrIsPrefix(ct.ObjectName(), wi.msg.Prefix) {
		return filepath.SkipDir
	}

	// e.g., when `markerDir` "b/c/d/" we skip directories "a/", "b/a/",
	// "b/b/" etc. but do not skip entire "b/" and "b/c/" since it is our
	// parent that we need to traverse ("b/" < "b/c/d/").
	if wi.markerDir != "" && ct.ObjectName() < wi.markerDir && !strings.HasPrefix(wi.markerDir, ct.ObjectName()) {
		return filepath.SkipDir
	}

	return nil
}

func (wi *walkInfo) match(objName string) bool {
	if wi.msg.Prefix != "" && !cmn.ObjHasPrefix(objName, wi.msg.Prefix) {
		return false
	}
	return wi.msg.ContinuationToken == "" || !cmn.TokenGreaterEQ(wi.msg.ContinuationToken, objName)
}

// new entry to be added to the listed page (note: slow path)
func (wi *walkInfo) ls(lom *core.LOM, status uint16) (e *cmn.LsoEnt) {
	e = &cmn.LsoEnt{Name: lom.ObjName, Flags: status | apc.EntryIsCached}

	if lom.IsFntl() {
		orig := lom.OrigFntl()
		if orig != nil {
			saved := lom.PushFntl(orig)
			if wi.msg.IsFlagSet(apc.LsVerChanged) {
				checkRemoteMD(lom, e)
			}
			lom.PopFntl(saved)
			e.Name = orig[1]
		}
	} else if wi.msg.IsFlagSet(apc.LsVerChanged) {
		checkRemoteMD(lom, e)
	}
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
		return
	}
	wi.setWanted(e, lom)
	wi.lomVisitedCb(lom)
	return
}

// NOTE: slow path if lom.Bck is remote
func checkRemoteMD(lom *core.LOM, e *cmn.LsoEnt) {
	res := lom.CheckRemoteMD(false /*locked*/, false /*sync*/, nil /*origReq*/)
	switch {
	case res.Eq:
		debug.AssertNoErr(res.Err)
	case cos.IsNotExist(res.Err, res.ErrCode):
		e.SetVerRemoved()
	default:
		e.SetVerChanged()
	}
}

// Performs a number of syscalls to load object metadata.
func (wi *walkInfo) callback(fqn string, de fs.DirEntry) (entry *cmn.LsoEnt, err error) {
	if de.IsDir() {
		return
	}

	lom := core.AllocLOM("")
	entry, err = wi._cb(lom, fqn)
	core.FreeLOM(lom)
	return entry, err
}

func (wi *walkInfo) _cb(lom *core.LOM, fqn string) (*cmn.LsoEnt, error) {
	if err := lom.PreInit(fqn); err != nil {
		return nil, err
	}
	if !wi.match(lom.ObjName) {
		return nil, nil
	}
	if err := lom.PostInit(); err != nil {
		return nil, err
	}

	_, local, err := lom.HrwTarget(wi.smap)
	if err != nil {
		return nil, err
	}

	status := uint16(apc.LocOK)
	if !local {
		status = apc.LocMisplacedNode
	} else if !lom.IsHRW() {
		// preliminary - IsCopy below
		status = apc.LocMisplacedMountpath
	}

	// [shortcut]: name-only optimizes-out loading md (NOTE: won't show misplaced and copies)
	if wi.msg.IsFlagSet(apc.LsNameOnly) && !fs.HasPrefixFntl(lom.ObjName) {
		if !isOK(status) {
			return nil, nil
		}
		return wi.ls(lom, status), nil
	}

	// load
	if err := lom.Load(isOK(status) /*cache it*/, false /*locked*/); err != nil {
		if cmn.IsErrObjNought(err) || !isOK(status) {
			return nil, nil
		}
		return nil, err
	}
	if lom.IsFntl() {
		// FIXME: revisit
		status = apc.LocOK
	}
	if local && lom.IsCopy() {
		// still may change below
		status = apc.LocIsCopy
	}
	if isOK(status) {
		return wi.ls(lom, status), nil
	}

	if !wi.msg.IsFlagSet(apc.LsMissing) {
		return nil, nil
	}

	// for every copy: check hrw mountpath location ("main replica")
	if local && status == apc.LocIsCopy {
		var (
			hlom   = core.AllocLOM("")
			hrwFQN = *lom.HrwFQN
		)
		debug.Assert(hrwFQN != lom.FQN)
		if err := hlom.InitFQN(hrwFQN, lom.Bucket()); err != nil {
			core.FreeLOM(hlom)
			return nil, err
		}
		if err := hlom.Load(true /*cache it*/, false /*locked*/); err != nil {
			status = apc.LocIsCopyMissingObj
		}
		core.FreeLOM(hlom)
	}

	return wi.ls(lom, status), nil
}

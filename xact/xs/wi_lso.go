// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
		lomVisitedCb lomVisitedCb
		msg          *apc.LsoMsg
		markerDir    string
		wanted       cos.BitFlags
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
	return
}

func (wi *walkInfo) lsmsg() *apc.LsoMsg { return wi.msg }

// Checks if the directory should be processed by cache list call
// Does checks:
//   - Object name must start with prefix (if it is set)
//   - Object name is not in early processed directories by the previous call:
//     paging support
func (wi *walkInfo) processDir(fqn string) error {
	ct, err := core.NewCTFromFQN(fqn, nil)
	if err != nil {
		return nil
	}

	if !cmn.DirHasOrIsPrefix(ct.ObjectName(), wi.msg.Prefix) {
		return filepath.SkipDir
	}

	// When markerDir = "b/c/d/" we should skip directories: "a/", "b/a/",
	// "b/b/" etc. but should not skip entire "b/" or "b/c/" since it is our
	// parent which we want to traverse (see that: "b/" < "b/c/d/").
	if wi.markerDir != "" && ct.ObjectName() < wi.markerDir && !strings.HasPrefix(wi.markerDir, ct.ObjectName()) {
		return filepath.SkipDir
	}

	return nil
}

// Returns true if LOM is to be included in the result set.
func (wi *walkInfo) match(lom *core.LOM) bool {
	if !cmn.ObjHasPrefix(lom.ObjName, wi.msg.Prefix) {
		return false
	}
	return wi.msg.ContinuationToken == "" || !cmn.TokenGreaterEQ(wi.msg.ContinuationToken, lom.ObjName)
}

// new entry to be added to the listed page (note: slow path)
func (wi *walkInfo) ls(lom *core.LOM, status uint16) (e *cmn.LsoEntry) {
	e = &cmn.LsoEntry{Name: lom.ObjName, Flags: status | apc.EntryIsCached}
	if wi.msg.IsFlagSet(apc.LsVerChanged) {
		checkRemoteMD(lom, e)
	}
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
		return
	}
	wi.setWanted(e, lom)
	wi.lomVisitedCb(lom)
	return
}

// NOTE: slow path
func checkRemoteMD(lom *core.LOM, e *cmn.LsoEntry) {
	if !lom.Bucket().HasVersioningMD() {
		debug.Assert(false, lom.Cname())
		return
	}
	eq, errCode, err := lom.CheckRemoteMD(false /*locked*/, false /*sync*/)
	switch {
	case eq:
		debug.AssertNoErr(err)
	case cos.IsNotExist(err, errCode):
		e.SetVerRemoved()
	default:
		e.SetVerChanged()
	}
}

// Performs a number of syscalls to load object metadata.
func (wi *walkInfo) callback(fqn string, de fs.DirEntry) (entry *cmn.LsoEntry, err error) {
	if de.IsDir() {
		return
	}
	lom := core.AllocLOM("")
	entry, err = wi.cb(lom, fqn)
	core.FreeLOM(lom)
	return
}

func (wi *walkInfo) cb(lom *core.LOM, fqn string) (*cmn.LsoEntry, error) {
	status := uint16(apc.LocOK)
	if err := lom.InitFQN(fqn, nil); err != nil {
		return nil, err
	}

	if !wi.match(lom) {
		return nil, nil
	}

	_, local, err := lom.HrwTarget(wi.smap)
	if err != nil {
		return nil, err
	}
	if !local {
		status = apc.LocMisplacedNode
	} else if !lom.IsHRW() {
		// preliminary
		status = apc.LocMisplacedMountpath
	}

	// shortcut #1: name-only optimizes-out loading md (NOTE: won't show misplaced and copies)
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
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
	if local {
		// check hrw mountpath location
		hlom := &core.LOM{}
		if err := hlom.InitFQN(lom.HrwFQN, lom.Bucket()); err != nil {
			return nil, err
		}
		if err := hlom.Load(true /*cache it*/, false /*locked*/); err != nil {
			mirror := lom.MirrorConf()
			if mirror.Enabled && mirror.Copies > 1 {
				status = apc.LocIsCopyMissingObj
			}
		}
	}
	return wi.ls(lom, status), nil
}

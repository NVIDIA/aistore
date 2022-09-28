// Package objwalk provides common context and helper methods for object listing and
// object querying.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
)

type (
	ctxKey int

	// used to traverse local filesystem and collect objects info
	WalkInfo struct {
		t            cluster.Target
		smap         *cluster.Smap
		postCallback PostCallbackFunc
		propNeeded   map[string]bool
		prefix       string
		marker       string
		markerDir    string
		msg          *apc.ListObjsMsg
		timeFormat   string
	}

	PostCallbackFunc func(lom *cluster.LOM)
)

const (
	CtxPostCallbackKey ctxKey = iota
)

var wiProps = []string{
	apc.GetPropsSize,
	apc.GetPropsAtime,
	apc.GetPropsChecksum,
	apc.GetPropsVersion,
	apc.GetPropsStatus,
	apc.GetPropsCopies,
	apc.GetTargetURL,
}

func isOK(status uint16) bool { return status == apc.LocOK }

// TODO: `msg.StartAfter`
func NewWalkInfo(ctx context.Context, t cluster.Target, msg *apc.ListObjsMsg) *WalkInfo {
	// Marker is always a file name, so we need to strip filename from the path
	markerDir := ""
	if msg.ContinuationToken != "" {
		markerDir = filepath.Dir(msg.ContinuationToken)
		if markerDir == "." {
			markerDir = ""
		}
	}

	// A small optimization: set boolean variables to avoid
	// strings.Contains() for every entry.
	postCallback, _ := ctx.Value(CtxPostCallbackKey).(PostCallbackFunc)

	//
	// TODO -- FIXME: don't allocate and don't use `propNeeded`, use `msg` instead. ???
	//
	propNeeded := make(map[string]bool, len(wiProps))
	for _, prop := range wiProps {
		propNeeded[prop] = msg.WantProp(prop)
	}
	return &WalkInfo{
		t:            t, // targetrunner
		smap:         t.Sowner().Get(),
		postCallback: postCallback,
		prefix:       msg.Prefix,            // TODO -- FIXME: remove
		marker:       msg.ContinuationToken, // TODO -- FIXME: remove
		markerDir:    markerDir,
		msg:          msg,
		timeFormat:   msg.TimeFormat, // TODO -- FIXME: remove
		propNeeded:   propNeeded,     // TODO -- FIXME: remove
	}
}

func (wi *WalkInfo) needSize() bool    { return wi.propNeeded[apc.GetPropsSize] }
func (wi *WalkInfo) needAtime() bool   { return wi.propNeeded[apc.GetPropsAtime] }
func (wi *WalkInfo) needCksum() bool   { return wi.propNeeded[apc.GetPropsChecksum] }
func (wi *WalkInfo) needVersion() bool { return wi.propNeeded[apc.GetPropsVersion] }
func (wi *WalkInfo) needStatus() bool  { return wi.propNeeded[apc.GetPropsStatus] } //nolint:unused // left for consistency
func (wi *WalkInfo) needCopies() bool  { return wi.propNeeded[apc.GetPropsCopies] }

// TODO -- FIXME: rename as target-info, remove URL, add node ID and mountpath instead
func (wi *WalkInfo) needTargetURL() bool { return wi.propNeeded[apc.GetTargetURL] }

// Checks if the directory should be processed by cache list call
// Does checks:
//   - Object name must start with prefix (if it is set)
//   - Object name is not in early processed directories by the previous call:
//     paging support
func (wi *WalkInfo) ProcessDir(fqn string) error {
	ct, err := cluster.NewCTFromFQN(fqn, nil)
	if err != nil {
		return nil
	}

	if !cmn.DirNameContainsPrefix(ct.ObjectName(), wi.prefix) {
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
func (wi *WalkInfo) match(lom *cluster.LOM) bool {
	if !cmn.ObjNameContainsPrefix(lom.ObjName, wi.prefix) {
		return false
	}
	if wi.marker != "" && cmn.TokenIncludesObject(wi.marker, lom.ObjName) {
		return false
	}
	return true
}

// new entry to be added to the listed page (TODO: add/support EC info)
func (wi *WalkInfo) ls(lom *cluster.LOM, status uint16) *cmn.ObjEntry {
	entry := &cmn.ObjEntry{Name: lom.ObjName, Flags: status | apc.EntryIsCached}
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
		return entry
	}
	if wi.needAtime() {
		entry.Atime = cos.FormatUnixNano(lom.AtimeUnix(), wi.timeFormat)
	}
	if wi.needCksum() && lom.Checksum() != nil {
		entry.Checksum = lom.Checksum().Value()
	}
	if wi.needVersion() {
		entry.Version = lom.Version()
	}
	if wi.needCopies() {
		// TODO -- FIXME: may not be true - double-check
		entry.Copies = int16(lom.NumCopies())
	}
	//
	// TODO -- FIXME: add/support EC info
	//
	if wi.needTargetURL() {
		entry.TargetURL = wi.t.Snode().URL(cmn.NetPublic)
	}
	if wi.needSize() {
		entry.Size = lom.SizeBytes()
	}
	if wi.postCallback != nil {
		wi.postCallback(lom)
	}
	return entry
}

// By default, Callback performs a number of syscalls to load object metadata.
// A note in re cmn.LsNameOnly (usage below):
//
//	the flag cmn.LsNameOnly optimizes-out loading object metadata. If defined,
//	the function returns (only the) name and status.
func (wi *WalkInfo) Callback(fqn string, de fs.DirEntry) (entry *cmn.ObjEntry, err error) {
	if de.IsDir() {
		return
	}
	lom := cluster.AllocLOM("")
	entry, err = wi.cb(lom, fqn)
	cluster.FreeLOM(lom)
	return
}

func (wi *WalkInfo) cb(lom *cluster.LOM, fqn string) (*cmn.ObjEntry, error) {
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

	// shortcut #1: name-only (NOTE: won't show misplaced and copies)
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
		if !isOK(status) {
			return wi.ls(lom, status), nil
		}
		return nil, nil
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

	if !wi.msg.IsFlagSet(apc.LsAll) {
		return nil, nil
	}
	if local {
		// check hrw mountpath location
		hlom := &cluster.LOM{}
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

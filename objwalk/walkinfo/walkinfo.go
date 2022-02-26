// Package walkinfo provides common context and helper methods for object listing and
// object querying.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package walkinfo

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
		objectFilter cluster.ObjectFilter
		propNeeded   map[string]bool
		prefix       string
		Marker       string
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

func isObjMoved(status uint16) bool {
	return status == apc.ObjStatusMovedNode || status == apc.ObjStatusMovedMpath
}

func NewWalkInfo(ctx context.Context, t cluster.Target, msg *apc.ListObjsMsg) *WalkInfo {
	// TODO: this should be removed.
	// TODO: we should take care of `msg.StartAfter`.
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

	propNeeded := make(map[string]bool, len(wiProps))
	for _, prop := range wiProps {
		propNeeded[prop] = msg.WantProp(prop)
	}
	return &WalkInfo{
		t:            t, // targetrunner
		smap:         t.Sowner().Get(),
		postCallback: postCallback,
		prefix:       msg.Prefix,
		Marker:       msg.ContinuationToken,
		markerDir:    markerDir,
		msg:          msg,
		timeFormat:   msg.TimeFormat,
		propNeeded:   propNeeded,
	}
}

func (wi *WalkInfo) needSize() bool      { return wi.propNeeded[apc.GetPropsSize] }
func (wi *WalkInfo) needAtime() bool     { return wi.propNeeded[apc.GetPropsAtime] }
func (wi *WalkInfo) needCksum() bool     { return wi.propNeeded[apc.GetPropsChecksum] }
func (wi *WalkInfo) needVersion() bool   { return wi.propNeeded[apc.GetPropsVersion] }
func (wi *WalkInfo) needStatus() bool    { return wi.propNeeded[apc.GetPropsStatus] } //nolint:unused // left for consistency
func (wi *WalkInfo) needCopies() bool    { return wi.propNeeded[apc.GetPropsCopies] }
func (wi *WalkInfo) needTargetURL() bool { return wi.propNeeded[apc.GetTargetURL] }

// Checks if the directory should be processed by cache list call
// Does checks:
//  - Object name must start with prefix (if it is set)
//  - Object name is not in early processed directories by the previous call:
//    paging support
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

func (wi *WalkInfo) SetObjectFilter(f cluster.ObjectFilter) {
	wi.objectFilter = f
}

// Returns true if the LOM matches all criteria for including the object
// to the resulting bucket list.
func (wi *WalkInfo) matchObj(lom *cluster.LOM) bool {
	if !cmn.ObjNameContainsPrefix(lom.ObjName, wi.prefix) {
		return false
	}
	if wi.Marker != "" && cmn.TokenIncludesObject(wi.Marker, lom.ObjName) {
		return false
	}
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
		return true
	}
	return wi.objectFilter == nil || wi.objectFilter(lom)
}

// Adds an info about cached object to the list if:
//  - its name starts with prefix (if prefix is set)
//  - it has not been already returned by previous page request
//  - this target responses getobj request for the object
// NOTE: When only object names are requested, objectFilter and postCallback
//       are not called because there will be no metadata to look at (see
//       WalkInfo.Callback() for details)
func (wi *WalkInfo) lsObject(lom *cluster.LOM, objStatus uint16) *cmn.BucketEntry {
	if !wi.matchObj(lom) {
		return nil
	}

	// add the obj to the page
	fileInfo := &cmn.BucketEntry{
		Name:  lom.ObjName,
		Flags: objStatus | apc.EntryIsCached,
	}
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
		return fileInfo
	}

	if wi.needAtime() {
		fileInfo.Atime = cos.FormatUnixNano(lom.AtimeUnix(), wi.timeFormat)
	}
	if wi.needCksum() && lom.Checksum() != nil {
		fileInfo.Checksum = lom.Checksum().Value()
	}
	if wi.needVersion() {
		fileInfo.Version = lom.Version()
	}
	if wi.needCopies() {
		fileInfo.Copies = int16(lom.NumCopies())
	}
	if wi.needTargetURL() {
		fileInfo.TargetURL = wi.t.Snode().URL(cmn.NetPublic)
	}
	if wi.needSize() {
		fileInfo.Size = lom.SizeBytes()
	}
	if wi.postCallback != nil {
		wi.postCallback(lom)
	}
	return fileInfo
}

// By default, Callback performs a number of syscalls to load object metadata.
// A note in re cmn.LsNameOnly (usage below):
//    the flag cmn.LsNameOnly optimizes-out loading object metadata. If defined,
//    the function returns (only the) name and status.
func (wi *WalkInfo) Callback(fqn string, de fs.DirEntry) (*cmn.BucketEntry, error) {
	if de.IsDir() {
		return nil, nil
	}

	var objStatus uint16 = apc.ObjStatusOK
	lom := &cluster.LOM{FQN: fqn}
	if err := lom.Init(cmn.Bck{}); err != nil {
		return nil, err
	}

	_, local, err := lom.HrwTarget(wi.smap)
	if err != nil {
		return nil, err
	}
	if !local {
		objStatus = apc.ObjStatusMovedNode
	} else if !lom.IsHRW() {
		objStatus = apc.ObjStatusMovedMpath
	}

	if isObjMoved(objStatus) && !wi.msg.IsFlagSet(apc.LsMisplaced) {
		return nil, nil
	}
	if wi.msg.IsFlagSet(apc.LsNameOnly) {
		return wi.lsObject(lom, objStatus), nil
	}

	if err := lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		if cmn.IsErrObjNought(err) {
			return nil, nil
		}
		return nil, err
	}
	if lom.IsCopy() {
		return nil, nil
	}
	return wi.lsObject(lom, objStatus), nil
}

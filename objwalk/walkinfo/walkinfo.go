// Package objwalk provides core functionality for reading the list of a bucket objects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package walkinfo

import (
	"context"
	"path/filepath"
	"strings"

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
		msg          *cmn.ListObjsMsg
		timeFormat   string
	}

	PostCallbackFunc func(lom *cluster.LOM)
)

const (
	CtxPostCallbackKey ctxKey = iota
)

var wiProps = []string{
	cmn.GetPropsSize,
	cmn.GetPropsAtime,
	cmn.GetPropsChecksum,
	cmn.GetPropsVersion,
	cmn.GetPropsStatus,
	cmn.GetPropsCopies,
	cmn.GetTargetURL,
}

func isObjMoved(status uint16) bool {
	return status == cmn.ObjStatusMovedNode || status == cmn.ObjStatusMovedMpath
}

func NewWalkInfo(ctx context.Context, t cluster.Target, msg *cmn.ListObjsMsg) *WalkInfo {
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

func (wi *WalkInfo) needSize() bool      { return wi.propNeeded[cmn.GetPropsSize] }
func (wi *WalkInfo) needAtime() bool     { return wi.propNeeded[cmn.GetPropsAtime] }
func (wi *WalkInfo) needCksum() bool     { return wi.propNeeded[cmn.GetPropsChecksum] }
func (wi *WalkInfo) needVersion() bool   { return wi.propNeeded[cmn.GetPropsVersion] }
func (wi *WalkInfo) needStatus() bool    { return wi.propNeeded[cmn.GetPropsStatus] } //nolint:unused // left for consistency
func (wi *WalkInfo) needCopies() bool    { return wi.propNeeded[cmn.GetPropsCopies] }
func (wi *WalkInfo) needTargetURL() bool { return wi.propNeeded[cmn.GetTargetURL] }

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
	if wi.msg.IsFlagSet(cmn.LsOnlyNames) {
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
		Flags: objStatus | cmn.EntryIsCached,
	}
	if wi.msg.IsFlagSet(cmn.LsOnlyNames) {
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
		fileInfo.TargetURL = wi.t.Snode().URL(cmn.NetworkPublic)
	}
	if wi.needSize() {
		fileInfo.Size = lom.SizeBytes()
	}
	if wi.postCallback != nil {
		wi.postCallback(lom)
	}
	return fileInfo
}

// By default, Callback does a few syscalls to load file attributes and xatrrs
// to fill the object info. If a client needs only object names, the bucket
// list can be sped up by setting the flag cmn.LsOnlyNames which skips
// calling expensive filesystem requests. When cmn.LsOnlyNames is set, the
// Callback fills only object name and status.
func (wi *WalkInfo) Callback(fqn string, de fs.DirEntry) (*cmn.BucketEntry, error) {
	if de.IsDir() {
		return nil, nil
	}

	var objStatus uint16 = cmn.ObjStatusOK
	lom := &cluster.LOM{FQN: fqn}
	if err := lom.Init(cmn.Bck{}); err != nil {
		return nil, err
	}

	_, local, err := lom.HrwTarget(wi.smap)
	if err != nil {
		return nil, err
	}
	if !local {
		objStatus = cmn.ObjStatusMovedNode
	} else if !lom.IsHRW() {
		objStatus = cmn.ObjStatusMovedMpath
	}

	if isObjMoved(objStatus) && !wi.msg.IsFlagSet(cmn.LsMisplaced) {
		return nil, nil
	}
	// LsOnlyNames skips loading object's metadata.
	if wi.msg.IsFlagSet(cmn.LsOnlyNames) {
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

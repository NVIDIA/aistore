// Package objwalk provides core functionality for reading the list of a bucket objects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/karrick/godirwalk"
)

type (
	Walk struct {
		msg     *cmn.SelectMsg
		ctx     context.Context
		t       cluster.Target
		bucket  string
		isLocal bool
	}
	mresp struct {
		infos      *allfinfos
		failedPath string
		err        error
	}
)

func (w *Walk) newFileWalk(bucket string, msg *cmn.SelectMsg) *allfinfos {
	// Marker is always a file name, so we need to strip filename from path
	markerDir := ""
	if msg.PageMarker != "" {
		markerDir = filepath.Dir(msg.PageMarker)
		if markerDir == "." {
			markerDir = ""
		}
	}

	// A small optimization: set boolean variables need* to avoid
	// doing string search(strings.Contains) for every entry.
	ci := &allfinfos{
		t:            w.t, // targetrunner
		smap:         w.t.GetSmap(),
		objs:         make([]*cmn.BucketEntry, 0, cmn.DefaultPageSize),
		prefix:       msg.Prefix,
		marker:       msg.PageMarker,
		markerDir:    markerDir,
		msg:          msg,
		lastFilePath: "",
		bucket:       bucket,
		fileCount:    0,
		rootLength:   0,
		limit:        cmn.DefaultPageSize, // maximum number files to return
		needAtime:    msg.WantProp(cmn.GetPropsAtime),
		needChkSum:   msg.WantProp(cmn.GetPropsChecksum),
		needVersion:  msg.WantProp(cmn.GetPropsVersion),
		needStatus:   msg.WantProp(cmn.GetPropsStatus),
		needCopies:   msg.WantProp(cmn.GetPropsCopies),
	}

	if msg.PageSize != 0 {
		ci.limit = msg.PageSize
	}

	return ci
}

func NewWalk(ctx context.Context, bucket string, isLocal bool, msg *cmn.SelectMsg, t cluster.Target) *Walk {
	return &Walk{
		msg:     msg,
		ctx:     ctx,
		t:       t,
		bucket:  bucket,
		isLocal: isLocal,
	}
}

// LocalObjPage walks local filesystems and collects all object for a given
// bucket. NOTE: the bucket can be local or cloud one. In latter case the
// function returns the list of cloud objects cached locally
func (w *Walk) LocalObjPage() (*cmn.BucketList, error) {
	availablePaths, _ := fs.Mountpaths.Get()
	ch := make(chan *mresp, len(fs.CSM.RegisteredContentTypes)*len(availablePaths))
	wg := &sync.WaitGroup{}

	// function to traverse one mountpoint
	walkMpath := func(dir string) {
		r := &mresp{w.newFileWalk(w.bucket, w.msg), "", nil}
		if w.msg.Fast {
			r.infos.limit = math.MaxInt64 // return all objects in one response
		}
		if _, err := os.Stat(dir); err != nil {
			if !os.IsNotExist(err) {
				r.failedPath = dir
				r.err = err
			}
			ch <- r // not an error, just skip the path
			wg.Done()
			return
		}
		r.infos.rootLength = len(dir) + 1 // +1 for separator between bucket and filename
		if w.msg.Fast {
			// return all object names and sizes (and only names and sizes)
			err := godirwalk.Walk(dir, &godirwalk.Options{
				Callback: r.infos.listwalkfFast,
				Unsorted: true,
			})
			if err != nil {
				glog.Errorf("Failed to traverse path %q, err: %v", dir, err)
				r.failedPath = dir
				r.err = err
			}
		} else if err := filepath.Walk(dir, r.infos.listwalkf); err != nil {
			glog.Errorf("Failed to traverse path %q, err: %v", dir, err)
			r.failedPath = dir
			r.err = err
		}
		ch <- r
		wg.Done()
	}

	// Traverse all mountpoints in parallel.
	// If any mountpoint traversing fails others keep running until they complete.
	// But in this case all collected data is thrown away because the partial result
	// makes paging inconsistent
	for contentType, contentResolver := range fs.CSM.RegisteredContentTypes {
		if !contentResolver.PermToProcess() {
			continue
		}
		for _, mpathInfo := range availablePaths {
			wg.Add(1)
			dir := mpathInfo.MakePathBucket(contentType, w.bucket, w.isLocal)
			go walkMpath(dir)
		}
	}
	wg.Wait()
	close(ch)

	// combine results into one long list
	// real size of page is set in newFileWalk, so read it from any of results inside loop
	pageSize := cmn.DefaultPageSize
	objLists := make([][]*cmn.BucketEntry, 0)
	for r := range ch {
		if r.err != nil {
			if !os.IsNotExist(r.err) {
				w.t.FSHC(r.err, r.failedPath)
				return nil, fmt.Errorf("failed to read %s", r.failedPath)
			}
			continue
		}
		objLists = append(objLists, r.infos.objs)
	}

	maxSize := pageSize
	if w.msg.Fast {
		maxSize = 0
	}
	bckEntries, marker := ConcatObjLists(objLists, maxSize)
	bucketList := &cmn.BucketList{
		Entries:    bckEntries,
		PageMarker: marker,
	}

	if w.msg.WantProp(cmn.GetTargetURL) {
		for _, e := range bucketList.Entries {
			e.TargetURL = w.t.Snode().URL(cmn.NetworkPublic)
		}
	}

	return bucketList, nil
}

// CloudObjPage reads a page of objects in a cloud bucket. NOTE: if a request
// wants cached object list, the function returns only local data without
// talking to cloud provider.
// After reading cloud object list, the function fills it with information
// that is available only locally(copies, targetURL etc).
func (w *Walk) CloudObjPage(cached bool) (*cmn.BucketList, error) {
	if cached {
		return w.LocalObjPage()
	}

	bucketList, err, _ := w.t.CloudIntf().ListBucket(w.ctx, w.bucket, w.msg)
	if err != nil {
		return nil, err
	}

	needURL := w.msg.WantProp(cmn.GetTargetURL)
	needAtime := w.msg.WantProp(cmn.GetPropsAtime)
	needChkSum := w.msg.WantProp(cmn.GetPropsChecksum)
	needVersion := w.msg.WantProp(cmn.GetPropsVersion)
	needCopies := w.msg.WantProp(cmn.GetPropsCopies)
	config := cmn.GCO.Get()
	localURL := w.t.Snode().URL(cmn.NetworkPublic)
	localID := w.t.Snode().DaemonID
	smap := w.t.GetSmap()
	for _, e := range bucketList.Entries {
		si, _ := cluster.HrwTarget(w.bucket, e.Name, smap)
		if si.DaemonID != localID {
			continue
		}

		if needURL {
			e.TargetURL = localURL
		}

		lom, errstr := cluster.LOM{T: w.t, Bucket: w.bucket, Objname: e.Name, BucketProvider: cmn.CloudBs}.Init(config)
		if errstr != "" {
			continue
		}
		_, errstr = lom.Load(true)
		if errstr != "" || !lom.Exists() {
			continue
		}

		e.SetCached()
		if needAtime {
			e.Atime = cmn.FormatTime(lom.Atime(), w.msg.TimeFormat)
		}
		if needChkSum && lom.Cksum() != nil {
			_, storedCksum := lom.Cksum().Get()
			e.Checksum = storedCksum
		}
		if needVersion {
			e.Version = lom.Version()
		}
		if needCopies {
			e.Copies = int16(lom.NumCopies())
		}
	}

	return bucketList, nil
}

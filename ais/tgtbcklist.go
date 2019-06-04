// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	jsoniter "github.com/json-iterator/go"
	"github.com/karrick/godirwalk"
)

type (
	allfinfos struct {
		t            *targetrunner
		files        []*cmn.BucketEntry
		prefix       string
		marker       string
		markerDir    string
		msg          *cmn.SelectMsg
		lastFilePath string
		bucket       string
		fileCount    int
		rootLength   int
		limit        int
		needAtime    bool
		needCtime    bool
		needChkSum   bool
		needVersion  bool
		needStatus   bool
		needCopies   bool
	}
)

// Checks if the directory should be processed by cache list call
// Does checks:
//  - Object name must start with prefix (if it is set)
//  - Object name is not in early processed directories by the previous call:
//    paging support
func (ci *allfinfos) processDir(fqn string) error {
	if len(fqn) <= ci.rootLength {
		return nil
	}

	// every directory has to either:
	// - start with prefix (for levels higher than prefix: prefix="ab", directory="abcd/def")
	// - or include prefix (for levels deeper than prefix: prefix="a/", directory="a/b/")
	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(ci.prefix, relname) && !strings.HasPrefix(relname, ci.prefix) {
		return filepath.SkipDir
	}

	// When markerDir = "b/c/d/" we should skip directories: "a/", "b/a/",
	// "b/b/" etc. but should not skip entire "b/" or "b/c/" since it is our
	// parent which we want to traverse (see that: "b/" < "b/c/d/").
	if ci.markerDir != "" && relname < ci.markerDir && !strings.HasPrefix(ci.markerDir, relname) {
		return filepath.SkipDir
	}

	return nil
}

// Adds an info about cached object to the list if:
//  - its name starts with prefix (if prefix is set)
//  - it has not been already returned by previous page request
//  - this target responses getobj request for the object
func (ci *allfinfos) lsObject(lom *cluster.LOM, osfi os.FileInfo, objStatus string) error {
	relname := lom.FQN[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}
	if ci.marker != "" && relname <= ci.marker {
		return nil
	}
	// add the obj to the page
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:     relname,
		Atime:    "",
		IsCached: true,
		Status:   objStatus,
		Copies:   1,
	}
	_, _ = lom.Load(true) // FIXME: handle errors
	if ci.needAtime {
		fileInfo.Atime = cmn.FormatTime(lom.Atime(), ci.msg.TimeFormat)
	}
	if ci.needCtime {
		fileInfo.Ctime = cmn.FormatTime(osfi.ModTime(), ci.msg.TimeFormat)
	}
	if ci.needChkSum && lom.Cksum() != nil {
		_, storedCksum := lom.Cksum().Get()
		fileInfo.Checksum = storedCksum
	}
	if ci.needVersion {
		fileInfo.Version = lom.Version()
	}
	if ci.needCopies {
		fileInfo.Copies = int16(lom.NumCopies())
	}
	fileInfo.Size = osfi.Size()
	ci.files = append(ci.files, fileInfo)
	ci.lastFilePath = lom.FQN
	return nil
}

// fast alternative of generic listwalk: do not fetch any object information
// Always returns all objects - no paging required. But the result may have
// 'ghost' or duplicated  objects.
// The only supported SelectMsg feature is 'Prefix' - it does not slow down.
func (ci *allfinfos) listwalkfFast(fqn string, de *godirwalk.Dirent) error {
	if de.IsDir() {
		return ci.processDir(fqn)
	}

	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:   relname,
		Status: cmn.ObjStatusOK,
	}
	ci.files = append(ci.files, fileInfo)
	return nil
}

func (ci *allfinfos) listwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if errstr := cmn.PathWalkErr(err); errstr != "" {
			glog.Errorf(errstr)
			return err
		}
		return nil
	}
	if ci.fileCount >= ci.limit {
		return filepath.SkipDir
	}
	if osfi.IsDir() {
		return ci.processDir(fqn)
	}
	// FIXME: check the logic vs local/global rebalance
	var (
		objStatus = cmn.ObjStatusOK
	)
	lom, errstr := cluster.LOM{T: ci.t, FQN: fqn}.Init()
	if errstr != "" {
		glog.Errorf("%s: %s", lom, errstr) // proceed to list this object anyway
	}
	_, errstr = lom.Load(true)
	if !lom.Exists() {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	if lom.Misplaced() {
		objStatus = cmn.ObjStatusMoved
	} else {
		if errstr != "" {
			glog.Errorf("%s: %s", lom, errstr) // proceed to list this object anyway
		}
		si, errstr := hrwTarget(lom.Bucket, lom.Objname, ci.t.smapowner.get())
		if errstr != "" {
			glog.Errorf("%s: %s", lom, errstr)
		}
		if ci.t.si.DaemonID != si.DaemonID {
			objStatus = cmn.ObjStatusMoved
		}
	}
	return ci.lsObject(lom, osfi, objStatus)
}

// List bucket returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string,
	bckIsLocal bool, actionMsg *actionMsgInternal) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		bmd := t.bmdowner.get()
		glog.Infof("%s %s <= (%s)", r.Method, bmd.Bstring(bucket, bckIsLocal), pid)
	}
	useCache, errstr, errcode := t.checkCacheQueryParameter(r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	getMsgJSON, err := jsoniter.Marshal(actionMsg.Value)
	if err != nil {
		errstr := fmt.Sprintf("Unable to marshal 'value' in request: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}

	var msg cmn.SelectMsg
	err = jsoniter.Unmarshal(getMsgJSON, &msg)
	if err != nil {
		errstr := fmt.Sprintf("Unable to unmarshal 'value' in request to a cmn.SelectMsg: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if bckIsLocal {
		tag = "local"
		ok = t.listBucketAsync(w, r, bucket, bckIsLocal, &msg)
		return // ======================================>
	}
	// cloud bucket
	msg.Fast = false // fast mode does not apply to Cloud buckets
	if useCache {
		tag = "cloud cached"
		jsbytes, errstr = t.listCachedObjects(bucket, &msg, false /* local */)
	} else {
		tag = "cloud"
		jsbytes, err, errcode = getcloudif().listbucket(t.contextWithAuth(r.Header), bucket, &msg)
		if err != nil {
			errstr = fmt.Sprintf("error listing cloud bucket %s: %d(%v)", bucket, errcode, err)
		}
	}
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

// should not be called for local buckets
func (t *targetrunner) listCachedObjects(bucket string, msg *cmn.SelectMsg,
	bckIsLocal bool) (outbytes []byte, errstr string) {
	reslist, err := t.prepareLocalObjectList(bucket, msg, bckIsLocal)
	if err != nil {
		return nil, err.Error()
	}

	outbytes, err = jsoniter.Marshal(reslist)
	if err != nil {
		return nil, err.Error()
	}
	return
}

func (t *targetrunner) prepareLocalObjectList(bucket string, msg *cmn.SelectMsg,
	bckIsLocal bool) (*cmn.BucketList, error) {
	type mresp struct {
		infos      *allfinfos
		failedPath string
		err        error
	}

	availablePaths, _ := fs.Mountpaths.Get()
	ch := make(chan *mresp, len(fs.CSM.RegisteredContentTypes)*len(availablePaths))
	wg := &sync.WaitGroup{}

	// function to traverse one mountpoint
	walkMpath := func(dir string) {
		r := &mresp{t.newFileWalk(bucket, msg), "", nil}
		if msg.Fast {
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
		if msg.Fast {
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
			dir := mpathInfo.MakePathBucket(contentType, bucket, bckIsLocal)
			go walkMpath(dir)
		}
	}
	wg.Wait()
	close(ch)

	// combine results into one long list
	// real size of page is set in newFileWalk, so read it from any of results inside loop
	pageSize := cmn.DefaultPageSize
	bckEntries := make([]*cmn.BucketEntry, 0)
	fileCount := 0
	for r := range ch {
		if r.err != nil {
			if !os.IsNotExist(r.err) {
				t.fshc(r.err, r.failedPath)
				return nil, fmt.Errorf("failed to read %s", r.failedPath)
			}
			continue
		}

		pageSize = r.infos.limit
		bckEntries = append(bckEntries, r.infos.files...)
		fileCount += r.infos.fileCount
	}

	// determine whether the fast-listed bucket is /cold/, and load lom cache if it is
	if msg.Fast {
		const minloaded = 10 // check that many randomly-selected
		if fileCount > minloaded {
			go func(bckEntries []*cmn.BucketEntry) {
				var (
					bckProvider = cmn.BckProviderFromLocal(bckIsLocal)
					l           = len(bckEntries)
					m           = l / minloaded
					loaded      int
				)
				if l < minloaded {
					return
				}
				for i := 0; i < l; i += m {
					lom, errstr := cluster.LOM{T: t, Bucket: bucket,
						Objname: bckEntries[i].Name, BucketProvider: bckProvider}.Init()
					if errstr == "" && lom.IsLoaded() { // loaded?
						loaded++
					}
				}
				renew := loaded < minloaded/2
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Errorf("%s: loaded %d/%d, renew=%t", t.si, loaded, minloaded, renew)
				}
				if renew {
					t.xactions.renewBckLoadLomCache(bucket, t, bckIsLocal)
				}
			}(bckEntries)
		}
	}

	marker := ""
	// - sort the result but only if the set is greater than page size
	//   For corner case: we have objects with replicas on page threshold
	//   we have to sort taking status into account. Otherwise wrong
	//   one(Status=moved) may get into the response
	// - return the page size
	// - do not sort fast list (see Unsorted above)
	if !msg.Fast && fileCount > pageSize {
		ifLess := func(i, j int) bool {
			if bckEntries[i].Name == bckEntries[j].Name {
				return bckEntries[i].Status < bckEntries[j].Status
			}
			return bckEntries[i].Name < bckEntries[j].Name
		}
		sort.Slice(bckEntries, ifLess)
		// set extra infos to nil to avoid memory leaks
		// see NOTE on https://github.com/golang/go/wiki/SliceTricks
		for i := pageSize; i < fileCount; i++ {
			bckEntries[i] = nil
		}
		bckEntries = bckEntries[:pageSize]
		marker = bckEntries[pageSize-1].Name
	}

	bucketList := &cmn.BucketList{
		Entries:    bckEntries,
		PageMarker: marker,
	}

	if strings.Contains(msg.Props, cmn.GetTargetURL) {
		for _, e := range bucketList.Entries {
			e.TargetURL = t.si.PublicNet.DirectURL
		}
	}

	return bucketList, nil
}

// asynchronous list bucket request
// - creates a new task that collects objects in background
// - returns status of a running task by its ID
// - returns the result of a task by its ID
// TODO: support cloud buckets
func (t *targetrunner) listBucketAsync(w http.ResponseWriter, r *http.Request, bucket string, bckIsLocal bool, msg *cmn.SelectMsg) bool {
	cmn.Assert(bckIsLocal)
	query := r.URL.Query()
	taskAction := query.Get(cmn.URLParamTaskAction)
	// create task call
	if taskAction == cmn.ListTaskStart {
		xact := t.xactions.renewBckListXact(t, bucket, bckIsLocal, msg)
		if xact == nil {
			return false
		}

		w.WriteHeader(http.StatusAccepted)
		return true
	}

	xactStats := t.xactions.GetTaskXact(msg.TaskID)
	// task never started
	if xactStats == nil {
		t.invalmsghdlr(w, r, "Task not found", http.StatusNotFound)
		return false
	}
	// task still running
	if !xactStats.Get().Finished() {
		w.WriteHeader(http.StatusAccepted)
		return true
	}
	// task has finished
	xtask, ok := xactStats.Get().(*xactBckListTask)
	if !ok || xtask == nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Invalid task type: %T", xactStats.Get()), http.StatusInternalServerError)
		return false
	}
	st := (*taskState)(xtask.res.Load())
	if st.Err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Task failed: %v", st.Err), http.StatusInternalServerError)
		return false
	}

	if taskAction == cmn.ListTaskResult {
		// return the final result only if it is requested explicitly
		jsbytes, err := jsoniter.Marshal(st.Result)
		cmn.AssertNoErr(err)
		return t.writeJSON(w, r, jsbytes, "listbucket")
	}

	// default action: return task status 200 = successfully completed
	w.WriteHeader(http.StatusOK)
	return true
}

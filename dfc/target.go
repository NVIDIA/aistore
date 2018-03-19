// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/golang/glog"
)

const (
	cachedPageSize = 10000 // the number of cached file infos returned in one page
)

type mountPath struct {
	Path string
	Fsid syscall.Fsid
}
type fipair struct {
	relname string
	os.FileInfo
	atime time.Time
}
type allfinfos struct {
	finfos     []fipair
	rootLength int
}

type cachedInfos struct {
	files        []*BucketEntry
	fileCount    int
	rootLength   int
	prefix       string
	marker       string
	markerDirs   []string
	needAtime    bool
	msg          *GetMsg
	lastFilePath string
	t            *targetrunner
	bucket       string
}

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif       cloudif // multi-cloud vendor support
	smap          *Smap
	proxysi       *daemonInfo
	xactinp       *xactInProgress
	starttime     time.Time
	lbmap         *lbmap
	rtnamemap     *rtnamemap
	prefetchQueue chan filesWithDeadline
}

// start target runner
func (t *targetrunner) run() error {
	t.httprunner.init(getstorstats())
	t.httprunner.kalive = gettargetkalive()
	t.smap = &Smap{}                                 // cluster map
	t.xactinp = newxactinp()                         // extended actions
	t.lbmap = &lbmap{LBmap: make(map[string]string)} // local (cache-only) buckets
	t.rtnamemap = newrtnamemap(128)                  // lock/unlock name

	if status, err := t.register(false); err != nil {
		glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
		if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
			glog.Errorf("Target %s: retrying registration...", t.si.DaemonID)
			time.Sleep(time.Second * 3)
			if _, err = t.register(false); err != nil {
				glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
				glog.Errorf("Target %s is terminating", t.si.DaemonID)
				return err
			}
			glog.Errorf("Success: target %s joined the cluster", t.si.DaemonID)
		} else {
			return err
		}
	}
	// fill-in mpaths
	ctx.mountpaths = make(map[string]*mountPath, len(ctx.config.FSpaths))
	if t.testingFSPpaths() {
		glog.Infof("Warning: configuring %d fspaths for testing", ctx.config.TestFSP.Count)
		t.testCachepathMounts()
	} else {
		t.fspath2mpath()
		t.mpath2Fsid() // enforce FS uniqueness
	}
	for mpath := range ctx.mountpaths {
		cloudbctsfqn := mpath + "/" + ctx.config.CloudBuckets
		if err := CreateDir(cloudbctsfqn); err != nil {
			glog.Fatalf("FATAL: cannot create cloud buckets dir %q, err: %v", cloudbctsfqn, err)
		}
		localbctsfqn := mpath + "/" + ctx.config.LocalBuckets
		if err := CreateDir(localbctsfqn); err != nil {
			glog.Fatalf("FATAL: cannot create local buckets dir %q, err: %v", localbctsfqn, err)
		}
	}

	// cloud provider
	if ctx.config.CloudProvider == amazoncloud {
		// TODO: sessions
		t.cloudif = &awsimpl{t}

	} else {
		assert(ctx.config.CloudProvider == googlecloud)
		t.cloudif = &gcpimpl{t}
	}
	// init capacity
	rr := getstorstatsrunner()
	rr.initCapacity()
	// prefetch
	t.prefetchQueue = make(chan filesWithDeadline, prefetchChanSize)

	//
	// REST API: register storage target's handler(s) and start listening
	//
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", t.filehdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon, t.daemonhdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon+"/", t.daemonhdlr) // FIXME
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rpush+"/", t.pushhdlr)
	t.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Target %s is ready", t.si.DaemonID)
	glog.Flush()
	t.starttime = time.Now()
	return t.httprunner.run()
}

// stop gracefully
func (t *targetrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.name, err)
	sleep := t.xactinp.abortAll()
	close(t.rtnamemap.abrt)
	if t.httprunner.h != nil {
		t.unregister() // ignore errors
	}
	t.httprunner.stop(err)
	if sleep {
		time.Sleep(time.Second)
	}
}

// target registration with proxy
func (t *targetrunner) register(keepalive bool) (status int, err error) {
	jsbytes, err := json.Marshal(t.si)
	if err != nil {
		return 0, fmt.Errorf("Unexpected failure to json-marshal %+v, err: %v", t.si, err)
	}
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rcluster
	if keepalive {
		url += "/" + Rkeepalive
		_, err, _, status = t.call(t.proxysi, url, http.MethodPost, jsbytes, kalivetimeout)
	} else {
		_, err, _, status = t.call(t.proxysi, url, http.MethodPost, jsbytes)
	}
	return
}

func (t *targetrunner) unregister() (status int, err error) {
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rcluster
	url += "/" + Rdaemon + "/" + t.si.DaemonID
	_, err, _, status = t.call(t.proxysi, url, http.MethodDelete, nil)
	return
}

//==============
//
// http handlers
//
//==============

// "/"+Rversion+"/"+Rfiles
func (t *targetrunner) filehdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpfilget(w, r)
	case http.MethodPut:
		t.httpfilput(w, r)
	case http.MethodDelete:
		t.httpfildelete(w, r)
	case http.MethodPost:
		t.httpfilpost(w, r)
	case http.MethodHead:
		t.httpfilhead(w, r)
	default:
		invalhdlr(w, r)
	}
}

// checkCloudVersion returns if versions of an object differ in Cloud and DFC cache
// and the object should be refreshed from Cloud storage
// It should be called only in case of the object is present in DFC cache
func (t *targetrunner) checkCloudVersion(bucket, objname, version string) (vchanged bool, errstr string, errcode int) {
	var objmeta map[string]string
	if objmeta, errstr, errcode = t.cloudif.headobject(bucket, objname); errstr != "" {
		return
	}
	if cloudVersion, ok := objmeta["version"]; ok {
		if version != cloudVersion {
			glog.Infof("Object %s/%s version changed, current version %s (old/local %s)",
				bucket, objname, cloudVersion, version)
			vchanged = true
		}
	}
	return
}

// "/"+Rversion+"/"+Rfiles+"/"+bucket [+"/"+objname]
//
// checks if the object exists locally (if not, downloads it)
// and sends it back via http
func (t *targetrunner) httpfilget(w http.ResponseWriter, r *http.Request) {
	var (
		nhobj                        cksumvalue
		coldget, exclusive, vchanged bool
		bucket, objname, fqn         string
		uname, errstr, version       string
		size                         int64
		errcode                      int
		props                        *objectProps
	)
	cksumcfg := &ctx.config.CksumConfig
	versioncfg := &ctx.config.VersionConfig
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname = apitems[0], ""
	if len(apitems) > 1 {
		objname = apitems[1]
	}
	if strings.Contains(bucket, "/") {
		errstr = fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	//
	// list the bucket and return
	//
	if len(objname) == 0 {
		t.listbucket(w, r, bucket)
		return
	}
	//
	// serialize on the name
	//
	fqn, uname, exclusive = t.fqn(bucket, objname), bucket+objname, true
	t.rtnamemap.lockname(uname, exclusive, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer func(locktype *bool) { t.rtnamemap.unlockname(uname, *locktype) }(&exclusive)

	//
	// get the object from the bucket
	//
	if coldget, size, version, errstr = t.getchecklocal(bucket, objname, fqn); errstr != "" {
		t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
		return
	}
	// FIXME - TODO: split ValidateWarmGet into a) validate and b) get new if invalid
	// the second flag controls whether the original request blocks on version update
	if !coldget && versioncfg.ValidateWarmGet && version != "" {
		if vchanged, errstr, errcode = t.checkCloudVersion(bucket, objname, version); errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
		coldget = vchanged
	}
	if coldget {
		// FIXME - TODO: with rename similar to PUT
		// getfqn := fmt.Sprintf("%s.%d", fqn, time.Now().UnixNano())
		if props, errstr, errcode = getcloudif().getobj(fqn, bucket, objname); errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
		size, nhobj = props.size, props.nhobj
		t.statsif.add("numcoldget", 1)
		t.statsif.add("bytesloaded", size)
		if vchanged {
			t.statsif.add("bytesvchanged", size)
			t.statsif.add("numvchanged", 1)
		}
	}
	//
	// downgrade lock(name)
	//
	exclusive = false
	t.rtnamemap.downgradelock(uname)
	//
	// local file => http response
	//
	file, err := os.Open(fqn)
	if err != nil {
		if os.IsPermission(err) {
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", fqn)
			t.invalmsghdlr(w, r, errstr, http.StatusForbidden)
		} else {
			errstr = fmt.Sprintf("Failed to open local file %s, err: %v", fqn, err)
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
		}
		return
	}

	defer file.Close()
	if !coldget && cksumcfg.Checksum != ChecksumNone {
		hashbinary, errstr := Getxattr(fqn, xattrXXHashVal)
		if errstr == "" && hashbinary != nil {
			nhobj = newcksumvalue(cksumcfg.Checksum, string(hashbinary))
		}
	}
	if nhobj != nil {
		htype, hval := nhobj.get()
		w.Header().Add(HeaderDfcChecksumType, htype)
		w.Header().Add(HeaderDfcChecksumVal, hval)
	}
	if size == 0 {
		errstr = fmt.Sprintf("Unexpected: object %s/%s size is zero", bucket, objname)
		t.invalmsghdlr(w, r, errstr)
		return // likely, an error
	}
	slab := selectslab(size)
	buf := slab.alloc()
	defer slab.free(buf)
	// copy
	written, err := io.CopyBuffer(w, file, buf)
	if err != nil {
		errstr = fmt.Sprintf("Failed to send file %s, err: %v", fqn, err)
		t.invalmsghdlr(w, r, errstr)
	} else if glog.V(3) {
		glog.Infof("GET: sent %s (%.2f MB)", fqn, float64(written)/1000/1000)
	}
	if coldget && props.version != "" {
		Setxattr(fqn, xattrObjVersion, []byte(props.version))
	}
	t.statsif.add("numget", 1)
}

func (t *targetrunner) getchecklocal(bucket, objname, fqn string) (coldget bool, size int64, version string, errstr string) {
	finfo, err := os.Stat(fqn)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			if t.islocalBucket(bucket) {
				errstr = fmt.Sprintf("GET local: file %s (object %s/%s) does not exist", fqn, bucket, objname)
				return
			}
			coldget = true
		case os.IsPermission(err):
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", fqn)
		default:
			errstr = fmt.Sprintf("Failed to fstat %s, err: %v", fqn, err)
		}
		return
	}
	size = finfo.Size()
	if bytes, errs := Getxattr(fqn, xattrObjVersion); errs == "" {
		version = string(bytes)
	}
	return
}

// "/"+Rversion+"/"+Rpush+"/"+bucket
func (t *targetrunner) pushhdlr(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, "push"); apitems == nil {
		return
	}
	bucket := apitems[0]

	if strings.Contains(bucket, "/") {
		s := fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		t.invalmsghdlr(w, r, s)
		return
	}
	if pusher, ok := w.(http.Pusher); ok {
		objnamebytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			s := fmt.Sprintf("Could not read Request Body: %v", err)
			if err == io.EOF {
				trailer := r.Trailer.Get("Error")
				if trailer != "" {
					s = fmt.Sprintf("pushhdlr: Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
				}
			}
			t.invalmsghdlr(w, r, s)
			return
		}
		objnames := make([]string, 0)
		err = json.Unmarshal(objnamebytes, &objnames)
		if err != nil {
			s := fmt.Sprintf("Could not unmarshal objnames: %v", err)
			t.invalmsghdlr(w, r, s)
			return
		}

		for _, objname := range objnames {
			err := pusher.Push("/v1/files/"+bucket+"/"+objname, nil)
			if err != nil {
				t.invalmsghdlr(w, r, "Error Pushing "+"/v1/files/"+bucket+"/"+objname+": "+err.Error())
				return
			}
		}
	} else {
		t.invalmsghdlr(w, r, "Pusher Unavailable - could not push files.")
		return
	}

	_, err := w.Write([]byte("Pushed Object List "))
	if err != nil {
		s := fmt.Sprintf("Error writing response: %v", err)
		t.invalmsghdlr(w, r, s)
	}
}

// should not be called for local buckets
func (t *targetrunner) listCachedObjects(bucket string, msg *GetMsg) (outbytes []byte, errstr string, errcode int) {
	var err error

	if t.islocalBucket(bucket) {
		return nil, fmt.Sprintf("Cache is unavailable for local bucket %s", bucket), 0
	}

	needAtime := strings.Contains(msg.GetProps, GetPropsAtime)
	// Split a marker into separate directory list to make pagination
	// more effective. All directories in the list are skipped by
	// filepath.Walk. The last directory of the marker must be excluded
	// from the list because the marker can point to the middle of it
	markerDirs := make([]string, 0)
	if msg.GetPageMarker != "" && strings.Contains(msg.GetPageMarker, "/") {
		idx := strings.LastIndex(msg.GetPageMarker, "/")
		markerDirs = strings.Split(msg.GetPageMarker[:idx], "/")
		markerDirs = markerDirs[:len(markerDirs)-1]
	}
	allfinfos := cachedInfos{make([]*BucketEntry, 0, cachedPageSize), 0, 0, msg.GetPrefix, msg.GetPageMarker, markerDirs, needAtime, msg, "", t, bucket}

	// We need stable order of mountpaths
	mpathList := make([]string, 0, len(ctx.mountpaths))
	for mpath := range ctx.mountpaths {
		mpathList = append(mpathList, mpath)
	}
	sort.Strings(mpathList)

	for _, mpath := range mpathList {
		localbucketfqn := mpath + "/" + ctx.config.CloudBuckets + "/" + bucket
		_, err = os.Stat(localbucketfqn)
		if err != nil {
			continue
		}

		allfinfos.rootLength = len(localbucketfqn) + 1 // +1 for separator between bucket and filename
		if err = filepath.Walk(localbucketfqn, allfinfos.listwalkf); err != nil {
			errstr = fmt.Sprintf("Failed to traverse mpath %q, err: %v", mpath, err)
			glog.Errorf(errstr)
		}
	}

	if err == nil {
		var reslist = BucketList{Entries: allfinfos.files}
		// Mark the batch as truncated if it is full
		if len(allfinfos.files) >= cachedPageSize {
			reslist.PageMarker = allfinfos.lastFilePath
		}
		outbytes, err = json.Marshal(reslist)
	}
	if err != nil {
		errstr = fmt.Sprintf("Failed to traverse cached objects: %v", err.Error())
		glog.Errorf(errstr)
	}

	return
}

func (t *targetrunner) doLocalBucketList(w http.ResponseWriter, r *http.Request, bucket string, msg *GetMsg) {
	finfos := allfinfos{make([]fipair, 0, 128), 0}
	for mpath := range ctx.mountpaths {
		localbucketfqn := mpath + "/" + ctx.config.LocalBuckets + "/" + bucket
		finfos.rootLength = len(localbucketfqn) + 1 // +1 for separator between bucket and filename
		if err := filepath.Walk(localbucketfqn, finfos.listwalkf); err != nil {
			glog.Errorf("Failed to traverse mpath %q, err: %v", mpath, err)
		}
	}

	t.statsif.add("numlist", 1)
	var reslist = BucketList{Entries: make([]*BucketEntry, 0, len(finfos.finfos))}
	for _, fi := range finfos.finfos {
		if msg.GetPrefix != "" && !strings.HasPrefix(fi.relname, msg.GetPrefix) {
			continue
		}

		entry := &BucketEntry{}
		entry.Name = fi.relname

		if strings.Contains(msg.GetProps, GetPropsSize) {
			entry.Size = fi.Size()
		}
		if strings.Contains(msg.GetProps, GetPropsCtime) {
			t := fi.ModTime()
			switch msg.GetTimeFormat {
			case "":
				fallthrough
			case RFC822:
				entry.Ctime = t.Format(time.RFC822)
			default:
				entry.Ctime = t.Format(msg.GetTimeFormat)
			}
		}
		if strings.Contains(msg.GetProps, GetPropsChecksum) {
			fqn := t.fqn(bucket, fi.relname)
			xxhex, errstr := Getxattr(fqn, xattrXXHashVal)
			if errstr == "" {
				entry.Checksum = hex.EncodeToString(xxhex)
			}
		}
		if strings.Contains(msg.GetProps, GetPropsAtime) {
			if msg.GetTimeFormat == "" {
				entry.Atime = fi.atime.Format(RFC822)
			} else {
				entry.Atime = fi.atime.Format(msg.GetTimeFormat)
			}
		}
		reslist.Entries = append(reslist.Entries, entry)
	}
	jsbytes, err := json.Marshal(reslist)
	assert(err == nil, err)
	t.writeJSON(w, r, jsbytes, "listbucket")
}

func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	islocal, errstr, errcode := t.checkLocalQueryParameter(bucket, r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
	}
	useCache, errstr, errcode := t.checkCacheQueryParameter(r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
	}

	msg := &GetMsg{}
	if t.readJSON(w, r, msg) != nil {
		return
	}
	if islocal {
		t.doLocalBucketList(w, r, bucket, msg)
		return
	}
	if useCache {
		// read local file infos
		jsbytes, errstr, errcode = t.listCachedObjects(bucket, msg)
	} else {
		// do cloud request
		if jsbytes, errstr, errcode = getcloudif().listbucket(bucket, msg); errstr == "" {
			t.statsif.add("numlist", 1)
		}
	}
	if errstr != "" {
		if errcode == 0 {
			t.invalmsghdlr(w, r, errstr)
		} else {
			t.invalmsghdlr(w, r, errstr, errcode)
		}
		return
	}
	t.writeJSON(w, r, jsbytes, "listbucket")
}

func (all *allfinfos) listwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("listwalkf callback invoked with err: %v", err)
		return err
	}
	if osfi.IsDir() {
		// Listbucket doesn't need to return directories
		return nil
	}
	relname := fqn[all.rootLength:]
	atime, _, _ := getAmTimes(osfi)
	all.finfos = append(all.finfos, fipair{relname, osfi, atime})
	return nil
}

func (ci *cachedInfos) processDir(fqn string) error {
	if len(fqn) <= ci.rootLength {
		return nil
	}

	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(ci.prefix, relname) {
		return filepath.SkipDir
	}

	if len(ci.markerDirs) != 0 {
		dirs := strings.Split(fqn, "/")
		maxIdx := len(dirs)
		if len(ci.markerDirs) < maxIdx {
			maxIdx = len(ci.markerDirs)
		}
		for idx := 0; idx < maxIdx; idx++ {
			if dirs[idx] < ci.markerDirs[idx] {
				return filepath.SkipDir
			}
		}
	}

	return nil
}

func (ci *cachedInfos) processRegularFile(fqn string, osfi os.FileInfo) error {
	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}

	if ci.marker != "" && fqn <= ci.marker {
		return nil
	}

	si, errstr := hrwTarget(ci.bucket+"/"+relname, ci.t.smap)
	if errstr != "" {
		glog.Errorln(errstr)
		return nil
	}
	if si.DaemonID != ci.t.si.DaemonID {
		// this target is not responsible for returning this object
		return nil
	}

	// the file passed all checks - add it to the batch
	ci.fileCount++
	fileInfo := &BucketEntry{Name: relname, Atime: "", IsCached: true}
	if ci.needAtime {
		atime, _, _ := getAmTimes(osfi)
		if ci.msg.GetTimeFormat == "" {
			fileInfo.Atime = atime.Format(RFC822)
		} else {
			fileInfo.Atime = atime.Format(ci.msg.GetTimeFormat)
		}
	}
	ci.files = append(ci.files, fileInfo)
	ci.lastFilePath = fqn
	return nil
}

func (ci *cachedInfos) listwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("listwalkf callback invoked with err: %v", err)
		return err
	}

	if ci.fileCount >= cachedPageSize {
		return filepath.SkipDir
	}

	if osfi.IsDir() {
		return ci.processDir(fqn)
	}

	return ci.processRegularFile(fqn, osfi)
}

func (t *targetrunner) httpfilput(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		fmt.Println("Problem in put with URL " + r.URL.Path)
		return
	}
	query := r.URL.Query()

	from, to, bucket, objname := query.Get(ParamFromID), query.Get(ParamToID), apitems[0], ""
	if len(apitems) > 1 {
		objname = strings.Join(apitems[1:], "/")
	}
	if from != "" && to != "" {
		// Rebalance: "/"+Rversion+"/"+Rfiles + "/"+bucket+"/"+objname+"?from_id="+from_id+"&to_id="+to_id
		if objname == "" {
			s := "Invalid URL: missing object name to copy"
			t.invalmsghdlr(w, r, s)
			return
		}
		size, errstr := t.dorebalance(r, from, to, bucket, objname)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		t.statsif.add("numrecvfiles", 1)
		t.statsif.add("numrecvbytes", size)
	} else {
		// PUT: "/"+Rversion+"/"+Rfiles+"/"+bucket+"/"+objname
		errstr, errcode := t.doput(w, r, bucket, objname)
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
	}
}

func (t *targetrunner) doput(w http.ResponseWriter, r *http.Request, bucket, objname string) (errstr string, errcode int) {
	var (
		file                       *os.File
		err                        error
		hdhobj, nhobj              cksumvalue
		xxhashval                  string
		htype, hval, nhtype, nhval string
		sgl                        *SGLIO
	)
	cksumcfg := &ctx.config.CksumConfig
	fqn := t.fqn(bucket, objname)
	putfqn := fmt.Sprintf("%s.%d", fqn, time.Now().UnixNano())
	if glog.V(3) {
		glog.Infof("PUT: %s => %s", fqn, putfqn)
	}
	hdhobj = newcksumvalue(r.Header.Get(HeaderDfcChecksumType), r.Header.Get(HeaderDfcChecksumVal))
	if hdhobj != nil {
		htype, hval = hdhobj.get()
	}
	// optimize out if the checksums do match
	if hdhobj != nil && cksumcfg.Checksum != ChecksumNone {
		file, err = os.Open(fqn)
		// exists - compute checksum and compare with the caller's
		if err == nil {
			slab := selectslab(0) // unknown size
			buf := slab.alloc()
			if htype == ChecksumXXHash {
				xx := xxhash.New64()
				xxhashval, errstr = ComputeXXHash(file, buf, xx)
			} else {
				errstr = fmt.Sprintf("Unsupported checksum type %s", htype)
			}
			// not a critical error
			if errstr != "" {
				glog.Warningf("Warning: Bad checksum: %s: %v", fqn, errstr)
			}
			slab.free(buf)
			// not a critical error
			if err = file.Close(); err != nil {
				glog.Warningf("Unexpected failure to close %s once xxhash-ed, err: %v", fqn, err)
			}
			if errstr == "" && xxhashval == hval {
				glog.Infof("Existing %s is valid: PUT is a no-op", fqn)
				return
			}
		}
	}
	inmem := (ctx.config.AckPolicy.Put == AckWhenInMem)
	if sgl, nhobj, _, errstr = t.receive(putfqn, inmem, objname, "", hdhobj, r.Body); errstr != "" {
		return
	}
	if nhobj != nil {
		nhtype, nhval = nhobj.get()
		assert(hdhobj == nil || htype == nhtype)
	}
	// validate checksum when and if provided
	if hval != "" && nhval != "" && hval != nhval {
		errstr = fmt.Sprintf("Bad checksum: %s/%s %s %s... != %s...", bucket, objname, htype, hval[:8], nhval[:8])
		return
	}
	// commit
	if sgl == nil {
		return t.putCommit(bucket, objname, putfqn, fqn, nhobj, false)
	}
	// FIXME: AA: use xaction
	go t.sglToCloudAsync(sgl, bucket, objname, putfqn, fqn, nhobj)
	return
}

func (t *targetrunner) sglToCloudAsync(sgl *SGLIO, bucket, objname, putfqn, fqn string, nhobj cksumvalue) {
	slab := selectslab(sgl.Size())
	buf := slab.alloc()
	defer func() {
		sgl.Free()
		slab.free(buf)
	}()
	// sgl => fqn sequence
	file, err := CreateFile(putfqn)
	if err != nil {
		glog.Errorln("sglToCloudAsync: create", putfqn, err)
		return
	}
	reader := NewReader(sgl)
	written, err := io.CopyBuffer(file, reader, buf)
	if err != nil {
		glog.Errorln("sglToCloudAsync: CopyBuffer", err)
		if err1 := file.Close(); err != nil {
			glog.Errorf("Nested error %v => (remove %s => err: %v)", err, putfqn, err1)
		}
		if err2 := os.Remove(putfqn); err != nil {
			glog.Errorf("Nested error %v => (remove %s => err: %v)", err, putfqn, err2)
		}
		return
	}
	assert(written == sgl.Size())
	err = file.Close()
	if err != nil {
		glog.Errorln("sglToCloudAsync: Close", err)
		if err1 := os.Remove(putfqn); err != nil {
			glog.Errorf("Nested error %v => (remove %s => err: %v)", err, putfqn, err1)
		}
		return
	}
	errstr, _ := t.putCommit(bucket, objname, putfqn, fqn, nhobj, false)
	if errstr != "" {
		glog.Errorln("sglToCloudAsync: commit", errstr)
		return
	}
	glog.Infof("sglToCloudAsync: %s/%s done", bucket, objname)
}

func (t *targetrunner) putCommit(bucket, objname, putfqn, fqn string, nhobj cksumvalue, rebalance bool) (errstr string, errcode int) {
	var (
		file *os.File
		err  error
	)
	defer func() {
		if errstr != "" {
			if err = os.Remove(putfqn); err != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, putfqn, err)
			}
		}
	}()
	// cloud
	if !t.islocalBucket(bucket) && !rebalance {
		if file, err = os.Open(putfqn); err != nil {
			errstr = fmt.Sprintf("Failed to reopen %s err: %v", putfqn, err)
			return
		}
		if errstr, errcode = getcloudif().putobj(file, bucket, objname, nhobj); errstr != "" {
			_ = file.Close()
			return
		}
		if err = file.Close(); err != nil {
			glog.Errorf("Unexpected failure to close an already PUT file %s, err: %v", putfqn, err)
			_ = os.Remove(putfqn)
			return
		}
	}
	// when all set and done:
	if errstr = t.putSafeRename(bucket, objname, putfqn, fqn); errstr != "" {
		return
	}
	// FIXME: PUT must be returning the version - use it here to "finalize"
	if errstr = finalizeobj(fqn, nhobj); errstr != "" {
		return
	}
	t.statsif.add("numput", 1)
	return
}

func (t *targetrunner) putSafeRename(bucket, objname, putfqn, fqn string) (errstr string) {
	uname := bucket + objname
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	if err := os.Rename(putfqn, fqn); err != nil {
		errstr = fmt.Sprintf("Unexpected failure to rename %s => %s, err: %v", putfqn, fqn, err)
	} else {
		glog.Infof("PUT done: %s <= %s", fqn, putfqn)
	}
	t.rtnamemap.unlockname(uname, true)
	return
}

func (t *targetrunner) dorebalance(r *http.Request, from, to, bucket, objname string) (size int64, errstr string) {
	if t.si.DaemonID != from && t.si.DaemonID != to {
		errstr = fmt.Sprintf("File copy: %s is not the intended source %s nor the destination %s",
			t.si.DaemonID, from, to)
		return
	}
	fqn := t.fqn(bucket, objname)

	if t.si.DaemonID == from {
		//
		// the source
		//
		uname := bucket + objname
		t.rtnamemap.lockname(uname, false, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
		defer t.rtnamemap.unlockname(uname, false)

		finfo, err := os.Stat(fqn)
		if glog.V(3) {
			glog.Infof("Rebalance from %q: bucket %q objname %q => to %q", from, bucket, objname, to)
		}
		if err != nil && os.IsNotExist(err) {
			errstr = fmt.Sprintf("File copy: %s does not exist at the source %s", fqn, t.si.DaemonID)
			return
		}
		si, ok := t.smap.Smap[to]
		if !ok {
			errstr = fmt.Sprintf("File copy: unknown destination %s (do syncsmap?)", to)
			return
		}
		size = finfo.Size()
		if errstr = t.sendfile(r.Method, bucket, objname, si, size, ""); errstr != "" {
			return
		}
		if glog.V(3) {
			glog.Infof("Sent %q to %s (size %.2f MB)", fqn, to, float64(finfo.Size())/1000/1000)
		}
	} else {
		//
		// the destination
		//
		if glog.V(3) {
			glog.Infof("Rebalance to %q: bucket %q objname %q <= from %q", to, bucket, objname, from)
		}
		putfqn := fmt.Sprintf("%s.%d", fqn, time.Now().UnixNano())
		_, err := os.Stat(fqn)
		if err != nil && os.IsExist(err) {
			glog.Infof("File copy: %s already exists at the destination %s", fqn, t.si.DaemonID)
			return // not an error, nothing to do
		}
		var (
			hdhobj = newcksumvalue(r.Header.Get(HeaderDfcChecksumType), r.Header.Get(HeaderDfcChecksumVal))
			nhobj  cksumvalue
			inmem  = false // TODO
		)
		if _, nhobj, size, errstr = t.receive(putfqn, inmem, objname, "", hdhobj, r.Body); errstr != "" {
			return
		}
		if nhobj != nil {
			nhtype, nhval := nhobj.get()
			htype, hval := hdhobj.get()
			assert(htype == nhtype)
			if hval != nhval {
				errstr = fmt.Sprintf("Bad checksum at the destination %s: %s/%s %s %s... != %s...",
					t.si.DaemonID, bucket, objname, htype, hval[:8], nhval[:8])
				return
			}
		}
		errstr, _ = t.putCommit(bucket, objname, putfqn, fqn, nhobj, true)
	}
	return
}

func (t *targetrunner) httpfildelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname string
		msg             ActionMsg
		evict           bool
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname = apitems[0], ""
	if len(apitems) > 1 {
		objname = strings.Join(apitems[1:], "/")
	}

	b, err := ioutil.ReadAll(r.Body)
	if err == nil && len(b) > 0 {
		err = json.Unmarshal(b, &msg)
		if err == nil {
			evict = (msg.Action == ActEvict)
		}
	} else if err != nil {
		s := fmt.Sprintf("fildelete: Failed to read %s request, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("fildelete: Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		t.invalmsghdlr(w, r, s)
		return
	}
	if objname == "" && len(b) > 0 {
		// It must be a List/Range request, since there is no object name
		t.deletefiles(w, r, msg)
		return
	} else if objname != "" {
		err := t.fildelete(bucket, objname, evict)
		if err != nil {
			s := fmt.Sprintf("Error deleting %s/%s: %v", bucket, objname, err)
			t.invalmsghdlr(w, r, s)
		}
		return
	}
	s := fmt.Sprintf("Invalid API request: No object name or message body.")
	t.invalmsghdlr(w, r, s)
}

func (t *targetrunner) fildelete(bucket, objname string, evict bool) error {
	var (
		errstr  string
		errcode int
	)
	fqn := t.fqn(bucket, objname)
	uname := bucket + objname
	localbucket := t.islocalBucket(bucket)

	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	if !localbucket && !evict {
		errstr, errcode = getcloudif().deleteobj(bucket, objname)
		t.statsif.add("numdelete", 1)
		if errstr != "" {
			if errcode == 0 {
				return fmt.Errorf("%s", errstr)
			}
			return fmt.Errorf("%d: %s", errcode, errstr)
		}
	}

	finfo, err := os.Stat(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			if localbucket && !evict {
				return fmt.Errorf("DELETE local: file %s (local bucket %s, object %s) does not exist",
					fqn, bucket, objname)
			}

			// Do try to delete non-cached objects.
			return nil

		}
	}
	if !(evict && localbucket) {
		// Don't evict from a local bucket (this would be deletion)
		if err := os.Remove(fqn); err != nil {
			return err
		} else if evict {
			t.statsif.add("bytesevicted", finfo.Size())
			t.statsif.add("filesevicted", 1)
		}
	}
	return nil
}

func (t *targetrunner) httpfilpost(w http.ResponseWriter, r *http.Request) {
	var msg ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}

	switch msg.Action {
	case ActPrefetch:
		t.prefetchfiles(w, r, msg)
	case ActRename:
		t.renamefile(w, r, msg)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}

}

func (t *targetrunner) renamefile(w http.ResponseWriter, r *http.Request, msg ActionMsg) {
	var errstr string

	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Rfiles); apitems == nil {
		return
	}

	bucket, objname := apitems[0], strings.Join(apitems[1:], "/")
	newobjname := msg.Name
	fqn, uname := t.fqn(bucket, objname), bucket+objname
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	finfo, err := os.Stat(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Rename/move: failed to fstat %s (local bucket %s, object %s), err: %v",
			fqn, bucket, objname, err)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	si, errstr := hrwTarget(bucket+"/"+newobjname, t.smap)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	// local rename
	if si.DaemonID == t.si.DaemonID {
		newfqn := t.fqn(bucket, newobjname)
		dirname := filepath.Dir(newfqn)
		if err := CreateDir(dirname); err != nil {
			errstr = fmt.Sprintf("Unexpected failure to create local dir %s, err: %v", dirname, err)
			t.invalmsghdlr(w, r, errstr)
		} else if err := os.Rename(fqn, newfqn); err != nil {
			errstr = fmt.Sprintf("Failed to rename %s => %s, err: %v", fqn, newfqn, err)
			t.invalmsghdlr(w, r, errstr)
		} else {
			t.statsif.add("numrename", 1)
			if glog.V(3) {
				glog.Infof("Renamed %s => %s", fqn, newfqn)
			}
		}
		return
	}
	// move/migrate
	glog.Infof("Migrating [%s %s => %s] %s => %s", bucket, objname, newobjname, t.si.DaemonID, si.DaemonID)

	if errstr = t.sendfile(http.MethodPut, bucket, objname, si, finfo.Size(), newobjname); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
}

func (t *targetrunner) prefetchfiles(w http.ResponseWriter, r *http.Request, msg ActionMsg) {
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		t.invalmsghdlr(w, r, "Could not parse List/Range Message: ActionMsg.Value was not map[string]interface{}")
		return
	}
	if _, ok := jsmap["objnames"]; ok {
		// Prefetch with List
		if prefetchMsg, err := parseListMsg(jsmap); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Could not parse PrefetchMsg: %v", err))
		} else {
			t.prefetchList(w, r, prefetchMsg)
		}
	} else {
		// Prefetch with Range
		if prefetchRangeMsg, err := parseRangeMsg(jsmap); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Could not parse PrefetchMsg: %v", err))
		} else {
			t.prefetchRange(w, r, prefetchRangeMsg)
		}
	}
}

func (t *targetrunner) deletefiles(w http.ResponseWriter, r *http.Request, msg ActionMsg) {
	evict := msg.Action == ActEvict
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		t.invalmsghdlr(w, r, "Could not parse List/Range Message: ActionMsg.Value was not map[string]interface{}")
		return
	}
	if _, ok := jsmap["objnames"]; ok {
		// Delete with List
		if deleteMsg, err := parseListMsg(jsmap); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Could not parse PrefetchMsg: %v", err))
		} else if evict {
			t.evictList(w, r, deleteMsg)
		} else {
			t.deleteList(w, r, deleteMsg)
		}
	} else {
		// Delete with Range
		if deleteMsg, err := parseRangeMsg(jsmap); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Could not parse PrefetchMsg: %v", err))
		} else if evict {
			t.evictRange(w, r, deleteMsg)
		} else {
			t.deleteRange(w, r, deleteMsg)
		}
	}
}

func (t *targetrunner) sendfile(method, bucket, objname string, destsi *daemonInfo, size int64, newobjname string) string {
	var (
		xxhashval string
		errstr    string
	)
	if size == 0 {
		return fmt.Sprintf("Unexpected: %s/%s size is zero", bucket, objname)
	}
	cksumcfg := &ctx.config.CksumConfig
	if newobjname == "" {
		newobjname = objname
	}
	fromid, toid := t.si.DaemonID, destsi.DaemonID // source=self and destination
	url := destsi.DirectURL + "/" + Rversion + "/" + Rfiles + "/"
	url += bucket + "/" + newobjname
	url += fmt.Sprintf("?%s=%s&%s=%s", ParamFromID, fromid, ParamToID, toid)

	fqn := t.fqn(bucket, objname)
	file, err := os.Open(fqn)
	if err != nil {
		return fmt.Sprintf("Failed to open %q, err: %v", fqn, err)
	}
	defer file.Close()

	slab := selectslab(size)
	if cksumcfg.Checksum != ChecksumNone {
		assert(cksumcfg.Checksum == ChecksumXXHash)
		buf := slab.alloc()
		xx := xxhash.New64()
		if xxhashval, errstr = ComputeXXHash(file, buf, xx); errstr != "" {
			slab.free(buf)
			return errstr
		}
		slab.free(buf)
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return fmt.Sprintf("Unexpected fseek failure when sending %q from %s, err: %v", fqn, t.si.DaemonID, err)
	}
	//
	// do send
	//
	request, err := http.NewRequest(method, url, file)
	if err != nil {
		return fmt.Sprintf("Unexpected failure to create %s request %s, err: %v", method, url, err)
	}
	if xxhashval != "" {
		request.Header.Set(HeaderDfcChecksumType, ChecksumXXHash)
		request.Header.Set(HeaderDfcChecksumVal, xxhashval)
	}
	response, err := t.httpclient.Do(request)
	if err != nil {
		return fmt.Sprintf("Failed to send %q from %s, err: %v", fqn, t.si.DaemonID, err)
	}
	if response != nil {
		defer response.Body.Close()
		_, err = ioutil.ReadAll(response.Body)
		if err != nil {
			s := fmt.Sprintf("Failed to read response body %q from %s, err: %v", fqn, t.si.DaemonID, err)
			if err == io.EOF {
				trailer := response.Trailer.Get("Error")
				if trailer != "" {
					s = fmt.Sprintf("Failed to read response body %q from %s, err: %v, trailer: %s",
						fqn, t.si.DaemonID, err, trailer)
				}
			}
			return s
		}
	}
	t.statsif.add("numsentfiles", 1)
	t.statsif.add("numsentbytes", size)
	return ""
}

func (t *targetrunner) httpfilhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket      string
		islocal     bool
		errstr      string
		errcode     int
		bucketprops map[string]string
	)

	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket = apitems[0]
	if strings.Contains(bucket, "/") {
		errstr = fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		t.invalmsghdlr(w, r, errstr)
		return
	}

	islocal, errstr, errcode = t.checkLocalQueryParameter(bucket, r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	if !islocal {
		bucketprops, errstr, errcode = getcloudif().headbucket(bucket)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
	} else {
		bucketprops = make(map[string]string)
		bucketprops[HeaderServer] = dfclocal
	}

	for k, v := range bucketprops {
		w.Header().Add(k, v)
	}
}

func (t *targetrunner) checkCacheQueryParameter(r *http.Request) (useCache bool, errstr string, errcode int) {
	useCacheStr := r.URL.Query().Get(ParamCached)
	if useCacheStr != "" && useCacheStr != "true" && useCacheStr != "false" {
		errstr = fmt.Sprintf("Invalid parameter: \"%s=%s\". Must be [\"\",\"true\",\"false\"]", ParamCached, useCacheStr)
		errcode = http.StatusInternalServerError
		return
	}

	useCache = useCacheStr == "true"
	return
}

func (t *targetrunner) checkLocalQueryParameter(bucket string, r *http.Request) (islocal bool, errstr string, errcode int) {
	// If a client provides the local parameter, but the bucket is not loca
	islocal = t.islocalBucket(bucket)
	proxylocalstr := r.URL.Query().Get(ParamLocal)
	if proxylocalstr != "" && proxylocalstr != "true" && proxylocalstr != "false" {
		errstr = fmt.Sprintf("Invalid parameter: \"%s=%s\". Must be [\"\",\"true\",\"false\"]", ParamLocal, proxylocalstr)
		errcode = http.StatusInternalServerError
		return
	}
	proxylocal := proxylocalstr == "true"
	if proxylocalstr != "" && islocal != proxylocal {
		errstr = fmt.Sprintf("Mismatch with islocalbucket: Client( %v ), Target( %v )", proxylocal, islocal)
		errcode = http.StatusInternalServerError
	}
	return
}

//
// Cloud bucket + object => (local hashed path, fully qualified filename)
//
func (t *targetrunner) fqn(bucket, objname string) string {
	mpath := hrwMpath(bucket + "/" + objname)
	if t.islocalBucket(bucket) {
		return mpath + "/" + ctx.config.LocalBuckets + "/" + bucket + "/" + objname
	}
	return mpath + "/" + ctx.config.CloudBuckets + "/" + bucket + "/" + objname
}

//===========================
//
// control plane
//
//===========================

// "/"+Rversion+"/"+Rdaemon
func (t *targetrunner) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpdaeget(w, r)
	case http.MethodPut:
		t.httpdaeput(w, r)
	case http.MethodPost:
		t.httpdaepost(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
}

func (t *targetrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	// PUT '{Smap}' /v1/daemon/(syncsmap|rebalance)
	if len(apitems) > 0 && (apitems[0] == Rsyncsmap || apitems[0] == Rebalance) {
		t.httpdaeputSmap(w, r, apitems)
		return
	}
	// PUT '{lbmap}' /v1/daemon/localbuckets
	if len(apitems) > 0 && apitems[0] == Rsynclb {
		t.httpdaeputLBMap(w, r, apitems)
		return
	}
	//
	// other PUT /daemon actions
	//
	var msg ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: Not a string"))
		} else if errstr := t.setconfig(msg.Name, value); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		} else if msg.Name == "lru_enabled" && value == "false" {
			_, lruxact := t.xactinp.find(ActLRU)
			if lruxact != nil {
				if glog.V(3) {
					glog.Infof("Aborting LRU due to lru_enabled config change")
				}
				lruxact.abort()
			}
		}
	case ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		t.invalmsghdlr(w, r, s)
	}
}

func (t *targetrunner) httpdaeputSmap(w http.ResponseWriter, r *http.Request, apitems []string) {
	curversion := t.smap.Version
	var newsmap *Smap
	if t.readJSON(w, r, &newsmap) != nil {
		return
	}
	if curversion == newsmap.Version {
		return
	}
	if curversion > newsmap.Version {
		err := fmt.Errorf("Warning: attempt to downgrade Smap verion %d to %d", curversion, newsmap.Version)
		glog.Errorln(err)
		t.kalive.onerr(err, 0)
		return
	}
	glog.Infof("%s: new Smap version %d (old %d)", apitems[0], newsmap.Version, curversion)

	// check whether this target is present in the new Smap
	// rebalance? (nothing to rebalance if the new map is a strict subset of the old)
	// assign proxysi
	// log
	existentialQ, isSubset := false, true
	for id, si := range newsmap.Smap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			glog.Infoln("target:", si, "<= self")
		} else {
			glog.Infoln("target:", si)
		}
		if _, ok := t.smap.Smap[id]; !ok {
			isSubset = false
		}
	}
	assert(existentialQ)

	t.smap, t.proxysi = newsmap, newsmap.ProxySI
	if apitems[0] == Rsyncsmap {
		return
	}
	if isSubset {
		glog.Infoln("nothing to rebalance: new Smap is a strict subset of the old")
		return
	}
	// xaction
	go t.runRebalance()
}

func (t *targetrunner) httpdaeputLBMap(w http.ResponseWriter, r *http.Request, apitems []string) {
	curversion := t.lbmap.Version
	newlbmap := &lbmap{LBmap: make(map[string]string)}
	if t.readJSON(w, r, newlbmap) != nil {
		return
	}
	if curversion == newlbmap.Version {
		return
	}
	if curversion > newlbmap.Version {
		glog.Errorf("Warning: attempt to downgrade lbmap verion %d to %d", curversion, newlbmap.Version)
		return
	}
	glog.Infof("%s: new lbmap version %d (old %d)", apitems[0], newlbmap.Version, curversion)
	// destroylb
	for bucket := range t.lbmap.LBmap {
		_, ok := newlbmap.LBmap[bucket]
		if !ok {
			glog.Infof("Destroy local bucket %s", bucket)
			for mpath := range ctx.mountpaths {
				localbucketfqn := mpath + "/" + ctx.config.LocalBuckets + "/" + bucket
				if err := os.RemoveAll(localbucketfqn); err != nil {
					glog.Errorf("Failed to destroy local bucket dir %q, err: %v", localbucketfqn, err)
				}
			}
		}
	}
	t.lbmap = newlbmap
	for mpath := range ctx.mountpaths {
		for bucket := range t.lbmap.LBmap {
			localbucketfqn := mpath + "/" + ctx.config.LocalBuckets + "/" + bucket
			if err := CreateDir(localbucketfqn); err != nil {
				glog.Errorf("Failed to create local bucket dir %q, err: %v", localbucketfqn, err)
			}
		}
	}
}

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	var msg GetMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	var (
		jsbytes []byte
		err     error
	)
	switch msg.GetWhat {
	case GetWhatConfig:
		jsbytes, err = json.Marshal(ctx.config)
		assert(err == nil, err)
	case GetWhatSmap:
		jsbytes, err = json.Marshal(t.si)
		assert(err == nil, err)
	case GetWhatStats:
		rr := getstorstatsrunner()
		rr.Lock()
		jsbytes, err = json.Marshal(rr)
		rr.Unlock()
		assert(err == nil, err)
	default:
		s := fmt.Sprintf("Unexpected GetMsg <- JSON [%v]", msg)
		t.invalmsghdlr(w, r, s)
	}
	t.writeJSON(w, r, jsbytes, "httpdaeget")
}

// management interface to register (unregistered) self
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	if status, err := t.register(false); err != nil {
		s := fmt.Sprintf("Target %s failed to register with proxy, status %d, err: %v", t.si.DaemonID, status, err)
		t.invalmsghdlr(w, r, s)
		return
	}
	if glog.V(3) {
		glog.Infof("Registered self %s", t.si.DaemonID)
	}
}

//==============================================================================
//
// target's utilities and helpers
//
//==============================================================================
func (t *targetrunner) islocalBucket(bucket string) bool {
	_, ok := t.lbmap.LBmap[bucket]
	return ok
}

func (t *targetrunner) testingFSPpaths() bool {
	return ctx.config.TestFSP.Count > 0
}

func (t *targetrunner) fqn2bckobj(fqn string) (bucket, objname string, ok bool) {
	fn := func(path string) bool {
		if strings.HasPrefix(fqn, path) {
			rempath := fqn[len(path):]
			items := strings.SplitN(rempath, "/", 2)
			bucket, objname = items[0], items[1]
			return true
		}
		return false
	}
	for mpath := range ctx.mountpaths {
		if fn(mpath + "/" + ctx.config.CloudBuckets + "/") {
			ok = len(objname) > 0
			return
		}
		if fn(mpath + "/" + ctx.config.LocalBuckets + "/") {
			assert(t.islocalBucket(bucket))
			ok = len(objname) > 0
			return
		}
	}
	return
}

func (t *targetrunner) fspath2mpath() {
	for fp := range ctx.config.FSpaths {
		if _, err := os.Stat(fp); err != nil {
			glog.Fatalf("FATAL: fspath %q does not exist, err: %v", fp, err)
		}
		statfs := syscall.Statfs_t{}
		if err := syscall.Statfs(fp, &statfs); err != nil {
			glog.Fatalf("FATAL: cannot statfs fspath %q, err: %v", fp, err)
		}

		mp := &mountPath{Path: fp, Fsid: statfs.Fsid}
		_, ok := ctx.mountpaths[mp.Path]
		assert(!ok)
		ctx.mountpaths[mp.Path] = mp
	}
}

// create local directories to test multiple fspaths
func (t *targetrunner) testCachepathMounts() {
	var instpath string
	if ctx.config.TestFSP.Instance > 0 {
		instpath = ctx.config.TestFSP.Root + strconv.Itoa(ctx.config.TestFSP.Instance) + "/"
	} else {
		// e.g. when docker
		instpath = ctx.config.TestFSP.Root
	}
	for i := 0; i < ctx.config.TestFSP.Count; i++ {
		var mpath string
		if ctx.config.TestFSP.Count > 1 {
			mpath = instpath + strconv.Itoa(i+1)
		} else {
			mpath = instpath[0 : len(instpath)-1]
		}
		if err := CreateDir(mpath); err != nil {
			glog.Fatalf("FATAL: cannot create test cache dir %q, err: %v", mpath, err)
			return
		}
		statfs := syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, &statfs); err != nil {
			glog.Fatalf("FATAL: cannot statfs mpath %q, err: %v", mpath, err)
			return
		}
		mp := &mountPath{Path: mpath, Fsid: statfs.Fsid}
		_, ok := ctx.mountpaths[mp.Path]
		assert(!ok)
		ctx.mountpaths[mp.Path] = mp
	}
}

func (t *targetrunner) mpath2Fsid() (fsmap map[syscall.Fsid]string) {
	fsmap = make(map[syscall.Fsid]string, len(ctx.mountpaths))
	for _, mountpath := range ctx.mountpaths {
		mp2, ok := fsmap[mountpath.Fsid]
		if ok {
			if !t.testingFSPpaths() {
				glog.Fatalf("FATAL: duplicate FSID %v: mpath1 %q, mpath2 %q", mountpath.Fsid, mountpath.Path, mp2)
			}
			continue
		}
		fsmap[mountpath.Fsid] = mountpath.Path
	}
	return
}

//=====
// on err: closes and removes the file; othwerise closes and returns the size;
// empty omd5 or oxxhash: not considered an exception even when the configuration says otherwise;
// xxhash is always preferred over md5
//=====
func (t *targetrunner) receive(fqn string, inmem bool, objname, omd5 string, ohobj cksumvalue,
	reader io.Reader) (sgl *SGLIO, nhobj cksumvalue, written int64, errstr string) {
	var (
		err                  error
		file                 *os.File
		filewriter           io.Writer
		ohtype, ohval, nhval string
		cksumcfg             = &ctx.config.CksumConfig
	)
	// ack policy = memory
	if inmem {
		sgl = NewSGLIO(0)
		filewriter = sgl
	} else {
		if file, err = CreateFile(fqn); err != nil {
			errstr = fmt.Sprintf("Failed to create %s, err: %s", fqn, err)
			return
		}
		filewriter = file
	}
	slab := selectslab(0)
	buf := slab.alloc()
	defer func() { // free & cleanup on err
		slab.free(buf)
		if errstr == "" {
			return
		}
		if err = file.Close(); err != nil {
			glog.Errorf("Nested: failed to close received file %s, err: %v", fqn, err)
		}
		if err = os.Remove(fqn); err != nil {
			glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, fqn, err)
		}
	}()
	// receive and checksum
	if cksumcfg.Checksum != ChecksumNone {
		assert(cksumcfg.Checksum == ChecksumXXHash)
		xx := xxhash.New64()
		if written, errstr = ReceiveAndChecksum(filewriter, reader, buf, xx); errstr != "" {
			return
		}
		hashIn64 := xx.Sum64()
		hashInBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(hashInBytes, hashIn64)
		nhval = hex.EncodeToString(hashInBytes)
		nhobj = newcksumvalue(ChecksumXXHash, nhval)
		if ohobj != nil {
			ohtype, ohval = ohobj.get()
			assert(ohtype == ChecksumXXHash)
			if ohval != nhval {
				errstr = fmt.Sprintf("Bad checksum: %s %s %s... != %s... computed for the %q",
					objname, cksumcfg.Checksum, ohval[:8], nhval[:8], fqn)
				t.statsif.add("numbadchecksum", 1)
				t.statsif.add("bytesbadchecksum", written)
				return
			}
		}
	} else if omd5 != "" && cksumcfg.ValidateColdGet {
		md5 := md5.New()
		if written, errstr = ReceiveAndChecksum(filewriter, reader, buf, md5); errstr != "" {
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash := hex.EncodeToString(hashInBytes)
		if omd5 != md5hash {
			errstr = fmt.Sprintf("Bad checksum: cold GET %s md5 %s... != %s... computed for the %q",
				objname, ohval[:8], nhval[:8], fqn)
			t.statsif.add("numbadchecksum", 1)
			t.statsif.add("bytesbadchecksum", written)
			return
		}
	} else {
		if written, errstr = ReceiveAndChecksum(filewriter, reader, buf); errstr != "" {
			return
		}
	}
	// close and done
	if inmem {
		return
	}
	if err = file.Close(); err != nil {
		errstr = fmt.Sprintf("Failed to close received file %s, err: %v", fqn, err)
	}
	return
}

func finalizeobj(fqn string, nhobj cksumvalue) (errstr string) {
	if nhobj == nil {
		return
	}
	htype, hval := nhobj.get()
	assert(htype == ChecksumXXHash)
	errstr = Setxattr(fqn, xattrXXHashVal, []byte(hval))
	return
}

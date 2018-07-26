/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/OneOfOne/xxhash"
)

const (
	DefaultPageSize  = 1000      // the number of cached file infos returned in one page
	internalPageSize = 10000     // number of objects in a page for internal call between target and proxy to get atime/iscached
	MaxPageSize      = 64 * 1024 // max number of objects in a page (warning logged if requested page size exceeds this limit)
	workfileprefix   = ".~~~."
	doesnotexist     = "does not exist"
	maxBytesInMem    = 256 * KiB
)

type allfinfos struct {
	files        []*BucketEntry
	fileCount    int
	rootLength   int
	prefix       string
	marker       string
	markerDir    string
	needAtime    bool
	needCtime    bool
	needChkSum   bool
	needVersion  bool
	msg          *GetMsg
	lastFilePath string
	t            *targetrunner
	bucket       string
	limit        int
}

type uxprocess struct {
	starttime time.Time
	spid      string
	pid       int64
}

type thealthstatus struct {
	IsRebalancing bool `json:"is_rebalancing"`
	// NOTE: include core stats and other info as needed
}

type renamectx struct {
	bucketFrom string
	bucketTo   string
	t          *targetrunner
}

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif       cloudif // multi-cloud vendor support
	xactinp       *xactInProgress
	uxprocess     *uxprocess
	rtnamemap     *rtnamemap
	prefetchQueue chan filesWithDeadline
	statsdC       statsd.Client
	authn         *authManager
}

// start target runner
func (t *targetrunner) run() error {
	var ereg error
	t.httprunner.init(getstorstatsrunner(), false)
	t.httprunner.keepalive = gettargetkeepalive()
	t.xactinp = newxactinp()        // extended actions
	t.rtnamemap = newrtnamemap(128) // lock/unlock name

	bucketmd := newBucketMD()
	t.bmdowner.put(bucketmd)

	smap := newSmap()
	smap.Tmap[t.si.DaemonID] = t.si
	t.smapowner.put(smap)
	for i := 0; i < maxRetrySeconds; i++ {
		var status int
		if status, ereg = t.register(0); ereg != nil {
			if IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("Target %s: retrying registration...", t.si.DaemonID)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}
	if ereg != nil {
		glog.Errorf("Target %s failed to register, err: %v", t.si.DaemonID, ereg)
		glog.Errorf("Target %s is terminating", t.si.DaemonID)
		return ereg
	}

	t.createBucketDirs("local", ctx.config.LocalBuckets, makePathLocal)
	t.createBucketDirs("cloud", ctx.config.CloudBuckets, makePathCloud)
	t.detectMpathChanges()

	// cloud provider
	if ctx.config.CloudProvider == ProviderAmazon {
		// TODO: sessions
		t.cloudif = &awsimpl{t}

	} else {
		assert(ctx.config.CloudProvider == ProviderGoogle)
		t.cloudif = &gcpimpl{t}
	}

	// prefetch
	t.prefetchQueue = make(chan filesWithDeadline, prefetchChanSize)

	t.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
	}

	//
	// REST API: register storage target's handler(s) and start listening
	//
	t.httprunner.registerhdlr(URLPath(Rversion, Rbuckets)+"/", t.bucketHandler)
	t.httprunner.registerhdlr(URLPath(Rversion, Robjects)+"/", t.objectHandler)
	t.httprunner.registerhdlr(URLPath(Rversion, Rdaemon), t.daemonHandler)
	t.httprunner.registerhdlr(URLPath(Rversion, Rpush)+"/", t.pushHandler)
	t.httprunner.registerhdlr(URLPath(Rversion, Rhealth), t.httpHealth)
	t.httprunner.registerhdlr(URLPath(Rversion, Rvote)+"/", t.voteHandler)
	t.httprunner.registerhdlr(URLPath(Rversion, Rtokens), t.tokenHandler)
	t.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Target %s is ready", t.si.DaemonID)
	glog.Flush()
	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	var err error
	t.statsdC, err = statsd.New("localhost", 8125,
		fmt.Sprintf("dfctarget.%s", strings.Replace(t.si.DaemonID, ":", "_", -1)))
	if err != nil {
		glog.Info("Failed to connect to statd, running without statsd")
	}

	return t.httprunner.run()
}

// stop gracefully
func (t *targetrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.name, err)
	sleep := t.xactinp.abortAll()
	t.rtnamemap.stop()
	if t.httprunner.h != nil {
		t.unregister() // ignore errors
	}

	t.httprunner.stop(err)
	if sleep {
		time.Sleep(time.Second)
	}
}

// target registration with proxy
func (t *targetrunner) register(timeout time.Duration) (int, error) {
	var (
		newbucketmd bucketMD
		res         callResult
	)
	if timeout == 0 {
		res = t.join(false, "")
	} else { // keepalive
		url, psi := t.getPrimaryURLAndSI()
		res = t.registerToURL(url, psi, timeout, false, "")
	}
	if res.err != nil {
		return res.status, res.err
	}
	// not being sent at cluster startup and keepalive..
	if len(res.outjson) > 0 {
		err := json.Unmarshal(res.outjson, &newbucketmd)
		assert(err == nil, err)
		t.bmdowner.Lock()
		v := t.bmdowner.get().version()
		if v > newbucketmd.version() {
			glog.Errorf("register target - got bucket-metadata: local version %d > %d", v, newbucketmd.version())
		} else {
			glog.Infof("register target - got bucket-metadata: upgrading local version %d to %d", v, newbucketmd.version())
		}
		t.bmdowner.put(&newbucketmd)
		t.bmdowner.Unlock()
	}
	return 0, nil
}

func (t *targetrunner) unregister() (int, error) {
	url, si := t.getPrimaryURLAndSI()
	url += URLPath(Rversion, Rcluster, Rdaemon, t.si.DaemonID)
	res := t.call(nil, si, url, http.MethodDelete, nil)
	return res.status, res.err
}

//===========================================================================================
//
// http handlers: data and metadata
//
//===========================================================================================

// verb /Rversion/Rbuckets
func (t *targetrunner) bucketHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpbckget(w, r)
	case http.MethodDelete:
		t.httpbckdelete(w, r)
	case http.MethodPost:
		t.httpbckpost(w, r)
	case http.MethodHead:
		t.httpbckhead(w, r)
	default:
		invalhdlr(w, r)
	}
}

// verb /Rversion/Robjects
func (t *targetrunner) objectHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpobjget(w, r)
	case http.MethodPut:
		t.httpobjput(w, r)
	case http.MethodDelete:
		t.httpobjdelete(w, r)
	case http.MethodPost:
		t.httpobjpost(w, r)
	case http.MethodHead:
		t.httpobjhead(w, r)
	default:
		invalhdlr(w, r)
	}
}

// GET /Rversion/Rbuckets/bucket-name
func (t *targetrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	// all cloud bucket names
	if bucket == "*" {
		t.getbucketnames(w, r)
		return
	}
	s := fmt.Sprintf("Invalid route /buckets/%s", bucket)
	t.invalmsghdlr(w, r, s)
}

// GET /Rversion/Robjects/bucket[+"/"+objname]
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *targetrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	var (
		nhobj                         cksumvalue
		bucket, objname, fqn          string
		uname, errstr, version        string
		size                          int64
		props                         *objectProps
		started                       time.Time
		errcode                       int
		coldget, vchanged, inNextTier bool
	)
	started = time.Now()
	cksumcfg := &ctx.config.Cksum
	versioncfg := &ctx.config.Ver
	ct := t.contextWithAuth(r)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	offset, length, readRange, errstr := t.validateOffsetAndLength(r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	bucketmd := t.bmdowner.get()
	islocal := bucketmd.islocal(bucket)
	errstr, errcode = t.checkLocalQueryParameter(bucket, r, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	// lockname(ro)
	fqn, uname = t.fqn(bucket, objname, islocal), uniquename(bucket, objname)
	t.rtnamemap.lockname(uname, false, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)

	// existence, access & versioning
	if coldget, size, version, errstr = t.lookupLocally(bucket, objname, fqn); islocal && errstr != "" {
		errcode = http.StatusInternalServerError
		// given certain conditions (below) make an effort to locate the object cluster-wide
		if strings.Contains(errstr, doesnotexist) {
			errcode = http.StatusNotFound
			aborted, running := t.xactinp.isAbortedOrRunningRebalance()
			if aborted || running {
				if props := t.getFromNeighbor(bucket, objname, r, islocal); props != nil {
					size, nhobj = props.size, props.nhobj
					goto existslocally
				}
			} else {
				_, p := bucketmd.get(bucket, islocal)
				if p.NextTierURL != "" {
					if inNextTier, errstr, errcode = t.objectInNextTier(p.NextTierURL, bucket, objname); inNextTier {
						props, errstr, errcode = t.getObjectNextTier(p.NextTierURL, bucket, objname, fqn)
						if errstr == "" {
							size, nhobj = props.size, props.nhobj
							goto existslocally
						}
						glog.Errorf("Error getting object from next tier after successful lookup, err: %s,"+
							" HTTP status code: %d", errstr, errcode)
					}
				}
			}
		}
		t.invalmsghdlr(w, r, errstr, errcode)
		t.rtnamemap.unlockname(uname, false)
		return
	}

	if !coldget && !islocal {
		if versioncfg.ValidateWarmGet && (version != "" &&
			t.versioningConfigured(bucket)) {
			if vchanged, errstr, errcode = t.checkCloudVersion(
				ct, bucket, objname, version); errstr != "" {
				t.invalmsghdlr(w, r, errstr, errcode)
				t.rtnamemap.unlockname(uname, false)
				return
			}
			// TODO: add a knob to return what's cached while upgrading the version async
			coldget = vchanged
		}
	}
	if !coldget && cksumcfg.ValidateWarmGet && cksumcfg.Checksum != ChecksumNone {
		validChecksum, errstr := t.validateObjectChecksum(fqn, cksumcfg.Checksum, size)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			t.rtnamemap.unlockname(uname, false)
			return
		}
		if !validChecksum {
			if islocal {
				if err := os.Remove(fqn); err != nil {
					glog.Warningf("Bad checksum, failed to remove %s/%s, err: %v", bucket, objname, err)
				}
				t.invalmsghdlr(w, r, fmt.Sprintf("Bad checksum %s/%s", bucket, objname), http.StatusInternalServerError)
				t.rtnamemap.unlockname(uname, false)
				return
			}
			coldget = true
		}
	}
	if coldget {
		t.rtnamemap.unlockname(uname, false)
		if props, errstr, errcode = t.coldget(ct, bucket, objname, false); errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
		size, nhobj = props.size, props.nhobj
	}

existslocally:
	// note: coldget() keeps the read lock if successful
	defer t.rtnamemap.unlockname(uname, false)

	//
	// local file => http response
	//
	if size == 0 {
		glog.Warningf("Unexpected: object %s/%s size is 0 (zero)", bucket, objname)
	}
	returnRangeChecksum := cksumcfg.Checksum != ChecksumNone && readRange && cksumcfg.EnableReadRangeChecksum
	if !coldget && !returnRangeChecksum && cksumcfg.Checksum != ChecksumNone {
		hashbinary, errstr := Getxattr(fqn, XattrXXHashVal)
		if errstr == "" && hashbinary != nil {
			nhobj = newcksumvalue(cksumcfg.Checksum, string(hashbinary))
		}
	}
	if nhobj != nil && !returnRangeChecksum {
		htype, hval := nhobj.get()
		w.Header().Add(HeaderDfcChecksumType, htype)
		w.Header().Add(HeaderDfcChecksumVal, hval)
	}
	if props != nil && props.version != "" {
		w.Header().Add(HeaderDfcObjVersion, props.version)
	}

	file, err := os.Open(fqn)
	if err != nil {
		if os.IsPermission(err) {
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", fqn)
			t.invalmsghdlr(w, r, errstr, http.StatusForbidden)
		} else {
			errstr = fmt.Sprintf("Failed to open local file %s, err: %v", fqn, err)
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
		}
		t.fshc(err, fqn)
		return
	}

	defer file.Close()
	if readRange {
		size = length
	}

	slab := selectslab(size)
	buf := slab.alloc()
	defer slab.free(buf)

	var sgl *SGLIO
	defer func() {
		if sgl != nil {
			sgl.Free()
		}
	}()
	var rangeReader io.ReadSeeker = io.NewSectionReader(file, offset, length)
	if returnRangeChecksum {
		var cksum string
		cksum, sgl, rangeReader, errstr = t.rangeCksum(file, fqn, length, offset, buf)
		if errstr != "" {
			glog.Errorln(t.errHTTP(r, errstr, http.StatusInternalServerError))
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			return

		}
		w.Header().Add(HeaderDfcChecksumType, cksumcfg.Checksum)
		w.Header().Add(HeaderDfcChecksumVal, cksum)
	}

	var written int64
	if readRange {
		written, err = io.CopyBuffer(w, rangeReader, buf)
	} else {
		// copy
		written, err = io.CopyBuffer(w, file, buf)
	}
	if err != nil {
		errstr = fmt.Sprintf("Failed to send file %s, err: %v", fqn, err)
		glog.Errorln(t.errHTTP(r, errstr, http.StatusInternalServerError))
		t.fshc(err, fqn)
		t.statsif.add("numerr", 1)
		return
	}
	if !coldget {
		getatimerunner().touch(fqn)
	}
	if glog.V(4) {
		s := fmt.Sprintf("GET: %s/%s, %.2f MB, %d µs", bucket, objname, float64(written)/MiB, time.Since(started)/1000)
		if coldget {
			s += " (cold)"
		}
		glog.Infoln(s)
	}

	delta := time.Since(started)
	t.statsdC.Send("get",
		metric{statsd.Counter, "count", 1},
		metric{statsd.Timer, "latency", float64(delta / time.Millisecond)})
	t.statsif.addMany("numget", int64(1), "getlatency", int64(delta/1000))
}

func (t *targetrunner) rangeCksum(file *os.File, fqn string, length int64, offset int64, buf []byte) (
	cksum string, sgl *SGLIO, rangeReader io.ReadSeeker, errstr string) {
	rangeReader = io.NewSectionReader(file, offset, length)
	xx := xxhash.New64()
	if length <= maxBytesInMem {
		sgl = NewSGLIO(uint64(length))
		_, err := ReceiveAndChecksum(sgl, rangeReader, buf, xx)
		if err != nil {
			errstr = fmt.Sprintf("failed to read byte range, offset:%d, length:%d from %s, err: %v", offset, length, fqn, err)
			t.fshc(err, fqn)
			return
		}
		// overriding rangeReader here to read from the sgl
		rangeReader = NewReader(sgl)
	}

	_, err := rangeReader.Seek(0, io.SeekStart)
	if err != nil {
		errstr = fmt.Sprintf("failed to seek file %s to beginning, err: %v", fqn, err)
		t.fshc(err, fqn)
		return
	}

	hashIn64 := xx.Sum64()
	hashInBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hashInBytes, hashIn64)
	cksum = hex.EncodeToString(hashInBytes)
	return
}

func (t *targetrunner) validateOffsetAndLength(r *http.Request) (
	offset int64, length int64, readRange bool, errstr string) {
	query := r.URL.Query()
	offsetStr, lengthStr := query.Get(URLParamOffset), query.Get(URLParamLength)
	if offsetStr == "" && lengthStr == "" {
		return
	}
	errstr = fmt.Sprintf("Invalid offset: [%s] and length: [%s] combination", offsetStr, lengthStr)
	// Specifying only one is invalid
	if offsetStr == "" || lengthStr == "" {
		return
	}
	offset, err := strconv.ParseInt(url.QueryEscape(offsetStr), 10, 64)
	if err != nil || offset < 0 {
		return
	}
	length, err = strconv.ParseInt(url.QueryEscape(lengthStr), 10, 64)
	if err != nil || length <= 0 {
		return
	}
	return offset, length, true, ""
}

// PUT /Rversion/Robjects/bucket-name/object-name
func (t *targetrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	from, to := query.Get(URLParamFromID), query.Get(URLParamToID)
	objname := strings.Join(apitems[1:], "/")
	if from != "" && to != "" {
		// REBALANCE "?from_id="+from_id+"&to_id="+to_id
		if objname == "" {
			s := "Invalid URL: missing object name"
			t.invalmsghdlr(w, r, s)
			return
		}
		if errstr := t.dorebalance(r, from, to, bucket, objname); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
	} else {
		// PUT
		pid := query.Get(URLParamDaemonID)
		if pid == "" {
			t.invalmsghdlr(w, r, "PUT requests are expected to be redirected")
			return
		}
		if t.smapowner.get().getProxy(pid) == nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("PUT from an unknown proxy/gateway ID '%s' - Smap out of sync?", pid))
			return
		}
		errstr, errcode := t.doput(w, r, bucket, objname)
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
		}
	}
}

// DELETE { action} /Rversion/Rbuckets/bucket-name
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket  string
		msg     ActionMsg
		started = time.Now()
		ok      = true
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	defer func() {
		if ok && err == nil && bool(glog.V(4)) {
			glog.Infof("DELETE list|range: %s, %d µs", bucket, time.Since(started)/1000)
		}
	}()
	if err == nil && len(b) > 0 {
		err = json.Unmarshal(b, &msg)
	}
	if err != nil {
		s := fmt.Sprintf("Failed to read %s body, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		t.invalmsghdlr(w, r, s)
		return
	}
	if len(b) > 0 { // must be a List/Range request
		t.deletefiles(w, r, msg) // FIXME: must return ok or err
		return
	}
	s := fmt.Sprintf("Invalid API request: no message body")
	t.invalmsghdlr(w, r, s)
	ok = false
}

// DELETE [ { action } ] /Rversion/Robjects/bucket-name/object-name
func (t *targetrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname string
		msg             ActionMsg
		evict           bool
		started         = time.Now()
		ok              = true
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	objname = strings.Join(apitems[1:], "/")

	b, err := ioutil.ReadAll(r.Body)
	defer func() {
		if ok && err == nil && bool(glog.V(4)) {
			glog.Infof("DELETE: %s/%s, %d µs", bucket, objname, time.Since(started)/1000)
		}
	}()
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
	if objname != "" {
		err := t.fildelete(t.contextWithAuth(r), bucket, objname, evict)
		if err != nil {
			s := fmt.Sprintf("Error deleting %s/%s: %v", bucket, objname, err)
			t.invalmsghdlr(w, r, s)
		}
		return
	}
	s := fmt.Sprintf("Invalid API request: No object name or message body.")
	t.invalmsghdlr(w, r, s)
	ok = false
}

// POST /Rversion/Rbuckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var msg ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActPrefetch:
		t.prefetchfiles(w, r, msg)
	case ActRenameLB:
		apitems := t.restAPIItems(r.URL.Path, 5)
		if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
			return
		}
		lbucket := apitems[0]
		if !t.validatebckname(w, r, lbucket) {
			return
		}
		bucketFrom, bucketTo := lbucket, msg.Name
		bucketmd := t.bmdowner.get()
		ok, props := bucketmd.get(bucketFrom, true)
		if !ok {
			s := fmt.Sprintf("Local bucket %s does not exist", bucketFrom)
			t.invalmsghdlr(w, r, s)
			return
		}
		bmd4ren, ok := msg.Value.(map[string]interface{})
		if !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Unexpected Value format %+v, %T", msg.Value, msg.Value))
			return
		}
		v1, ok1 := bmd4ren["version"]
		v2, ok2 := v1.(float64)
		if !ok1 || !ok2 {
			t.invalmsghdlr(w, r, fmt.Sprintf("Invalid Value format (%+v, %T), (%+v, %T)", v1, v1, v2, v2))
			return
		}
		version := int64(v2)
		if bucketmd.version() != version {
			glog.Warning("bucket-metadata version %d != %d - proceeding to rename anyway", bucketmd.version(), version)
		}
		clone := bucketmd.clone()
		if errstr := t.renamelocalbucket(bucketFrom, bucketTo, props, clone); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		glog.Infof("renamed local bucket %s => %s, bucket-metadata version %d", bucketFrom, bucketTo, clone.version())
	case ActListObjects:
		apitems := t.restAPIItems(r.URL.Path, 5)
		if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
			return
		}
		lbucket := apitems[0]
		if !t.validatebckname(w, r, lbucket) {
			return
		}
		// list the bucket and return
		tag, ok := t.listbucket(w, r, lbucket, &msg)
		if ok {
			delta := time.Since(started)
			t.statsdC.Send("list",
				metric{statsd.Counter, "count", 1},
				metric{statsd.Timer, "latency", float64(delta / time.Millisecond)})
			lat := int64(delta / 1000)
			t.statsif.addMany("numlist", int64(1), "listlatency", lat)
			if glog.V(3) {
				glog.Infof("LIST %s: %s, %d µs", tag, lbucket, lat)
			}
		}
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// POST /Rversion/Robjects/bucket-name/object-name
func (t *targetrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActRename:
		t.renamefile(w, r, msg)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// HEAD /Rversion/Rbuckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket      string
		islocal     bool
		errstr      string
		errcode     int
		bucketprops simplekvs
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	bucketmd := t.bmdowner.get()
	islocal = bucketmd.islocal(bucket)
	errstr, errcode = t.checkLocalQueryParameter(bucket, r, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	if !islocal {
		bucketprops, errstr, errcode = getcloudif().headbucket(t.contextWithAuth(r), bucket)
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
	} else {
		bucketprops = make(simplekvs)
		bucketprops[CloudProvider] = ProviderDfc
		bucketprops[Versioning] = VersionLocal
	}
	// double check if we support versioning internally for the bucket
	if !t.versioningConfigured(bucket) {
		bucketprops[Versioning] = VersionNone
	}

	for k, v := range bucketprops {
		w.Header().Add(k, v)
	}
	_, props := bucketmd.get(bucket, islocal)
	w.Header().Add(NextTierURL, props.NextTierURL)
	w.Header().Add(ReadPolicy, props.ReadPolicy)
	w.Header().Add(WritePolicy, props.WritePolicy)
}

// HEAD /Rversion/Robjects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname, errstr string
		islocal, checkCached    bool
		errcode                 int
		objmeta                 simplekvs
	)
	checkCached, _ = parsebool(r.URL.Query().Get(URLParamCheckCached))
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	islocal = t.bmdowner.get().islocal(bucket)
	errstr, errcode = t.checkLocalQueryParameter(bucket, r, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	if islocal || checkCached {
		fqn := t.fqn(bucket, objname, islocal)
		var (
			size    int64
			version string
		)
		if _, size, version, errstr = t.lookupLocally(bucket, objname, fqn); errstr != "" {
			status := http.StatusNotFound
			http.Error(w, http.StatusText(status), status)
			return
		} else if checkCached {
			return
		}
		objmeta = make(simplekvs)
		objmeta["size"] = strconv.FormatInt(size, 10)
		objmeta["version"] = version
		glog.Infoln("httpobjhead FOUND:", bucket, objname, size, version)
	} else {
		objmeta, errstr, errcode = getcloudif().headobject(t.contextWithAuth(r), bucket, objname)
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
	}
	for k, v := range objmeta {
		w.Header().Add(k, v)
	}
}

// handler for: "/"+Rversion+"/"+Rtokens
func (t *targetrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		t.httpTokenDelete(w, r)
	default:
		invalhdlr(w, r)
	}
}

// GET /Rversion/Rhealth
func (t *targetrunner) httpHealth(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	from := query.Get(URLParamFromID)

	aborted, running := t.xactinp.isAbortedOrRunningRebalance()
	status := &thealthstatus{IsRebalancing: aborted || running}

	jsbytes, err := json.Marshal(status)
	assert(err == nil, err)
	if ok := t.writeJSON(w, r, jsbytes, "thealthstatus"); !ok {
		return
	}

	t.keepalive.heardFrom(from, false)
}

//  /Rversion/Rpush/bucket-name
func (t *targetrunner) pushHandler(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rpush); apitems == nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	if pusher, ok := w.(http.Pusher); ok {
		objnamebytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			s := fmt.Sprintf("Could not read Request Body: %v", err)
			if err == io.EOF {
				trailer := r.Trailer.Get("Error")
				if trailer != "" {
					s = fmt.Sprintf("pushhdlr: Failed to read %s request, err: %v, trailer: %s",
						r.Method, err, trailer)
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
			err := pusher.Push("/"+Rversion+"/"+Robjects+"/"+bucket+"/"+objname, nil)
			if err != nil {
				t.invalmsghdlr(w, r, "Error Pushing "+bucket+"/"+objname+": "+err.Error())
				return
			}
		}
	} else {
		t.invalmsghdlr(w, r, "Pusher Unavailable")
		return
	}

	_, err := w.Write([]byte("Pushed Object List "))
	if err != nil {
		s := fmt.Sprintf("Error writing response: %v", err)
		t.invalmsghdlr(w, r, s)
	}
}

//====================================================================================
//
// supporting methods and misc
//
//====================================================================================
func (t *targetrunner) renamelocalbucket(bucketFrom, bucketTo string, p BucketProps, clone *bucketMD) (errstr string) {
	// ready to receive migrated obj-s _after_ that point
	// insert directly w/o incrementing the version (metasyncer will do at the end of the operation)
	clone.LBmap[bucketTo] = p
	t.bmdowner.put(clone)

	wg := &sync.WaitGroup{}

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	ch := make(chan string, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		fromdir := filepath.Join(makePathLocal(mpathInfo.Path), bucketFrom)
		wg.Add(1)
		go func(fromdir string, wg *sync.WaitGroup) {
			time.Sleep(time.Millisecond * 100) // FIXME: 2-phase for the targets to 1) prep (above) and 2) rebalance
			ch <- t.renameOne(fromdir, bucketFrom, bucketTo)
			wg.Done()
		}(fromdir, wg)
	}
	wg.Wait()
	close(ch)
	for errstr = range ch {
		if errstr != "" {
			return
		}
	}
	for _, mpathInfo := range availablePaths {
		fromdir := filepath.Join(makePathLocal(mpathInfo.Path), bucketFrom)
		if err := os.RemoveAll(fromdir); err != nil {
			glog.Errorf("Failed to remove dir %s", fromdir)
		}
	}
	clone.del(bucketFrom, true)
	return
}

func (t *targetrunner) renameOne(fromdir, bucketFrom, bucketTo string) (errstr string) {
	renctx := &renamectx{bucketFrom: bucketFrom, bucketTo: bucketTo, t: t}

	if err := filepath.Walk(fromdir, renctx.walkf); err != nil {
		errstr = fmt.Sprintf("Failed to rename %s, err: %v", fromdir, err)
	}
	return
}

func (renctx *renamectx) walkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("walkf invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	if iswork, _ := renctx.t.isworkfile(fqn); iswork { // FIXME: work files indicate work in progress..
		return nil
	}
	bucket, objname, errstr := renctx.t.fqn2bckobj(fqn)
	if errstr == "" {
		if bucket != renctx.bucketFrom {
			return fmt.Errorf("Unexpected: bucket %s != %s bucketFrom", bucket, renctx.bucketFrom)
		}
	}
	if errstr = renctx.t.renameobject(bucket, objname, renctx.bucketTo, objname); errstr != "" {
		return fmt.Errorf(errstr)
	}
	return nil
}

// checkCloudVersion returns if versions of an object differ in Cloud and DFC cache
// and the object should be refreshed from Cloud storage
// It should be called only in case of the object is present in DFC cache
func (t *targetrunner) checkCloudVersion(ct context.Context, bucket, objname, version string) (vchanged bool, errstr string, errcode int) {
	var objmeta simplekvs
	if objmeta, errstr, errcode = t.cloudif.headobject(ct, bucket, objname); errstr != "" {
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

func (t *targetrunner) getFromNeighbor(bucket, objname string, r *http.Request, islocal bool) (props *objectProps) {
	neighsi := t.lookupRemotely(bucket, objname)
	if neighsi == nil {
		return
	}
	if glog.V(4) {
		glog.Infof("getFromNeighbor: found %s/%s at %s", bucket, objname, neighsi.DaemonID)
	}

	geturl := fmt.Sprintf("%s%s?%s=%t", neighsi.DirectURL, r.URL.Path, URLParamLocal, islocal)
	//
	// http request
	//
	newr, err := http.NewRequest(http.MethodGet, geturl, nil)
	if err != nil {
		glog.Errorf("Unexpected failure to create %s request %s, err: %v", http.MethodGet, geturl, err)
		return
	}
	// Do
	contextwith, cancel := context.WithTimeout(context.Background(), ctx.config.Timeout.SendFile)
	defer cancel()
	newrequest := newr.WithContext(contextwith)

	response, err := t.httpclientLongTimeout.Do(newrequest)
	if err != nil {
		glog.Errorf("Failed to GET redirect URL %q, err: %v", geturl, err)
		return
	}
	var (
		nhobj   cksumvalue
		errstr  string
		size    int64
		hval    = response.Header.Get(HeaderDfcChecksumVal)
		htype   = response.Header.Get(HeaderDfcChecksumType)
		hdhobj  = newcksumvalue(htype, hval)
		version = response.Header.Get(HeaderDfcObjVersion)
		fqn     = t.fqn(bucket, objname, islocal)
		getfqn  = t.fqn2workfile(fqn)
	)
	if _, nhobj, size, errstr = t.receive(getfqn, objname, "", hdhobj, response.Body); errstr != "" {
		response.Body.Close()
		glog.Errorf(errstr)
		return
	}
	response.Body.Close()
	if nhobj != nil {
		nhtype, nhval := nhobj.get()
		assert(hdhobj == nil || htype == nhtype)
		if hval != "" && nhval != "" && hval != nhval {
			glog.Errorf("Bad checksum: %s/%s %s %s... != %s...", bucket, objname, htype, hval[:8], nhval[:8])
			return
		}
	}
	// commit
	if err = os.Rename(getfqn, fqn); err != nil {
		glog.Errorf("Failed to rename %s => %s, err: %v", getfqn, fqn, err)
		return
	}
	props = &objectProps{version: version, size: size, nhobj: nhobj}
	if errstr = t.finalizeobj(fqn, props); errstr != "" {
		glog.Errorf("finalizeobj %s/%s: %s (%+v)", bucket, objname, errstr, props)
		props = nil
		return
	}
	if glog.V(4) {
		glog.Infof("getFromNeighbor: got %s/%s from %s, size %d, cksum %+v", bucket, objname, neighsi.DaemonID, size, nhobj)
	}
	return
}

func (t *targetrunner) coldget(ct context.Context, bucket, objname string, prefetch bool) (props *objectProps, errstr string, errcode int) {
	var (
		bucketmd    = t.bmdowner.get()
		islocal     = bucketmd.islocal(bucket)
		fqn         = t.fqn(bucket, objname, islocal)
		uname       = uniquename(bucket, objname)
		getfqn      = t.fqn2workfile(fqn)
		versioncfg  = &ctx.config.Ver
		cksumcfg    = &ctx.config.Cksum
		errv        string
		nextTierURL string
		vchanged    bool
		inNextTier  bool
		bucketProps BucketProps
		err         error
	)
	// one cold GET at a time
	if prefetch {
		if !t.rtnamemap.trylockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}) {
			glog.Infof("PREFETCH: cold GET race: %s/%s - skipping", bucket, objname)
			return nil, "skip", 0
		}
	} else {
		t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	}
	// existence, access & versioning
	coldget, size, version, eexists := t.lookupLocally(bucket, objname, fqn)
	if !coldget && eexists == "" && !islocal {
		if versioncfg.ValidateWarmGet && version != "" && t.versioningConfigured(bucket) {
			vchanged, errv, _ = t.checkCloudVersion(ct, bucket, objname, version)
			if errv == "" {
				coldget = vchanged
			}
		}

		if !coldget && cksumcfg.ValidateWarmGet && cksumcfg.Checksum != ChecksumNone {
			validChecksum, errstr := t.validateObjectChecksum(fqn, cksumcfg.Checksum, size)
			if errstr == "" {
				coldget = !validChecksum
			} else {
				glog.Warningf("Failed while validating checksum. Error: [%s]", errstr)
			}
		}
	}
	if !coldget && eexists == "" {
		props = &objectProps{version: version, size: size}
		xxhashval, _ := Getxattr(fqn, XattrXXHashVal)
		if xxhashval != nil {
			cksumcfg := &ctx.config.Cksum
			props.nhobj = newcksumvalue(cksumcfg.Checksum, string(xxhashval))
		}
		glog.Infof("cold GET race: %s/%s, size=%d, version=%s - nothing to do", bucket, objname, size, version)
		goto ret
	}
	// cold
	_, bucketProps = bucketmd.get(bucket, islocal)
	nextTierURL = bucketProps.NextTierURL
	if nextTierURL != "" && bucketProps.ReadPolicy == RWPolicyNextTier {
		if inNextTier, errstr, errcode = t.objectInNextTier(nextTierURL, bucket, objname); errstr != "" {
			t.rtnamemap.unlockname(uname, true)
			return
		}
	}
	if inNextTier {
		if props, errstr, errcode = t.getObjectNextTier(nextTierURL, bucket, objname, getfqn); errstr != "" {
			glog.Errorf("Error getting object from next tier after successful lookup, err: %s, HTTP "+
				"status code: %d", errstr, errcode)
		}
	}
	if !inNextTier || (inNextTier && errstr != "") {
		if props, errstr, errcode = getcloudif().getobj(ct, getfqn, bucket, objname); errstr != "" {
			t.rtnamemap.unlockname(uname, true)
			return
		}
	}
	defer func() {
		if errstr != "" {
			t.rtnamemap.unlockname(uname, true)
			if errRemove := os.Remove(getfqn); errRemove != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, getfqn, errRemove)
				t.fshc(errRemove, getfqn)
			}
		}
	}()
	if err = os.Rename(getfqn, fqn); err != nil {
		errstr = fmt.Sprintf("Unexpected failure to rename %s => %s, err: %v", getfqn, fqn, err)
		t.fshc(err, fqn)
		return
	}
	if errstr = t.finalizeobj(fqn, props); errstr != "" {
		return
	}
ret:
	//
	// NOTE: GET - downgrade and keep the lock, PREFETCH - unlock
	//
	if prefetch {
		t.rtnamemap.unlockname(uname, true)
	} else {
		if vchanged {
			t.statsdC.Send("get.cold",
				metric{statsd.Counter, "count", 1},
				metric{statsd.Counter, "bytesloaded", props.size},
				metric{statsd.Counter, "vchanged", 1},
				metric{statsd.Counter, "bytesvchanged", props.size})
			t.statsif.addMany("numcoldget", int64(1), "bytesloaded", props.size, "bytesvchanged", props.size, "numvchanged", int64(1))
		} else {
			t.statsdC.Send("get.cold",
				metric{statsd.Counter, "count", 1},
				metric{statsd.Counter, "bytesloaded", props.size})
			t.statsif.addMany("numcoldget", int64(1), "bytesloaded", props.size)
		}
		t.rtnamemap.downgradelock(uname)
	}
	return
}

func (t *targetrunner) lookupLocally(bucket, objname, fqn string) (coldget bool, size int64, version, errstr string) {
	finfo, err := os.Stat(fqn)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			errstr = fmt.Sprintf("GET local: %s (%s/%s) %s", fqn, bucket, objname, doesnotexist)
			coldget = true
			return
		case os.IsPermission(err):
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", fqn)
		default:
			errstr = fmt.Sprintf("Failed to fstat %s, err: %v", fqn, err)
			t.fshc(err, fqn)
		}
		return
	}
	size = finfo.Size()
	if bytes, errs := Getxattr(fqn, XattrObjVersion); errs == "" {
		version = string(bytes)
	}
	return
}

func (t *targetrunner) lookupRemotely(bucket, objname string) *daemonInfo {
	res := t.broadcastNeighbors(
		URLPath(Rversion, Robjects, bucket, objname),
		nil, // query
		http.MethodHead,
		nil,
		t.smapowner.get(),
		ctx.config.Timeout.MaxKeepalive,
	)

	for r := range res {
		if r.err == nil {
			return r.si
		}
	}

	return nil
}

// should not be called for local buckets
func (t *targetrunner) listCachedObjects(bucket string, msg *GetMsg) (outbytes []byte, errstr string, errcode int) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		return nil, err.Error(), 0
	}

	outbytes, err = json.Marshal(reslist)
	if err != nil {
		return nil, err.Error(), 0
	}
	return
}

func (t *targetrunner) prepareLocalObjectList(bucket string, msg *GetMsg) (*BucketList, error) {
	type mresp struct {
		infos      *allfinfos
		failedPath string
		err        error
	}

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	ch := make(chan *mresp, len(availablePaths))
	wg := &sync.WaitGroup{}

	// function to traverse one mountpoint
	walkMpath := func(dir string) {
		r := &mresp{t.newFileWalk(bucket, msg), "", nil}
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
		if err := filepath.Walk(dir, r.infos.listwalkf); err != nil {
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
	islocal := t.bmdowner.get().islocal(bucket)
	for _, mpathInfo := range availablePaths {
		wg.Add(1)
		var localDir string
		if islocal {
			localDir = filepath.Join(makePathLocal(mpathInfo.Path), bucket)
		} else {
			localDir = filepath.Join(makePathCloud(mpathInfo.Path), bucket)
		}

		go walkMpath(localDir)
	}
	wg.Wait()
	close(ch)

	// combine results into one long list
	// real size of page is set in newFileWalk, so read it from any of results inside loop
	pageSize := DefaultPageSize
	allfinfos := make([]*BucketEntry, 0)
	fileCount := 0
	for r := range ch {
		if r.err != nil {
			t.fshc(r.err, r.failedPath)
			return nil, fmt.Errorf("Failed to read %s", r.failedPath)
		}

		pageSize = r.infos.limit
		allfinfos = append(allfinfos, r.infos.files...)
		fileCount += r.infos.fileCount
	}

	// sort the result and return only first `pageSize` entries
	marker := ""
	if fileCount > pageSize {
		ifLess := func(i, j int) bool {
			return allfinfos[i].Name < allfinfos[j].Name
		}
		sort.Slice(allfinfos, ifLess)
		// set extra infos to nil to avoid memory leaks
		// see NOTE on https://github.com/golang/go/wiki/SliceTricks
		for i := pageSize; i < fileCount; i++ {
			allfinfos[i] = nil
		}
		allfinfos = allfinfos[:pageSize]
		marker = allfinfos[pageSize-1].Name
	}

	bucketList := &BucketList{
		Entries:    allfinfos,
		PageMarker: marker,
	}

	if strings.Contains(msg.GetProps, GetTargetURL) {
		for _, e := range bucketList.Entries {
			e.TargetURL = t.si.DirectURL
		}
	}

	return bucketList, nil
}

func (t *targetrunner) getbucketnames(w http.ResponseWriter, r *http.Request) {
	bucketnames := &BucketNames{Cloud: make([]string, 0), Local: make([]string, 0, 64)}
	bucketmd := t.bmdowner.get()
	for bucket := range bucketmd.LBmap {
		bucketnames.Local = append(bucketnames.Local, bucket)
	}

	q := r.URL.Query()
	localonly, _ := parsebool(q.Get(URLParamLocal))
	if !localonly {
		buckets, errstr, errcode := getcloudif().getbucketnames(t.contextWithAuth(r))
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
		bucketnames.Cloud = buckets
	}

	jsbytes, err := json.Marshal(bucketnames)
	assert(err == nil, err)
	t.writeJSON(w, r, jsbytes, "getbucketnames")
}

func (t *targetrunner) doLocalBucketList(w http.ResponseWriter, r *http.Request, bucket string, msg *GetMsg) (errstr string, ok bool) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		errstr = fmt.Sprintf("List local bucket %s failed, err: %v", bucket, err)
		return
	}
	jsbytes, err := json.Marshal(reslist)
	assert(err == nil, err)
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

// List bucket returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string, actionMsg *ActionMsg) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	islocal := t.bmdowner.get().islocal(bucket)
	errstr, errcode = t.checkLocalQueryParameter(bucket, r, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	useCache, errstr, errcode := t.checkCacheQueryParameter(r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	getMsgJson, err := json.Marshal(actionMsg.Value)
	if err != nil {
		errstr := fmt.Sprintf("Unable to marshal 'value' in request: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}

	var msg GetMsg
	err = json.Unmarshal(getMsgJson, &msg)
	if err != nil {
		errstr := fmt.Sprintf("Unable to unmarshal 'value' in request to a GetMsg: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if islocal {
		tag = "local"
		if errstr, ok = t.doLocalBucketList(w, r, bucket, &msg); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
		return // ======================================>
	}
	// cloud bucket
	if useCache {
		tag = "cloud cached"
		jsbytes, errstr, errcode = t.listCachedObjects(bucket, &msg)
	} else {
		tag = "cloud"
		jsbytes, errstr, errcode = getcloudif().listbucket(t.contextWithAuth(r), bucket, &msg)
	}
	if errstr != "" {
		if errcode == 0 {
			t.invalmsghdlr(w, r, errstr)
		} else {
			t.invalmsghdlr(w, r, errstr, errcode)
		}
		return
	}
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

func (t *targetrunner) newFileWalk(bucket string, msg *GetMsg) *allfinfos {
	// Marker is always a file name, so we need to strip filename from path
	markerDir := ""
	if msg.GetPageMarker != "" {
		markerDir = filepath.Dir(msg.GetPageMarker)
	}

	// A small optimization: set boolean variables need* to avoid
	// doing string search(strings.Contains) for every entry.
	ci := &allfinfos{make([]*BucketEntry, 0, DefaultPageSize),
		0,                 // fileCount
		0,                 // rootLength
		msg.GetPrefix,     // prefix
		msg.GetPageMarker, // marker
		markerDir,         // markerDir
		strings.Contains(msg.GetProps, GetPropsAtime),    // needAtime
		strings.Contains(msg.GetProps, GetPropsCtime),    // needCtime
		strings.Contains(msg.GetProps, GetPropsChecksum), // needChkSum
		strings.Contains(msg.GetProps, GetPropsVersion),  // needVersion
		msg,             // GetMsg
		"",              // lastFilePath - next page marker
		t,               // targetrunner
		bucket,          // bucket
		DefaultPageSize, // limit - maximun number of objects to return
	}

	if msg.GetPageSize != 0 {
		ci.limit = msg.GetPageSize
	}

	return ci
}

// Checks if the directory should be processed by cache list call
// Does checks:
//  - Object name must start with prefix (if it is set)
//  - Object name is not in early processed directories by the previos call:
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

	if ci.markerDir != "" && relname < ci.markerDir {
		return filepath.SkipDir
	}

	return nil
}

// Adds an info about cached object to the list if:
//  - its name starts with prefix (if prefix is set)
//  - it has not been already returned by previous page request
//  - this target responses getobj request for the object
func (ci *allfinfos) processRegularFile(fqn string, osfi os.FileInfo) error {
	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}

	if ci.marker != "" && relname <= ci.marker {
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
	if ci.needCtime {
		t := osfi.ModTime()
		switch ci.msg.GetTimeFormat {
		case "":
			fallthrough
		case RFC822:
			fileInfo.Ctime = t.Format(time.RFC822)
		default:
			fileInfo.Ctime = t.Format(ci.msg.GetTimeFormat)
		}
	}
	if ci.needChkSum {
		xxhex, errstr := Getxattr(fqn, XattrXXHashVal)
		if errstr == "" {
			fileInfo.Checksum = hex.EncodeToString(xxhex)
		}
	}
	if ci.needVersion {
		version, errstr := Getxattr(fqn, XattrObjVersion)
		if errstr == "" {
			fileInfo.Version = string(version)
		}
	}
	fileInfo.Size = osfi.Size()
	ci.files = append(ci.files, fileInfo)
	ci.lastFilePath = fqn
	return nil
}

func (ci *allfinfos) listwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		glog.Errorf("listwalkf callback invoked with err: %v", err)
		return err
	}
	if ci.fileCount >= ci.limit {
		return filepath.SkipDir
	}
	if osfi.IsDir() {
		return ci.processDir(fqn)
	}
	if iswork, _ := ci.t.isworkfile(fqn); iswork {
		return nil
	}
	_, _, errstr := ci.t.fqn2bckobj(fqn)
	if errstr != "" {
		glog.Errorln(errstr)
		return nil
	}

	return ci.processRegularFile(fqn, osfi)
}

// After putting a new version it updates xattr attributes for the object
// Local bucket:
//  - if bucket versioning is enable("all" or "local") then the version is autoincremented
// Cloud bucket:
//  - if the Cloud returns a new version id then save it to xattr
// In both case a new checksum is saved to xattrs
func (t *targetrunner) doput(w http.ResponseWriter, r *http.Request, bucket, objname string) (errstr string, errcode int) {
	var (
		file                       *os.File
		err                        error
		hdhobj, nhobj              cksumvalue
		xxhashval                  string
		htype, hval, nhtype, nhval string
		sgl                        *SGLIO
		started                    time.Time
	)
	started = time.Now()
	cksumcfg := &ctx.config.Cksum
	islocal := t.bmdowner.get().islocal(bucket)
	fqn := t.fqn(bucket, objname, islocal)
	putfqn := t.fqn2workfile(fqn)
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
				xxhashval, errstr = ComputeXXHash(file, buf)
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
				glog.Infof("Existing %s/%s is valid: PUT is a no-op", bucket, objname)
				return
			}
		}
	}
	if sgl, nhobj, _, errstr = t.receive(putfqn, objname, "", hdhobj, r.Body); errstr != "" {
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
	props := &objectProps{nhobj: nhobj}
	if sgl == nil {
		errstr, errcode = t.putCommit(t.contextWithAuth(r), bucket, objname, putfqn, fqn, props, false /*rebalance*/)
		if errstr == "" {
			delta := time.Since(started)
			t.statsdC.Send("put",
				metric{statsd.Counter, "count", 1},
				metric{statsd.Timer, "latency", float64(delta / time.Millisecond)})
			lat := int64(delta / 1000)
			t.statsif.addMany("numput", int64(1), "putlatency", lat)
			if glog.V(4) {
				glog.Infof("PUT: %s/%s, %d µs", bucket, objname, lat)
			}
		}
		return
	}
	// FIXME: use xaction
	go t.sglToCloudAsync(t.contextWithAuth(r), sgl, bucket, objname, putfqn, fqn, props)
	return
}

func (t *targetrunner) sglToCloudAsync(ct context.Context, sgl *SGLIO, bucket, objname, putfqn, fqn string, objprops *objectProps) {
	slab := selectslab(sgl.Size())
	buf := slab.alloc()
	defer func() {
		sgl.Free()
		slab.free(buf)
	}()
	// sgl => fqn sequence
	file, err := CreateFile(putfqn)
	if err != nil {
		t.fshc(err, putfqn)
		glog.Errorln("sglToCloudAsync: create", putfqn, err)
		return
	}
	reader := NewReader(sgl)
	written, err := io.CopyBuffer(file, reader, buf)
	if err != nil {
		t.fshc(err, putfqn)
		glog.Errorln("sglToCloudAsync: CopyBuffer", err)
		if err1 := file.Close(); err1 != nil {
			glog.Errorf("Nested error %v => (remove %s => err: %v)", err, putfqn, err1)
		}
		if err2 := os.Remove(putfqn); err2 != nil {
			glog.Errorf("Nested error %v => (remove %s => err: %v)", err, putfqn, err2)
		}
		return
	}
	assert(written == sgl.Size())
	err = file.Close()
	if err != nil {
		glog.Errorln("sglToCloudAsync: Close", err)
		if err1 := os.Remove(putfqn); err1 != nil {
			glog.Errorf("Nested error %v => (remove %s => err: %v)", err, putfqn, err1)
		}
		return
	}
	errstr, _ := t.putCommit(ct, bucket, objname, putfqn, fqn, objprops, false /*rebalance*/)
	if errstr != "" {
		glog.Errorln("sglToCloudAsync: commit", errstr)
		return
	}
	glog.Infof("sglToCloudAsync: %s/%s", bucket, objname)
}

func (t *targetrunner) putCommit(ct context.Context, bucket, objname, putfqn, fqn string,
	objprops *objectProps, rebalance bool) (errstr string, errcode int) {
	var (
		err     error
		renamed bool
	)
	errstr, errcode, err, renamed = t.doPutCommit(ct, bucket, objname, putfqn, fqn, objprops, rebalance)
	if errstr != "" && !os.IsNotExist(err) && !renamed {
		t.fshc(err, putfqn)
		if err = os.Remove(putfqn); err != nil {
			glog.Errorf("Nested error: %s => (remove %s => err: %v)", errstr, putfqn, err)
		}
	}
	return
}

func (t *targetrunner) doPutCommit(ct context.Context, bucket, objname, putfqn, fqn string,
	objprops *objectProps, rebalance bool) (errstr string, errcode int, err error, renamed bool) {
	var (
		file     *os.File
		bucketmd = t.bmdowner.get()
		islocal  = bucketmd.islocal(bucket)
	)

	if !islocal && !rebalance {
		if file, err = os.Open(putfqn); err != nil {
			errstr = fmt.Sprintf("Failed to reopen %s err: %v", putfqn, err)
			return
		}
		_, p := bucketmd.get(bucket, islocal)
		if p.NextTierURL != "" && p.WritePolicy == RWPolicyNextTier {
			if errstr, errcode = t.putObjectNextTier(p.NextTierURL, bucket, objname, file); errstr != "" {
				glog.Errorf("Error putting bucket/object: %s/%s to next tier, err: %s, HTTP status code: %d",
					bucket, objname, errstr, errcode)
				file, err = os.Open(putfqn)
				if err != nil {
					errstr = fmt.Sprintf("Failed to reopen %s err: %v", putfqn, err)
				} else {
					objprops.version, errstr, errcode = getcloudif().putobj(ct, file, bucket, objname, objprops.nhobj)
				}
			}
		} else {
			objprops.version, errstr, errcode = getcloudif().putobj(ct, file, bucket, objname, objprops.nhobj)
		}
	} else if islocal {
		if t.versioningConfigured(bucket) {
			if objprops.version, errstr = t.increaseObjectVersion(fqn); errstr != "" {
				return
			}
		}
		_, p := bucketmd.get(bucket, islocal)
		if p.NextTierURL != "" {
			if file, err = os.Open(putfqn); err != nil {
				errstr = fmt.Sprintf("Failed to reopen %s err: %v", putfqn, err)
			} else if errstr, errcode = t.putObjectNextTier(p.NextTierURL, bucket, objname, file); errstr != "" {
				glog.Errorf("Error putting bucket/object: %s/%s to next tier, err: %s, HTTP status code: %d",
					bucket, objname, errstr, errcode)
			}
		}
	}
	file.Close()
	if errstr != "" {
		return
	}

	// when all set and done:
	uname := uniquename(bucket, objname)
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)

	if err = os.Rename(putfqn, fqn); err != nil {
		t.rtnamemap.unlockname(uname, true)
		errstr = fmt.Sprintf("Failed to rename %s => %s, err: %v", putfqn, fqn, err)
		return
	}
	renamed = true
	if errstr = t.finalizeobj(fqn, objprops); errstr != "" {
		t.rtnamemap.unlockname(uname, true)
		glog.Errorf("finalizeobj %s/%s: %s (%+v)", bucket, objname, errstr, objprops)
		return
	}
	t.rtnamemap.unlockname(uname, true)
	return
}

func (t *targetrunner) dorebalance(r *http.Request, from, to, bucket, objname string) (errstr string) {
	if t.si.DaemonID != from && t.si.DaemonID != to {
		errstr = fmt.Sprintf("File copy: %s is not the intended source %s nor the destination %s",
			t.si.DaemonID, from, to)
		return
	}
	var size int64
	bucketmd := t.bmdowner.get()
	fqn := t.fqn(bucket, objname, bucketmd.islocal(bucket))
	ver := bucketmd.version()
	if t.si.DaemonID == from {
		//
		// the source
		//
		uname := uniquename(bucket, objname)
		t.rtnamemap.lockname(uname, false, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
		defer t.rtnamemap.unlockname(uname, false)

		finfo, err := os.Stat(fqn)
		if glog.V(3) {
			glog.Infof("Rebalance %s/%s from %s (self) to %s", bucket, objname, from, to)
		}
		if err != nil && os.IsNotExist(err) {
			errstr = fmt.Sprintf("File copy: %s %s at the source %s", fqn, doesnotexist, t.si.DaemonID)
			return
		}
		si := t.smapowner.get().getTarget(to)
		if si == nil {
			errstr = fmt.Sprintf("File copy: unknown destination %s (Smap not in-sync?)", to)
			return
		}
		size = finfo.Size()
		if errstr = t.sendfile(r.Method, bucket, objname, si, size, "", ""); errstr != "" {
			return
		}
		if glog.V(4) {
			glog.Infof("Rebalance %s/%s done, %.2f MB", bucket, objname, float64(size)/MiB)
		}
	} else {
		//
		// the destination
		//
		if glog.V(3) {
			glog.Infof("Rebalance %s/%s from %s to %s (self, %s, ver %d)", bucket, objname, from, to, fqn, ver)
		}
		putfqn := t.fqn2workfile(fqn)
		_, err := os.Stat(fqn)
		if err != nil && os.IsExist(err) {
			glog.Infof("File copy: %s already exists at the destination %s", fqn, t.si.DaemonID)
			return // not an error, nothing to do
		}
		var (
			hdhobj = newcksumvalue(r.Header.Get(HeaderDfcChecksumType), r.Header.Get(HeaderDfcChecksumVal))
			props  = &objectProps{version: r.Header.Get(HeaderDfcObjVersion)}
		)
		if _, props.nhobj, size, errstr = t.receive(putfqn, objname, "", hdhobj, r.Body); errstr != "" {
			return
		}
		if props.nhobj != nil {
			nhtype, nhval := props.nhobj.get()
			htype, hval := hdhobj.get()
			assert(htype == nhtype)
			if hval != nhval {
				errstr = fmt.Sprintf("Bad checksum at the destination %s: %s/%s %s %s... != %s...",
					t.si.DaemonID, bucket, objname, htype, hval[:8], nhval[:8])
				return
			}
		}
		errstr, _ = t.putCommit(t.contextWithAuth(r), bucket, objname, putfqn, fqn, props, true /*rebalance*/)
		if errstr == "" {
			t.statsdC.Send("rebalance.receive",
				metric{statsd.Counter, "files", 1},
				metric{statsd.Counter, "bytes", size})
			t.statsif.addMany("numrecvfiles", int64(1), "numrecvbytes", size)
		}
	}
	return
}

func (t *targetrunner) fildelete(ct context.Context, bucket, objname string, evict bool) error {
	var (
		errstr  string
		errcode int
	)
	islocal := t.bmdowner.get().islocal(bucket)
	fqn := t.fqn(bucket, objname, islocal)
	uname := uniquename(bucket, objname)

	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	if !islocal && !evict {
		if errstr, errcode = getcloudif().deleteobj(ct, bucket, objname); errstr != "" {
			if errcode == 0 {
				return fmt.Errorf("%s", errstr)
			}
			return fmt.Errorf("%d: %s", errcode, errstr)
		}

		t.statsdC.Send("delete", metric{statsd.Counter, "count", 1})
		t.statsif.add("numdelete", 1)
	}

	finfo, err := os.Stat(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			if islocal && !evict {
				return fmt.Errorf("DELETE local: file %s (local bucket %s, object %s) %s", fqn, bucket, objname, doesnotexist)
			}

			// Do try to delete non-cached objects.
			return nil
		}
	}
	if !(evict && islocal) {
		// Don't evict from a local bucket (this would be deletion)
		if err := os.Remove(fqn); err != nil {
			return err
		} else if evict {
			t.statsdC.Send("evict",
				metric{statsd.Counter, "files", 1},
				metric{statsd.Counter, "bytes", finfo.Size()})
			t.statsif.addMany("filesevicted", int64(1), "bytesevicted", finfo.Size())
		}
	}
	return nil
}

func (t *targetrunner) renamefile(w http.ResponseWriter, r *http.Request, msg ActionMsg) {
	var errstr string

	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname := apitems[0], strings.Join(apitems[1:], "/")
	if !t.validatebckname(w, r, bucket) {
		return
	}
	newobjname := msg.Name
	islocal := t.bmdowner.get().islocal(bucket)
	fqn := t.fqn(bucket, objname, islocal)
	uname := uniquename(bucket, objname)
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)

	if errstr = t.renameobject(bucket, objname, bucket, newobjname); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
	t.rtnamemap.unlockname(uname, true)
}

func (t *targetrunner) renameobject(bucketFrom, objnameFrom, bucketTo, objnameTo string) (errstr string) {
	var si *daemonInfo
	if si, errstr = HrwTarget(bucketTo, objnameTo, t.smapowner.get()); errstr != "" {
		return
	}
	bucketmd := t.bmdowner.get()
	islocalFrom := bucketmd.islocal(bucketFrom)
	fqn := t.fqn(bucketFrom, objnameFrom, islocalFrom)
	finfo, err := os.Stat(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Rename/move: failed to fstat %s (%s/%s), err: %v", fqn, bucketFrom, objnameFrom, err)
		return
	}
	// local rename
	if si.DaemonID == t.si.DaemonID {
		islocalTo := bucketmd.islocal(bucketTo)
		newfqn := t.fqn(bucketTo, objnameTo, islocalTo)
		dirname := filepath.Dir(newfqn)
		if err := CreateDir(dirname); err != nil {
			errstr = fmt.Sprintf("Unexpected failure to create local dir %s, err: %v", dirname, err)
		} else if err := os.Rename(fqn, newfqn); err != nil {
			errstr = fmt.Sprintf("Failed to rename %s => %s, err: %v", fqn, newfqn, err)
		} else {
			t.statsdC.Send("rename", metric{statsd.Counter, "count", 1})
			t.statsif.add("numrename", 1)
			if glog.V(3) {
				glog.Infof("Renamed %s => %s", fqn, newfqn)
			}
		}
		return
	}
	// move/migrate
	glog.Infof("Migrating %s/%s at %s => %s/%s at %s", bucketFrom, objnameFrom, t.si.DaemonID, bucketTo, objnameTo, si.DaemonID)

	errstr = t.sendfile(http.MethodPut, bucketFrom, objnameFrom, si, finfo.Size(), bucketTo, objnameTo)
	return
}

func (t *targetrunner) prefetchfiles(w http.ResponseWriter, r *http.Request, msg ActionMsg) {
	detail := fmt.Sprintf(" (%s, %s, %T)", msg.Action, msg.Name, msg.Value)
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		t.invalmsghdlr(w, r, "Unexpected ActionMsg.Value format"+detail)
		return
	}
	if _, ok := jsmap["objnames"]; ok {
		// Prefetch with List
		if prefetchMsg, errstr := parseListMsg(jsmap); errstr != "" {
			t.invalmsghdlr(w, r, errstr+detail)
		} else {
			t.prefetchList(w, r, prefetchMsg)
		}
	} else {
		// Prefetch with Range
		if prefetchRangeMsg, errstr := parseRangeMsg(jsmap); errstr != "" {
			t.invalmsghdlr(w, r, errstr+detail)
		} else {
			t.prefetchRange(w, r, prefetchRangeMsg)
		}
	}
}

func (t *targetrunner) deletefiles(w http.ResponseWriter, r *http.Request, msg ActionMsg) {
	evict := msg.Action == ActEvict
	detail := fmt.Sprintf(" (%s, %s, %T)", msg.Action, msg.Name, msg.Value)
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		t.invalmsghdlr(w, r, "deletefiles: invalid ActionMsg.Value format"+detail)
		return
	}
	if _, ok := jsmap["objnames"]; ok {
		// Delete with List
		if deleteMsg, errstr := parseListMsg(jsmap); errstr != "" {
			t.invalmsghdlr(w, r, errstr+detail)
		} else if evict {
			t.evictList(w, r, deleteMsg)
		} else {
			t.deleteList(w, r, deleteMsg)
		}
	} else {
		// Delete with Range
		if deleteMsg, errstr := parseRangeMsg(jsmap); errstr != "" {
			t.invalmsghdlr(w, r, errstr+detail)
		} else if evict {
			t.evictRange(w, r, deleteMsg)
		} else {
			t.deleteRange(w, r, deleteMsg)
		}
	}
}

// Rebalancing supports versioning. If an object in DFC cache has version in
// xattrs then the sender adds to HTTP header object version. A receiver side
// reads version from headers and set xattrs if the version is not empty
func (t *targetrunner) sendfile(method, bucket, objname string, destsi *daemonInfo, size int64, newbucket, newobjname string) string {
	var (
		xxhashval string
		errstr    string
		version   []byte
	)
	if size == 0 {
		glog.Warningf("Unexpected: %s/%s size is zero", bucket, objname)
	}
	cksumcfg := &ctx.config.Cksum
	if newobjname == "" {
		newobjname = objname
	}
	if newbucket == "" {
		newbucket = bucket
	}
	fromid, toid := t.si.DaemonID, destsi.DaemonID // source=self and destination
	url := destsi.DirectURL + "/" + Rversion + "/" + Robjects + "/"
	url += newbucket + "/" + newobjname
	url += fmt.Sprintf("?%s=%s&%s=%s", URLParamFromID, fromid, URLParamToID, toid)
	islocal := t.bmdowner.get().islocal(bucket)
	fqn := t.fqn(bucket, objname, islocal)
	file, err := os.Open(fqn)
	if err != nil {
		return fmt.Sprintf("Failed to open %q, err: %v", fqn, err)
	}
	defer file.Close()

	if version, errstr = Getxattr(fqn, XattrObjVersion); errstr != "" {
		glog.Errorf("Failed to read %q xattr %s, err %s", fqn, XattrObjVersion, errstr)
	}

	slab := selectslab(size)
	if cksumcfg.Checksum != ChecksumNone {
		assert(cksumcfg.Checksum == ChecksumXXHash)
		buf := slab.alloc()
		if xxhashval, errstr = ComputeXXHash(file, buf); errstr != "" {
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
	// http request
	//
	request, err := http.NewRequest(method, url, file)
	if err != nil {
		return fmt.Sprintf("Unexpected failure to create %s request %s, err: %v", method, url, err)
	}
	if xxhashval != "" {
		request.Header.Set(HeaderDfcChecksumType, ChecksumXXHash)
		request.Header.Set(HeaderDfcChecksumVal, xxhashval)
	}
	if len(version) != 0 {
		request.Header.Set(HeaderDfcObjVersion, string(version))
	}
	// Do
	contextwith, cancel := context.WithTimeout(context.Background(), ctx.config.Timeout.SendFile)
	defer cancel()
	newrequest := request.WithContext(contextwith)

	response, err := t.httpclientLongTimeout.Do(newrequest)

	// err handle
	if err != nil {
		errstr = fmt.Sprintf("Failed to sendfile %s/%s at %s => %s/%s at %s, err: %v",
			bucket, objname, fromid, newbucket, newobjname, toid, err)
		if response != nil && response.StatusCode > 0 {
			errstr += fmt.Sprintf(", status %s", response.Status)
		}
		return errstr
	}
	if _, err = ioutil.ReadAll(response.Body); err != nil {
		errstr = fmt.Sprintf("Failed to read sendfile response: %s/%s at %s => %s/%s at %s, err: %v",
			bucket, objname, fromid, newbucket, newobjname, toid, err)
		if err == io.EOF {
			trailer := response.Trailer.Get("Error")
			if trailer != "" {
				errstr += fmt.Sprintf(", trailer: %s", trailer)
			}
		}
		response.Body.Close()
		return errstr
	}
	response.Body.Close()
	// stats
	t.statsdC.Send("rebalance.send",
		metric{statsd.Counter, "files", 1},
		metric{statsd.Counter, "bytes", size})
	t.statsif.addMany("numsentfiles", int64(1), "numsentbytes", size)
	return ""
}

func (t *targetrunner) checkCacheQueryParameter(r *http.Request) (useCache bool, errstr string, errcode int) {
	useCacheStr := r.URL.Query().Get(URLParamCached)
	var err error
	if useCache, err = parsebool(useCacheStr); err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter: %s=%s (expecting: '' | true | false)",
			URLParamCached, useCacheStr)
		errcode = http.StatusInternalServerError
	}
	return
}

func (t *targetrunner) checkLocalQueryParameter(bucket string, r *http.Request, islocal bool) (errstr string, errcode int) {
	proxylocalstr := r.URL.Query().Get(URLParamLocal)
	if proxylocal, err := parsebool(proxylocalstr); err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter for bucket %s: %s=%s (expecting bool)", bucket, URLParamLocal, proxylocalstr)
		errcode = http.StatusInternalServerError
	} else if proxylocalstr != "" && islocal != proxylocal {
		errstr = fmt.Sprintf("islocalbucket(%s) mismatch: %t (proxy) != %t (target %s)", bucket, proxylocal, islocal, t.si.DaemonID)
		errcode = http.StatusInternalServerError
	}
	return
}

//===========================
//
// control plane
//
//===========================

// "/"+Rversion+"/"+Rdaemon
func (t *targetrunner) daemonHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpdaeget(w, r)
	case http.MethodPut:
		t.httpdaeput(w, r)
	case http.MethodPost:
		t.httpdaepost(w, r)
	case http.MethodDelete:
		t.httpdaedelete(w, r)
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
	if len(apitems) > 0 {
		switch apitems[0] {
		// PUT /v1/daemon/proxy/newprimaryproxyid
		case Rproxy:
			t.httpdaesetprimaryproxy(w, r, apitems)
			return
		case Rsyncsmap:
			var newsmap = &Smap{}
			if t.readJSON(w, r, newsmap) != nil {
				return
			}
			if errstr := t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to sync Smap: %s", errstr))
			}
			glog.Infof("%s: %s v%d done", t.si.DaemonID, Rsyncsmap, newsmap.version())
			return
		case Rmetasync:
			t.receiveMeta(w, r)
			return
		case Rmountpaths:
			t.enableMountpath(w, r)
			return
		default:
		}
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
			_, lruxact := t.xactinp.findU(ActLRU)
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

func (t *targetrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request, apitems []string) {
	var (
		prepare bool
		err     error
	)
	if len(apitems) != 2 {
		s := fmt.Sprintf("Incorrect number of API items: %d, should be: %d", len(apitems), 2)
		t.invalmsghdlr(w, r, s)
		return
	}

	proxyid := apitems[1]
	query := r.URL.Query()
	preparestr := query.Get(URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL Parameter: %v", URLParamPrepare, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	smap := t.smapowner.get()
	psi := smap.getProxy(proxyid)
	if psi == nil {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", proxyid, smap.pp())
		t.invalmsghdlr(w, r, s)
		return
	}
	if prepare {
		if glog.V(4) {
			glog.Info("Preparation step: do nothing")
		}
		return
	}
	t.smapowner.Lock()
	smap = t.smapowner.get()
	if smap.ProxySI.DaemonID != psi.DaemonID {
		clone := smap.clone()
		clone.ProxySI = psi
		if s := t.smapowner.persist(clone, false /*saveSmap*/); s != "" {
			t.smapowner.Unlock()
			t.invalmsghdlr(w, r, s)
			return
		}
		t.smapowner.put(clone)
	}
	t.smapowner.Unlock()
}

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	getWhat := r.URL.Query().Get(URLParamWhat)
	var (
		jsbytes []byte
		err     error
	)
	switch getWhat {
	case GetWhatConfig:
		jsbytes, err = json.Marshal(ctx.config)
		assert(err == nil, err)
	case GetWhatSmap:
		jsbytes, err = json.Marshal(t.smapowner.get())
		assert(err == nil, err)
	case GetWhatSmapVote:
		_, xx := t.xactinp.findL(ActElection)
		vote := xx != nil
		msg := SmapVoteMsg{VoteInProgress: vote, Smap: t.smapowner.get(), BucketMD: t.bmdowner.get()}
		jsbytes, err = json.Marshal(msg)
		assert(err == nil, err)
	case GetWhatStats:
		rst := getstorstatsrunner()
		rst.RLock()
		jsbytes, err = json.Marshal(rst)
		rst.RUnlock()
		assert(err == nil, err)
	case GetWhatXaction:
		kind := r.URL.Query().Get(URLParamProps)
		if errstr := isXactionQueryable(kind); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		xactionStatsRetriever := t.getXactionStatsRetriever(kind)
		allXactionDetails := t.getXactionsByType(kind)
		jsbytes = xactionStatsRetriever.getStats(allXactionDetails)
	case GetWhatMountpaths:
		mpList := MountpathList{
			Available: make([]string, 0),
			Disabled:  make([]string, 0),
		}

		availablePaths, disabledPaths := ctx.mountpaths.Mountpaths()
		for _, mp := range availablePaths {
			mpList.Available = append(mpList.Available, mp.Path)
		}
		for _, mp := range disabledPaths {
			mpList.Disabled = append(mpList.Disabled, mp.Path)
		}
		jsbytes, err = json.Marshal(&mpList)
		if err != nil {
			s := fmt.Sprintf("Failed to marshal mountpaths: %v", err)
			t.invalmsghdlr(w, r, s)
			return
		}
	default:
		s := fmt.Sprintf("Unexpected GET request, what: [%s]", getWhat)
		t.invalmsghdlr(w, r, s)
		return
	}
	t.writeJSON(w, r, jsbytes, "httpdaeget")
}

func (t *targetrunner) getXactionStatsRetriever(kind string) XactionStatsRetriever {
	var xactionStatsRetriever XactionStatsRetriever

	// No need to handle default since kind has already been validated by
	// this point.
	switch kind {
	case XactionRebalance:
		xactionStatsRetriever = RebalanceTargetStats{}
	case XactionPrefetch:
		xactionStatsRetriever = PrefetchTargetStats{}
	}

	return xactionStatsRetriever
}

func (t *targetrunner) getXactionsByType(kind string) []XactionDetails {
	allXactionDetails := []XactionDetails{}
	for _, xaction := range t.xactinp.xactinp {
		if xaction.getkind() == kind {
			status := XactionStatusCompleted
			if !xaction.finished() {
				status = XactionStatusInProgress
			}

			xactionStats := XactionDetails{
				Id:        xaction.getid(),
				StartTime: xaction.getStartTime(),
				EndTime:   xaction.getEndTime(),
				Status:    status,
			}

			allXactionDetails = append(allXactionDetails, xactionStats)
		}
	}

	return allXactionDetails
}

// management interface to register (unregistered) self
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	if len(apitems) > 0 && apitems[0] == Rregister {
		if glog.V(3) {
			glog.Infoln("Sending register signal to target keepalive control channel")
		}
		gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: register}
		return
	}
	if status, err := t.register(0); err != nil {
		s := fmt.Sprintf("Target %s failed to register with proxy, status %d, err: %v", t.si.DaemonID, status, err)
		t.invalmsghdlr(w, r, s)
		return
	}
	if glog.V(3) {
		glog.Infof("Registered self %s", t.si.DaemonID)
	}
}

func (t *targetrunner) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	if glog.V(3) {
		glog.Infoln("Sending unregister signal to target keepalive control channel")
	}
	gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: unregister}
}

//====================== common for both cold GET and PUT ======================================
//
// on err: closes and removes the file; otherwise closes and returns the size;
// empty omd5 or oxxhash: not considered an exception even when the configuration says otherwise;
// xxhash is always preferred over md5
//
//==============================================================================================
func (t *targetrunner) receive(fqn string, objname, omd5 string, ohobj cksumvalue,
	reader io.Reader) (sgl *SGLIO, nhobj cksumvalue, written int64, errstr string) {
	var (
		err                  error
		file                 *os.File
		filewriter           io.Writer
		ohtype, ohval, nhval string
		cksumcfg             = &ctx.config.Cksum
	)

	if file, err = CreateFile(fqn); err != nil {
		t.fshc(err, fqn)
		errstr = fmt.Sprintf("Failed to create %s, err: %s", fqn, err)
		return
	}
	filewriter = file
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
		if written, err = ReceiveAndChecksum(filewriter, reader, buf, xx); err != nil {
			errstr = err.Error()
			t.fshc(err, fqn)
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

				t.statsdC.Send("error.badchecksum.xxhash",
					metric{statsd.Counter, "count", 1},
					metric{statsd.Counter, "bytes", written})
				t.statsif.addMany("numbadchecksum", int64(1), "bytesbadchecksum", written)
				return
			}
		}
	} else if omd5 != "" && cksumcfg.ValidateColdGet {
		md5 := md5.New()
		if written, err = ReceiveAndChecksum(filewriter, reader, buf, md5); err != nil {
			errstr = err.Error()
			t.fshc(err, fqn)
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash := hex.EncodeToString(hashInBytes)
		if omd5 != md5hash {
			errstr = fmt.Sprintf("Bad checksum: cold GET %s md5 %s... != %s... computed for the %q",
				objname, ohval[:8], nhval[:8], fqn)

			t.statsdC.Send("error.badchecksum.md5",
				metric{statsd.Counter, "count", 1},
				metric{statsd.Counter, "bytes", written})
			t.statsif.addMany("numbadchecksum", int64(1), "bytesbadchecksum", written)
			return
		}
	} else {
		if written, err = ReceiveAndChecksum(filewriter, reader, buf); err != nil {
			errstr = err.Error()
			t.fshc(err, fqn)
			return
		}
	}
	if err = file.Close(); err != nil {
		errstr = fmt.Sprintf("Failed to close received file %s, err: %v", fqn, err)
	}
	return
}

//==============================================================================
//
// target's misc utilities and helpers
//
//==============================================================================
func (t *targetrunner) starttime() time.Time {
	return t.uxprocess.starttime
}

// (bucket, object) => (local hashed path, fully qualified name aka fqn)
func (t *targetrunner) fqn(bucket, objname string, islocal bool) string {
	mpath := hrwMpath(bucket, objname)
	if islocal {
		return filepath.Join(makePathLocal(mpath), bucket, objname)
	}
	return filepath.Join(makePathCloud(mpath), bucket, objname)
}

// the opposite
func (t *targetrunner) fqn2bckobj(fqn string) (bucket, objname, errstr string) {
	fn := func(path string) bool {
		if strings.HasPrefix(fqn, path) {
			rempath := fqn[len(path):]
			items := strings.SplitN(rempath, "/", 2)
			bucket, objname = items[0], items[1]
			return true
		}
		return false
	}
	ok := true
	bucketmd := t.bmdowner.get()
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	for _, mpathInfo := range availablePaths {
		if fn(makePathCloud(mpathInfo.Path) + "/") {
			ok = len(objname) > 0 && t.fqn(bucket, objname, false) == fqn
			break
		}
		if fn(makePathLocal(mpathInfo.Path) + "/") {
			islocal := bucketmd.islocal(bucket)
			ok = islocal && len(objname) > 0 && t.fqn(bucket, objname, true) == fqn
			break
		}
	}
	if !ok {
		errstr = fmt.Sprintf("Cannot convert %q => %s/%s - localbuckets or device mount paths changed?", fqn, bucket, objname)
	}
	return
}

func (t *targetrunner) fqn2workfile(fqn string) (workfqn string) {
	dir, base := filepath.Split(fqn)
	assert(strings.HasSuffix(dir, "/"), dir+" : "+base)
	assert(base != "", dir+" : "+base)

	tiebreaker := strconv.FormatInt(time.Now().UnixNano(), 16)
	workfqn = dir + workfileprefix + base + "." + tiebreaker[5:] + "." + t.uxprocess.spid
	return
}

func (t *targetrunner) isworkfile(workfqn string) (iswork, isold bool) {
	dir, base := filepath.Split(workfqn)
	if !strings.HasSuffix(dir, "/") {
		return
	}
	if base == "" {
		return
	}
	if !strings.HasPrefix(base, workfileprefix) {
		return
	}
	i := strings.LastIndex(base, ".")
	if i < 0 {
		return
	}
	pid, err := strconv.ParseInt(base[i+1:], 16, 64)
	if err != nil {
		return
	}
	iswork = true
	isold = pid != t.uxprocess.pid
	return
}

// create local directories to test multiple fspaths
func (t *targetrunner) testCachepathMounts() {
	var instpath string
	if ctx.config.TestFSP.Instance > 0 {
		instpath = filepath.Join(ctx.config.TestFSP.Root, strconv.Itoa(ctx.config.TestFSP.Instance))
	} else {
		// container, VM, etc.
		instpath = ctx.config.TestFSP.Root
	}
	for i := 0; i < ctx.config.TestFSP.Count; i++ {
		var mpath string
		if testingFSPpaths() {
			mpath = filepath.Join(instpath, strconv.Itoa(i+1))
		} else {
			mpath = instpath[0 : len(instpath)-1]
		}
		if err := CreateDir(mpath); err != nil {
			glog.Errorf("FATAL: cannot create test cache dir %q, err: %v", mpath, err)
			os.Exit(1)
		}

		err := ctx.mountpaths.AddMountpath(mpath)
		assert(err == nil, err)
	}
}

func (t *targetrunner) createBucketDirs(s, basename string, f func(basePath string) string) {
	if basename == "" {
		glog.Errorf("FATAL: empty basename for the %s buckets directory", s)
		os.Exit(1)
	}
	for mpath := range ctx.mountpaths.Available {
		dir := f(mpath)
		if _, ok := ctx.mountpaths.Available[dir]; ok {
			glog.Errorf("FATAL: local namespace partitioning conflict: %s vs %s", mpath, dir)
			os.Exit(1)
		}
		if err := CreateDir(dir); err != nil {
			glog.Errorf("FATAL: cannot create %s buckets dir %q, err: %v", s, dir, err)
			os.Exit(1)
		}
	}
}

func (t *targetrunner) detectMpathChanges() {
	// mpath config dir
	mpathconfigfqn := filepath.Join(ctx.config.Confdir, mpname)

	// load old/prev and compare
	var (
		changed bool
		old     = &fs.MountedFS{
			Available: make(map[string]*fs.MountpathInfo),
			Disabled:  make(map[string]*fs.MountpathInfo),
		}
	)
	if err := LocalLoad(mpathconfigfqn, old); err != nil {
		if !os.IsNotExist(err) && err != io.EOF {
			glog.Errorf("Failed to load old mpath config %q, err: %v", mpathconfigfqn, err)
		}
	} else if len(old.Available) != len(ctx.mountpaths.Available) {
		changed = true
	} else {
		for k := range old.Available {
			if _, ok := ctx.mountpaths.Available[k]; !ok {
				changed = true
			}
		}
	}
	if changed {
		glog.Errorf("%s: detected change in the mountpath configuration at %s", t.si.DaemonID, mpathconfigfqn)
		glog.Errorln("OLD: ====================")
		glog.Errorln(old.Pprint())
		glog.Errorln("NEW: ====================")
		glog.Errorln(ctx.mountpaths.Pprint())
	}
	// persist
	if err := LocalSave(mpathconfigfqn, ctx.mountpaths); err != nil {
		glog.Errorf("Error writing config file: %v", err)
	}
}

// versioningConfigured returns true if versioning for a given bucket is enabled
// NOTE:
//    AWS bucket versioning can be disabled on the cloud. In this case we do not
//    save/read/update version using xattrs. And the function returns that the
//    versioning is unsupported even if versioning is 'all' or 'cloud'.
func (t *targetrunner) versioningConfigured(bucket string) bool {
	islocal := t.bmdowner.get().islocal(bucket)
	versioning := ctx.config.Ver.Versioning
	if islocal {
		return versioning == VersionAll || versioning == VersionLocal
	}
	return versioning == VersionAll || versioning == VersionCloud
}

// xattrs
func (t *targetrunner) finalizeobj(fqn string, objprops *objectProps) (errstr string) {
	if objprops.nhobj != nil {
		htype, hval := objprops.nhobj.get()
		assert(htype == ChecksumXXHash)
		if errstr = Setxattr(fqn, XattrXXHashVal, []byte(hval)); errstr != "" {
			return errstr
		}
	}
	if objprops.version != "" {
		errstr = Setxattr(fqn, XattrObjVersion, []byte(objprops.version))
	}
	return
}

// increaseObjectVersion increments the current version xattrs and returns the new value.
// If the current version is empty (local bucket versioning (re)enabled, new file)
// the version is set to "1"
func (t *targetrunner) increaseObjectVersion(fqn string) (newVersion string, errstr string) {
	const initialVersion = "1"
	var (
		err    error
		vbytes []byte
	)
	_, err = os.Stat(fqn)
	if err != nil && os.IsNotExist(err) {
		newVersion = initialVersion
		return
	}
	if err != nil {
		errstr = fmt.Sprintf("Unexpected failure to read stats of file %s, err: %v", fqn, err)
		return
	}

	if vbytes, errstr = Getxattr(fqn, XattrObjVersion); errstr != "" {
		return
	}
	if currValue, err := strconv.Atoi(string(vbytes)); err != nil {
		newVersion = initialVersion
	} else {
		newVersion = fmt.Sprintf("%d", currValue+1)
	}

	return
}

// fshc wakes up FSHC and makes it to run filesystem check
// immediately if err != nil
func (t *targetrunner) fshc(err error, filepath string) {
	glog.Errorf("FSHealthChecker called with error: %#v, file: %s", err, filepath)

	if !isIOError(err) {
		return
	}

	keyName := fqn2mountPath(filepath)
	if keyName != "" {
		t.statsdC.Send(keyName+".io.errors", metric{statsd.Counter, "count", 1})
	}
	if ctx.config.FSChecker.Enabled {
		getfshealthchecker().onerr(filepath)
	}
}

// Decrypts token and retreives userID from request header
// Returns empty userID in case of token is invalid
func (t *targetrunner) userFromRequest(r *http.Request) (*authRec, error) {
	if r == nil {
		return nil, nil
	}

	token := ""
	tokenParts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(tokenParts) == 2 && tokenParts[0] == tokenStart {
		token = tokenParts[1]
	}

	if token == "" {
		// no token in header = use default credentials
		return nil, nil
	}

	authrec, err := t.authn.validateToken(token)
	if err != nil {
		return nil, fmt.Errorf("Failed to decrypt token [%s]: %v", token, err)
	}

	return authrec, nil
}

// If Authn server is enabled then the function tries to read a user credentials
// (at this moment userID is enough) from HTTP request header: looks for
// 'Authorization' header and decrypts it.
// Extracted user information is put to context that is passed to all consumers
func (t *targetrunner) contextWithAuth(r *http.Request) context.Context {
	ct := context.Background()

	if ctx.config.Auth.CredDir == "" || !ctx.config.Auth.Enabled {
		return ct
	}

	user, err := t.userFromRequest(r)
	if err != nil {
		glog.Errorf("Failed to extract token: %v", err)
		return ct
	}

	if user != nil {
		ct = context.WithValue(ct, ctxUserID, user.userID)
		ct = context.WithValue(ct, ctxCredsDir, ctx.config.Auth.CredDir)
		ct = context.WithValue(ct, ctxUserCreds, user.creds)
	}

	return ct
}

// builds fqn of directory for local buckets from mountpath
func makePathLocal(basePath string) string {
	return filepath.Join(basePath, ctx.config.LocalBuckets)
}

// builds fqn of directory for cloud buckets from mountpath
func makePathCloud(basePath string) string {
	return filepath.Join(basePath, ctx.config.CloudBuckets)
}

func (t *targetrunner) enableMountpath(w http.ResponseWriter, r *http.Request) {
	mp := MountpathReq{}
	if t.readJSON(w, r, &mp) != nil {
		return
	}
	if mp.Mountpath == "" {
		t.invalmsghdlr(w, r, "Mountpath is not defined")
		return
	}

	enabled, exists := ctx.mountpaths.EnableMountpath(mp.Mountpath)
	if !enabled && exists {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !enabled && !exists {
		t.invalmsghdlr(w, r, fmt.Sprintf("Mountpath %s not found", mp.Mountpath), http.StatusNotFound)
		return
	}

	glog.Infof("Reenabled mountpath %s", mp.Mountpath)
}

func (t *targetrunner) receiveMeta(w http.ResponseWriter, r *http.Request) {
	var payload = make(simplekvs)
	if t.readJSON(w, r, &payload) != nil {
		return
	}

	newsmap, oldsmap, actionsmap, errstr := t.extractSmap(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newsmap != nil {
		errstr = t.receiveSmap(newsmap, oldsmap, actionsmap)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
	}

	newbucketmd, actionlb, errstr := t.extractbucketmd(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newbucketmd != nil {
		if errstr = t.receiveBucketMD(newbucketmd, actionlb); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
	}
}

// FIXME: use the message
func (t *targetrunner) receiveBucketMD(newbucketmd *bucketMD, msg *ActionMsg) (errstr string) {
	if msg.Action == "" {
		glog.Infof("receive bucket-metadata: version %d", newbucketmd.version())
	} else {
		glog.Infof("receive bucket-metadata: version %d, action %s", newbucketmd.version(), msg.Action)
	}
	t.bmdowner.Lock()
	bucketmd := t.bmdowner.get()
	myver := bucketmd.version()
	if newbucketmd.version() <= myver {
		t.bmdowner.Unlock()
		if newbucketmd.version() < myver {
			errstr = fmt.Sprintf("Attempt to downgrade bucket-metadata version %d to %d", myver, newbucketmd.version())
		}
		return
	}
	t.bmdowner.put(newbucketmd)
	t.bmdowner.Unlock()

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	for bucket := range bucketmd.LBmap {
		if _, ok := newbucketmd.LBmap[bucket]; !ok {
			glog.Infof("Destroy local bucket %s", bucket)
			for _, mpathInfo := range availablePaths {
				localbucketfqn := filepath.Join(makePathLocal(mpathInfo.Path), bucket)
				if err := os.RemoveAll(localbucketfqn); err != nil {
					glog.Errorf("Failed to destroy local bucket dir %q, err: %v", localbucketfqn, err)
				}
			}
		}
	}
	for _, mpathInfo := range availablePaths {
		for bucket := range bucketmd.LBmap {
			localbucketfqn := filepath.Join(makePathLocal(mpathInfo.Path), bucket)
			if err := CreateDir(localbucketfqn); err != nil {
				glog.Errorf("Failed to create local bucket dir %q, err: %v", localbucketfqn, err)
			}
		}
	}
	return
}

func (t *targetrunner) receiveSmap(newsmap, oldsmap *Smap, msg *ActionMsg) (errstr string) {
	var (
		newtargetid  string
		existentialQ bool
	)
	var s string
	if oldsmap.version() > 0 {
		s = fmt.Sprintf(" (prev non-local %d)", oldsmap.version())
	}
	if msg.Action != "" {
		s = ", action " + msg.Action
	}
	glog.Infof("receive Smap: version %d%s, ntargets %d, primary %s%s",
		newsmap.version(), s, newsmap.countTargets(), newsmap.ProxySI.DaemonID, s)
	newlen := len(newsmap.Tmap)
	oldlen := len(oldsmap.Tmap)

	// check whether this target is present in the new Smap
	// rebalance? (nothing to rebalance if the new map is a strict subset of the old)
	// assign proxysi
	// log
	infoln := make([]string, 0, newlen)
	for id, si := range newsmap.Tmap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			infoln = append(infoln, fmt.Sprintf("target: %s <= self", si))
		} else {
			infoln = append(infoln, fmt.Sprintf("target: %s", si))
		}
		if oldlen == 0 {
			continue
		}
		if _, ok := oldsmap.Tmap[id]; !ok {
			if newtargetid != "" {
				glog.Warningf("More than one new target (%s, %s) in the new Smap?", newtargetid, id)
			}
			newtargetid = id
		}
	}
	if !existentialQ {
		errstr = fmt.Sprintf("Not finding self %s in the new %s", t.si.DaemonID, newsmap.pp())
		return
	}

	errstr = t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */)
	if errstr != "" {
		return
	}
	if !newsmap.isPresent(t.si, false) {
		// FIXME: investigate further
		s := fmt.Sprintf("Warning: received Smap without self '%s': %s", t.si.DaemonID, newsmap.pp())
		glog.Errorln(s)
	}

	for _, ln := range infoln {
		glog.Infoln(ln)
	}
	if msg.Action == ActRebalance {
		go t.runRebalance(newsmap, newtargetid)
		return
	}
	if oldlen == 0 {
		return
	}
	if newtargetid == "" {
		if newlen != oldlen {
			assert(newlen < oldlen)
			glog.Infoln("nothing to rebalance: new Smap is a strict subset of the old")
		} else {
			glog.Infof("nothing to rebalance: num (%d) and IDs of the targets did not change", newlen)
		}
		return
	}
	if !ctx.config.Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}
	uptime := time.Since(t.starttime())
	if uptime < ctx.config.Rebalance.StartupDelayTime && t.si.DaemonID != newtargetid {
		glog.Infof("not auto-rebalancing: uptime %v < %v", uptime, ctx.config.Rebalance.StartupDelayTime)
		aborted, running := t.xactinp.isAbortedOrRunningRebalance()
		if aborted && !running {
			f := func() {
				time.Sleep(ctx.config.Rebalance.StartupDelayTime - uptime)
				t.runRebalance(t.smapowner.get(), "")
			}
			go runRebalanceOnce.Do(f) // only once at startup
		}
		return
	}
	// xaction
	go t.runRebalance(newsmap, newtargetid)
	return
}

// broadcastNeighbors sends a message ([]byte) to all neighboring targets belongs to a smap
func (t *targetrunner) broadcastNeighbors(path string, query url.Values, method string, body []byte,
	smap *Smap, timeout ...time.Duration) chan callResult {

	if len(smap.Tmap) < 2 {
		// no neighbor, returns empty channel, so caller doesnt have to check channel is nil
		ch := make(chan callResult)
		close(ch)
		return ch
	}

	var servers []*daemonInfo
	for _, s := range smap.Tmap {
		if s.DaemonID != t.si.DaemonID {
			servers = append(servers, s)
		}
	}

	return t.broadcast(path, query, method, body, servers, timeout...)
}

func (t *targetrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rtokens); apitems == nil {
		return
	}

	if err := t.readJSON(w, r, tokenList); err != nil {
		s := fmt.Sprintf("Invalid token list: %v", err)
		t.invalmsghdlr(w, r, s)
		return
	}

	t.authn.updateRevokedList(tokenList)
}

func (t *targetrunner) validateObjectChecksum(fqn string, checksumAlgo string, slabSize int64) (validChecksum bool, errstr string) {
	if checksumAlgo != ChecksumXXHash {
		errstr := fmt.Sprintf("Unsupported checksum algorithm: [%s]", checksumAlgo)
		return false, errstr
	}

	hashbinary, errstr := Getxattr(fqn, XattrXXHashVal)
	if errstr != "" {
		errstr = fmt.Sprintf("Unable to read checksum of object [%s], err: %s", fqn, errstr)
		return false, errstr
	}

	if hashbinary == nil {
		glog.Warningf("%s has no checksum - cannot validate", fqn)
		return true, ""
	}

	file, err := os.Open(fqn)
	if err != nil {
		errstr := fmt.Sprintf("Failed to read object %s, err: %v", fqn, err)
		return false, errstr
	}

	slab := selectslab(slabSize)
	buf := slab.alloc()
	xxHashValue, errstr := ComputeXXHash(file, buf)
	file.Close()
	slab.free(buf)

	if errstr != "" {
		errstr := fmt.Sprintf("Unable to compute xxHash, err: %s", errstr)
		return false, errstr
	}

	return string(hashbinary) == xxHashValue, ""
}

// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/dfc/statsd"
	"github.com/OneOfOne/xxhash"
	"github.com/golang/glog"
)

const (
	DefaultPageSize  = 1000      // the number of cached file infos returned in one page
	internalPageSize = 10000     // number of objects in a page for internal call between target and proxy to get atime/iscached
	MaxPageSize      = 64 * 1024 // the maximum number of objects in a page (waring is logged in case of requested page size exceeds the limit)
	workfileprefix   = ".~~~."
)

const doesnotexist = "does not exist"

type mountPath struct {
	Path string       `json:"path"`
	Fsid syscall.Fsid `json:"fsid"`
}

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
	lbmap         *lbmap
	rtnamemap     *rtnamemap
	prefetchQueue chan filesWithDeadline
	statsdC       statsd.Client
}

// start target runner
func (t *targetrunner) run() error {
	t.httprunner.init(getstorstatsrunner(), false)
	t.httprunner.kalive = gettargetkalive()
	t.xactinp = newxactinp()                         // extended actions
	t.lbmap = &lbmap{LBmap: make(map[string]string)} // local (cache-only) buckets
	t.rtnamemap = newrtnamemap(128)                  // lock/unlock name

	if status, err := t.register(0); err != nil {
		glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
		if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
			glog.Errorf("Target %s: retrying registration...", t.si.DaemonID)
			time.Sleep(time.Second * 3)
			if _, err = t.register(0); err != nil {
				glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
				glog.Errorf("Target %s is terminating", t.si.DaemonID)
				return err
			}
			glog.Errorf("Success: target %s joined the cluster", t.si.DaemonID)
		} else {
			return err
		}
	}
	// fill-in, detect changes, persist
	t.startupMpaths()

	// cloud provider
	if ctx.config.CloudProvider == ProviderAmazon {
		// TODO: sessions
		t.cloudif = &awsimpl{t}

	} else {
		assert(ctx.config.CloudProvider == ProviderGoogle)
		t.cloudif = &gcpimpl{t}
	}
	// init capacity
	rr := getstorstatsrunner()
	rr.init()
	// prefetch
	t.prefetchQueue = make(chan filesWithDeadline, prefetchChanSize)

	//
	// REST API: register storage target's handler(s) and start listening
	//
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rbuckets+"/", t.buckethdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Robjects+"/", t.objecthdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon, t.daemonhdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon+"/", t.daemonhdlr) // FIXME
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rpush+"/", t.pushhdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rhealth, t.httphealth)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rvote+"/", t.votehdlr)
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
func (t *targetrunner) register(timeout time.Duration) (status int, err error) {
	jsbytes, err := json.Marshal(t.si)
	if err != nil {
		return 0, fmt.Errorf("Unexpected failure to json-marshal %+v, err: %v", t.si, err)
	}
	var url string
	if t.proxysi.DaemonID != "" {
		url = t.proxysi.DirectURL
	} else {
		// Smap has not yet been synced:
		url = ctx.config.Proxy.Primary.URL
	}
	url += "/" + Rversion + "/" + Rcluster
	var si *daemonInfo
	if t.proxysi != nil {
		si = &t.proxysi.daemonInfo
	}
	if timeout > 0 { // keepalive
		url += "/" + Rkeepalive
		_, err, _, status = t.call(si, url, http.MethodPost, jsbytes, timeout)
	} else {
		_, err, _, status = t.call(si, url, http.MethodPost, jsbytes)
	}
	return
}

func (t *targetrunner) unregister() (status int, err error) {
	var url string
	if t.proxysi.DaemonID != "" {
		url = t.proxysi.DirectURL
	} else {
		// Smap has not yet been synched:
		url = ctx.config.Proxy.Primary.URL
	}
	url += "/" + Rversion + "/" + Rcluster + "/" + Rdaemon + "/" + t.si.DaemonID
	_, err, _, status = t.call(&t.proxysi.daemonInfo, url, http.MethodDelete, nil)
	return
}

//===========================================================================================
//
// http handlers: data and metadata
//
//===========================================================================================

// verb /Rversion/Rbuckets
func (t *targetrunner) buckethdlr(w http.ResponseWriter, r *http.Request) {
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
func (t *targetrunner) objecthdlr(w http.ResponseWriter, r *http.Request) {
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
	started := time.Now()
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
	// list the bucket and return
	tag, ok := t.listbucket(w, r, bucket)
	if ok {
		delta := time.Since(started)
		t.statsdC.Send("list",
			statsd.Metric{
				Type:  statsd.Counter,
				Name:  "count",
				Value: 1,
			},
			statsd.Metric{
				Type:  statsd.Timer,
				Name:  "latency",
				Value: float64(delta / time.Millisecond),
			},
		)

		lat := int64(delta / 1000)
		t.statsif.addMany("numlist", int64(1), "listlatency", lat)
		if glog.V(3) {
			glog.Infof("LIST %s: %s, %d µs", tag, bucket, lat)
		}
	}
}

// GET /Rversion/Robjects/bucket[+"/"+objname]
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *targetrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	var (
		nhobj                  cksumvalue
		bucket, objname, fqn   string
		uname, errstr, version string
		size                   int64
		props                  *objectProps
		started                time.Time
		errcode                int
		coldget, vchanged      bool
	)
	started = time.Now()
	cksumcfg := &ctx.config.Cksum
	versioncfg := &ctx.config.Ver
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	islocal, errstr, errcode := t.checkLocalQueryParameter(bucket, r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	//
	// lockname(ro)
	//
	fqn, uname = t.fqn(bucket, objname), t.uname(bucket, objname)
	t.rtnamemap.lockname(uname, false, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	// existence, access & versioning
	if coldget, size, version, errstr = t.lookupLocally(bucket, objname, fqn); errstr != "" {
		//
		// given certain conditions (below) make an effort to locate the object cluster-wide
		//
		if islocal && strings.Contains(errstr, doesnotexist) {
			aborted, running := t.xactinp.isAbortedOrRunningRebalance()
			if aborted || running {
				if props := t.getFromNeighbor(bucket, objname, r, islocal); props != nil {
					size, nhobj = props.size, props.nhobj
					goto existslocally
				}
			}
		}
		t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
		t.rtnamemap.unlockname(uname, false)
		return
	}

	if !coldget && !islocal && versioncfg.ValidateWarmGet && version != "" && t.versioningConfigured(bucket) {
		if vchanged, errstr, errcode = t.checkCloudVersion(bucket, objname, version); errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			t.rtnamemap.unlockname(uname, false)
			return
		}
		// TODO: add a knob to return what's cached while upgrading the version async
		coldget = vchanged
	}
	if coldget {
		t.rtnamemap.unlockname(uname, false)
		if props, errstr, errcode = t.coldget(bucket, objname, false); errstr != "" {
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
		errstr = fmt.Sprintf("Unexpected: object %s/%s size is 0 (zero)", bucket, objname)
		t.invalmsghdlr(w, r, errstr)
		return // likely, an error
	}
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
		return
	}

	defer file.Close()
	slab := selectslab(size)
	buf := slab.alloc()
	defer slab.free(buf)
	// copy
	written, err := io.CopyBuffer(w, file, buf)
	if err != nil {
		errstr = fmt.Sprintf("Failed to send file %s, err: %v", fqn, err)
		t.invalmsghdlr(w, r, errstr)
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
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  "latency",
			Value: float64(delta / time.Millisecond),
		},
	)

	t.statsif.addMany("numget", int64(1), "getlatency", int64(delta/1000))
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
		err := t.fildelete(bucket, objname, evict)
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
		lbmap4ren, ok := msg.Value.(map[string]interface{})
		if !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Unexpected Action.Value format %+v, %T", msg.Value, msg.Value))
		}
		//
		// convert/cast generic ActionMsg.Value
		//
		versionfloat, ok := lbmap4ren["version"].(float64)
		if !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Unexpected Action.Value format (version) %+v, %T", msg.Value, msg.Value))
		}
		version := int64(versionfloat)
		lbmapif, ok := lbmap4ren["l_bmap"].(map[string]interface{})
		if !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Unexpected Action.Value format (l_bmap) %+v, %T", msg.Value, msg.Value))
		}
		if errstr := t.renamelocalbucket(bucketFrom, bucketTo, version, lbmapif); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
		glog.Infof("renamed local bucket %s => %s, lbmap version %d", bucketFrom, bucketTo, t.lbmap.versionLocked())
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

// HEAD /Rversion/Robjects/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket      string
		islocal     bool
		errstr      string
		errcode     int
		bucketprops map[string]string
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
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
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
	} else {
		bucketprops = make(map[string]string)
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
}

// HEAD /Rversion/Robjects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname, errstr string
		islocal                 bool
		errcode                 int
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	islocal, errstr, errcode = t.checkLocalQueryParameter(bucket, r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	var objmeta map[string]string
	if !islocal {
		objmeta, errstr, errcode = getcloudif().headobject(bucket, objname)
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
	} else {
		fqn := t.fqn(bucket, objname)
		var (
			size    int64
			version string
		)
		if _, size, version, errstr = t.lookupLocally(bucket, objname, fqn); errstr != "" {
			status := http.StatusNotFound
			http.Error(w, http.StatusText(status), status)
			return
		}
		objmeta = make(map[string]string)
		objmeta["size"] = strconv.FormatInt(size, 10)
		objmeta["version"] = version
		glog.Infoln("httpobjhead FOUND:", bucket, objname, size, version)
	}
	for k, v := range objmeta {
		w.Header().Add(k, v)
	}
}

// GET /Rversion/Rhealth
func (t *targetrunner) httphealth(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	from := query.Get(URLParamFromID)

	aborted, running := t.xactinp.isAbortedOrRunningRebalance()
	status := &thealthstatus{IsRebalancing: aborted || running}

	jsbytes, err := json.Marshal(status)
	assert(err == nil, err)
	ok := t.writeJSON(w, r, jsbytes, "thealthstatus")
	if ok && from == t.proxysi.DaemonID {
		t.kalive.timestamp(t.proxysi.DaemonID)
	}
}

//  /Rversion/Rpush/bucket-name
func (t *targetrunner) pushhdlr(w http.ResponseWriter, r *http.Request) {
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
func (t *targetrunner) renamelocalbucket(bucketFrom, bucketTo string, version int64, lbmapif map[string]interface{}) (errstr string) {
	t.lbmap.lock()
	defer t.lbmap.unlock()

	if t.lbmap.version() != version {
		glog.Warning("lbmap version %d != %d - proceeding to rename anyway", t.lbmap.version(), version)
	}
	// FIXME: TODO: (temp shortcut)
	//              must be two-phase Tx with the 1st phase to copy (and rollback if need be),
	//              and the 2nd phase - to confirm and delete old
	//              there's also the risk of races vs ongoing GETs and PUTs generating workfiles, etc.

	// 1. lbmap = lbmap-from-the-request
	t.lbmap.Version = version
	t.lbmap.LBmap = make(map[string]string, len(lbmapif))
	for k, v := range lbmapif {
		t.lbmap.LBmap[k] = v.(string)
	}
	for mpath := range ctx.mountpaths.Available {
		todir := filepath.Join(makePathLocal(mpath), bucketTo)
		if err := CreateDir(todir); err != nil {
			errstr = fmt.Sprintf("Failed to create dir %s, err: %v", todir, err)
			return
		}
	}
	// 2. rename
	t.lbmap.add(bucketTo) // must be done before
	for mpath := range ctx.mountpaths.Available {
		fromdir := filepath.Join(makePathLocal(mpath), bucketFrom)
		if errstr = t.renameOne(fromdir, bucketFrom, bucketTo); errstr != "" {
			glog.Errorln(errstr) /* beyond the point of no return */
		}
	}
	// 3. delete
	for mpath := range ctx.mountpaths.Available {
		fromdir := filepath.Join(makePathLocal(mpath), bucketFrom)
		if err := os.RemoveAll(fromdir); err != nil {
			glog.Errorf("Failed to remove dir %s", fromdir)
		}
	}
	// 4. cleanup - NOTE that the local lbmap version is advanced by +=2 at this point
	t.lbmap.del(bucketFrom)
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
	newfqn := renctx.t.fqn(renctx.bucketTo, objname)
	dirname := filepath.Dir(newfqn)
	if err := CreateDir(dirname); err != nil {
		return fmt.Errorf("Error creating local dir %s, err: %v", dirname, err)
	}
	return os.Rename(fqn, newfqn)
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
	defer response.Body.Close()
	var (
		nhobj   cksumvalue
		errstr  string
		size    int64
		hval    = response.Header.Get(HeaderDfcChecksumVal)
		htype   = response.Header.Get(HeaderDfcChecksumType)
		hdhobj  = newcksumvalue(htype, hval)
		version = response.Header.Get(HeaderDfcObjVersion)
		fqn     = t.fqn(bucket, objname)
		getfqn  = t.fqn2workfile(fqn)
		inmem   = false // TODO: optimize
	)
	if _, nhobj, size, errstr = t.receive(getfqn, inmem, objname, "", hdhobj, response.Body); errstr != "" {
		glog.Errorf(errstr)
		return
	}
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

func (t *targetrunner) coldget(bucket, objname string, prefetch bool) (props *objectProps, errstr string, errcode int) {
	var (
		fqn        = t.fqn(bucket, objname)
		uname      = t.uname(bucket, objname)
		getfqn     = t.fqn2workfile(fqn)
		versioncfg = &ctx.config.Ver
		errv       = ""
		vchanged   = false
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
	if !coldget && eexists == "" && !t.islocalBucket(bucket) && versioncfg.ValidateWarmGet && version != "" && t.versioningConfigured(bucket) {
		vchanged, errv, _ = t.checkCloudVersion(bucket, objname, version)
		if errv == "" {
			coldget = vchanged
		}
	}
	if !coldget && eexists == "" {
		props = &objectProps{version: version, size: size}
		xxhashval, _ := Getxattr(fqn, xattrXXHashVal)
		if xxhashval != nil {
			cksumcfg := &ctx.config.Cksum
			props.nhobj = newcksumvalue(cksumcfg.Checksum, string(xxhashval))
		}
		glog.Infof("cold GET race: %s/%s, size=%d, version=%s - nothing to do", bucket, objname, size, version)
		goto ret
	}
	// cold
	if props, errstr, errcode = getcloudif().getobj(getfqn, bucket, objname); errstr != "" {
		t.rtnamemap.unlockname(uname, true)
		return
	}
	defer func() {
		if errstr != "" {
			t.rtnamemap.unlockname(uname, true)
			if err := os.Remove(getfqn); err != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, getfqn, err)
			}
			t.runFSKeeper(fmt.Errorf("%s", fqn))
		}
	}()
	if err := os.Rename(getfqn, fqn); err != nil {
		errstr = fmt.Sprintf("Unexpected failure to rename %s => %s, err: %v", getfqn, fqn, err)
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
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "bytesloaded",
					Value: props.size,
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "vchanged",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "bytesvchanged",
					Value: props.size,
				},
			)

			t.statsif.addMany("numcoldget", int64(1), "bytesloaded", props.size, "bytesvchanged", props.size, "numvchanged", int64(1))
		} else {
			t.statsdC.Send("coldget",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "bytesloaded",
					Value: props.size,
				},
			)

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
			if t.islocalBucket(bucket) {
				errstr = fmt.Sprintf("GET local: %s (%s/%s) %s", fqn, bucket, objname, doesnotexist)
				return
			}
			coldget = true
		case os.IsPermission(err):
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", fqn)
		default:
			errstr = fmt.Sprintf("Failed to fstat %s, err: %v", fqn, err)
			t.runFSKeeper(fmt.Errorf("%s", fqn))
		}
		return
	}
	size = finfo.Size()
	if bytes, errs := Getxattr(fqn, xattrObjVersion); errs == "" {
		version = string(bytes)
	} else {
		t.runFSKeeper(fmt.Errorf("%s", fqn))
	}
	return
}

func (t *targetrunner) lookupRemotely(bucket, objname string) (tsi *daemonInfo) {
	if len(t.smap.Smap) < 2 {
		return
	}
	wg := &sync.WaitGroup{}
	resch := make(chan *daemonInfo, len(t.smap.Smap)-1)
	for sid, si := range t.smap.Smap {
		if sid == t.si.DaemonID {
			continue
		}
		wg.Add(1)
		go func(si *daemonInfo) {
			defer wg.Done()
			url := si.DirectURL + "/" + Rversion + "/" + Robjects + "/" + bucket + "/" + objname
			_, err, _, _ := t.call(si, url, http.MethodHead, nil, ctx.config.Timeout.MaxKeepalive)
			if err == nil {
				resch <- si
			} else {
				resch <- nil
			}
		}(si)
	}
	wg.Wait()
	close(resch)
	for res := range resch {
		if res != nil {
			tsi = res
			break
		}
	}
	return
}

// should not be called for local buckets
func (t *targetrunner) listCachedObjects(bucket string, msg *GetMsg) (outbytes []byte, errstr string, errcode int) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		return nil, err.Error(), 0
	}

	outbytes, err = json.Marshal(reslist)
	return
}

func (t *targetrunner) prepareLocalObjectList(bucket string, msg *GetMsg) (bucketList *BucketList, err error) {
	type mresp struct {
		infos *allfinfos
		err   error
	}
	ch := make(chan *mresp, len(ctx.mountpaths.Available))
	wg := &sync.WaitGroup{}
	isLocal := t.islocalBucket(bucket)

	// function to traverse one mountpoint
	walkMpath := func(dir string) {
		defer wg.Done()
		r := &mresp{t.newFileWalk(bucket, msg), nil}

		if _, err = os.Stat(dir); err != nil {
			if !os.IsNotExist(err) {
				r.err = err
			}
			// it means there was no PUT(for local bucket) or GET(for cloud bucket - cache is empty) yet
			// Not an error, just skip the path
			ch <- r
			return
		}

		r.infos.rootLength = len(dir) + 1 // +1 for separator between bucket and filename
		if err = filepath.Walk(dir, r.infos.listwalkf); err != nil {
			glog.Errorf("Failed to traverse path %q, err: %v", dir, err)
			r.err = err
		}
		ch <- r
	}

	// Traverse all mountpoints in parallel.
	// If any mountpoint traversing fails others keep running until they complete.
	// But in this case all collected data is thrown away because the partial result
	// makes paging inconsistent
	for mpath := range ctx.mountpaths.Available {
		wg.Add(1)
		localdir := ""
		if isLocal {
			localdir = filepath.Join(makePathLocal(mpath), bucket)
		} else {
			localdir = filepath.Join(makePathCloud(mpath), bucket)
		}

		go walkMpath(localdir)
	}
	wg.Wait()
	close(ch)

	// combine results into one long list
	// real size of page is set in newFileWalk, so read it from any of results inside loop
	pageSize := DefaultPageSize
	allfinfos := make([]*BucketEntry, 0, 0)
	fileCount := 0
	for r := range ch {
		if r.err != nil {
			t.runFSKeeper(r.err)
			return nil, r.err
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

	bucketList = &BucketList{
		Entries:    allfinfos,
		PageMarker: marker,
	}
	return bucketList, nil
}

func (t *targetrunner) getbucketnames(w http.ResponseWriter, r *http.Request) {
	buckets, errstr, errcode := getcloudif().getbucketnames()
	if errstr != "" {
		if errcode == 0 {
			t.invalmsghdlr(w, r, errstr)
		} else {
			t.invalmsghdlr(w, r, errstr, errcode)
		}
		return
	}
	bucketnames := &BucketNames{Cloud: buckets, Local: make([]string, 0, 64)}
	for bucket := range t.lbmap.LBmap {
		bucketnames.Local = append(bucketnames.Local, bucket)
	}
	jsbytes, err := json.Marshal(bucketnames)
	assert(err == nil, err)
	t.writeJSON(w, r, jsbytes, "getbucketnames")
	return
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
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	islocal, errstr, errcode := t.checkLocalQueryParameter(bucket, r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	useCache, errstr, errcode := t.checkCacheQueryParameter(r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	msg := &GetMsg{}
	if t.readJSON(w, r, msg) != nil {
		return
	}
	if islocal {
		tag = "local"
		if errstr, ok = t.doLocalBucketList(w, r, bucket, msg); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
		return // ======================================>
	}
	// cloud bucket
	if useCache {
		tag = "cloud cached"
		jsbytes, errstr, errcode = t.listCachedObjects(bucket, msg)
	} else {
		tag = "cloud"
		jsbytes, errstr, errcode = getcloudif().listbucket(bucket, msg)
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
		fqn := ci.t.fqn(ci.bucket, relname)
		xxhex, errstr := Getxattr(fqn, xattrXXHashVal)
		if errstr == "" {
			fileInfo.Checksum = hex.EncodeToString(xxhex)
		}
	}
	if ci.needVersion {
		fqn := ci.t.fqn(ci.bucket, relname)
		version, errstr := Getxattr(fqn, xattrObjVersion)
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

// After putting a new version it updates xattr attrubutes for the object
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
	fqn := t.fqn(bucket, objname)
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
				glog.Infof("Existing %s/%s is valid: PUT is a no-op", bucket, objname)
				return
			}
		}
	}
	inmem := (ctx.config.Experimental.AckPut == AckWhenInMem)
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
	props := &objectProps{nhobj: nhobj}
	if sgl == nil {
		errstr, errcode = t.putCommit(bucket, objname, putfqn, fqn, props, false /*rebalance*/)
		if errstr == "" {
			delta := time.Since(started)
			t.statsdC.Send("put",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency",
					Value: float64(delta / time.Millisecond),
				},
			)

			lat := int64(delta / 1000)
			t.statsif.addMany("numput", int64(1), "putlatency", lat)
			if glog.V(4) {
				glog.Infof("PUT: %s/%s, %d µs", bucket, objname, lat)
			}
		}
		return
	}
	// FIXME: use xaction
	go t.sglToCloudAsync(sgl, bucket, objname, putfqn, fqn, props)
	return
}

func (t *targetrunner) sglToCloudAsync(sgl *SGLIO, bucket, objname, putfqn, fqn string, objprops *objectProps) {
	slab := selectslab(sgl.Size())
	buf := slab.alloc()
	defer func() {
		sgl.Free()
		slab.free(buf)
	}()
	// sgl => fqn sequence
	file, err := CreateFile(putfqn)
	if err != nil {
		t.runFSKeeper(fmt.Errorf("%s", putfqn))
		glog.Errorln("sglToCloudAsync: create", putfqn, err)
		return
	}
	reader := NewReader(sgl)
	written, err := io.CopyBuffer(file, reader, buf)
	if err != nil {
		t.runFSKeeper(fmt.Errorf("%s", putfqn))
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
	errstr, _ := t.putCommit(bucket, objname, putfqn, fqn, objprops, false /*rebalance*/)
	if errstr != "" {
		glog.Errorln("sglToCloudAsync: commit", errstr)
		return
	}
	glog.Infof("sglToCloudAsync: %s/%s", bucket, objname)
}

func (t *targetrunner) putCommit(bucket, objname, putfqn, fqn string,
	objprops *objectProps, rebalance bool) (errstr string, errcode int) {
	var (
		file          *os.File
		err           error
		renamed       bool
		isBucketLocal = t.islocalBucket(bucket)
	)
	defer func() {
		if errstr != "" && !os.IsNotExist(err) && !renamed {
			if err = os.Remove(putfqn); err != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", errstr, putfqn, err)
			}
			t.runFSKeeper(fmt.Errorf("%s", putfqn))
		}
	}()
	// cloud
	if !isBucketLocal && !rebalance {
		if file, err = os.Open(putfqn); err != nil {
			errstr = fmt.Sprintf("Failed to reopen %s err: %v", putfqn, err)
			return
		}
		if objprops.version, errstr, errcode = getcloudif().putobj(file, bucket, objname, objprops.nhobj); errstr != "" {
			_ = file.Close()
			return
		}
		if err = file.Close(); err != nil {
			glog.Errorf("Unexpected failure to close an already PUT file %s, err: %v", putfqn, err)
			_ = os.Remove(putfqn)
			return
		}
	}

	if isBucketLocal && t.versioningConfigured(bucket) {
		if objprops.version, errstr = t.increaseObjectVersion(fqn); errstr != "" {
			return
		}
	}

	// when all set and done:
	uname := t.uname(bucket, objname)
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)
	if err = os.Rename(putfqn, fqn); err != nil {
		errstr = fmt.Sprintf("Failed to rename %s => %s, err: %v", putfqn, fqn, err)
		return
	}
	renamed = true
	if errstr = t.finalizeobj(fqn, objprops); errstr != "" {
		glog.Errorf("finalizeobj %s/%s: %s (%+v)", bucket, objname, errstr, objprops)
		return
	}
	return
}

func (t *targetrunner) dorebalance(r *http.Request, from, to, bucket, objname string) (errstr string) {
	if t.si.DaemonID != from && t.si.DaemonID != to {
		errstr = fmt.Sprintf("File copy: %s is not the intended source %s nor the destination %s",
			t.si.DaemonID, from, to)
		return
	}
	var size int64
	fqn := t.fqn(bucket, objname)
	if t.si.DaemonID == from {
		//
		// the source
		//
		uname := t.uname(bucket, objname)
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
		si, ok := t.smap.Smap[to]
		if !ok {
			errstr = fmt.Sprintf("File copy: unknown destination %s (do syncsmap?)", to)
			return
		}
		size = finfo.Size()
		if errstr = t.sendfile(r.Method, bucket, objname, si, size, ""); errstr != "" {
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
			glog.Infof("Rebalance %s/%s from %s to %s (self)", bucket, objname, from, to)
		}
		putfqn := t.fqn2workfile(fqn)
		_, err := os.Stat(fqn)
		if err != nil && os.IsExist(err) {
			glog.Infof("File copy: %s already exists at the destination %s", fqn, t.si.DaemonID)
			return // not an error, nothing to do
		}
		var (
			hdhobj = newcksumvalue(r.Header.Get(HeaderDfcChecksumType), r.Header.Get(HeaderDfcChecksumVal))
			inmem  = false // TODO
			props  = &objectProps{version: r.Header.Get(HeaderDfcObjVersion)}
		)
		if _, props.nhobj, size, errstr = t.receive(putfqn, inmem, objname, "", hdhobj, r.Body); errstr != "" {
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
		errstr, _ = t.putCommit(bucket, objname, putfqn, fqn, props, true /*rebalance*/)
		if errstr == "" {
			t.statsdC.Send("rebalance.receive",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "files",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "bytes",
					Value: size,
				},
			)

			t.statsif.addMany("numrecvfiles", int64(1), "numrecvbytes", size)
		}
	}
	return
}

func (t *targetrunner) fildelete(bucket, objname string, evict bool) error {
	var (
		errstr  string
		errcode int
	)
	fqn := t.fqn(bucket, objname)
	uname := t.uname(bucket, objname)
	localbucket := t.islocalBucket(bucket)

	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	if !localbucket && !evict {
		errstr, errcode = getcloudif().deleteobj(bucket, objname)
		if errstr != "" {
			if errcode == 0 {
				return fmt.Errorf("%s", errstr)
			}
			return fmt.Errorf("%d: %s", errcode, errstr)
		}

		t.statsdC.Send("delete",
			statsd.Metric{
				Type:  statsd.Counter,
				Name:  "count",
				Value: 1,
			},
		)

		t.statsif.add("numdelete", 1)
	}

	finfo, err := os.Stat(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			if localbucket && !evict {
				return fmt.Errorf("DELETE local: file %s (local bucket %s, object %s) %s", fqn, bucket, objname, doesnotexist)
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
			t.statsdC.Send("evict",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "files",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "bytes",
					Value: finfo.Size(),
				},
			)

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
	fqn, uname := t.fqn(bucket, objname), t.uname(bucket, objname)
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
			t.statsdC.Send("rename",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
			)

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
func (t *targetrunner) sendfile(method, bucket, objname string, destsi *daemonInfo, size int64, newobjname string) string {
	var (
		xxhashval string
		errstr    string
		version   []byte
	)
	if size == 0 {
		return fmt.Sprintf("Unexpected: %s/%s size is zero", bucket, objname)
	}
	cksumcfg := &ctx.config.Cksum
	if newobjname == "" {
		newobjname = objname
	}
	fromid, toid := t.si.DaemonID, destsi.DaemonID // source=self and destination
	url := destsi.DirectURL + "/" + Rversion + "/" + Robjects + "/"
	url += bucket + "/" + newobjname
	url += fmt.Sprintf("?%s=%s&%s=%s", URLParamFromID, fromid, URLParamToID, toid)

	fqn := t.fqn(bucket, objname)
	file, err := os.Open(fqn)
	if err != nil {
		return fmt.Sprintf("Failed to open %q, err: %v", fqn, err)
	}
	defer file.Close()

	if version, errstr = Getxattr(fqn, xattrObjVersion); errstr != "" {
		glog.Errorf("Failed to read %q xattr %s, err %s", fqn, xattrObjVersion, errstr)
	}

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
		errstr = fmt.Sprintf("Failed to sendfile %s/%s %s => %s, err: %v", bucket, objname, fromid, toid, err)
		if response != nil && response.StatusCode > 0 {
			errstr += fmt.Sprintf(", status %s", response.Status)
		}
		return errstr
	}
	defer response.Body.Close()
	if _, err = ioutil.ReadAll(response.Body); err != nil {
		errstr = fmt.Sprintf("Failed to read sendfile response: %s/%s %s => %s, err: %v", bucket, objname, fromid, toid, err)
		if err == io.EOF {
			trailer := response.Trailer.Get("Error")
			if trailer != "" {
				errstr += fmt.Sprintf(", trailer: %s", trailer)
			}
		}
		return errstr
	}

	// stats
	t.statsdC.Send("rebalance.send",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "files",
			Value: 1,
		},
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "bytes",
			Value: size,
		},
	)

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

func (t *targetrunner) checkLocalQueryParameter(bucket string, r *http.Request) (islocal bool, errstr string, errcode int) {
	islocal = t.islocalBucket(bucket)
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

	if len(apitems) > 0 {
		switch apitems[0] {
		// PUT '{Smap}' /v1/daemon/(syncsmap|rebalance)
		case Rsyncsmap, Rebalance:
			newtargetid := r.URL.Query().Get(URLParamNewTargetID)
			t.httpdaeputSmap(w, r, apitems, newtargetid)
			return
		// PUT '{lbmap}' /v1/daemon/localbuckets
		case Rsynclb:
			t.httpdaeputLBMap(w, r, apitems)
			return
		// PUT /v1/daemon/proxy/newprimaryproxyid
		case Rproxy:
			t.httpdaesetprimaryproxy(w, r, apitems)
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
			_, lruxact := t.xactinp.findUnlocked(ActLRU)
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

func (t *targetrunner) httpdaeputSmap(w http.ResponseWriter, r *http.Request, apitems []string, newtargetid string) {
	var newsmap = &Smap{}
	if t.readJSON(w, r, newsmap) != nil {
		return
	}
	newlen := len(newsmap.Smap)
	glog.Infof("%s: new Smap version %d, num targets %d, newtargetid=%s", apitems[0], newsmap.Version, newlen, newtargetid)

	smapLock.Lock() //=======================================================

	curversion := t.smap.Version
	if curversion == newsmap.Version {
		smapLock.Unlock()
		return
	}
	if curversion > newsmap.Version {
		smapLock.Unlock()
		err := fmt.Errorf("Warning: attempt to downgrade Smap verion %d to %d", curversion, newsmap.Version)
		glog.Errorln(err)
		t.kalive.onerr(err, 0)
		return
	}
	oldlen := len(t.smap.Smap)

	// check whether this target is present in the new Smap
	// rebalance? (nothing to rebalance if the new map is a strict subset of the old)
	// assign proxysi
	// log
	existentialQ, isSubset := false, true
	infoln := make([]string, 0, newlen)
	for id, si := range newsmap.Smap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			infoln = append(infoln, fmt.Sprintf("target: %s <= self", si))
		} else {
			infoln = append(infoln, fmt.Sprintf("target: %s", si))
		}
		if _, ok := t.smap.Smap[id]; !ok {
			isSubset = false
		}
	}
	assert(existentialQ)

	// t.smap = newsmap & set primary
	err := t.setPrimaryProxyAndSmapUnlocked(newsmap)

	// rebalancing x-action to get its own copy
	assert(t.smap == newsmap)
	smap4xaction := &Smap{}
	copyStruct(smap4xaction, t.smap)

	smapLock.Unlock() //=====================================================

	for _, ln := range infoln {
		glog.Infoln(ln)
	}
	if err != nil {
		s := fmt.Sprintf("Failed to set Primary Proxy to %s, err: %v", newsmap.ProxySI.DaemonID, err)
		t.invalmsghdlr(w, r, s)
		return
	}
	if apitems[0] == Rsyncsmap {
		return
	}
	if isSubset {
		if newlen != oldlen {
			assert(newlen < oldlen)
			glog.Infoln("nothing to rebalance: new Smap is a strict subset of the old")
		} else {
			glog.Infof("nothing to rebalance: num (%d) and IDs of the targets did not change", newlen)
		}
		return
	}
	if newtargetid != "" {
		if !ctx.config.Rebalance.Enabled { // config check
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
					currsmap := &Smap{}
					smapLock.Lock()
					copyStruct(currsmap, t.smap)
					smapLock.Unlock()
					t.runRebalance(currsmap, "")
				}
				go runRebalanceOnce.Do(f) // only once at startup
			}
			return
		}
	}

	// xaction
	go t.runRebalance(smap4xaction, newtargetid)
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
			for mpath := range ctx.mountpaths.Available {
				localbucketfqn := filepath.Join(makePathLocal(mpath), bucket)
				if err := os.RemoveAll(localbucketfqn); err != nil {
					glog.Errorf("Failed to destroy local bucket dir %q, err: %v", localbucketfqn, err)
				}
			}
		}
	}
	t.lbmap = newlbmap
	for mpath := range ctx.mountpaths.Available {
		for bucket := range t.lbmap.LBmap {
			localbucketfqn := filepath.Join(makePathLocal(mpath), bucket)
			if err := CreateDir(localbucketfqn); err != nil {
				glog.Errorf("Failed to create local bucket dir %q, err: %v", localbucketfqn, err)
			}
		}
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
	err = t.setPrimaryProxyLocked(proxyid, "" /* primaryToRemove */, prepare)
	if err != nil {
		s := fmt.Sprintf("Failed to Set Primary Proxy to %v: %v", proxyid, err)
		t.invalmsghdlr(w, r, s)
		return
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
		jsbytes, err = json.Marshal(t.smap)
		assert(err == nil, err)
	case GetWhatSmapVote:
		msg := SmapVoteMsg{
			VoteInProgress: false,
			Smap:           t.smap,
		}
		jsbytes, err := json.Marshal(msg)
		assert(err == nil, err)
		t.writeJSON(w, r, jsbytes, "httpdaeget")
	case GetWhatStats:
		rr := getstorstatsrunner()
		rr.Lock()
		jsbytes, err = json.Marshal(rr)
		rr.Unlock()
		assert(err == nil, err)
	default:
		s := fmt.Sprintf("Unexpected GetMsg <- JSON [%v]", msg)
		t.invalmsghdlr(w, r, s)
		return
	}
	t.writeJSON(w, r, jsbytes, "httpdaeget")
}

// management interface to register (unregistered) self
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
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

//====================== common for both cold GET and PUT ======================================
//
// on err: closes and removes the file; othwerise closes and returns the size;
// empty omd5 or oxxhash: not considered an exception even when the configuration says otherwise;
// xxhash is always preferred over md5
//
//==============================================================================================
func (t *targetrunner) receive(fqn string, inmem bool, objname, omd5 string, ohobj cksumvalue,
	reader io.Reader) (sgl *SGLIO, nhobj cksumvalue, written int64, errstr string) {
	var (
		err                  error
		file                 *os.File
		filewriter           io.Writer
		ohtype, ohval, nhval string
		cksumcfg             = &ctx.config.Cksum
	)
	// ack policy = memory
	if inmem {
		sgl = NewSGLIO(0)
		filewriter = sgl
	} else {
		if file, err = CreateFile(fqn); err != nil {
			t.runFSKeeper(fmt.Errorf("%s", fqn))
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
		t.runFSKeeper(fmt.Errorf("%s", fqn))
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

				t.statsdC.Send("error.badchecksum.xxhash",
					statsd.Metric{
						Type:  statsd.Counter,
						Name:  "count",
						Value: 1,
					},
					statsd.Metric{
						Type:  statsd.Counter,
						Name:  "bytes",
						Value: written,
					},
				)

				t.statsif.addMany("numbadchecksum", int64(1), "bytesbadchecksum", written)
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

			t.statsdC.Send("error.badchecksum.md5",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "bytes",
					Value: written,
				},
			)

			t.statsif.addMany("numbadchecksum", int64(1), "bytesbadchecksum", written)
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

//==============================================================================
//
// target's misc utilities and helpers
//
//==============================================================================
func (t *targetrunner) starttime() time.Time {
	return t.uxprocess.starttime
}

func (t *targetrunner) islocalBucket(bucket string) bool {
	_, ok := t.lbmap.LBmap[bucket]
	return ok
}

func (t *targetrunner) testingFSPpaths() bool {
	return ctx.config.TestFSP.Count > 0
}

func (t *targetrunner) uname(bucket, objname string) string {
	return bucket + objname
}

// (bucket, object) => (local hashed path, fully qualified name aka fqn)
func (t *targetrunner) fqn(bucket, objname string) string {
	mpath := hrwMpath(bucket + "/" + objname)
	if t.islocalBucket(bucket) {
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
	for mpath := range ctx.mountpaths.Available {
		if fn(makePathCloud(mpath) + "/") {
			ok = len(objname) > 0 && t.fqn(bucket, objname) == fqn
			break
		}
		if fn(makePathLocal(mpath) + "/") {
			ok = t.islocalBucket(bucket) && len(objname) > 0 && t.fqn(bucket, objname) == fqn
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

func (t *targetrunner) fspath2mpath() {
	for fp := range ctx.config.FSpaths {
		if len(fp) > 1 {
			fp = strings.TrimSuffix(fp, "/")
		}
		if _, err := os.Stat(fp); err != nil {
			glog.Fatalf("FATAL: fspath %q %s, err: %v", fp, doesnotexist, err)
		}
		statfs := syscall.Statfs_t{}
		if err := syscall.Statfs(fp, &statfs); err != nil {
			glog.Fatalf("FATAL: cannot statfs fspath %q, err: %v", fp, err)
		}
		mp := &mountPath{Path: fp, Fsid: statfs.Fsid}
		_, ok := ctx.mountpaths.Available[mp.Path]
		if ok {
			glog.Fatalf("FATAL: invalid config: duplicated fspath %q", fp)
		}
		ctx.mountpaths.Available[mp.Path] = mp
	}
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
		if t.testingFSPpaths() {
			mpath = filepath.Join(instpath, strconv.Itoa(i+1))
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
		_, ok := ctx.mountpaths.Available[mp.Path]
		assert(!ok)
		ctx.mountpaths.Available[mp.Path] = mp
	}
}

func (t *targetrunner) mpath2Fsid() (fsmap map[syscall.Fsid]string) {
	fsmap = make(map[syscall.Fsid]string, len(ctx.mountpaths.Available))
	for _, mountpath := range ctx.mountpaths.Available {
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

func (t *targetrunner) startupMpaths() {
	// fill-in mpaths
	ctx.mountpaths.Available = make(map[string]*mountPath, len(ctx.config.FSpaths))
	ctx.mountpaths.Offline = make(map[string]*mountPath, len(ctx.config.FSpaths))
	if t.testingFSPpaths() {
		glog.Infof("Warning: configuring %d fspaths for testing", ctx.config.TestFSP.Count)
		t.testCachepathMounts()
	} else {
		t.fspath2mpath()
		t.mpath2Fsid() // enforce FS uniqueness
	}

	for mpath := range ctx.mountpaths.Available {
		cloudbctsfqn := makePathCloud(mpath)
		if err := CreateDir(cloudbctsfqn); err != nil {
			glog.Fatalf("FATAL: cannot create cloud buckets dir %q, err: %v", cloudbctsfqn, err)
		}
		localbctsfqn := makePathLocal(mpath)
		if err := CreateDir(localbctsfqn); err != nil {
			glog.Fatalf("FATAL: cannot create local buckets dir %q, err: %v", localbctsfqn, err)
		}
	}
	// mpath config dir
	mpathconfigfqn := filepath.Join(ctx.config.Confdir, mpname)
	if ctx.config.TestFSP.Instance > 0 {
		instancedir := filepath.Join(ctx.config.Confdir, strconv.Itoa(ctx.config.TestFSP.Instance))
		if err := CreateDir(instancedir); err != nil {
			glog.Errorf("Failed to create instance confdir %q, err: %v", instancedir, err)
			return
		}
		mpathconfigfqn = filepath.Join(instancedir, mpname)
	}
	// load old/prev and compare
	var (
		changed bool
		old     = &mountedFS{}
	)
	old.Available, old.Offline = make(map[string]*mountPath), make(map[string]*mountPath)
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
		glog.Errorf("Detected change in the mpath configuration at %s", mpathconfigfqn)
		if b, err := json.MarshalIndent(old, "", "\t"); err == nil {
			glog.Errorln("OLD: ====================")
			glog.Errorln(string(b))
		}
		if b, err := json.MarshalIndent(ctx.mountpaths, "", "\t"); err == nil {
			glog.Errorln("NEW: ====================")
			glog.Errorln(string(b))
		}
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
	islocal := t.islocalBucket(bucket)
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
		if errstr = Setxattr(fqn, xattrXXHashVal, []byte(hval)); errstr != "" {
			return errstr
		}
	}
	if objprops.version != "" {
		errstr = Setxattr(fqn, xattrObjVersion, []byte(objprops.version))
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

	if vbytes, errstr = Getxattr(fqn, xattrObjVersion); errstr != "" {
		return
	}
	if currValue, err := strconv.Atoi(string(vbytes)); err != nil {
		newVersion = initialVersion
	} else {
		newVersion = fmt.Sprintf("%d", currValue+1)
	}

	return
}

// runFSKeeper wakes up FSKeeper and makes it to run filesystem check
// immediately if err != nil
func (t *targetrunner) runFSKeeper(err error) {
	if ctx.config.FSKeeper.Enabled {
		getfskeeper().onerr(err)
	}
}

// builds fqn of directory for local buckets from mountpath
func makePathLocal(basePath string) string {
	return filepath.Join(basePath, ctx.config.LocalBuckets)
}

// builds fqn of directory for cloud buckets from mountpath
func makePathCloud(basePath string) string {
	return filepath.Join(basePath, ctx.config.CloudBuckets)
}

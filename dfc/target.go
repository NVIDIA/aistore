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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
	"github.com/NVIDIA/dfcpub/dfc/util/readers"
	"github.com/NVIDIA/dfcpub/iosgl"
	"github.com/OneOfOne/xxhash"
	"github.com/json-iterator/go"
)

const (
	internalPageSize = 10000     // number of objects in a page for internal call between target and proxy to get atime/iscached
	MaxPageSize      = 64 * 1024 // max number of objects in a page (warning logged if requested page size exceeds this limit)
	workfileprefix   = ".~~~."
	doesnotexist     = "does not exist"
	maxBytesInMem    = 256 * KiB
)

type (
	allfinfos struct {
		t            *targetrunner
		files        []*api.BucketEntry
		prefix       string
		marker       string
		markerDir    string
		msg          *api.GetMsg
		lastFilePath string
		bucket       string
		fileCount    int
		rootLength   int
		limit        int
		needAtime    bool
		needCtime    bool
		needChkSum   bool
		needVersion  bool
	}

	uxprocess struct {
		starttime time.Time
		spid      string
		pid       int64
	}

	thealthstatus struct {
		IsRebalancing bool `json:"is_rebalancing"`
		// NOTE: include core stats and other info as needed
	}

	renamectx struct {
		bucketFrom string
		bucketTo   string
		t          *targetrunner
	}

	regstate struct {
		sync.Mutex
		disabled bool // target was unregistered by internal event (e.g, all mountpaths are down)
	}

	fspathDispatcher interface {
		DisableMountpath(path string, why string) (disabled, exists bool)
	}
)

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif        cloudif // multi-cloud backend
	uxprocess      *uxprocess
	rtnamemap      *rtnamemap
	prefetchQueue  chan filesWithDeadline
	authn          *authManager
	clusterStarted int64
	regstate       regstate // registration state - the state of being registered (with the proxy) or maybe not
	fsprg          fsprungroup
	readahead      readaheadif
}

// start target runner
func (t *targetrunner) run() error {
	var ereg error
	t.httprunner.init(getstorstatsrunner(), false)
	t.httprunner.keepalive = gettargetkeepalive()

	dryinit()

	t.rtnamemap = newrtnamemap(128) // lock/unlock name

	bucketmd := newBucketMD()
	t.bmdowner.put(bucketmd)

	smap := newSmap()
	smap.Tmap[t.si.DaemonID] = t.si
	t.smapowner.put(smap)
	for i := 0; i < maxRetrySeconds; i++ {
		var status int
		if status, ereg = t.register(false, defaultTimeout); ereg != nil {
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

	go t.pollClusterStarted()

	err := t.createBucketDirs("local", ctx.config.LocalBuckets, makePathLocal)
	if err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	err = t.createBucketDirs("cloud", ctx.config.CloudBuckets, makePathCloud)
	if err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	t.detectMpathChanges()

	// cloud provider
	if ctx.config.CloudProvider == api.ProviderAmazon {
		// TODO: sessions
		t.cloudif = &awsimpl{t}

	} else {
		assert(ctx.config.CloudProvider == api.ProviderGoogle)
		t.cloudif = &gcpimpl{t}
	}

	// prefetch
	t.prefetchQueue = make(chan filesWithDeadline, prefetchChanSize)

	t.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

	//
	// REST API: register storage target's handler(s) and start listening
	//

	// Public network
	t.registerPublicNetHandler(api.URLPath(api.Version, api.Buckets)+"/", t.bucketHandler)
	t.registerPublicNetHandler(api.URLPath(api.Version, api.Objects)+"/", t.objectHandler)
	t.registerPublicNetHandler(api.URLPath(api.Version, api.Daemon), t.daemonHandler)
	t.registerPublicNetHandler(api.URLPath(api.Version, api.Push)+"/", t.pushHandler)
	t.registerPublicNetHandler(api.URLPath(api.Version, api.Tokens), t.tokenHandler)
	t.registerPublicNetHandler("/", invalhdlr)

	// Internal network
	t.registerInternalNetHandler(api.URLPath(api.Version, api.Metasync), t.metasyncHandler)
	t.registerInternalNetHandler(api.URLPath(api.Version, api.Health), t.healthHandler)
	t.registerInternalNetHandler(api.URLPath(api.Version, api.Vote), t.voteHandler)
	if ctx.config.Net.UseIntra {
		t.registerInternalNetHandler("/", invalhdlr)
	}

	glog.Infof("Target %s is ready", t.si.DaemonID)
	glog.Flush()
	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	_ = t.initStatsD("dfctarget")
	sr := getstorstatsrunner()
	sr.Core.statsdC = &t.statsdC

	getfshealthchecker().SetDispatcher(t)

	return t.httprunner.run()
}

// stop gracefully
func (t *targetrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.name, err)
	sleep := t.xactinp.abortAll()
	t.rtnamemap.stop()
	if t.publicServer.s != nil {
		t.unregister() // ignore errors
	}

	t.httprunner.stop(err)
	if sleep {
		time.Sleep(time.Second)
	}
}

// target registration with proxy
func (t *targetrunner) register(keepalive bool, timeout time.Duration) (int, error) {
	var (
		newbucketmd bucketMD
		res         callResult
	)
	if !keepalive {
		res = t.join(false, nil)
	} else { // keepalive
		url, psi := t.getPrimaryURLAndSI()
		res = t.registerToURL(url, psi, timeout, false, nil, keepalive)
	}
	if res.err != nil {
		return res.status, res.err
	}
	// not being sent at cluster startup and keepalive..
	if len(res.outjson) > 0 {
		err := jsoniter.Unmarshal(res.outjson, &newbucketmd)
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
	smap := t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return 0, nil
	}
	args := callArgs{
		si: smap.ProxySI,
		req: reqArgs{
			method: http.MethodDelete,
			path:   api.URLPath(api.Version, api.Cluster, api.Daemon, t.si.DaemonID),
		},
		timeout: defaultTimeout,
	}
	res := t.call(args)
	return res.status, res.err
}

//===========================================================================================
//
// http handlers: data and metadata
//
//===========================================================================================

// verb /v1/buckets
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

// verb /v1/objects
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

// GET /v1/buckets/bucket-name
func (t *targetrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, api.Version, api.Buckets); apitems == nil {
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

// GET /v1/objects/bucket[+"/"+objname]
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *targetrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	var (
		nhobj                         cksumvalue
		bucket, objname, fqn          string
		uname, errstr, version        string
		size, rangeoff, rangelen      int64
		props                         *objectProps
		started                       time.Time
		errcode                       int
		coldget, vchanged, inNextTier bool
		err                           error
		file                          *os.File
		written                       int64
	)
	//
	// 1. start, validate, readahead
	//
	started = time.Now()
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, api.Version, api.Objects); apitems == nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	rangeoff, rangelen, errstr = t.offsetAndLength(query)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	bucketmd := t.bmdowner.get()
	islocal := bucketmd.islocal(bucket)
	fqn = t.fqn(bucket, objname, islocal)
	if !dryRun.disk {
		if x := query.Get(api.URLParamReadahead); x != "" { // FIXME
			t.readahead.ahead(fqn, rangeoff, rangelen)
			return
		}
	}
	if glog.V(4) {
		pid := query.Get(api.URLParamProxyID)
		glog.Infof("%s %s/%s <= %s", r.Method, bucket, objname, pid)
	}
	if redelta := t.redirectLatency(started, query); redelta != 0 {
		t.statsif.add("get.redir.μs", redelta)
	}
	errstr, errcode = t.checkIsLocal(bucket, bucketmd, query, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	//
	// 2. coldget, maybe
	//
	var (
		cksumcfg   = &ctx.config.Cksum
		versioncfg = &ctx.config.Ver
		ct         = t.contextWithAuth(r)
	)
	// lockname(ro)
	uname = uniquename(bucket, objname)
	t.rtnamemap.lockname(uname, false, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)

	// bucket-level checksumming should override global
	if bucketProps, _, defined := bucketmd.propsAndChecksum(bucket); defined {
		cksumcfg = &bucketProps.CksumConf
	}

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
	if coldget && !dryRun.disk {
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

	//
	// 3. read local, write http (note: coldget() keeps the read lock if successful)
	//
existslocally:
	var (
		sgl            *iosgl.SGL
		slab           *iosgl.Slab
		buf            []byte
		rangeReader    io.ReadSeeker
		reader         io.Reader
		rahsize        int64
		rahfqn, rahsgl = t.readahead.get(fqn)
		sendmore       bool
	)
	defer func() {
		rahfqn.got()
		t.rtnamemap.unlockname(uname, false)
		if file != nil {
			file.Close()
		}
		if buf != nil {
			slab.Free(buf)
		}
		if sgl != nil {
			sgl.Free()
		}
	}()

	cksumRange := cksumcfg.Checksum != ChecksumNone && rangelen > 0 && cksumcfg.EnableReadRangeChecksum
	if !coldget && !cksumRange && cksumcfg.Checksum != ChecksumNone {
		hashbinary, errstr := Getxattr(fqn, XattrXXHashVal)
		if errstr == "" && hashbinary != nil {
			nhobj = newcksumvalue(cksumcfg.Checksum, string(hashbinary))
		}
	}
	if nhobj != nil && !cksumRange {
		htype, hval := nhobj.get()
		w.Header().Add(api.HeaderDFCChecksumType, htype)
		w.Header().Add(api.HeaderDFCChecksumVal, hval)
	}
	if props != nil && props.version != "" {
		w.Header().Add(api.HeaderDFCObjVersion, props.version)
	}

	// loopback if disk IO is disabled
	if dryRun.disk {
		if !dryRun.network {
			rd := readers.NewRandReader(dryRun.size)
			if _, err = io.Copy(w, rd); err != nil {
				errstr = fmt.Sprintf("dry-run: failed to send random response, err: %v", err)
				t.statsif.add("err.n", 1)
				return
			}
		}

		delta := time.Since(started)
		t.statsif.addMany(namedVal64{"get.n", 1}, namedVal64{"get.μs", int64(delta)})
		return
	}
	if size == 0 {
		glog.Warningf("%s/%s size is 0 (zero)", bucket, objname)
		return
	}
	if rahsgl != nil {
		rahsize = rahsgl.Size()
		if rangelen == 0 {
			sendmore = rahsize < size
		} else {
			sendmore = rahsize < rangelen
		}
	}
	if rahsize == 0 || sendmore {
		file, err = os.Open(fqn)
		if err != nil {
			if os.IsPermission(err) {
				errstr = fmt.Sprintf("Permission to access %s denied, err: %v", fqn, err)
				t.invalmsghdlr(w, r, errstr, http.StatusForbidden)
			} else {
				errstr = fmt.Sprintf("Failed to open %s, err: %v", fqn, err)
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			}
			t.fshc(err, fqn)
			return
		}
	}

sendmore:
	if rahsgl != nil && rahsize > 0 {
		// reader = iosgl.NewReader(rahsgl) - NOTE
		reader = rahsgl
		slab = rahsgl.Slab()
		buf = slab.Alloc()
		glog.Infof("%s readahead %d", fqn, rahsize) // FIXME: DEBUG
	} else if rangelen == 0 {
		if rahsize > 0 {
			file.Seek(rahsize, io.SeekStart)
		}
		reader = file
		slab = iosgl.SelectSlab(size)
		buf = slab.Alloc()
	} else {
		if rahsize > 0 {
			rangeoff += rahsize
			rangelen -= rahsize
		}
		slab = iosgl.SelectSlab(rangelen)
		buf = slab.Alloc()
		if cksumRange {
			assert(rahsize == 0, "NOT IMPLEMENTED YET") // TODO
			var cksum string
			cksum, sgl, rangeReader, errstr = t.rangeCksum(file, fqn, rangeoff, rangelen, buf)
			if errstr != "" {
				glog.Errorln(t.errHTTP(r, errstr, http.StatusInternalServerError))
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return

			}
			reader = rangeReader
			w.Header().Add(api.HeaderDFCChecksumType, cksumcfg.Checksum)
			w.Header().Add(api.HeaderDFCChecksumVal, cksum)
		} else {
			reader = io.NewSectionReader(file, rangeoff, rangelen)
		}
	}

	if !dryRun.network {
		written, err = io.CopyBuffer(w, reader, buf)
		errstr = fmt.Sprintf("Failed to GET %s, err: %v", fqn, err)
	} else {
		written, err = io.CopyBuffer(ioutil.Discard, reader, buf)
		errstr = fmt.Sprintf("dry-run: failed to read/discard %s, err: %v", fqn, err)
	}
	if err != nil {
		glog.Errorln(t.errHTTP(r, errstr, http.StatusInternalServerError))
		t.fshc(err, fqn)
		t.statsif.add("err.n", 1)
		return
	}
	if sendmore {
		rahsgl = nil
		sendmore = false
		goto sendmore
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
	t.statsif.addMany(namedVal64{"get.n", 1}, namedVal64{"get.μs", int64(delta)})
}

func (t *targetrunner) rangeCksum(file *os.File, fqn string, offset, length int64, buf []byte) (
	cksum string, sgl *iosgl.SGL, rangeReader io.ReadSeeker, errstr string) {
	rangeReader = io.NewSectionReader(file, offset, length)
	xx := xxhash.New64()
	if length <= maxBytesInMem {
		sgl = iosgl.NewSGL(uint64(length))
		_, err := ReceiveAndChecksum(sgl, rangeReader, buf, xx)
		if err != nil {
			errstr = fmt.Sprintf("failed to read byte range, offset:%d, length:%d from %s, err: %v", offset, length, fqn, err)
			t.fshc(err, fqn)
			return
		}
		// overriding rangeReader here to read from the sgl
		rangeReader = iosgl.NewReader(sgl)
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

func (t *targetrunner) offsetAndLength(query url.Values) (offset, length int64, errstr string) {
	offsetStr, lengthStr := query.Get(api.URLParamOffset), query.Get(api.URLParamLength)
	if offsetStr == "" && lengthStr == "" {
		return
	}
	s := fmt.Sprintf("Invalid offset [%s] and/or length [%s]", offsetStr, lengthStr)
	if offsetStr == "" || lengthStr == "" {
		errstr = s
		return
	}
	o, err1 := strconv.ParseInt(url.QueryEscape(offsetStr), 10, 64)
	l, err2 := strconv.ParseInt(url.QueryEscape(lengthStr), 10, 64)
	if err1 != nil || err2 != nil || o < 0 || l <= 0 {
		errstr = s
		return
	}
	offset, length = o, l
	return
}

// PUT /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, api.Version, api.Objects); apitems == nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	from, to := query.Get(api.URLParamFromID), query.Get(api.URLParamToID)
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
		if redelta := t.redirectLatency(time.Now(), query); redelta != 0 {
			t.statsif.add("put.redir.μs", redelta)
		}
		// PUT
		pid := query.Get(api.URLParamProxyID)
		if glog.V(4) {
			glog.Infof("%s %s/%s <= %s", r.Method, bucket, objname, pid)
		}
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

// DELETE { action} /v1/objects/bucket-name
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket  string
		msg     api.ActionMsg
		started = time.Now()
		ok      = true
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, api.Version, api.Buckets); apitems == nil {
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
		err = jsoniter.Unmarshal(b, &msg)
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

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname string
		msg             api.ActionMsg
		evict           bool
		started         = time.Now()
		ok              = true
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, api.Version, api.Objects); apitems == nil {
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
		err = jsoniter.Unmarshal(b, &msg)
		if err == nil {
			evict = (msg.Action == api.ActEvict)
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

// POST /v1/buckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var msg api.ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case api.ActPrefetch:
		t.prefetchfiles(w, r, msg)
	case api.ActRenameLB:
		apitems := t.restAPIItems(r.URL.Path, 5)
		if apitems = t.checkRestAPI(w, r, apitems, 1, api.Version, api.Buckets); apitems == nil {
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
			glog.Warningf("bucket-metadata version %d != %d - proceeding to rename anyway", bucketmd.version(), version)
		}
		clone := bucketmd.clone()
		if errstr := t.renamelocalbucket(bucketFrom, bucketTo, props, clone); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		glog.Infof("renamed local bucket %s => %s, bucket-metadata version %d", bucketFrom, bucketTo, clone.version())
	case api.ActListObjects:
		apitems := t.restAPIItems(r.URL.Path, 5)
		if apitems = t.checkRestAPI(w, r, apitems, 1, api.Version, api.Buckets); apitems == nil {
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
			t.statsif.addMany(namedVal64{"lst.n", 1}, namedVal64{"lst.μs", int64(delta)})
			if glog.V(3) {
				glog.Infof("LIST %s: %s, %d µs", tag, lbucket, int64(delta/time.Microsecond))
			}
		}
	case api.ActRechecksum:
		apitems := t.restAPIItems(r.URL.Path, 5)
		if apitems = t.checkRestAPI(w, r, apitems, 1, api.Version, api.Buckets); apitems == nil {
			return
		}
		bucket := apitems[0]
		if !t.validatebckname(w, r, bucket) {
			return
		}
		// re-checksum the bucket and return
		t.runRechecksumBucket(bucket)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// POST /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg api.ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case api.ActRename:
		t.renamefile(w, r, msg)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket      string
		islocal     bool
		errstr      string
		errcode     int
		bucketprops simplekvs
		cksumcfg    *cksumconfig
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, api.Version, api.Buckets); apitems == nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(api.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	bucketmd := t.bmdowner.get()
	islocal = bucketmd.islocal(bucket)
	errstr, errcode = t.checkIsLocal(bucket, bucketmd, query, islocal)
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
		bucketprops[api.HeaderCloudProvider] = api.ProviderDFC
		bucketprops[api.HeaderVersioning] = VersionLocal
	}
	// double check if we support versioning internally for the bucket
	if !t.versioningConfigured(bucket) {
		bucketprops[api.HeaderVersioning] = VersionNone
	}

	for k, v := range bucketprops {
		w.Header().Add(k, v)
	}

	props, _, defined := bucketmd.propsAndChecksum(bucket)
	// include checksumming settings in the response
	cksumcfg = &ctx.config.Cksum
	if defined {
		cksumcfg = &props.CksumConf
	}

	w.Header().Add(api.HeaderNextTierURL, props.NextTierURL)
	w.Header().Add(api.HeaderReadPolicy, props.ReadPolicy)
	w.Header().Add(api.HeaderWritePolicy, props.WritePolicy)
	w.Header().Add(api.HeaderBucketChecksumType, cksumcfg.Checksum)
	w.Header().Add(api.HeaderBucketValidateColdGet, strconv.FormatBool(cksumcfg.ValidateColdGet))
	w.Header().Add(api.HeaderBucketValidateWarmGet, strconv.FormatBool(cksumcfg.ValidateWarmGet))
	w.Header().Add(api.HeaderBucketValidateRange, strconv.FormatBool(cksumcfg.EnableReadRangeChecksum))
}

// HEAD /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname, errstr string
		islocal, checkCached    bool
		errcode                 int
		objmeta                 simplekvs
	)
	checkCached, _ = parsebool(r.URL.Query().Get(api.URLParamCheckCached))
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, api.Version, api.Objects); apitems == nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(api.URLParamProxyID)
		glog.Infof("%s %s/%s <= %s", r.Method, bucket, objname, pid)
	}
	bucketmd := t.bmdowner.get()
	islocal = bucketmd.islocal(bucket)
	errstr, errcode = t.checkIsLocal(bucket, bucketmd, query, islocal)
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

// handler for: /v1/tokens
func (t *targetrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		t.httpTokenDelete(w, r)
	default:
		invalhdlr(w, r)
	}
}

// [METHOD] /v1/metasync
func (t *targetrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		t.metasyncHandlerPut(w, r)
	default:
		invalhdlr(w, r)
	}
}

// PUT /v1/metasync
func (t *targetrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	var payload = make(simplekvs)
	if err := t.readJSON(w, r, &payload); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	newsmap, actionsmap, errstr := t.extractSmap(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newsmap != nil {
		errstr = t.receiveSmap(newsmap, actionsmap)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
	}

	newbucketmd, actionlb, errstr := t.extractbucketmd(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newbucketmd != nil {
		if errstr = t.receiveBucketMD(newbucketmd, actionlb, "receive"); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
	}

	revokedTokens, errstr := t.extractRevokedTokenList(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	t.authn.updateRevokedList(revokedTokens)
}

// GET /v1/health
func (t *targetrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	from := query.Get(api.URLParamFromID)

	aborted, running := t.xactinp.isAbortedOrRunningRebalance()
	status := &thealthstatus{IsRebalancing: aborted || running}

	jsbytes, err := jsoniter.Marshal(status)
	assert(err == nil, err)
	if ok := t.writeJSON(w, r, jsbytes, "thealthstatus"); !ok {
		return
	}

	t.keepalive.heardFrom(from, false)
}

// [METHOD] /v1/push/bucket-name
func (t *targetrunner) pushHandler(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, api.Version, api.Push); apitems == nil {
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
		err = jsoniter.Unmarshal(objnamebytes, &objnames)
		if err != nil {
			s := fmt.Sprintf("Could not unmarshal objnames: %v", err)
			t.invalmsghdlr(w, r, s)
			return
		}

		for _, objname := range objnames {
			err := pusher.Push(api.URLPath(api.Version, api.Objects, bucket, objname), nil)
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
		// Create directory for new local bucket
		toDir := filepath.Join(makePathLocal(mpathInfo.Path), bucketTo)
		if err := CreateDir(toDir); err != nil {
			ch <- fmt.Sprintf("Failed to create dir %s, error: %v", toDir, err)
			continue
		}

		wg.Add(1)
		fromdir := filepath.Join(makePathLocal(mpathInfo.Path), bucketFrom)
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
	bucket, objname, err := renctx.t.fqn2bckobj(fqn)
	if err == nil {
		if bucket != renctx.bucketFrom {
			return fmt.Errorf("Unexpected: bucket %s != %s bucketFrom", bucket, renctx.bucketFrom)
		}
	}
	if errstr := renctx.t.renameobject(bucket, objname, renctx.bucketTo, objname); errstr != "" {
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

	geturl := fmt.Sprintf("%s%s?%s=%t", neighsi.PublicNet.DirectURL, r.URL.Path, api.URLParamLocal, islocal)
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
		hval    = response.Header.Get(api.HeaderDFCChecksumVal)
		htype   = response.Header.Get(api.HeaderDFCChecksumType)
		hdhobj  = newcksumvalue(htype, hval)
		version = response.Header.Get(api.HeaderDFCObjVersion)
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

	// check if bucket-level checksumming configuration should override cluster-level configuration
	bucketProps, _, defined := bucketmd.propsAndChecksum(bucket)
	if defined {
		cksumcfg = &bucketProps.CksumConf
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
			props.nhobj = newcksumvalue(cksumcfg.Checksum, string(xxhashval))
		}
		glog.Infof("cold GET race: %s/%s, size=%d, version=%s - nothing to do", bucket, objname, size, version)
		goto ret
	}
	// cold
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
			t.statsif.addMany(namedVal64{"get.cold.n", 1}, namedVal64{"get.cold.size", props.size},
				namedVal64{"vchange.size", props.size}, namedVal64{"vchange.n", 1})
		} else {
			t.statsif.addMany(namedVal64{"get.cold.n", 1}, namedVal64{"get.cold.size", props.size})
		}
		t.rtnamemap.downgradelock(uname)
	}
	return
}

func (t *targetrunner) lookupLocally(bucket, objname, fqn string) (coldget bool, size int64, version, errstr string) {
	if dryRun.disk {
		return false, dryRun.size, "1", ""
	}

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
		api.URLPath(api.Version, api.Objects, bucket, objname),
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
func (t *targetrunner) listCachedObjects(bucket string, msg *api.GetMsg) (outbytes []byte, errstr string, errcode int) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		return nil, err.Error(), 0
	}

	outbytes, err = jsoniter.Marshal(reslist)
	if err != nil {
		return nil, err.Error(), 0
	}
	return
}

func (t *targetrunner) prepareLocalObjectList(bucket string, msg *api.GetMsg) (*api.BucketList, error) {
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
	pageSize := api.DefaultPageSize
	allfinfos := make([]*api.BucketEntry, 0)
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

	bucketList := &api.BucketList{
		Entries:    allfinfos,
		PageMarker: marker,
	}

	if strings.Contains(msg.GetProps, api.GetTargetURL) {
		for _, e := range bucketList.Entries {
			e.TargetURL = t.si.PublicNet.DirectURL
		}
	}

	return bucketList, nil
}

func (t *targetrunner) getbucketnames(w http.ResponseWriter, r *http.Request) {
	bucketmd := t.bmdowner.get()

	bucketnames := &api.BucketNames{
		Local: make([]string, 0, len(bucketmd.LBmap)),
		Cloud: make([]string, 0, 64),
	}
	for bucket := range bucketmd.LBmap {
		bucketnames.Local = append(bucketnames.Local, bucket)
	}

	q := r.URL.Query()
	localonly, _ := parsebool(q.Get(api.URLParamLocal))
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

	jsbytes, err := jsoniter.Marshal(bucketnames)
	assert(err == nil, err)
	t.writeJSON(w, r, jsbytes, "getbucketnames")
}

func (t *targetrunner) doLocalBucketList(w http.ResponseWriter, r *http.Request, bucket string, msg *api.GetMsg) (errstr string, ok bool) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		errstr = fmt.Sprintf("List local bucket %s failed, err: %v", bucket, err)
		return
	}
	jsbytes, err := jsoniter.Marshal(reslist)
	assert(err == nil, err)
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

// List bucket returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string, actionMsg *api.ActionMsg) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(api.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	bucketmd := t.bmdowner.get()
	islocal := bucketmd.islocal(bucket)
	errstr, errcode = t.checkIsLocal(bucket, bucketmd, query, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	useCache, errstr, errcode := t.checkCacheQueryParameter(r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	getMsgJson, err := jsoniter.Marshal(actionMsg.Value)
	if err != nil {
		errstr := fmt.Sprintf("Unable to marshal 'value' in request: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}

	var msg api.GetMsg
	err = jsoniter.Unmarshal(getMsgJson, &msg)
	if err != nil {
		errstr := fmt.Sprintf("Unable to unmarshal 'value' in request to a api.GetMsg: %v", actionMsg.Value)
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

func (t *targetrunner) newFileWalk(bucket string, msg *api.GetMsg) *allfinfos {
	// Marker is always a file name, so we need to strip filename from path
	markerDir := ""
	if msg.GetPageMarker != "" {
		markerDir = filepath.Dir(msg.GetPageMarker)
	}

	// A small optimization: set boolean variables need* to avoid
	// doing string search(strings.Contains) for every entry.
	ci := &allfinfos{t, // targetrunner
		make([]*api.BucketEntry, 0, api.DefaultPageSize),     // ls
		msg.GetPrefix,                                        // prefix
		msg.GetPageMarker,                                    // marker
		markerDir,                                            // markerDir
		msg,                                                  // GetMsg
		"",                                                   // lastFilePath - next page marker
		bucket,                                               // bucket
		0,                                                    // fileCount
		0,                                                    // rootLength
		api.DefaultPageSize,                                  // limit - maximun number of objects to return
		strings.Contains(msg.GetProps, api.GetPropsAtime),    // needAtime
		strings.Contains(msg.GetProps, api.GetPropsCtime),    // needCtime
		strings.Contains(msg.GetProps, api.GetPropsChecksum), // needChkSum
		strings.Contains(msg.GetProps, api.GetPropsVersion),  // needVersion
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
	fileInfo := &api.BucketEntry{Name: relname, Atime: "", IsCached: true}
	if ci.needAtime {
		atime, _, _ := getAmTimes(osfi)
		if ci.msg.GetTimeFormat == "" {
			fileInfo.Atime = atime.Format(api.RFC822)
		} else {
			fileInfo.Atime = atime.Format(ci.msg.GetTimeFormat)
		}
	}
	if ci.needCtime {
		t := osfi.ModTime()
		switch ci.msg.GetTimeFormat {
		case "":
			fallthrough
		case api.RFC822:
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
	_, _, err = ci.t.fqn2bckobj(fqn)
	if err != nil {
		glog.Error(err)
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
		sgl                        *iosgl.SGL
		started                    time.Time
	)
	started = time.Now()
	islocal := t.bmdowner.get().islocal(bucket)
	fqn := t.fqn(bucket, objname, islocal)
	putfqn := t.fqn2workfile(fqn)
	cksumcfg := &ctx.config.Cksum
	if bucketProps, _, defined := t.bmdowner.get().propsAndChecksum(bucket); defined {
		cksumcfg = &bucketProps.CksumConf
	}
	hdhobj = newcksumvalue(r.Header.Get(api.HeaderDFCChecksumType), r.Header.Get(api.HeaderDFCChecksumVal))

	if hdhobj != nil {
		htype, hval = hdhobj.get()
	}
	// optimize out if the checksums do match
	if hdhobj != nil && cksumcfg.Checksum != ChecksumNone && !dryRun.disk && !dryRun.network {
		file, err = os.Open(fqn)
		// exists - compute checksum and compare with the caller's
		if err == nil {
			slab := iosgl.SelectSlab(0)
			buf := slab.Alloc()
			if htype == ChecksumXXHash {
				xxhashval, errstr = ComputeXXHash(file, buf)
			} else {
				errstr = fmt.Sprintf("Unsupported checksum type %s", htype)
			}
			// not a critical error
			if errstr != "" {
				glog.Warningf("Warning: Bad checksum: %s: %v", fqn, errstr)
			}
			slab.Free(buf)
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
	if hval != "" && nhval != "" && hval != nhval && !dryRun.disk && !dryRun.network {
		errstr = fmt.Sprintf("Bad checksum: %s/%s %s %s... != %s...", bucket, objname, htype, hval[:8], nhval[:8])
		return
	}
	// commit
	props := &objectProps{nhobj: nhobj}
	if sgl == nil {
		if !dryRun.disk && !dryRun.network {
			errstr, errcode = t.putCommit(t.contextWithAuth(r), bucket, objname, putfqn, fqn, props, false /*rebalance*/)
		}
		if errstr == "" {
			delta := time.Since(started)
			t.statsif.addMany(namedVal64{"put.n", 1}, namedVal64{"put.μs", int64(delta)})
			if glog.V(4) {
				glog.Infof("PUT: %s/%s, %d µs", bucket, objname, int64(delta/time.Microsecond))
			}
		}
		return
	}
	// FIXME: use xaction
	go t.sglToCloudAsync(t.contextWithAuth(r), sgl, bucket, objname, putfqn, fqn, props)
	return
}

func (t *targetrunner) sglToCloudAsync(ct context.Context, sgl *iosgl.SGL, bucket, objname, putfqn, fqn string, objprops *objectProps) {
	slab := iosgl.SelectSlab(sgl.Size())
	buf := slab.Alloc()
	defer func() {
		sgl.Free()
		slab.Free(buf)
	}()
	// sgl => fqn sequence
	file, err := CreateFile(putfqn)
	if err != nil {
		t.fshc(err, putfqn)
		glog.Errorln("sglToCloudAsync: create", putfqn, err)
		return
	}
	reader := iosgl.NewReader(sgl)
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
		if glog.V(4) {
			glog.Infof("Rebalance %s/%s from %s to %s (self, %s, ver %d)", bucket, objname, from, to, fqn, ver)
		}
		putfqn := t.fqn2workfile(fqn)
		_, err := os.Stat(fqn)
		if err != nil && os.IsExist(err) {
			glog.Infof("File copy: %s already exists at the destination %s", fqn, t.si.DaemonID)
			return // not an error, nothing to do
		}
		var (
			hdhobj = newcksumvalue(r.Header.Get(api.HeaderDFCChecksumType), r.Header.Get(api.HeaderDFCChecksumVal))
			props  = &objectProps{version: r.Header.Get(api.HeaderDFCObjVersion)}
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
			t.statsif.addMany(namedVal64{"rx.n", 1}, namedVal64{"rx.size", size})
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

		t.statsif.add("del.n", 1)
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
			t.statsif.addMany(namedVal64{"lru.evict.n", 1}, namedVal64{"lru.evict.size", finfo.Size()})
		}
	}
	return nil
}

func (t *targetrunner) renamefile(w http.ResponseWriter, r *http.Request, msg api.ActionMsg) {
	var errstr string

	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, api.Version, api.Objects); apitems == nil {
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
			t.statsif.add("ren.n", 1)
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

func (t *targetrunner) prefetchfiles(w http.ResponseWriter, r *http.Request, msg api.ActionMsg) {
	detail := fmt.Sprintf(" (%s, %s, %T)", msg.Action, msg.Name, msg.Value)
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		t.invalmsghdlr(w, r, "Unexpected api.ActionMsg.Value format"+detail)
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

func (t *targetrunner) deletefiles(w http.ResponseWriter, r *http.Request, msg api.ActionMsg) {
	evict := msg.Action == api.ActEvict
	detail := fmt.Sprintf(" (%s, %s, %T)", msg.Action, msg.Name, msg.Value)
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		t.invalmsghdlr(w, r, "deletefiles: invalid api.ActionMsg.Value format"+detail)
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
	if newobjname == "" {
		newobjname = objname
	}
	if newbucket == "" {
		newbucket = bucket
	}
	fromid, toid := t.si.DaemonID, destsi.DaemonID // source=self and destination
	url := destsi.PublicNet.DirectURL + api.URLPath(api.Version, api.Objects, newbucket, newobjname)
	url += fmt.Sprintf("?%s=%s&%s=%s", api.URLParamFromID, fromid, api.URLParamToID, toid)
	islocal := t.bmdowner.get().islocal(bucket)
	cksumcfg := &ctx.config.Cksum
	if bucketProps, _, defined := t.bmdowner.get().propsAndChecksum(bucket); defined {
		cksumcfg = &bucketProps.CksumConf
	}
	fqn := t.fqn(bucket, objname, islocal)
	file, err := os.Open(fqn)
	if err != nil {
		return fmt.Sprintf("Failed to open %q, err: %v", fqn, err)
	}
	defer file.Close()

	if version, errstr = Getxattr(fqn, XattrObjVersion); errstr != "" {
		glog.Errorf("Failed to read %q xattr %s, err %s", fqn, XattrObjVersion, errstr)
	}

	slab := iosgl.SelectSlab(size)
	if cksumcfg.Checksum != ChecksumNone {
		assert(cksumcfg.Checksum == ChecksumXXHash, "invalid checksum type: '"+cksumcfg.Checksum+"'")
		buf := slab.Alloc()
		if xxhashval, errstr = ComputeXXHash(file, buf); errstr != "" {
			slab.Free(buf)
			return errstr
		}
		slab.Free(buf)
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
		request.Header.Set(api.HeaderDFCChecksumType, ChecksumXXHash)
		request.Header.Set(api.HeaderDFCChecksumVal, xxhashval)
	}
	if len(version) != 0 {
		request.Header.Set(api.HeaderDFCObjVersion, string(version))
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
	t.statsif.addMany(namedVal64{"tx.n", 1}, namedVal64{"tx.size", size})
	return ""
}

func (t *targetrunner) checkCacheQueryParameter(r *http.Request) (useCache bool, errstr string, errcode int) {
	useCacheStr := r.URL.Query().Get(api.URLParamCached)
	var err error
	if useCache, err = parsebool(useCacheStr); err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter: %s=%s (expecting: '' | true | false)",
			api.URLParamCached, useCacheStr)
		errcode = http.StatusInternalServerError
	}
	return
}

func (t *targetrunner) checkIsLocal(bucket string, bucketmd *bucketMD, q url.Values, islocal bool) (errstr string, errcode int) {
	proxylocalstr := q.Get(api.URLParamLocal)
	if proxylocalstr == "" {
		return
	}
	proxylocal, err := parsebool(proxylocalstr)
	if err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter for bucket %s: %s=%s (expecting bool)", bucket, api.URLParamLocal, proxylocalstr)
		errcode = http.StatusInternalServerError
		return
	}
	if islocal == proxylocal {
		return
	}
	errstr = fmt.Sprintf("islocalbucket(%s) mismatch: %t (proxy) != %t (target %s)", bucket, proxylocal, islocal, t.si.DaemonID)
	errcode = http.StatusInternalServerError
	if s := q.Get(api.URLParamBMDVersion); s != "" {
		if v, err := strconv.ParseInt(s, 0, 64); err != nil {
			glog.Errorf("Unexpected: failed to convert %s to int, err: %v", s, err)
		} else if v < bucketmd.version() {
			glog.Errorf("bucket-metadata v%d > v%d (primary)", bucketmd.version(), v)
		} else if v > bucketmd.version() { // fixup
			glog.Errorf("Warning: bucket-metadata v%d < v%d (primary) - updating...", bucketmd.version(), v)
			t.bmdVersionFixup()
			islocal := t.bmdowner.get().islocal(bucket)
			if islocal == proxylocal {
				glog.Infof("Success: updated bucket-metadata to v%d - resolved 'islocal' mismatch", bucketmd.version())
				errstr, errcode = "", 0
			}
		}
	}
	return
}

func (t *targetrunner) bmdVersionFixup() {
	smap := t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return
	}
	psi := smap.ProxySI
	q := url.Values{}
	q.Add(api.URLParamWhat, api.GetWhatBucketMeta)
	args := callArgs{
		si: psi,
		req: reqArgs{
			method: http.MethodGet,
			base:   psi.InternalNet.DirectURL,
			path:   api.URLPath(api.Version, api.Daemon),
			query:  q,
		},
		timeout: ctx.config.Timeout.CplaneOperation,
	}
	res := t.call(args)
	if res.err != nil {
		glog.Errorf("Failed to get-what=%s, err: %v", api.GetWhatBucketMeta, res.err)
		return
	}
	newbucketmd := &bucketMD{}
	err := jsoniter.Unmarshal(res.outjson, newbucketmd)
	if err != nil {
		glog.Errorf("Unexpected: failed to unmarshal get-what=%s response from %s, err: %v [%v]",
			api.GetWhatBucketMeta, psi.InternalNet.DirectURL, err, string(res.outjson))
		return
	}
	var msg = api.ActionMsg{Action: "get-what=" + api.GetWhatBucketMeta}
	t.receiveBucketMD(newbucketmd, &msg, "fixup")
}

//===========================
//
// control plane
//
//===========================

// [METHOD] /v1/daemon
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
	if apitems = t.checkRestAPI(w, r, apitems, 0, api.Version, api.Daemon); apitems == nil {
		return
	}
	if len(apitems) > 0 {
		switch apitems[0] {
		// PUT /v1/daemon/proxy/newprimaryproxyid
		case api.Proxy:
			t.httpdaesetprimaryproxy(w, r, apitems)
			return
		case api.SyncSmap:
			var newsmap = &Smap{}
			if t.readJSON(w, r, newsmap) != nil {
				return
			}
			if errstr := t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to sync Smap: %s", errstr))
			}
			glog.Infof("%s: %s v%d done", t.si.DaemonID, api.SyncSmap, newsmap.version())
			return
		case api.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
		}
	}
	//
	// other PUT /daemon actions
	//
	var msg api.ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case api.ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse api.ActionMsg value: Not a string"))
		} else if errstr := t.setconfig(msg.Name, value); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		} else {
			glog.Infof("setconfig %s=%s", msg.Name, value)
			if msg.Name == "lru_enabled" && value == "false" {
				_, lruxact := t.xactinp.findU(api.ActLRU)
				if lruxact != nil {
					if glog.V(3) {
						glog.Infof("Aborting LRU due to lru_enabled config change")
					}
					lruxact.abort()
				}
			}
		}
	case api.ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected api.ActionMsg <- JSON [%v]", msg)
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
	preparestr := query.Get(api.URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL Parameter: %v", api.URLParamPrepare, err)
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
	getWhat := r.URL.Query().Get(api.URLParamWhat)
	switch getWhat {
	case api.GetWhatConfig, api.GetWhatSmap, api.GetWhatBucketMeta, api.GetWhatSmapVote, api.GetWhatDaemonInfo:
		t.httprunner.httpdaeget(w, r)
	case api.GetWhatStats:
		rst := getstorstatsrunner()
		rst.RLock()
		jsbytes, err := jsoniter.Marshal(rst)
		rst.RUnlock()
		assert(err == nil, err)
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case api.GetWhatXaction:
		kind := r.URL.Query().Get(api.URLParamProps)
		if errstr := api.ValidateXactionQueryable(kind); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		xactionStatsRetriever := t.getXactionStatsRetriever(kind) // FIXME
		allXactionDetails := t.getXactionsByType(kind)
		jsbytes := xactionStatsRetriever.getStats(allXactionDetails)
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case api.GetWhatMountpaths:
		mpList := api.MountpathList{}
		availablePaths, disabledPaths := ctx.mountpaths.Mountpaths()
		mpList.Available = make([]string, len(availablePaths))
		mpList.Disabled = make([]string, len(disabledPaths))

		idx := 0
		for mpath := range availablePaths {
			mpList.Available[idx] = mpath
			idx++
		}
		idx = 0
		for mpath := range disabledPaths {
			mpList.Disabled[idx] = mpath
			idx++
		}
		jsbytes, err := jsoniter.Marshal(&mpList)
		if err != nil {
			s := fmt.Sprintf("Failed to marshal mountpaths: %v", err)
			t.invalmsghdlr(w, r, s)
			return
		}
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	default:
		t.httprunner.httpdaeget(w, r)
	}
}

func (t *targetrunner) getXactionStatsRetriever(kind string) XactionStatsRetriever {
	var xactionStatsRetriever XactionStatsRetriever

	// No need to handle default since kind has already been validated by
	// this point.
	switch kind {
	case api.XactionRebalance:
		xactionStatsRetriever = RebalanceTargetStats{}
	case api.XactionPrefetch:
		xactionStatsRetriever = PrefetchTargetStats{}
	}

	return xactionStatsRetriever
}

func (t *targetrunner) getXactionsByType(kind string) []XactionDetails {
	allXactionDetails := []XactionDetails{}
	for _, xaction := range t.xactinp.xactinp {
		if xaction.getkind() == kind {
			status := api.XactionStatusCompleted
			if !xaction.finished() {
				status = api.XactionStatusInProgress
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

// register target
// enable/disable mountpath
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems := t.restAPIItems(r.URL.Path, 5)
	if apiItems = t.checkRestAPI(w, r, apiItems, 0, api.Version, api.Daemon); apiItems == nil {
		return
	}

	if len(apiItems) > 0 {
		switch apiItems[0] {
		case api.Register:
			if glog.V(3) {
				glog.Infoln("Sending register signal to target keepalive control channel")
			}
			gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: register}
			return
		case api.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
			t.invalmsghdlr(w, r, "unrecognized path in /daemon POST")
			return
		}
	}

	if status, err := t.register(false, defaultTimeout); err != nil {
		s := fmt.Sprintf("Target %s failed to register with proxy, status %d, err: %v", t.si.DaemonID, status, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	if glog.V(3) {
		glog.Infof("Registered self %s", t.si.DaemonID)
	}
}

// unregister
// remove mountpath
func (t *targetrunner) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	apiItems := t.restAPIItems(r.URL.Path, 5)
	if apiItems = t.checkRestAPI(w, r, apiItems, 1, api.Version, api.Daemon); apiItems == nil {
		return
	}

	switch apiItems[0] {
	case api.Mountpaths:
		t.handleMountpathReq(w, r)
		return
	case api.Unregister:
		if glog.V(3) {
			glog.Infoln("Sending unregister signal to target keepalive control channel")
		}
		gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: unregister}
		return
	default:
		t.invalmsghdlr(w, r, "unrecognized path in /daemon DELETE")
		return
	}
}

//====================== common for both cold GET and PUT ======================================
//
// on err: closes and removes the file; otherwise closes and returns the size;
// empty omd5 or oxxhash: not considered an exception even when the configuration says otherwise;
// xxhash is always preferred over md5
//
//==============================================================================================
func (t *targetrunner) receive(fqn string, objname, omd5 string, ohobj cksumvalue,
	reader io.Reader) (sgl *iosgl.SGL, nhobj cksumvalue, written int64, errstr string) {
	var (
		err                  error
		file                 *os.File
		filewriter           io.Writer
		ohtype, ohval, nhval string
		cksumcfg             = &ctx.config.Cksum
	)

	if dryRun.disk && dryRun.network {
		return
	}
	// try to override cksum config with bucket-level config
	if bucket, _, err := t.fqn2bckobj(fqn); err == nil {
		if bucketProps, _, defined := t.bmdowner.get().propsAndChecksum(bucket); defined {
			cksumcfg = &bucketProps.CksumConf
		}
	}

	if dryRun.network {
		reader = readers.NewRandReader(dryRun.size)
	}
	if !dryRun.disk {
		if file, err = CreateFile(fqn); err != nil {
			t.fshc(err, fqn)
			errstr = fmt.Sprintf("Failed to create %s, err: %s", fqn, err)
			return
		}
		filewriter = file
	} else {
		filewriter = ioutil.Discard
	}

	slab := iosgl.SelectSlab(0)
	buf := slab.Alloc()
	defer func() { // free & cleanup on err
		slab.Free(buf)
		if errstr == "" {
			return
		}
		if !dryRun.disk {
			if err = file.Close(); err != nil {
				glog.Errorf("Nested: failed to close received file %s, err: %v", fqn, err)
			}
			if err = os.Remove(fqn); err != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, fqn, err)
			}
		}
	}()

	if dryRun.disk || dryRun.network {
		if written, err = ReceiveAndChecksum(filewriter, reader, buf); err != nil {
			errstr = err.Error()
		}
		if !dryRun.disk {
			if err = file.Close(); err != nil {
				errstr = fmt.Sprintf("Failed to close received file %s, err: %v", fqn, err)
			}
		}
		return
	}

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

				t.statsif.addMany(namedVal64{"cksum.bad.n", 1}, namedVal64{"cksum.bad.size", written})
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

			t.statsif.addMany(namedVal64{"cksum.bad.n", 1}, namedVal64{"cksum.bad.size", written})
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

func (t *targetrunner) redirectLatency(started time.Time, query url.Values) (redelta int64) {
	s := query.Get(api.URLParamUnixTime)
	if s == "" {
		return
	}
	pts, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		glog.Errorf("Unexpected: failed to convert %s to int, err: %v", s, err)
		return
	}
	redelta = started.UnixNano() - pts
	return
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
func (t *targetrunner) fqn2bckobj(fqn string) (bucket, objName string, err error) {
	var isLocal bool

	_, bucket, objName, isLocal, err = splitFQN(fqn)
	if err != nil {
		return
	}

	bucketmd := t.bmdowner.get()
	if t.fqn(bucket, objName, isLocal) != fqn || bucketmd.islocal(bucket) != isLocal {
		err = fmt.Errorf("Cannot convert %q => %s/%s - localbuckets or device mountpaths changed?", fqn, bucket, objName)
		return
	}

	return
}

// changedMountpath checks if the mountpath for provided fqn has changed. This
// situation can happen when new mountpath is added or mountpath is moved from
// disabled to enabled.
func (t *targetrunner) changedMountpath(fqn string) (bool, string, error) {
	_, bucket, objName, isLocal, err := splitFQN(fqn)
	if err != nil {
		return false, "", err
	}
	newFQN := t.fqn(bucket, objName, isLocal)
	return fqn != newFQN, newFQN, nil
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
		mpath := filepath.Join(instpath, strconv.Itoa(i+1))
		if err := CreateDir(mpath); err != nil {
			glog.Errorf("FATAL: cannot create test cache dir %q, err: %v", mpath, err)
			os.Exit(1)
		}

		err := ctx.mountpaths.AddMountpath(mpath)
		assert(err == nil, err)
	}
}

func (t *targetrunner) createBucketDirs(s, basename string, f func(basePath string) string) error {
	if basename == "" {
		return fmt.Errorf("empty basename for the %s buckets directory - update your config", s)
	}
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	for _, mpathInfo := range availablePaths {
		dir := f(mpathInfo.Path)
		if _, exists := availablePaths[dir]; exists {
			return fmt.Errorf("local namespace partitioning conflict: %s vs %s", mpathInfo.Path, dir)
		}
		if err := CreateDir(dir); err != nil {
			return fmt.Errorf("cannot create %s buckets dir %q, err: %v", s, dir, err)
		}
	}
	return nil
}

func (t *targetrunner) detectMpathChanges() {
	// mpath config dir
	mpathconfigfqn := filepath.Join(ctx.config.Confdir, mpname)

	type mfs struct {
		Available StringSet `json:"available"`
		Disabled  StringSet `json:"disabled"`
	}

	// load old/prev and compare
	var (
		oldfs = mfs{}
		newfs = mfs{
			Available: make(StringSet),
			Disabled:  make(StringSet),
		}
	)

	availablePaths, disabledPath := ctx.mountpaths.Mountpaths()
	for mpath := range availablePaths {
		newfs.Available[mpath] = struct{}{}
	}
	for mpath := range disabledPath {
		newfs.Disabled[mpath] = struct{}{}
	}

	if err := LocalLoad(mpathconfigfqn, &oldfs); err != nil {
		if !os.IsNotExist(err) && err != io.EOF {
			glog.Errorf("Failed to load old mpath config %q, err: %v", mpathconfigfqn, err)
		}
		return
	}

	if !reflect.DeepEqual(oldfs, newfs) {
		oldfsPprint := fmt.Sprintf(
			"available: %v\ndisabled: %v",
			oldfs.Available.String(), oldfs.Disabled.String(),
		)
		newfsPprint := fmt.Sprintf(
			"available: %v\ndisabled: %v",
			newfs.Available.String(), newfs.Disabled.String(),
		)

		glog.Errorf("%s: detected change in the mountpath configuration at %s", t.si.DaemonID, mpathconfigfqn)
		glog.Errorln("OLD: ====================")
		glog.Errorln(oldfsPprint)
		glog.Errorln("NEW: ====================")
		glog.Errorln(newfsPprint)
	}
	// persist
	if err := LocalSave(mpathconfigfqn, newfs); err != nil {
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

// fshc wakes up FSHC and makes it to run filesystem check immediately if err != nil
func (t *targetrunner) fshc(err error, filepath string) {
	glog.Errorf("FSHC called with error: %#v, file: %s", err, filepath)

	if !isIOError(err) {
		return
	}

	keyName := fqn2mountPath(filepath)
	if keyName != "" {
		// keyName is the mountpath is the fspath - counting IO errors on a per basis..
		t.statsdC.Send(keyName+".io.errors", metric{statsd.Counter, "count", 1})
	}
	if ctx.config.FSHC.Enabled {
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

func (t *targetrunner) handleMountpathReq(w http.ResponseWriter, r *http.Request) {
	msg := api.ActionMsg{}
	if t.readJSON(w, r, &msg) != nil {
		t.invalmsghdlr(w, r, "Invalid mountpath request")
		return
	}

	mountpath, ok := msg.Value.(string)
	if !ok {
		t.invalmsghdlr(w, r, "Invalid value in request")
		return
	}

	if mountpath == "" {
		t.invalmsghdlr(w, r, "Mountpath is not defined")
		return
	}

	switch msg.Action {
	case api.ActMountpathEnable:
		t.handleEnableMountpathReq(w, r, mountpath)
	case api.ActMountpathDisable:
		t.handleDisableMountpathReq(w, r, mountpath)
	case api.ActMountpathAdd:
		t.handleAddMountpathReq(w, r, mountpath)
	case api.ActMountpathRemove:
		t.handleRemoveMountpathReq(w, r, mountpath)
	default:
		t.invalmsghdlr(w, r, "Invalid action in request")
	}
}

func (t *targetrunner) handleEnableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	enabled, exists := t.fsprg.enableMountpath(mountpath)
	if !enabled && exists {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !enabled && !exists {
		t.invalmsghdlr(w, r, fmt.Sprintf("Mountpath %s not found", mountpath), http.StatusNotFound)
		return
	}
}

func (t *targetrunner) handleDisableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	enabled, exists := t.fsprg.disableMountpath(mountpath)
	if !enabled && exists {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !enabled && !exists {
		t.invalmsghdlr(w, r, fmt.Sprintf("Mountpath %s not found", mountpath), http.StatusNotFound)
		return
	}
}

func (t *targetrunner) handleAddMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.addMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not add mountpath, error: %v", err))
		return
	}
}

func (t *targetrunner) handleRemoveMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.removeMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not remove mountpath, error: %v", err))
		return
	}
}

// FIXME: use the message
func (t *targetrunner) receiveBucketMD(newbucketmd *bucketMD, msg *api.ActionMsg, tag string) (errstr string) {
	if msg.Action == "" {
		glog.Infof("%s bucket-metadata: version %d", tag, newbucketmd.version())
	} else {
		glog.Infof("%s bucket-metadata: version %d, action %s", tag, newbucketmd.version(), msg.Action)
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
	// Remove buckets which don't exist in newbucketmd
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

	// Create buckets which don't exist in (old)bucketmd
	for bucket := range newbucketmd.LBmap {
		if _, ok := bucketmd.LBmap[bucket]; !ok {
			for _, mpathInfo := range availablePaths {
				localbucketfqn := filepath.Join(makePathLocal(mpathInfo.Path), bucket)
				if err := CreateDir(localbucketfqn); err != nil {
					glog.Errorf("Failed to create local bucket dir %q, err: %v", localbucketfqn, err)
				}
			}
		}
	}
	return
}

func (t *targetrunner) receiveSmap(newsmap *Smap, msg *api.ActionMsg) (errstr string) {
	var (
		newtargetid, s                 string
		existentialQ, aborted, running bool
		pid                            = newsmap.ProxySI.DaemonID
	)
	action, id := path.Split(msg.Action)
	if action != "" {
		newtargetid = id
	}
	if msg.Action != "" {
		s = ", action " + msg.Action
	}
	glog.Infof("receive Smap: v%d, ntargets %d, primary %s%s", newsmap.version(), newsmap.countTargets(), pid, s)
	for id, si := range newsmap.Tmap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			glog.Infof("target: %s <= self", si.DaemonID)
		} else {
			glog.Infof("target: %s", si.DaemonID)
		}
	}
	if !existentialQ {
		errstr = fmt.Sprintf("Not finding self %s in the new %s", t.si.DaemonID, newsmap.pp())
		return
	}
	if errstr = t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
		return
	}
	if msg.Action == api.ActGlobalReb {
		go t.runRebalance(newsmap, newtargetid)
		return
	}
	if !ctx.config.Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}
	if newtargetid == "" {
		return
	}
	glog.Infof("receiveSmap: newtargetid=%s", newtargetid)
	if atomic.LoadInt64(&t.clusterStarted) != 0 {
		go t.runRebalance(newsmap, newtargetid) // auto-rebalancing xaction
		return
	}
	aborted, running = t.xactinp.isAbortedOrRunningRebalance()
	if !aborted || running {
		glog.Infof("the cluster is starting up, rebalancing=(aborted=%t, running=%t)", aborted, running)
		return
	}
	// resume
	f := func() {
		glog.Infoln("waiting for cluster startup to resume rebalance...")
		for {
			time.Sleep(time.Second)
			if atomic.LoadInt64(&t.clusterStarted) != 0 {
				break
			}
		}
		glog.Infoln("resuming rebalance...")
		t.runRebalance(t.smapowner.get(), "")
	}
	go runRebalanceOnce.Do(f) // only once at startup
	return
}

func (t *targetrunner) pollClusterStarted() {
	for {
		time.Sleep(time.Second)
		smap := t.smapowner.get()
		if smap == nil || !smap.isValid() {
			continue
		}
		psi := smap.ProxySI
		args := callArgs{
			si: psi,
			req: reqArgs{
				method: http.MethodGet,
				base:   psi.InternalNet.DirectURL,
				path:   api.URLPath(api.Version, api.Health),
			},
			timeout: defaultTimeout,
		}
		res := t.call(args)
		if res.err != nil {
			continue
		}
		proxystats := &proxyCoreStats{}
		err := jsoniter.Unmarshal(res.outjson, proxystats)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]", psi.PublicNet.DirectURL, err, string(res.outjson))
			continue
		}
		if proxystats.Uptime != 0 {
			break
		}
	}
	atomic.StoreInt64(&t.clusterStarted, 1)
	glog.Infoln("cluster started up")
}

// broadcastNeighbors sends a message ([]byte) to all neighboring targets belongs to a smap
func (t *targetrunner) broadcastNeighbors(path string, query url.Values, method string, body []byte,
	smap *Smap, timeout time.Duration) chan callResult {

	if len(smap.Tmap) < 2 {
		// no neighbor, returns empty channel, so caller doesnt have to check channel is nil
		ch := make(chan callResult)
		close(ch)
		return ch
	}

	bcastArgs := bcastCallArgs{
		req: reqArgs{
			method: method,
			path:   path,
			query:  query,
			body:   body,
		},
		timeout: timeout,
		servers: []map[string]*daemonInfo{smap.Tmap},
	}
	return t.broadcast(bcastArgs)
}

func (t *targetrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, api.Version, api.Tokens); apitems == nil {
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

	slab := iosgl.SelectSlab(slabSize)
	buf := slab.Alloc()
	xxHashValue, errstr := ComputeXXHash(file, buf)
	file.Close()
	slab.Free(buf)

	if errstr != "" {
		errstr := fmt.Sprintf("Unable to compute xxHash, err: %s", errstr)
		return false, errstr
	}

	return string(hashbinary) == xxHashValue, ""
}

// unregisters the target and marks it as disabled by an internal event
func (t *targetrunner) disable() error {
	var (
		status int
		eunreg error
	)

	t.regstate.Lock()
	defer t.regstate.Unlock()

	if t.regstate.disabled {
		return nil
	}

	glog.Info("Disabling the target")
	for i := 0; i < maxRetrySeconds; i++ {
		if status, eunreg = t.unregister(); eunreg != nil {
			if IsErrConnectionRefused(eunreg) || status == http.StatusRequestTimeout {
				glog.Errorf("Target %s: retrying unregistration...", t.si.DaemonID)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || eunreg != nil {
		// do not update state on error
		if eunreg == nil {
			eunreg = fmt.Errorf("Error unregistering target, http error code: %d", status)
		}
		return eunreg
	}

	t.regstate.disabled = true
	glog.Info("Target has been disabled")
	return nil
}

// registers the target again if it was disabled by and internal event
func (t *targetrunner) enable() error {
	var (
		status int
		ereg   error
	)

	t.regstate.Lock()
	defer t.regstate.Unlock()

	if !t.regstate.disabled {
		return nil
	}

	glog.Info("Enabling the target")
	for i := 0; i < maxRetrySeconds; i++ {
		if status, ereg = t.register(false, defaultTimeout); ereg != nil {
			if IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("Target %s: retrying registration...", t.si.DaemonID)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || ereg != nil {
		// do not update state on error
		if ereg == nil {
			ereg = fmt.Errorf("Error registering target, http error code: %d", status)
		}
		return ereg
	}

	t.regstate.disabled = false
	glog.Info("Target has been enabled")
	return nil
}

// DisableMountpath implements fspathDispatcher interface
func (t *targetrunner) DisableMountpath(mountpath string, why string) (disabled, exists bool) {
	// TODO: notify admin that the mountpath is gone
	glog.Warningf("Disabling mountpath %s: %s", mountpath, why)
	return t.fsprg.disableMountpath(mountpath)
}

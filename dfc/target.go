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
	"errors"
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
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
	"github.com/NVIDIA/dfcpub/dfc/util/readers"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/transport"
	"github.com/OneOfOne/xxhash"
	"github.com/json-iterator/go"
)

const (
	internalPageSize = 10000     // number of objects in a page for internal call between target and proxy to get atime/iscached
	MaxPageSize      = 64 * 1024 // max number of objects in a page (warning logged if requested page size exceeds this limit)
	doesnotexist     = "does not exist"
	maxBytesInMem    = 256 * cmn.KiB
)

type (
	allfinfos struct {
		t            *targetrunner
		files        []*cmn.BucketEntry
		prefix       string
		marker       string
		markerDir    string
		msg          *cmn.GetMsg
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
		atimeRespCh  chan *atimeResponse
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
	readahead      readaheader
}

// start target runner
func (t *targetrunner) Run() error {
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
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
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

	err := t.createBucketDirs("local", ctx.config.LocalBuckets, fs.Mountpaths.MakePathLocal)
	if err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	err = t.createBucketDirs("cloud", ctx.config.CloudBuckets, fs.Mountpaths.MakePathCloud)
	if err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	t.detectMpathChanges()

	// cloud provider
	if ctx.config.CloudProvider == cmn.ProviderAmazon {
		// TODO: sessions
		t.cloudif = &awsimpl{t}

	} else {
		cmn.Assert(ctx.config.CloudProvider == cmn.ProviderGoogle)
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
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Buckets)+"/", t.bucketHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Objects)+"/", t.objectHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Daemon), t.daemonHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Push)+"/", t.pushHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Tokens), t.tokenHandler)
	transport.SetMux(cmn.NetworkPublic, t.publicServer.mux) // to register transport handlers at runtime
	t.registerPublicNetHandler("/", cmn.InvalidHandler)

	// Intra control network
	t.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Metasync), t.metasyncHandler)
	t.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Health), t.healthHandler)
	t.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Vote), t.voteHandler)
	if ctx.config.Net.UseIntraControl {
		transport.SetMux(cmn.NetworkIntraControl, t.intraControlServer.mux) // to register transport handlers at runtime
		t.registerIntraControlNetHandler("/", cmn.InvalidHandler)
	}

	// Intra data network
	if ctx.config.Net.UseIntraData {
		transport.SetMux(cmn.NetworkIntraData, t.intraDataServer.mux) // to register transport handlers at runtime
		t.registerIntraDataNetHandler(cmn.URLPath(cmn.Version, cmn.Objects)+"/", t.objectHandler)
		t.registerIntraDataNetHandler("/", cmn.InvalidHandler)
	}

	glog.Infof("Target %s is ready", t.si.DaemonID)
	glog.Flush()
	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	_ = t.initStatsD("dfctarget")
	sr := getstorstatsrunner()
	sr.Core.statsdC = &t.statsdC

	getfshealthchecker().SetDispatcher(t)

	aborted, _ := t.xactinp.isAbortedOrRunningLocalRebalance()
	if aborted {
		// resume local rebalance
		f := func() {
			glog.Infoln("resuming local rebalance...")
			t.runLocalRebalance()
		}
		go runLocalRebalanceOnce.Do(f) // only once at startup
	}

	return t.httprunner.run()
}

// stop gracefully
func (t *targetrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.Getname(), err)
	sleep := t.xactinp.abortAll()
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
		cmn.Assert(err == nil, err)
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
			path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, t.si.DaemonID),
		},
		timeout: defaultTimeout,
	}
	res := t.call(args)
	return res.status, res.err
}

func (t *targetrunner) bucketLRUEnabled(bucket string) bool {
	bucketmd := t.bmdowner.get()
	return bucketmd.lruEnabled(bucket)
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
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /buckets path")
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
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /objects path")
	}
}

// GET /v1/buckets/bucket-name
func (t *targetrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
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
		size, rangeOff, rangeLen      int64
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
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	rangeOff, rangeLen, errstr = t.offsetAndLength(query)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	bucketmd := t.bmdowner.get()
	islocal := bucketmd.islocal(bucket)
	fqn, errstr = cluster.FQN(bucket, objname, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if !dryRun.disk {
		if x := query.Get(cmn.URLParamReadahead); x != "" { // FIXME
			t.readahead.ahead(fqn, rangeOff, rangeLen)
			return
		}
	}
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s/%s <= %s", r.Method, bucket, objname, pid)
	}
	if redelta := t.redirectLatency(started, query); redelta != 0 {
		t.statsif.add(statGetRedirLatency, redelta)
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
	// Lock(ro)
	uname = cluster.Uname(bucket, objname)
	t.rtnamemap.Lock(uname, false)

	// bucket-level checksumming should override global
	if bucketProps, _, defined := bucketmd.propsAndChecksum(bucket); defined {
		cksumcfg = &bucketProps.CksumConfig
	}

	// existence, access & versioning
	if coldget, size, version, errstr = t.lookupLocally(bucket, objname, fqn); islocal && errstr != "" {
		errcode = http.StatusInternalServerError
		// given certain conditions (below) make an effort to locate the object
		if strings.Contains(errstr, doesnotexist) {
			errcode = http.StatusNotFound

			// check FS-wide (local rebalance is running)
			aborted, running := t.xactinp.isAbortedOrRunningLocalRebalance()
			if aborted || running {
				oldFQN, oldSize := t.getFromNeighborFS(bucket, objname, islocal)
				if oldFQN != "" {
					if glog.V(4) {
						glog.Infof("Local rebalance is not completed: file found at %s [size %s]",
							oldFQN, cmn.B2S(oldSize, 1))
					}
					fqn = oldFQN
					size = oldSize
					goto existslocally
				}
			}

			// check cluster-wide (global rebalance is running)
			aborted, running = t.xactinp.isAbortedOrRunningRebalance()
			if aborted || running {
				if props := t.getFromNeighbor(bucket, objname, r, islocal); props != nil {
					size, nhobj = props.size, props.nhobj
					if glog.V(4) {
						glog.Infof("Rebalance is not completed: found somewhere [size %s]", cmn.B2S(size, 1))
					}
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
		t.rtnamemap.Unlock(uname, false)
		return
	}

	if !coldget && !islocal {
		if versioncfg.ValidateWarmGet && (version != "" &&
			t.versioningConfigured(bucket)) {
			if vchanged, errstr, errcode = t.checkCloudVersion(
				ct, bucket, objname, version); errstr != "" {
				t.invalmsghdlr(w, r, errstr, errcode)
				t.rtnamemap.Unlock(uname, false)
				return
			}
			// TODO: add a knob to return what's cached while upgrading the version async
			coldget = vchanged
		}
	}
	if !coldget && cksumcfg.ValidateWarmGet && cksumcfg.Checksum != cmn.ChecksumNone {
		validChecksum, errstr := t.validateObjectChecksum(fqn, cksumcfg.Checksum, size)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			t.rtnamemap.Unlock(uname, false)
			return
		}
		if !validChecksum {
			if islocal {
				if err := os.Remove(fqn); err != nil {
					glog.Warningf("Bad checksum, failed to remove %s/%s, err: %v", bucket, objname, err)
				}
				t.invalmsghdlr(w, r, fmt.Sprintf("Bad checksum %s/%s", bucket, objname), http.StatusInternalServerError)
				t.rtnamemap.Unlock(uname, false)
				return
			}
			coldget = true
		}
	}
	if coldget && !dryRun.disk {
		t.rtnamemap.Unlock(uname, false)
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
		sgl                *memsys.SGL
		slab               *memsys.Slab2
		buf                []byte
		rangeReader        io.ReadSeeker
		reader             io.Reader
		rahSize            int64
		rahfcacher, rahsgl = t.readahead.get(fqn)
		sendMore           bool
	)
	defer func() {
		rahfcacher.got()
		t.rtnamemap.Unlock(uname, false)
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

	cksumRange := cksumcfg.Checksum != cmn.ChecksumNone && rangeLen > 0 && cksumcfg.EnableReadRangeChecksum
	if !coldget && !cksumRange && cksumcfg.Checksum != cmn.ChecksumNone {
		xxHashBinary, errstr := Getxattr(fqn, cmn.XattrXXHashVal)
		if errstr == "" && xxHashBinary != nil {
			nhobj = newcksumvalue(cksumcfg.Checksum, string(xxHashBinary))
		}
	}
	if nhobj != nil && !cksumRange {
		htype, hval := nhobj.get()
		w.Header().Add(cmn.HeaderDFCChecksumType, htype)
		w.Header().Add(cmn.HeaderDFCChecksumVal, hval)
	}
	if props != nil && props.version != "" {
		w.Header().Add(cmn.HeaderDFCObjVersion, props.version)
	}

	// loopback if disk IO is disabled
	if dryRun.disk {
		if !dryRun.network {
			rd := readers.NewRandReader(dryRun.size)
			if _, err = io.Copy(w, rd); err != nil {
				errstr = fmt.Sprintf("dry-run: failed to send random response, err: %v", err)
				t.statsif.add(statErrGetCount, 1)
				return
			}
		}

		delta := time.Since(started)
		t.statsif.addMany(namedVal64{statGetCount, 1}, namedVal64{statGetLatency, int64(delta)})
		return
	}
	if size == 0 {
		glog.Warningf("%s/%s size is 0 (zero)", bucket, objname)
		return
	}
	if rahsgl != nil {
		rahSize = rahsgl.Size()
		if rangeLen == 0 {
			sendMore = rahSize < size
		} else {
			sendMore = rahSize < rangeLen
		}
	}
	if rahSize == 0 || sendMore {
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

send:
	if rahsgl != nil && rahSize > 0 {
		// reader = memsys.NewReader(rahsgl) - NOTE
		reader = rahsgl
		slab = rahsgl.Slab()
		buf = slab.Alloc()
		glog.Infof("%s readahead %d", fqn, rahSize) // FIXME: DEBUG
	} else if rangeLen == 0 {
		if rahSize > 0 {
			file.Seek(rahSize, io.SeekStart)
		}
		reader = file
		buf, slab = gmem2.AllocFromSlab2(size)
	} else {
		if rahSize > 0 {
			rangeOff += rahSize
			rangeLen -= rahSize
		}
		buf, slab = gmem2.AllocFromSlab2(rangeLen)
		if cksumRange {
			cmn.Assert(rahSize == 0, "NOT IMPLEMENTED YET") // TODO
			var cksum string
			cksum, sgl, rangeReader, errstr = t.rangeCksum(file, fqn, rangeOff, rangeLen, buf)
			if errstr != "" {
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return

			}
			reader = rangeReader
			w.Header().Add(cmn.HeaderDFCChecksumType, cksumcfg.Checksum)
			w.Header().Add(cmn.HeaderDFCChecksumVal, cksum)
		} else {
			reader = io.NewSectionReader(file, rangeOff, rangeLen)
		}
	}

	if !dryRun.network {
		written, err = io.CopyBuffer(w, reader, buf)
	} else {
		written, err = io.CopyBuffer(ioutil.Discard, reader, buf)
	}
	if err != nil {
		if !dryRun.network {
			errstr = fmt.Sprintf("Failed to GET %s, err: %v", fqn, err)
		} else {
			errstr = fmt.Sprintf("dry-run: failed to read/discard %s, err: %v", fqn, err)
		}
		t.fshc(err, fqn)
		t.statsif.add(statErrGetCount, 1)
		return
	}
	if sendMore {
		rahsgl = nil
		sendMore = false
		goto send
	}

	if !coldget && bucketmd.lruEnabled(bucket) {
		getatimerunner().touch(fqn)
	}
	if glog.V(4) {
		s := fmt.Sprintf("GET: %s/%s, %.2f MB, %d µs", bucket, objname, float64(written)/cmn.MiB, time.Since(started)/1000)
		if coldget {
			s += " (cold)"
		}
		glog.Infoln(s)
	}

	delta := time.Since(started)
	t.statsif.addMany(namedVal64{statGetCount, 1}, namedVal64{statGetLatency, int64(delta)})
}

func (t *targetrunner) rangeCksum(file *os.File, fqn string, offset, length int64, buf []byte) (
	cksum string, sgl *memsys.SGL, rangeReader io.ReadSeeker, errstr string) {
	rangeReader = io.NewSectionReader(file, offset, length)
	xx := xxhash.New64()
	if length <= maxBytesInMem {
		sgl = gmem2.NewSGL(length)
		_, err := cmn.ReceiveAndChecksum(sgl, rangeReader, buf, xx)
		if err != nil {
			errstr = fmt.Sprintf("failed to read byte range, offset:%d, length:%d from %s, err: %v", offset, length, fqn, err)
			t.fshc(err, fqn)
			return
		}
		// overriding rangeReader here to read from the sgl
		rangeReader = memsys.NewReader(sgl)
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
	offsetStr, lengthStr := query.Get(cmn.URLParamOffset), query.Get(cmn.URLParamLength)
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
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	from, to := query.Get(cmn.URLParamFromID), query.Get(cmn.URLParamToID)
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
			t.statsif.add(statPutRedirLatency, redelta)
		}
		// PUT
		pid := query.Get(cmn.URLParamProxyID)
		if glog.V(4) {
			glog.Infof("%s %s/%s <= %s", r.Method, bucket, objname, pid)
		}
		if pid == "" {
			t.invalmsghdlr(w, r, "PUT requests are expected to be redirected")
			return
		}
		if t.smapowner.get().GetProxy(pid) == nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("PUT from an unknown proxy/gateway ID '%s' - Smap out of sync?", pid))
			return
		}

		errstr := ""
		errcode := 0
		if replica, replicaSrc := isReplicationPUT(r); !replica {
			// regular PUT
			errstr, errcode = t.doput(w, r, bucket, objname)
		} else {
			// replication PUT
			errstr = t.doReplicationPut(w, r, bucket, objname, replicaSrc)
		}
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
		msg     cmn.ActionMsg
		started = time.Now()
		ok      = true
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
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
		if err := t.listRangeOperation(r, apitems, msg); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to delete files: %v", err))
		}
		return
	}
	s := fmt.Sprintf("Invalid API request: no message body")
	t.invalmsghdlr(w, r, s)
	ok = false
}

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msg     cmn.ActionMsg
		evict   bool
		started = time.Now()
		ok      = true
	)
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	defer func() {
		if ok && err == nil && bool(glog.V(4)) {
			glog.Infof("DELETE: %s/%s, %d µs", bucket, objname, time.Since(started)/1000)
		}
	}()
	if err == nil && len(b) > 0 {
		err = jsoniter.Unmarshal(b, &msg)
		if err == nil {
			evict = (msg.Action == cmn.ActEvict)
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
	var msg cmn.ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	switch msg.Action {
	case cmn.ActPrefetch:
		if err := t.listRangeOperation(r, apitems, msg); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch files: %v", err))
		}
	case cmn.ActRenameLB:
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
		if errstr := t.renameLB(bucketFrom, bucketTo, props, clone); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		glog.Infof("renamed local bucket %s => %s, bucket-metadata version %d", bucketFrom, bucketTo, clone.version())
	case cmn.ActListObjects:
		lbucket := apitems[0]
		if !t.validatebckname(w, r, lbucket) {
			return
		}
		// list the bucket and return
		tag, ok := t.listbucket(w, r, lbucket, &msg)
		if ok {
			delta := time.Since(started)
			t.statsif.addMany(namedVal64{statListCount, 1}, namedVal64{statListLatency, int64(delta)})
			if glog.V(3) {
				glog.Infof("LIST %s: %s, %d µs", tag, lbucket, int64(delta/time.Microsecond))
			}
		}
	case cmn.ActRechecksum:
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
	var msg cmn.ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRename:
		t.renamefile(w, r, msg)
	case cmn.ActReplicate:
		t.replicate(w, r, msg)
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
		bucketprops cmn.SimpleKVs
		cksumcfg    *cmn.CksumConfig
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
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
		bucketprops = make(cmn.SimpleKVs)
		bucketprops[cmn.HeaderCloudProvider] = cmn.ProviderDFC
		bucketprops[cmn.HeaderVersioning] = cmn.VersionLocal
	}
	// double check if we support versioning internally for the bucket
	if !t.versioningConfigured(bucket) {
		bucketprops[cmn.HeaderVersioning] = cmn.VersionNone
	}

	for k, v := range bucketprops {
		w.Header().Add(k, v)
	}

	props, _, defined := bucketmd.propsAndChecksum(bucket)
	// include checksumming settings in the response
	cksumcfg = &ctx.config.Cksum
	if defined {
		cksumcfg = &props.CksumConfig
	}

	// include lru settings in the response
	w.Header().Add(cmn.HeaderNextTierURL, props.NextTierURL)
	w.Header().Add(cmn.HeaderReadPolicy, props.ReadPolicy)
	w.Header().Add(cmn.HeaderWritePolicy, props.WritePolicy)
	w.Header().Add(cmn.HeaderBucketChecksumType, cksumcfg.Checksum)
	w.Header().Add(cmn.HeaderBucketValidateColdGet, strconv.FormatBool(cksumcfg.ValidateColdGet))
	w.Header().Add(cmn.HeaderBucketValidateWarmGet, strconv.FormatBool(cksumcfg.ValidateWarmGet))
	w.Header().Add(cmn.HeaderBucketValidateRange, strconv.FormatBool(cksumcfg.EnableReadRangeChecksum))
	w.Header().Add(cmn.HeaderBucketLRULowWM, strconv.FormatUint(uint64(props.LowWM), 10))
	w.Header().Add(cmn.HeaderBucketLRUHighWM, strconv.FormatUint(uint64(props.HighWM), 10))
	w.Header().Add(cmn.HeaderBucketAtimeCacheMax, strconv.FormatUint(props.AtimeCacheMax, 10))
	w.Header().Add(cmn.HeaderBucketDontEvictTime, props.DontEvictTimeStr)
	w.Header().Add(cmn.HeaderBucketCapUpdTime, props.CapacityUpdTimeStr)
	w.Header().Add(cmn.HeaderBucketLRUEnabled, strconv.FormatBool(props.LRUEnabled))
}

// HEAD /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname, errstr string
		islocal, checkCached    bool
		errcode                 int
		objmeta                 cmn.SimpleKVs
	)
	checkCached, _ = parsebool(r.URL.Query().Get(cmn.URLParamCheckCached))
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
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
		fqn, errstr := cluster.FQN(bucket, objname, islocal)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
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
		objmeta = make(cmn.SimpleKVs)
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
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /tokens path")
	}
}

// [METHOD] /v1/metasync
func (t *targetrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		t.metasyncHandlerPut(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /metasync path")
	}
}

// PUT /v1/metasync
func (t *targetrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	var payload = make(cmn.SimpleKVs)
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
	from := query.Get(cmn.URLParamFromID)

	aborted, running := t.xactinp.isAbortedOrRunningRebalance()
	if !aborted && !running {
		aborted, running = t.xactinp.isAbortedOrRunningLocalRebalance()
	}
	status := &thealthstatus{IsRebalancing: aborted || running}

	jsbytes, err := jsoniter.Marshal(status)
	cmn.Assert(err == nil, err)
	if ok := t.writeJSON(w, r, jsbytes, "thealthstatus"); !ok {
		return
	}

	t.keepalive.heardFrom(from, false)
}

// [METHOD] /v1/push/bucket-name
func (t *targetrunner) pushHandler(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Push)
	if err != nil {
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
			err := pusher.Push(cmn.URLPath(cmn.Version, cmn.Objects, bucket, objname), nil)
			if err != nil {
				t.invalmsghdlr(w, r, "Error Pushing "+bucket+"/"+objname+": "+err.Error())
				return
			}
		}
	} else {
		t.invalmsghdlr(w, r, "Pusher Unavailable")
		return
	}

	if _, err = w.Write([]byte("Pushed Object List")); err != nil {
		s := fmt.Sprintf("Error writing response: %v", err)
		glog.Error(s)
	}
}

//====================================================================================
//
// supporting methods and misc
//
//====================================================================================
func (t *targetrunner) renameLB(bucketFrom, bucketTo string, p cmn.BucketProps, clone *bucketMD) (errstr string) {
	// ready to receive migrated obj-s _after_ that point
	// insert directly w/o incrementing the version (metasyncer will do at the end of the operation)
	clone.LBmap[bucketTo] = p
	t.bmdowner.put(clone)

	wg := &sync.WaitGroup{}

	availablePaths, _ := fs.Mountpaths.Mountpaths()
	ch := make(chan string, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		// Create directory for new local bucket
		toDir := filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path), bucketTo)
		if err := cmn.CreateDir(toDir); err != nil {
			ch <- fmt.Sprintf("Failed to create dir %s, error: %v", toDir, err)
			continue
		}

		wg.Add(1)
		fromdir := filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path), bucketFrom)
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
		fromdir := filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path), bucketFrom)
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
	if spec, _ := cluster.FileSpec(fqn); spec != nil && !spec.PermToProcess() { // FIXME: workfiles indicate work in progress..
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
	var objmeta cmn.SimpleKVs
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

	geturl := fmt.Sprintf("%s%s?%s=%t", neighsi.PublicNet.DirectURL, r.URL.Path, cmn.URLParamLocal, islocal)
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
		hval    = response.Header.Get(cmn.HeaderDFCChecksumVal)
		htype   = response.Header.Get(cmn.HeaderDFCChecksumType)
		hdhobj  = newcksumvalue(htype, hval)
		version = response.Header.Get(cmn.HeaderDFCObjVersion)
	)
	fqn, errstr := cluster.FQN(bucket, objname, islocal)
	if errstr != "" {
		response.Body.Close()
		glog.Error(errstr)
		return
	}
	getfqn := cluster.GenContentFQN(fqn, cluster.DefaultWorkfileType)
	if _, nhobj, size, errstr = t.receive(getfqn, objname, "", hdhobj, response.Body); errstr != "" {
		response.Body.Close()
		glog.Errorf(errstr)
		return
	}
	response.Body.Close()
	if nhobj != nil {
		nhtype, nhval := nhobj.get()
		cmn.Assert(hdhobj == nil || htype == nhtype)
		if hval != "" && nhval != "" && hval != nhval {
			glog.Errorf("Bad checksum: %s/%s %s %.8s... != %.8s...", bucket, objname, htype, hval, nhval)
			return
		}
	}
	// commit
	if err = os.Rename(getfqn, fqn); err != nil {
		glog.Errorf("Failed to rename %s => %s, err: %v", getfqn, fqn, err)
		return
	}
	props = &objectProps{version: version, size: size, nhobj: nhobj}
	if errstr = t.finalizeobj(fqn, bucket, props); errstr != "" {
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
		uname       = cluster.Uname(bucket, objname)
		versioncfg  = &ctx.config.Ver
		cksumcfg    = &ctx.config.Cksum
		errv        string
		nextTierURL string
		vchanged    bool
		inNextTier  bool
		bucketProps cmn.BucketProps
		err         error
	)
	fqn, errstr := cluster.FQN(bucket, objname, islocal)
	if errstr != "" {
		return nil, errstr, http.StatusBadRequest
	}
	getfqn := cluster.GenContentFQN(fqn, cluster.DefaultWorkfileType)
	// one cold GET at a time
	if prefetch {
		if !t.rtnamemap.TryLock(uname, true) {
			glog.Infof("PREFETCH: cold GET race: %s/%s - skipping", bucket, objname)
			return nil, "skip", 0
		}
	} else {
		t.rtnamemap.Lock(uname, true)
	}

	// check if bucket-level checksumming configuration should override cluster-level configuration
	bucketProps, _, defined := bucketmd.propsAndChecksum(bucket)
	if defined {
		cksumcfg = &bucketProps.CksumConfig
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

		if !coldget && cksumcfg.ValidateWarmGet && cksumcfg.Checksum != cmn.ChecksumNone {
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
		xxHashBinary, _ := Getxattr(fqn, cmn.XattrXXHashVal)
		if xxHashBinary != nil {
			props.nhobj = newcksumvalue(cksumcfg.Checksum, string(xxHashBinary))
		}
		glog.Infof("cold GET race: %s/%s, size=%d, version=%s - nothing to do", bucket, objname, size, version)
		goto ret
	}
	// cold
	nextTierURL = bucketProps.NextTierURL
	if nextTierURL != "" && bucketProps.ReadPolicy == cmn.RWPolicyNextTier {
		if inNextTier, errstr, errcode = t.objectInNextTier(nextTierURL, bucket, objname); errstr != "" {
			t.rtnamemap.Unlock(uname, true)
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
			t.rtnamemap.Unlock(uname, true)
			return
		}
	}
	defer func() {
		if errstr != "" {
			t.rtnamemap.Unlock(uname, true)
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
	if errstr = t.finalizeobj(fqn, bucket, props); errstr != "" {
		return
	}
ret:
	//
	// NOTE: GET - downgrade and keep the lock, PREFETCH - unlock
	//
	if prefetch {
		t.rtnamemap.Unlock(uname, true)
	} else {
		if vchanged {
			t.statsif.addMany(namedVal64{statGetColdCount, 1}, namedVal64{statGetColdSize, props.size},
				namedVal64{statVerChangeSize, props.size}, namedVal64{statVerChangeCount, 1})
		} else {
			t.statsif.addMany(namedVal64{statGetColdCount, 1}, namedVal64{statGetColdSize, props.size})
		}
		t.rtnamemap.DowngradeLock(uname)
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
	if bytes, errs := Getxattr(fqn, cmn.XattrObjVersion); errs == "" {
		version = string(bytes)
	}
	return
}

func (t *targetrunner) lookupRemotely(bucket, objname string) *cluster.Snode {
	res := t.broadcastNeighbors(
		cmn.URLPath(cmn.Version, cmn.Objects, bucket, objname),
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
func (t *targetrunner) listCachedObjects(bucket string, msg *cmn.GetMsg) (outbytes []byte, errstr string, errcode int) {
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

func (t *targetrunner) prepareLocalObjectList(bucket string, msg *cmn.GetMsg) (*cmn.BucketList, error) {
	type mresp struct {
		infos      *allfinfos
		failedPath string
		err        error
	}

	availablePaths, _ := fs.Mountpaths.Mountpaths()
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
			localDir = filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path), bucket)
		} else {
			localDir = filepath.Join(fs.Mountpaths.MakePathCloud(mpathInfo.Path), bucket)
		}

		go walkMpath(localDir)
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
			t.fshc(r.err, r.failedPath)
			return nil, fmt.Errorf("Failed to read %s", r.failedPath)
		}

		pageSize = r.infos.limit
		bckEntries = append(bckEntries, r.infos.files...)
		fileCount += r.infos.fileCount
	}

	// sort the result and return only first `pageSize` entries
	marker := ""
	if fileCount > pageSize {
		ifLess := func(i, j int) bool {
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

	if strings.Contains(msg.GetProps, cmn.GetTargetURL) {
		for _, e := range bucketList.Entries {
			e.TargetURL = t.si.PublicNet.DirectURL
		}
	}

	return bucketList, nil
}

func (t *targetrunner) getbucketnames(w http.ResponseWriter, r *http.Request) {
	bucketmd := t.bmdowner.get()

	bucketnames := &cmn.BucketNames{
		Local: make([]string, 0, len(bucketmd.LBmap)),
		Cloud: make([]string, 0, 64),
	}
	for bucket := range bucketmd.LBmap {
		bucketnames.Local = append(bucketnames.Local, bucket)
	}

	q := r.URL.Query()
	localonly, _ := parsebool(q.Get(cmn.URLParamLocal))
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
	cmn.Assert(err == nil, err)
	t.writeJSON(w, r, jsbytes, "getbucketnames")
}

func (t *targetrunner) doLocalBucketList(w http.ResponseWriter, r *http.Request, bucket string, msg *cmn.GetMsg) (errstr string, ok bool) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		errstr = fmt.Sprintf("List local bucket %s failed, err: %v", bucket, err)
		return
	}
	jsbytes, err := jsoniter.Marshal(reslist)
	cmn.Assert(err == nil, err)
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

// List bucket returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string, actionMsg *cmn.ActionMsg) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
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

	var msg cmn.GetMsg
	err = jsoniter.Unmarshal(getMsgJson, &msg)
	if err != nil {
		errstr := fmt.Sprintf("Unable to unmarshal 'value' in request to a cmn.GetMsg: %v", actionMsg.Value)
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

func (t *targetrunner) newFileWalk(bucket string, msg *cmn.GetMsg) *allfinfos {
	// Marker is always a file name, so we need to strip filename from path
	markerDir := ""
	if msg.GetPageMarker != "" {
		markerDir = filepath.Dir(msg.GetPageMarker)
	}

	// A small optimization: set boolean variables need* to avoid
	// doing string search(strings.Contains) for every entry.
	ci := &allfinfos{
		t:            t, // targetrunner
		files:        make([]*cmn.BucketEntry, 0, cmn.DefaultPageSize),
		prefix:       msg.GetPrefix,
		marker:       msg.GetPageMarker,
		markerDir:    markerDir,
		msg:          msg,
		lastFilePath: "",
		bucket:       bucket,
		fileCount:    0,
		rootLength:   0,
		limit:        cmn.DefaultPageSize, // maximum number files to return
		needAtime:    strings.Contains(msg.GetProps, cmn.GetPropsAtime),
		needCtime:    strings.Contains(msg.GetProps, cmn.GetPropsCtime),
		needChkSum:   strings.Contains(msg.GetProps, cmn.GetPropsChecksum),
		needVersion:  strings.Contains(msg.GetProps, cmn.GetPropsVersion),
		needStatus:   strings.Contains(msg.GetProps, cmn.GetPropsStatus),
		atimeRespCh:  make(chan *atimeResponse, 1),
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
func (ci *allfinfos) processRegularFile(fqn string, osfi os.FileInfo, objStatus string) error {
	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}

	if ci.marker != "" && relname <= ci.marker {
		return nil
	}

	// the file passed all checks - add it to the batch
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:     relname,
		Atime:    "",
		IsCached: true,
		Status:   objStatus,
	}
	if ci.needAtime {
		atimeResponse := <-getatimerunner().atime(fqn, ci.atimeRespCh)
		atime, ok := atimeResponse.accessTime, atimeResponse.ok
		if !ok {
			atime, _, _ = getAmTimes(osfi)
		}
		if ci.msg.GetTimeFormat == "" {
			fileInfo.Atime = atime.Format(cmn.RFC822)
		} else {
			fileInfo.Atime = atime.Format(ci.msg.GetTimeFormat)
		}
	}
	if ci.needCtime {
		t := osfi.ModTime()
		switch ci.msg.GetTimeFormat {
		case "":
			fallthrough
		case cmn.RFC822:
			fileInfo.Ctime = t.Format(time.RFC822)
		default:
			fileInfo.Ctime = t.Format(ci.msg.GetTimeFormat)
		}
	}
	if ci.needChkSum {
		xxHashBinary, errstr := Getxattr(fqn, cmn.XattrXXHashVal)
		if errstr == "" {
			fileInfo.Checksum = hex.EncodeToString(xxHashBinary)
		}
	}
	if ci.needVersion {
		version, errstr := Getxattr(fqn, cmn.XattrObjVersion)
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
	if spec, _ := cluster.FileSpec(fqn); spec != nil && !spec.PermToProcess() {
		return nil
	}

	objStatus := cmn.ObjStatusOK
	if ci.needStatus {
		bucket, objname, err := ci.t.fqn2bckobj(fqn)
		if err != nil {
			glog.Warning(err)
			objStatus = cmn.ObjStatusMoved
		}
		si, errstr := hrwTarget(bucket, objname, ci.t.smapowner.get())
		if errstr != "" || ci.t.si.DaemonID != si.DaemonID {
			glog.Warningf("Rebalanced object: %s/%s: %s", bucket, objname, errstr)
			objStatus = cmn.ObjStatusMoved
		}
	}

	return ci.processRegularFile(fqn, osfi, objStatus)
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
		xxHashVal                  string
		htype, hval, nhtype, nhval string
		sgl                        *memsys.SGL
		started                    time.Time
	)
	started = time.Now()
	islocal := t.bmdowner.get().islocal(bucket)
	fqn, errstr := cluster.FQN(bucket, objname, islocal)
	if errstr != "" {
		return errstr, http.StatusBadRequest
	}
	putfqn := cluster.GenContentFQN(fqn, cluster.DefaultWorkfileType)
	cksumcfg := &ctx.config.Cksum
	if bucketProps, _, defined := t.bmdowner.get().propsAndChecksum(bucket); defined {
		cksumcfg = &bucketProps.CksumConfig
	}
	hdhobj = newcksumvalue(r.Header.Get(cmn.HeaderDFCChecksumType), r.Header.Get(cmn.HeaderDFCChecksumVal))

	if hdhobj != nil {
		htype, hval = hdhobj.get()
	}
	// optimize out if the checksums do match
	if hdhobj != nil && cksumcfg.Checksum != cmn.ChecksumNone && !dryRun.disk && !dryRun.network {
		file, err = os.Open(fqn)
		// exists - compute checksum and compare with the caller's
		if err == nil {
			buf, slab := gmem2.AllocFromSlab2(0)
			if htype == cmn.ChecksumXXHash {
				xxHashVal, errstr = cmn.ComputeXXHash(file, buf)
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
			if errstr == "" && xxHashVal == hval {
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
		cmn.Assert(hdhobj == nil || htype == nhtype)
	}
	// validate checksum when and if provided
	if hval != "" && nhval != "" && hval != nhval && !dryRun.disk && !dryRun.network {
		errstr = fmt.Sprintf("Bad checksum: %s/%s %s %.8s... != %.8s...", bucket, objname, htype, hval, nhval)
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
			t.statsif.addMany(namedVal64{statPutCount, 1}, namedVal64{statPutLatency, int64(delta)})
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

func (t *targetrunner) doReplicationPut(w http.ResponseWriter, r *http.Request,
	bucket, objname, replicaSrc string) (errstr string) {

	var (
		started = time.Now()
		islocal = t.bmdowner.get().islocal(bucket)
	)

	fqn, errstr := cluster.FQN(bucket, objname, islocal)
	if errstr != "" {
		return errstr
	}
	err := getreplicationrunner().reqReceiveReplica(replicaSrc, fqn, r)

	if err != nil {
		return err.Error()
	}

	delta := time.Since(started)
	t.statsif.addMany(namedVal64{statReplPutCount, 1}, namedVal64{statReplPutLatency, int64(delta / time.Microsecond)})
	if glog.V(4) {
		glog.Infof("Replication PUT: %s/%s, %d µs", bucket, objname, int64(delta/time.Microsecond))
	}
	return ""
}

func (t *targetrunner) sglToCloudAsync(ct context.Context, sgl *memsys.SGL, bucket, objname, putfqn, fqn string, objprops *objectProps) {
	slab := sgl.Slab()
	buf := slab.Alloc()
	defer func() {
		sgl.Free()
		slab.Free(buf)
	}()
	// sgl => fqn sequence
	file, err := cmn.CreateFile(putfqn)
	if err != nil {
		t.fshc(err, putfqn)
		glog.Errorln("sglToCloudAsync: create", putfqn, err)
		return
	}
	reader := memsys.NewReader(sgl)
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
	cmn.Assert(written == sgl.Size())
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
	reopenFile := func() (io.ReadCloser, error) {
		return os.Open(putfqn)
	}

	if !islocal && !rebalance {
		if file, err = os.Open(putfqn); err != nil {
			errstr = fmt.Sprintf("Failed to reopen %s err: %v", putfqn, err)
			return
		}
		_, p := bucketmd.get(bucket, islocal)
		if p.NextTierURL != "" && p.WritePolicy == cmn.RWPolicyNextTier {
			if errstr, errcode = t.putObjectNextTier(p.NextTierURL, bucket, objname, file, reopenFile); errstr != "" {
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
			} else if errstr, errcode = t.putObjectNextTier(p.NextTierURL, bucket, objname, file, reopenFile); errstr != "" {
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
	uname := cluster.Uname(bucket, objname)
	t.rtnamemap.Lock(uname, true)

	if err = os.Rename(putfqn, fqn); err != nil {
		t.rtnamemap.Unlock(uname, true)
		errstr = fmt.Sprintf("Failed to rename %s => %s, err: %v", putfqn, fqn, err)
		return
	}
	renamed = true
	if errstr = t.finalizeobj(fqn, bucket, objprops); errstr != "" {
		t.rtnamemap.Unlock(uname, true)
		glog.Errorf("finalizeobj %s/%s: %s (%+v)", bucket, objname, errstr, objprops)
		return
	}
	t.rtnamemap.Unlock(uname, true)
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
	fqn, errstr := cluster.FQN(bucket, objname, bucketmd.islocal(bucket))
	if errstr != "" {
		return
	}
	ver := bucketmd.version()
	if t.si.DaemonID == from {
		//
		// the source
		//
		uname := cluster.Uname(bucket, objname)
		t.rtnamemap.Lock(uname, false)
		defer t.rtnamemap.Unlock(uname, false)

		finfo, err := os.Stat(fqn)
		if glog.V(3) {
			glog.Infof("Rebalance %s/%s from %s (self) to %s", bucket, objname, from, to)
		}
		if err != nil && os.IsNotExist(err) {
			errstr = fmt.Sprintf("File copy: %s %s at the source %s", fqn, doesnotexist, t.si.DaemonID)
			return
		}
		si := t.smapowner.get().GetTarget(to)
		if si == nil {
			errstr = fmt.Sprintf("File copy: unknown destination %s (Smap not in-sync?)", to)
			return
		}
		size = finfo.Size()
		if errstr = t.sendfile(r.Method, bucket, objname, si, size, "", ""); errstr != "" {
			return
		}
		if glog.V(4) {
			glog.Infof("Rebalance %s/%s done, %.2f MB", bucket, objname, float64(size)/cmn.MiB)
		}
	} else {
		//
		// the destination
		//
		if glog.V(4) {
			glog.Infof("Rebalance %s/%s from %s to %s (self, %s, ver %d)", bucket, objname, from, to, fqn, ver)
		}
		putfqn := cluster.GenContentFQN(fqn, cluster.DefaultWorkfileType)
		_, err := os.Stat(fqn)
		if err != nil && os.IsExist(err) {
			glog.Infof("File copy: %s already exists at the destination %s", fqn, t.si.DaemonID)
			return // not an error, nothing to do
		}
		var (
			hdhobj = newcksumvalue(r.Header.Get(cmn.HeaderDFCChecksumType), r.Header.Get(cmn.HeaderDFCChecksumVal))
			props  = &objectProps{version: r.Header.Get(cmn.HeaderDFCObjVersion)}
		)
		if timeStr := r.Header.Get(cmn.HeaderDFCObjAtime); timeStr != "" {
			if tm, err := time.Parse(time.RFC822, timeStr); err == nil {
				props.atime = tm
			}
		}
		if _, props.nhobj, size, errstr = t.receive(putfqn, objname, "", hdhobj, r.Body); errstr != "" {
			return
		}
		if props.nhobj != nil {
			nhtype, nhval := props.nhobj.get()
			htype, hval := hdhobj.get()
			cmn.Assert(htype == nhtype)
			if hval != nhval {
				errstr = fmt.Sprintf("Bad checksum at the destination %s: %s/%s %s %.8s... != %.8s...",
					t.si.DaemonID, bucket, objname, htype, hval, nhval)
				return
			}
		}
		errstr, _ = t.putCommit(t.contextWithAuth(r), bucket, objname, putfqn, fqn, props, true /*rebalance*/)
		if errstr == "" {
			t.statsif.addMany(namedVal64{statRxCount, 1}, namedVal64{statRxSize, size})
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
	fqn, errstr := cluster.FQN(bucket, objname, islocal)
	if errstr != "" {
		return errors.New(errstr)
	}
	uname := cluster.Uname(bucket, objname)

	t.rtnamemap.Lock(uname, true)
	defer t.rtnamemap.Unlock(uname, true)

	if !islocal && !evict {
		if errstr, errcode = getcloudif().deleteobj(ct, bucket, objname); errstr != "" {
			if errcode == 0 {
				return fmt.Errorf("%s", errstr)
			}
			return fmt.Errorf("%d: %s", errcode, errstr)
		}

		t.statsif.add(statDeleteCount, 1)
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
			t.statsif.addMany(namedVal64{statLruEvictCount, 1}, namedVal64{statLruEvictSize, finfo.Size()})
		}
	}
	return nil
}

func (t *targetrunner) renamefile(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	var errstr string

	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	newobjname := msg.Name
	uname := cluster.Uname(bucket, objname)
	t.rtnamemap.Lock(uname, true)

	if errstr = t.renameobject(bucket, objname, bucket, newobjname); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
	t.rtnamemap.Unlock(uname, true)
}

func (t *targetrunner) replicate(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, object := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	bucketmd := t.bmdowner.get()
	islocal := bucketmd.islocal(bucket)
	fqn, errstr := cluster.FQN(bucket, object, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	ok, props := bucketmd.get(bucket, islocal)
	if !ok {
		errstr = fmt.Sprintf("Failed to get bucket properties for bucket %q", bucket)
	} else if props.NextTierURL == "" {
		errstr = fmt.Sprintf("Bucket %q is not configured with next tier URL", bucket)
	} else if props.CloudProvider != cmn.ProviderDFC {
		errstr = fmt.Sprintf("Bucket %q is configured with invalid cloud provider: %q, expected provider: %q",
			bucket, props.CloudProvider, cmn.ProviderDFC)
	}
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	rr := getreplicationrunner()
	if err = rr.reqSendReplica(props.NextTierURL, fqn, false, replicationPolicySync); err != nil {
		errstr = fmt.Sprintf("Failed to replicate bucket/object: %s/%s, error: %v", bucket, object, err)
		t.invalmsghdlr(w, r, errstr)
		return
	}
}

func (t *targetrunner) renameobject(bucketFrom, objnameFrom, bucketTo, objnameTo string) (errstr string) {
	var (
		si     *cluster.Snode
		newfqn string
	)
	if si, errstr = hrwTarget(bucketTo, objnameTo, t.smapowner.get()); errstr != "" {
		return
	}
	bucketmd := t.bmdowner.get()
	islocalFrom := bucketmd.islocal(bucketFrom)
	fqn, errstr := cluster.FQN(bucketFrom, objnameFrom, islocalFrom)
	if errstr != "" {
		return
	}
	finfo, err := os.Stat(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Rename/move: failed to fstat %s (%s/%s), err: %v", fqn, bucketFrom, objnameFrom, err)
		return
	}
	// local rename
	if si.DaemonID == t.si.DaemonID {
		islocalTo := bucketmd.islocal(bucketTo)
		newfqn, errstr = cluster.FQN(bucketTo, objnameTo, islocalTo)
		if errstr != "" {
			return
		}
		dirname := filepath.Dir(newfqn)
		if err := cmn.CreateDir(dirname); err != nil {
			errstr = fmt.Sprintf("Unexpected failure to create local dir %s, err: %v", dirname, err)
		} else if err := os.Rename(fqn, newfqn); err != nil {
			errstr = fmt.Sprintf("Failed to rename %s => %s, err: %v", fqn, newfqn, err)
		} else {
			t.statsif.add(statRenameCount, 1)
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

// Rebalancing supports versioning. If an object in DFC cache has version in
// xattrs then the sender adds to HTTP header object version. A receiver side
// reads version from headers and set xattrs if the version is not empty
func (t *targetrunner) sendfile(method, bucket, objname string, destsi *cluster.Snode, size int64, newbucket, newobjname string) string {
	var (
		xxHashVal string
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
	url := destsi.PublicNet.DirectURL + cmn.URLPath(cmn.Version, cmn.Objects, newbucket, newobjname)
	url += fmt.Sprintf("?%s=%s&%s=%s", cmn.URLParamFromID, fromid, cmn.URLParamToID, toid)
	bucketmd := t.bmdowner.get()
	islocal := bucketmd.islocal(bucket)
	cksumcfg := &ctx.config.Cksum
	if bucketProps, _, defined := bucketmd.propsAndChecksum(bucket); defined {
		cksumcfg = &bucketProps.CksumConfig
	}
	fqn, errstr := cluster.FQN(bucket, objname, islocal)
	if errstr != "" {
		return errstr
	}

	var accessTime time.Time
	var ok bool
	if bucketmd.lruEnabled(bucket) {
		atimeResponse := <-getatimerunner().atime(fqn)
		accessTime, ok = atimeResponse.accessTime, atimeResponse.ok
	}

	// must read a file access before any operation: the next Open changes atime
	accessTimeStr := ""
	if ok {
		accessTimeStr = accessTime.Format(cmn.RFC822)
	} else {
		fileInfo, err := os.Stat(fqn)
		if err == nil {
			atime, mtime, _ := getAmTimes(fileInfo)
			if mtime.After(atime) {
				atime = mtime
			}
			accessTimeStr = atime.Format(cmn.RFC822)
		}
	}

	file, err := os.Open(fqn)
	if err != nil {
		return fmt.Sprintf("Failed to open %q, err: %v", fqn, err)
	}
	defer file.Close()

	if version, errstr = Getxattr(fqn, cmn.XattrObjVersion); errstr != "" {
		glog.Errorf("Failed to read %q xattr %s, err %s", fqn, cmn.XattrObjVersion, errstr)
	}

	if cksumcfg.Checksum != cmn.ChecksumNone {
		cmn.Assert(cksumcfg.Checksum == cmn.ChecksumXXHash, "invalid checksum type: '"+cksumcfg.Checksum+"'")
		buf, slab := gmem2.AllocFromSlab2(size)
		if xxHashVal, errstr = cmn.ComputeXXHash(file, buf); errstr != "" {
			slab.Free(buf)
			return errstr
		}
		slab.Free(buf)
		if _, err = file.Seek(0, 0); err != nil {
			return fmt.Sprintf("Unexpected fseek failure when sending %q from %s, err: %v", fqn, t.si.DaemonID, err)
		}
	}
	//
	// http request
	//
	request, err := http.NewRequest(method, url, file)
	if err != nil {
		return fmt.Sprintf("Unexpected failure to create %s request %s, err: %v", method, url, err)
	}
	if xxHashVal != "" {
		request.Header.Set(cmn.HeaderDFCChecksumType, cmn.ChecksumXXHash)
		request.Header.Set(cmn.HeaderDFCChecksumVal, xxHashVal)
	}
	if len(version) != 0 {
		request.Header.Set(cmn.HeaderDFCObjVersion, string(version))
	}
	if accessTimeStr != "" {
		request.Header.Set(cmn.HeaderDFCObjAtime, accessTimeStr)
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
	t.statsif.addMany(namedVal64{statTxCount, 1}, namedVal64{statTxSize, size})
	return ""
}

func (t *targetrunner) checkCacheQueryParameter(r *http.Request) (useCache bool, errstr string, errcode int) {
	useCacheStr := r.URL.Query().Get(cmn.URLParamCached)
	var err error
	if useCache, err = parsebool(useCacheStr); err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter: %s=%s (expecting: '' | true | false)",
			cmn.URLParamCached, useCacheStr)
		errcode = http.StatusInternalServerError
	}
	return
}

func (t *targetrunner) checkIsLocal(bucket string, bucketmd *bucketMD, q url.Values, islocal bool) (errstr string, errcode int) {
	proxylocalstr := q.Get(cmn.URLParamLocal)
	if proxylocalstr == "" {
		return
	}
	proxylocal, err := parsebool(proxylocalstr)
	if err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter for bucket %s: %s=%s (expecting bool)", bucket, cmn.URLParamLocal, proxylocalstr)
		errcode = http.StatusInternalServerError
		return
	}
	if islocal == proxylocal {
		return
	}
	errstr = fmt.Sprintf("islocalbucket(%s) mismatch: %t (proxy) != %t (target %s)", bucket, proxylocal, islocal, t.si.DaemonID)
	errcode = http.StatusInternalServerError
	if s := q.Get(cmn.URLParamBMDVersion); s != "" {
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
	q.Add(cmn.URLParamWhat, cmn.GetWhatBucketMeta)
	args := callArgs{
		si: psi,
		req: reqArgs{
			method: http.MethodGet,
			base:   psi.IntraControlNet.DirectURL,
			path:   cmn.URLPath(cmn.Version, cmn.Daemon),
			query:  q,
		},
		timeout: ctx.config.Timeout.CplaneOperation,
	}
	res := t.call(args)
	if res.err != nil {
		glog.Errorf("Failed to get-what=%s, err: %v", cmn.GetWhatBucketMeta, res.err)
		return
	}
	newbucketmd := &bucketMD{}
	err := jsoniter.Unmarshal(res.outjson, newbucketmd)
	if err != nil {
		glog.Errorf("Unexpected: failed to unmarshal get-what=%s response from %s, err: %v [%v]",
			cmn.GetWhatBucketMeta, psi.IntraControlNet.DirectURL, err, string(res.outjson))
		return
	}
	var msg = cmn.ActionMsg{Action: "get-what=" + cmn.GetWhatBucketMeta}
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
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /daemon path")
	}
	glog.Flush()
}

func (t *targetrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}
	if len(apitems) > 0 {
		switch apitems[0] {
		// PUT /v1/daemon/proxy/newprimaryproxyid
		case cmn.Proxy:
			t.httpdaesetprimaryproxy(w, r, apitems)
			return
		case cmn.SyncSmap:
			var newsmap = &smapX{}
			if t.readJSON(w, r, newsmap) != nil {
				return
			}
			if errstr := t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to sync Smap: %s", errstr))
			}
			glog.Infof("%s: %s v%d done", t.si.DaemonID, cmn.SyncSmap, newsmap.version())
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
		}
	}
	//
	// other PUT /daemon actions
	//
	var msg cmn.ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse cmn.ActionMsg value: Not a string"))
		} else if errstr := t.setconfig(msg.Name, value); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		} else {
			glog.Infof("setconfig %s=%s", msg.Name, value)
			if msg.Name == "lru_enabled" && value == "false" {
				_, lruxact := t.xactinp.findU(cmn.ActLRU)
				if lruxact != nil {
					if glog.V(3) {
						glog.Infof("Aborting LRU due to lru_enabled config change")
					}
					lruxact.Abort()
				}
			}
		}
	case cmn.ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected cmn.ActionMsg <- JSON [%v]", msg)
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
	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL Parameter: %v", cmn.URLParamPrepare, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	smap := t.smapowner.get()
	psi := smap.GetProxy(proxyid)
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
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	switch getWhat {
	case cmn.GetWhatConfig, cmn.GetWhatSmap, cmn.GetWhatBucketMeta, cmn.GetWhatSmapVote, cmn.GetWhatDaemonInfo:
		t.httprunner.httpdaeget(w, r)
	case cmn.GetWhatStats:
		rst := getstorstatsrunner()
		rst.RLock()
		jsbytes, err := jsoniter.Marshal(rst)
		rst.RUnlock()
		cmn.Assert(err == nil, err)
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case cmn.GetWhatXaction:
		kind := r.URL.Query().Get(cmn.URLParamProps)
		if errstr := validateXactionQueryable(kind); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		xactionStatsRetriever := t.getXactionStatsRetriever(kind) // FIXME
		allXactionDetails := t.getXactionsByType(kind)
		jsbytes := xactionStatsRetriever.getStats(allXactionDetails)
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case cmn.GetWhatMountpaths:
		mpList := cmn.MountpathList{}
		availablePaths, disabledPaths := fs.Mountpaths.Mountpaths()
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
	case cmn.XactionRebalance:
		xactionStatsRetriever = RebalanceTargetStats{}
	case cmn.XactionPrefetch:
		xactionStatsRetriever = PrefetchTargetStats{}
	}

	return xactionStatsRetriever
}

func (t *targetrunner) getXactionsByType(kind string) []XactionDetails {
	allXactionDetails := []XactionDetails{}
	for _, xaction := range t.xactinp.xactinp {
		if xaction.Kind() == kind {
			status := cmn.XactionStatusCompleted
			if !xaction.Finished() {
				status = cmn.XactionStatusInProgress
			}

			xactionStats := XactionDetails{
				Id:        xaction.ID(),
				StartTime: xaction.StartTime(),
				EndTime:   xaction.EndTime(),
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
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	if len(apiItems) > 0 {
		switch apiItems[0] {
		case cmn.Register:
			if glog.V(3) {
				glog.Infoln("Sending register signal to target keepalive control channel")
			}
			gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: register}
			return
		case cmn.Mountpaths:
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
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Mountpaths:
		t.handleMountpathReq(w, r)
		return
	case cmn.Unregister:
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
	reader io.Reader) (sgl *memsys.SGL, nhobj cksumvalue, written int64, errstr string) {
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
			cksumcfg = &bucketProps.CksumConfig
		}
	}

	if dryRun.network {
		reader = readers.NewRandReader(dryRun.size)
	}
	if !dryRun.disk {
		if file, err = cmn.CreateFile(fqn); err != nil {
			t.fshc(err, fqn)
			errstr = fmt.Sprintf("Failed to create %s, err: %s", fqn, err)
			return
		}
		filewriter = file
	} else {
		filewriter = ioutil.Discard
	}

	buf, slab := gmem2.AllocFromSlab2(0)
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
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf); err != nil {
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
	if cksumcfg.Checksum != cmn.ChecksumNone {
		cmn.Assert(cksumcfg.Checksum == cmn.ChecksumXXHash)
		xx := xxhash.New64()
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf, xx); err != nil {
			errstr = err.Error()
			t.fshc(err, fqn)
			return
		}
		hashIn64 := xx.Sum64()
		hashInBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(hashInBytes, hashIn64)
		nhval = hex.EncodeToString(hashInBytes)
		nhobj = newcksumvalue(cmn.ChecksumXXHash, nhval)
		if ohobj != nil {
			ohtype, ohval = ohobj.get()
			cmn.Assert(ohtype == cmn.ChecksumXXHash)
			if ohval != nhval {
				errstr = fmt.Sprintf("Bad checksum: %s %s %.8s... != %.8s... computed for the %q",
					objname, cksumcfg.Checksum, ohval, nhval, fqn)

				t.statsif.addMany(namedVal64{statErrCksumCount, 1}, namedVal64{statErrCksumSize, written})
				return
			}
		}
	} else if omd5 != "" && cksumcfg.ValidateColdGet {
		md5 := md5.New()
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf, md5); err != nil {
			errstr = err.Error()
			t.fshc(err, fqn)
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash := hex.EncodeToString(hashInBytes)
		if omd5 != md5hash {
			errstr = fmt.Sprintf("Bad checksum: cold GET %s md5 %.8s... != %.8s... computed for the %q",
				objname, ohval, nhval, fqn)

			t.statsif.addMany(namedVal64{statErrCksumCount, 1}, namedVal64{statErrCksumSize, written})
			return
		}
	} else {
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf); err != nil {
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
	s := query.Get(cmn.URLParamUnixTime)
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

// the opposite
func (t *targetrunner) fqn2bckobj(fqn string) (bucket, objName string, err error) {
	var (
		isLocal   bool
		parsedFQN fqnParsed
	)

	parsedFQN, err = fqn2info(fqn)
	if err != nil {
		return
	}

	bucket, objName, isLocal = parsedFQN.bucket, parsedFQN.objname, parsedFQN.islocal
	bucketmd := t.bmdowner.get()
	realFQN, errstr := cluster.FQN(bucket, objName, isLocal)
	if errstr != "" {
		return "", "", errors.New(errstr)
	}
	if realFQN != fqn || bucketmd.islocal(bucket) != isLocal {
		err = fmt.Errorf("Cannot convert %q => %s/%s - localbuckets or device mountpaths changed?", fqn, bucket, objName)
		return
	}

	return
}

// changedMountpath checks if the mountpath for provided fqn has changed. This
// situation can happen when new mountpath is added or mountpath is moved from
// disabled to enabled.
func (t *targetrunner) changedMountpath(fqn string) (bool, string, error) {
	parsedFQN, err := fqn2info(fqn)
	if err != nil {
		return false, "", err
	}
	bucket, objName, isLocal := parsedFQN.bucket, parsedFQN.objname, parsedFQN.islocal
	newFQN, errstr := cluster.FQN(bucket, objName, isLocal)
	if errstr != "" {
		return false, "", errors.New(errstr)
	}
	return fqn != newFQN, newFQN, nil
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
		if err := cmn.CreateDir(mpath); err != nil {
			glog.Errorf("FATAL: cannot create test cache dir %q, err: %v", mpath, err)
			os.Exit(1)
		}

		err := fs.Mountpaths.AddMountpath(mpath)
		cmn.Assert(err == nil, err)
	}
}

func (t *targetrunner) createBucketDirs(s, basename string, f func(basePath string) string) error {
	if basename == "" {
		return fmt.Errorf("empty basename for the %s buckets directory - update your config", s)
	}
	availablePaths, _ := fs.Mountpaths.Mountpaths()
	for _, mpathInfo := range availablePaths {
		dir := f(mpathInfo.Path)
		if _, exists := availablePaths[dir]; exists {
			return fmt.Errorf("local namespace partitioning conflict: %s vs %s", mpathInfo.Path, dir)
		}
		if err := cmn.CreateDir(dir); err != nil {
			return fmt.Errorf("cannot create %s buckets dir %q, err: %v", s, dir, err)
		}
	}
	return nil
}

func (t *targetrunner) detectMpathChanges() {
	// mpath config dir
	mpathconfigfqn := filepath.Join(ctx.config.Confdir, mpname)

	type mfs struct {
		Available cmn.StringSet `json:"available"`
		Disabled  cmn.StringSet `json:"disabled"`
	}

	// load old/prev and compare
	var (
		oldfs = mfs{}
		newfs = mfs{
			Available: make(cmn.StringSet),
			Disabled:  make(cmn.StringSet),
		}
	)

	availablePaths, disabledPath := fs.Mountpaths.Mountpaths()
	for mpath := range availablePaths {
		newfs.Available[mpath] = struct{}{}
	}
	for mpath := range disabledPath {
		newfs.Disabled[mpath] = struct{}{}
	}

	if err := cmn.LocalLoad(mpathconfigfqn, &oldfs); err != nil {
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
	if err := cmn.LocalSave(mpathconfigfqn, newfs); err != nil {
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
		return versioning == cmn.VersionAll || versioning == cmn.VersionLocal
	}
	return versioning == cmn.VersionAll || versioning == cmn.VersionCloud
}

// xattrs
func (t *targetrunner) finalizeobj(fqn, bucket string, objprops *objectProps) (errstr string) {
	if objprops.nhobj != nil {
		htype, hval := objprops.nhobj.get()
		cmn.Assert(htype == cmn.ChecksumXXHash)
		if errstr = Setxattr(fqn, cmn.XattrXXHashVal, []byte(hval)); errstr != "" {
			return errstr
		}
	}
	if objprops.version != "" {
		errstr = Setxattr(fqn, cmn.XattrObjVersion, []byte(objprops.version))
	}

	if !objprops.atime.IsZero() && t.bucketLRUEnabled(bucket) {
		getatimerunner().touch(fqn, objprops.atime)
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

	if vbytes, errstr = Getxattr(fqn, cmn.XattrObjVersion); errstr != "" {
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
	mpathInfo, _ := path2mpathInfo(filepath)
	if mpathInfo != nil {
		keyName := mpathInfo.Path
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

func (t *targetrunner) handleMountpathReq(w http.ResponseWriter, r *http.Request) {
	msg := cmn.ActionMsg{}
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
	case cmn.ActMountpathEnable:
		t.handleEnableMountpathReq(w, r, mountpath)
	case cmn.ActMountpathDisable:
		t.handleDisableMountpathReq(w, r, mountpath)
	case cmn.ActMountpathAdd:
		t.handleAddMountpathReq(w, r, mountpath)
	case cmn.ActMountpathRemove:
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
func (t *targetrunner) receiveBucketMD(newbucketmd *bucketMD, msg *cmn.ActionMsg, tag string) (errstr string) {
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

	availablePaths, _ := fs.Mountpaths.Mountpaths()
	// Remove buckets which don't exist in newbucketmd
	for bucket := range bucketmd.LBmap {
		if _, ok := newbucketmd.LBmap[bucket]; !ok {
			glog.Infof("Destroy local bucket %s", bucket)
			for _, mpathInfo := range availablePaths {
				localbucketfqn := filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path), bucket)
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
				localbucketfqn := filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path), bucket)
				if err := cmn.CreateDir(localbucketfqn); err != nil {
					glog.Errorf("Failed to create local bucket dir %q, err: %v", localbucketfqn, err)
				}
			}
		}
	}
	return
}

func (t *targetrunner) receiveSmap(newsmap *smapX, msg *cmn.ActionMsg) (errstr string) {
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
	glog.Infof("receive Smap: v%d, ntargets %d, primary %s%s", newsmap.version(), newsmap.CountTargets(), pid, s)
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
	if msg.Action == cmn.ActGlobalReb {
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
	// resume global rebalance
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
				base:   psi.IntraControlNet.DirectURL,
				path:   cmn.URLPath(cmn.Version, cmn.Health),
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
		if proxystats.Tracker[statUptimeLatency].Value != 0 {
			break
		}
	}
	atomic.StoreInt64(&t.clusterStarted, 1)
	glog.Infoln("cluster started up")
}

// broadcastNeighbors sends a message ([]byte) to all neighboring targets belongs to a smap
func (t *targetrunner) broadcastNeighbors(path string, query url.Values, method string, body []byte,
	smap *smapX, timeout time.Duration) chan callResult {

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
		servers: []map[string]*cluster.Snode{smap.Tmap},
	}
	return t.broadcast(bcastArgs)
}

func (t *targetrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Tokens); err != nil {
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
	if checksumAlgo != cmn.ChecksumXXHash {
		errstr := fmt.Sprintf("Unsupported checksum algorithm: [%s]", checksumAlgo)
		return false, errstr
	}

	xxHashBinary, errstr := Getxattr(fqn, cmn.XattrXXHashVal)
	if errstr != "" {
		errstr = fmt.Sprintf("Unable to read checksum of object [%s], err: %s", fqn, errstr)
		return false, errstr
	}

	if xxHashBinary == nil {
		glog.Warningf("%s has no checksum - cannot validate", fqn)
		return true, ""
	}

	file, err := os.Open(fqn)
	if err != nil {
		errstr := fmt.Sprintf("Failed to read object %s, err: %v", fqn, err)
		return false, errstr
	}

	buf, slab := gmem2.AllocFromSlab2(slabSize)
	xxHashVal, errstr := cmn.ComputeXXHash(file, buf)
	file.Close()
	slab.Free(buf)

	if errstr != "" {
		errstr := fmt.Sprintf("Unable to compute xxHash, err: %s", errstr)
		return false, errstr
	}

	return string(xxHashBinary) == xxHashVal, ""
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
			if cmn.IsErrConnectionRefused(eunreg) || status == http.StatusRequestTimeout {
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
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
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

func (t *targetrunner) getFromNeighborFS(bucket, object string, islocal bool) (fqn string, size int64) {
	availablePaths, _ := fs.Mountpaths.Mountpaths()
	fn := fs.Mountpaths.MakePathCloud
	if islocal {
		fn = fs.Mountpaths.MakePathLocal
	}
	for _, mpathInfo := range availablePaths {
		dir := fn(mpathInfo.Path)

		filePath := filepath.Join(dir, bucket, object)
		stat, err := os.Stat(filePath)
		if err == nil {
			return filePath, stat.Size()
		}
	}

	return "", 0
}

func (t *targetrunner) isRebalancing() bool {
	_, running := t.xactinp.isAbortedOrRunningRebalance()
	_, runningLocal := t.xactinp.isAbortedOrRunningLocalRebalance()
	return running || runningLocal
}

//====================== LRU: initiation  ======================================
//
// construct LRU contexts (lructx) and run them each of them in
// the respective goroutines which then do rest of the work - see lru.go
//
//==============================================================================

func (t *targetrunner) runLRU() {
	xlru := t.xactinp.renewLRU(t)
	if xlru == nil {
		return
	}
	wg := &sync.WaitGroup{}

	glog.Infof("LRU: %s started: dont-evict-time %v", xlru, ctx.config.LRU.DontEvictTime)

	//
	// NOTE the sequence: LRU local buckets first, Cloud buckets - second
	//

	availablePaths, _ := fs.Mountpaths.Mountpaths()
	for path, mpathInfo := range availablePaths {
		lctx := t.newlru(xlru, mpathInfo, fs.Mountpaths.MakePathLocal(path))
		wg.Add(1)
		go lctx.onelru(wg)
	}
	wg.Wait()
	for path, mpathInfo := range availablePaths {
		lctx := t.newlru(xlru, mpathInfo, fs.Mountpaths.MakePathCloud(path))
		wg.Add(1)
		go lctx.onelru(wg)
	}
	wg.Wait()

	if glog.V(4) {
		lruCheckResults(availablePaths)
	}
	xlru.EndTime(time.Now())
	glog.Infoln(xlru.String())
	t.xactinp.del(xlru.ID())
}

// construct lructx
func (t *targetrunner) newlru(xlru *xactLRU, mpathInfo *fs.MountpathInfo, bucketdir string) *lructx {
	lctx := &lructx{
		oldwork:     make([]*fileInfo, 0, 64),
		xlru:        xlru,
		t:           t,
		fs:          mpathInfo.FileSystem,
		bucketdir:   bucketdir,
		thrparams:   throttleParams{throttle: onDiskUtil | onFSUsed, fs: mpathInfo.FileSystem},
		atimeRespCh: make(chan *atimeResponse, 1),
		namelocker:  t.rtnamemap,
	}
	return lctx
}

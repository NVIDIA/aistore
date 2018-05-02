// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/dfc/statsd"
	"github.com/golang/glog"
)

const (
	syncmapldelay     = time.Second * 3
	syncmapsdelay     = time.Second
	maxmapsyncretries = 10
	tokenStart        = "Bearer"
)

// Keeps a target response when doing parallel requests to all targets
type bucketResp struct {
	outjson []byte
	err     error
	id      string
}

// A list of target local files: cached or local bucket
// Maximum number of files in the response is `pageSize` entries
type localFilePage struct {
	entries []*BucketEntry
	err     error
	id      string
	marker  string
}

type mapsBcastContext struct {
	sync.Mutex
	p           *proxyrunner
	action      string
	newtargetid string
	retry       bool
}

func wrapHandler(h http.HandlerFunc, wraps ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, w := range wraps {
		h = w(h)
	}

	return h
}

//===========================================================================
//
// proxy runner
//
//===========================================================================
type proxyrunner struct {
	httprunner
	starttime   time.Time
	smapversion int64
	confdir     string
	xactinp     *xactInProgress
	lbmap       *lbmap
	hintsmap    *Smap
	syncmapinp  int64
	primary     bool
	statsdC     statsd.Client
	authn       *authManager
}

// start proxy runner
func (p *proxyrunner) run() error {
	p.httprunner.init(getproxystatsrunner(), true)
	p.httprunner.kalive = getproxykalive()

	p.xactinp = newxactinp()
	// local (aka cache-only) buckets
	p.lbmap = &lbmap{LBmap: make(map[string]string)}
	lbpathname := filepath.Join(p.confdir, lbname)
	p.lbmap.lock()
	if LocalLoad(lbpathname, p.lbmap) != nil {
		// create empty
		p.lbmap.Version = 1
		if err := LocalSave(lbpathname, p.lbmap); err != nil {
			glog.Fatalf("FATAL: cannot store localbucket config, err: %v", err)
		}
	}
	p.lbmap.unlock()
	if ctx.config.TestFSP.Instance > 0 {
		// Create instance-specific smap directory when testing locally.
		instancedir := filepath.Join(ctx.config.Confdir, strconv.Itoa(ctx.config.TestFSP.Instance))
		if err := CreateDir(instancedir); err != nil {
			return fmt.Errorf("Failed to create instance confdir %q, err: %v", instancedir, err)
		}
	}

	isproxy := os.Getenv("DFCPRIMARYPROXY")

	/* A proxy starts as primary if either (or both):
	1. The DFCPRIMARYPROXY environment variable is set to a non-empty-string value.
	2. The ID of the primary proxy in the config file is its ID
	*/
	if isproxy == "" && ctx.config.Proxy.Primary.ID != p.si.DaemonID {
		// Register proxy if it isn't the Primary proxy
		err := p.registerWithRetry(ctx.config.Proxy.Primary.URL, 0)
		if err != nil {
			return fmt.Errorf("Failed to register with primary proxy: %v", err)
		}
		p.primary = false

		smap, err := p.getSmapFromPrimary()
		if err != nil {
			p.smap = &Smap{}
			return fmt.Errorf("Failed to get cluster map from Primary Proxy: %v", err)
		}
		err = p.setPrimaryProxyAndSmap(smap)
		if err != nil {
			return fmt.Errorf("Failed to set Primary Proxy and Smap: %v", err)
		}
	} else {
		smappathname := filepath.Join(p.confdir, smapname)
		if ctx.config.TestFSP.Instance > 0 {
			instancedir := filepath.Join(ctx.config.Confdir, strconv.Itoa(ctx.config.TestFSP.Instance))
			smappathname = filepath.Join(instancedir, smapname)
		}
		p.hintsmap = &Smap{Smap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
		if err := LocalLoad(smappathname, p.hintsmap); err != nil && !os.IsNotExist(err) {
			glog.Warningf("Failed to load existing hint smap: %v", err)
		}
		p.smap.Smap = make(map[string]*daemonInfo, 8)
		p.smap.Pmap = make(map[string]*proxyInfo, 8)
		p.smap.addProxy(&proxyInfo{
			daemonInfo: *p.si,
			Primary:    true,
		})
		p.primary = true

		go func() {
			// getSmapFromHintCluster will query all nodes in the hint Smap to attempt to retrieve the current cluster map.
			timeout := time.After(ctx.config.Proxy.StartupGetSmapMaximum)
			// This for loop will retry it until it succeeds (It will fail if called during a vote)
			for p.getSmapFromHintCluster() {
				select {
				case <-timeout:
					assert(false, "getSmapFromHintCluster took too long")
				default:
				}
				time.Sleep(3 * time.Second) // 3 Seconds is the same time delay as registerWithRetry
			}
		}()
	}
	p.smap.ProxySI = &proxyInfo{daemonInfo: *p.si, Primary: p.primary}
	// startup: sync local buckets and cluster map when the latter stabilizes
	if p.primary {
		go p.synchronizeMaps(clivars.ntargets, "", nil)
	}

	//
	// REST API: register proxy handlers and start listening
	//
	if ctx.config.Auth.Enabled {
		p.authn = &authManager{}
		p.httprunner.registerhdlr("/"+Rversion+"/"+Rbuckets+"/", wrapHandler(p.buckethdlr, p.checkHTTPAuth))
		p.httprunner.registerhdlr("/"+Rversion+"/"+Robjects+"/", wrapHandler(p.objecthdlr, p.checkHTTPAuth))
	} else {
		p.httprunner.registerhdlr("/"+Rversion+"/"+Rbuckets+"/", p.buckethdlr)
		p.httprunner.registerhdlr("/"+Rversion+"/"+Robjects+"/", p.objecthdlr)
	}
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon, p.daemonhdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon+"/", p.daemonhdlr) // FIXME
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster, p.clusterhdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster+"/", p.clusterhdlr) // FIXME
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rhealth, p.httphealth)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rvote+"/", p.votehdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rtokens, p.tokenhdlr)
	p.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Proxy %s is ready, primary=%t", p.si.DaemonID, p.primary)
	glog.Flush()
	p.starttime = time.Now()

	// Note: hard coding statsd's ip and port for two reasons:
	// 1. it is well known, conflicts are unlikely, less config is better
	// 2. if do need configuable, will make a separate change, easier to manage
	var err error
	p.statsdC, err = statsd.New("localhost", 8125,
		fmt.Sprintf("dfcproxy.%s", strings.Replace(p.si.DaemonID, ":", "_", -1)))
	if err != nil {
		glog.Info("Failed to connect to statd, running without statsd")
	}

	return p.httprunner.run()
}

func (p *proxyrunner) register(timeout time.Duration) (status int, err error) {
	var url string
	if p.proxysi.DaemonID != "" {
		url = p.proxysi.DirectURL
	} else {
		// Smap has not yet been synced
		url = ctx.config.Proxy.Primary.URL
	}
	return p.registerWithURL(url, timeout)
}

func (p *proxyrunner) registerWithURL(proxyurl string, timeout time.Duration) (status int, err error) {
	jsbytes, err := json.Marshal(p.si)
	assert(err == nil)

	url := proxyurl + "/" + Rversion + "/" + Rcluster + "/" + Rproxy
	if timeout > 0 {
		url += "/" + Rkeepalive
		_, err, _, status = p.call(nil, url, http.MethodPost, jsbytes, timeout)
	} else {
		_, err, _, status = p.call(nil, url, http.MethodPost, jsbytes)
	}
	return
}

func (p *proxyrunner) unregister() (status int, err error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s/%s", ctx.config.Proxy.Primary.URL, Rversion, Rcluster, Rdaemon, Rproxy, p.si.DaemonID)
	_, err, _, status = p.call(nil, url, http.MethodDelete, nil)
	return
}

// stop gracefully
func (p *proxyrunner) stop(err error) {
	glog.Infof("Stopping %s(primary=%v), err: %v", p.name, p.primary, err)
	p.xactinp.abortAll()

	if p.primary {
		// give targets and non primary proxies some time to unregister
		// FIXME: What if there is no map version change but still has unregistered proxy/targets?
		//        may be wait until all are unregistered by counting numbers
		version := p.smap.versionLocked()
		for i := 0; i < 20; i++ {
			time.Sleep(time.Second)
			v := p.smap.versionLocked()
			if version == v {
				break
			}

			version = v
		}
	}

	if p.httprunner.h != nil && !p.primary {
		// FIXME: The primary proxy should not unregister
		// If it unregisters, then the cluster map will be incorrect.
		// Potentially, it should notify the cluster that it is shutting down.
		_, unregerr := p.unregister()
		if unregerr != nil {
			glog.Errorf("Failed to unregister: %v", unregerr)
		}
	}

	p.statsdC.Close()
	p.httprunner.stop(err)
}

func (p *proxyrunner) registerWithRetry(url string, timeout time.Duration) error {
	if status, err := p.registerWithURL(url, timeout); err != nil {
		glog.Errorf("Proxy %s failed to register with primary proxy, err: %v", p.si.DaemonID, err)
		if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
			glog.Errorf("Proxy %s: retrying registration...", p.si.DaemonID)
			time.Sleep(time.Second * 3)
			if _, err = p.registerWithURL(url, timeout); err != nil {
				glog.Errorf("Proxy %s failed to register with primary proxy, err: %v", p.si.DaemonID, err)
				glog.Errorf("Proxy %s is terminating", p.si.DaemonID)
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func (p *proxyrunner) getSmapFromHintCluster() (retry bool) {
	/*
		getSmapFromHintCluster broadcasts a Get VoteSmap request to all nodes in the union of the current Smap and the hint Smap.
		If any of these requests return the fact that a vote is in progress, the function exits with retry = true.
		It not, it chooses the maximum version Smap returned and considers it the most recent cluster map.
		If it is not the primary in that Smap, it registers with the current primary.
	*/
	glog.Infoln("Starting smap broadcast")

	msg := GetMsg{GetWhat: GetWhatSmapVote}
	jsbytes, err := json.Marshal(msg)
	assert(err == nil)
	method := http.MethodGet
	urlfmt := fmt.Sprintf("%%s/%s/%s", Rversion, Rdaemon)

	// Broadcast to all nodes in the current Smap and the hint Smap
	unionsmap := p.unionSmapAndHintsmap()
	svms := make(chan SmapVoteMsg, unionsmap.count()+unionsmap.countProxies())

	callback := func(si *daemonInfo, r []byte, err error, status int) {
		// Get all non-zero version smaps retrieved.
		if err != nil {
			glog.Warningf("Error retrieving cluster map from %v: %v", si.DaemonID, err)
			return
		}
		svm := SmapVoteMsg{}
		if err = json.Unmarshal(r, &svm); err != nil {
			glog.Warningf("Error unmarshalling cluster map from %v: %v", si.DaemonID, err)
			return
		}
		if svm.Smap == nil || svm.Smap.version() == 0 {
			return
		}
		svms <- svm
	}

	p.broadcast(urlfmt, method, jsbytes, unionsmap, callback)
	// Retrieve maximum version Smap
	var maxVersionSmap *Smap
maxloop:
	for {
		select {
		case svm := <-svms:
			if svm.VoteInProgress {
				// FIXME: Currently, the primary proxy only discovers a vote if it requests the Smap from the candidate primary proxy.
				retry = true
				return
			}

			if maxVersionSmap == nil || svm.Smap.version() > maxVersionSmap.version() {
				maxVersionSmap = svm.Smap
			}
		default:
			break maxloop
		}
	}
	if maxVersionSmap == nil {
		// No other nodes have a cluster map
		glog.Infoln("No other cluster maps found; remaining primary.")
		return
	}
	// If there is a non-zero version smap:
	if maxVersionSmap.ProxySI.DaemonID == p.si.DaemonID {
		glog.Infoln("This proxy is primary in found cluster map; remaining primary.")
		p.smap.lock()
		defer p.smap.unlock()
		p.smap = maxVersionSmap
	} else {
		glog.Infoln("This proxy is not primary in found cluster map; registering with found primary.")
		err := p.registerWithRetry(maxVersionSmap.ProxySI.DirectURL, ctx.config.Timeout.Default)
		if err != nil {
			glog.Errorf("Error registering with primary proxy %v: %v. Retrying.", maxVersionSmap.ProxySI.DaemonID, err)
			retry = true
			return
		}
		p.smap.lock()
		defer p.smap.unlock()
		p.becomeNonPrimaryProxy()
		p.smap = maxVersionSmap
		err = p.setPrimaryProxy(maxVersionSmap.ProxySI.DaemonID, "" /* primaryToRemove */, false /* prepare */)
		/*
			If there is an error when setting the primary proxy to the one from a given Smap, after setting p.smap to that Smap, then there must be an issue in that Smap.
			It should always be possible to set the primary proxy to smap.ProxySI when the cluster map being used is smap.
		*/
		assert(err == nil, err)
	}

	return
}

func (p *proxyrunner) unionSmapAndHintsmap() *Smap {
	p.smap.lock()
	defer p.smap.unlock()
	// Keys in p.smap will override any shared keys with p.hintsmap
	return SmapUnion(p.hintsmap, p.smap)
}

func (p *proxyrunner) getSmapFromPrimary() (*Smap, error) {
	msg := GetMsg{GetWhat: GetWhatSmap}
	jsbytes, err := json.Marshal(msg)
	assert(err == nil)
	method := http.MethodGet
	var url string
	if p.proxysi.DaemonID != "" {
		url = p.proxysi.DirectURL
	} else {
		// Smap has not yet been synced
		url = ctx.config.Proxy.Primary.URL
	}
	url = fmt.Sprintf("%s/%s/%s", url, Rversion, Rdaemon)

	r, err, _, _ := p.call(&p.proxysi.daemonInfo, url, method, jsbytes)
	if err != nil {
		err = fmt.Errorf("Error retrieving cluster map from Primary Proxy: %v", err)
		return nil, err
	}
	smap := Smap{}
	if err = json.Unmarshal(r, &smap); err != nil {
		err = fmt.Errorf("Error unmarshalling cluster map from Primary Proxy: %v", err)
		return nil, err
	}

	return &smap, nil
}

//===========================================================================================
//
// http handlers: data and metadata
//
//===========================================================================================

// verb /Rversion/Rbuckets/
func (p *proxyrunner) buckethdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpbckget(w, r)
	case http.MethodDelete:
		p.httpbckdelete(w, r)
	case http.MethodPost:
		p.httpbckpost(w, r)
	case http.MethodHead:
		p.httpbckhead(w, r)
	default:
		invalhdlr(w, r)
	}
}

// verb /Rversion/Robjects/
func (p *proxyrunner) objecthdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpobjget(w, r)
	case http.MethodPut:
		p.httpobjput(w, r)
	case http.MethodDelete:
		p.httpobjdelete(w, r)
	case http.MethodPost:
		p.httpobjpost(w, r)
	default:
		invalhdlr(w, r)
	}
}

// GET /v1/buckets/bucket-name
func (p *proxyrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	if p.smap.count() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	// all bucket names
	if bucket == "*" {
		p.getbucketnames(w, r, bucket)
		return
	}
	// list the bucket
	pagemarker, ok := p.listbucket(w, r, bucket)
	if ok {
		delta := time.Since(started)
		p.statsdC.Send("list",
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
		p.statsif.addMany("numlist", int64(1), "listlatency", lat)

		if glog.V(3) {
			if pagemarker != "" {
				glog.Infof("LIST: %s, page %s, %d µs", bucket, pagemarker, lat)
			} else {
				glog.Infof("LIST: %s, %d µs", bucket, lat)
			}
		}
	}
}

// GET /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	if p.smap.count() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	si, errstr := hrwTarget(bucket+"/"+objname, p.smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := fmt.Sprintf("%s%s?%s=%t", si.DirectURL, r.URL.Path, URLParamLocal, p.islocalBucket(bucket))
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	if !ctx.config.Proxy.Primary.Passthru && len(objname) > 0 {
		glog.Infof("passthru=false: proxy initiates the GET %s/%s", bucket, objname)
		p.receiveDrop(w, r, redirecturl) // ignore error, proceed to http redirect
	}
	http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)

	// Note: ideally, would prefer to call statsd in statsif.add(), but it doesn't have type support,
	//       the name schema is different(statd doesn't need dup 'numget', it is get.count), statsif's dependency
	//       on the names, and (not that importantly) the unit is different (ms vs us).
	delta := time.Since(started)
	p.statsdC.Send("get",
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

	p.statsif.addMany("numget", int64(1), "getlatency", int64(delta/1000))
}

// PUT "/"+Rversion+"/"+Robjects
func (p *proxyrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Robjects); apitems == nil {
		return
	}
	bucket := apitems[0]
	//
	// FIXME: add protection agaist putting into non-existing local bucket
	//
	objname := strings.Join(apitems[1:], "/")
	si, errstr := hrwTarget(bucket+"/"+objname, p.smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := fmt.Sprintf("%s%s?%s=%t", si.DirectURL, r.URL.Path, URLParamLocal, p.islocalBucket(bucket))
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	delta := time.Since(started)
	p.statsdC.Send("put",
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

	p.statsif.addMany("numput", int64(1), "putlatency", int64(delta/1000))
}

// DELETE { action } /Rversion/Rbuckets
func (p *proxyrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var msg ActionMsg
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket := apitems[0]
	if err := p.readJSON(w, r, &msg); err != nil {
		return
	}
	switch msg.Action {
	case ActDestroyLB:
		p.deleteLocalBucket(w, r, bucket)
	case ActDelete, ActEvict:
		p.actionlistrange(w, r, &msg)
	default:
		p.invalmsghdlr(w, r, fmt.Sprintf("Unsupported Action: %s", msg.Action))
	}
}

// DELETE /Rversion/Robjects/object-name
func (p *proxyrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket := apitems[0]
	objname := strings.Join(apitems[1:], "/")
	si, errstr := hrwTarget(bucket+"/"+objname, p.smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}

	p.statsdC.Send("delete",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
	)

	p.statsif.add("numdelete", 1)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

// GET /Rversion/Rhealth
func (p *proxyrunner) httphealth(w http.ResponseWriter, r *http.Request) {
	proxycorestats := getproxystats()
	jsbytes, err := json.Marshal(proxycorestats)
	assert(err == nil, err)
	p.writeJSON(w, r, jsbytes, "targetcorestats")
}

// POST { action } /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	var msg ActionMsg
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	lbucket := apitems[0]
	if !p.validatebckname(w, r, lbucket) {
		return
	}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActCreateLB:
		if !p.checkPrimaryProxy("create local bucket", w, r) {
			return
		}
		p.lbmap.lock()
		defer p.lbmap.unlock()
		if !p.lbmap.add(lbucket) {
			s := fmt.Sprintf("Local bucket %s already exists", lbucket)
			p.invalmsghdlr(w, r, s)
			return
		}
		p.synclbmap(w, r)
	case ActRenameLB:
		if !p.checkPrimaryProxy("rename local bucket", w, r) {
			return
		}
		bucketFrom, bucketTo := lbucket, msg.Name
		if bucketFrom == "" || bucketTo == "" {
			errstr := fmt.Sprintf("Invalid rename local bucket request: empty name %s => %s",
				bucketFrom, bucketTo)
			p.invalmsghdlr(w, r, errstr)
			return
		}
		p.lbmap.lock()
		lbmap4ren := &lbmap{}
		copyStruct(lbmap4ren, p.lbmap)
		p.lbmap.unlock()

		_, ok := lbmap4ren.LBmap[bucketFrom]
		if !ok {
			s := fmt.Sprintf("Local bucket %s does not exist", bucketFrom)
			p.invalmsghdlr(w, r, s)
			return
		}
		_, ok = lbmap4ren.LBmap[bucketTo]
		if ok {
			s := fmt.Sprintf("Local bucket %s already exists", bucketTo)
			p.invalmsghdlr(w, r, s)
			return
		}
		// FIXME: must be 2-phase with the 1st phase to copy and the 2nd phase to confirm and delete old
		if !p.renamelocalbucket(bucketFrom, bucketTo, lbmap4ren, &msg, r.Method) {
			errstr := fmt.Sprintf("Failed to rename local bucket %s => %s", bucketFrom, bucketTo)
			p.invalmsghdlr(w, r, errstr)
		}
		glog.Infof("renamed local bucket %s => %s, lbmap version %d", bucketFrom, bucketTo, p.lbmap.versionLocked())
	case ActSyncLB:
		if !p.checkPrimaryProxy("synchronize LBmap", w, r) {
			return
		}
		p.lbmap.lock()
		defer p.lbmap.unlock()
		p.synclbmap(w, r)
	case ActPrefetch:
		p.actionlistrange(w, r, &msg)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

// POST { action } /v1/objects/bucket-name
func (p *proxyrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg ActionMsg
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Robjects); apitems == nil {
		return
	}
	lbucket := apitems[0]
	if !p.validatebckname(w, r, lbucket) {
		return
	}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActRename:
		p.filrename(w, r, &msg)
		return
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
		return
	}
}

// HEAD /v1/buckets/bucket-name
func (p *proxyrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	var si *daemonInfo
	// Use random map iteration order to choose a random target to redirect to
	for _, si = range p.smap.Smap {
		break
	}
	redirecturl := fmt.Sprintf("%s%s?%s=%t", si.DirectURL, r.URL.Path, URLParamLocal, p.islocalBucket(bucket))
	if glog.V(3) {
		glog.Infof("%s %s => %s", r.Method, bucket, si.DaemonID)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

//====================================================================================
//
// supporting methods and misc
//
//====================================================================================
func (p *proxyrunner) renamelocalbucket(bucketFrom, bucketTo string, lbmap4ren *lbmap, msg *ActionMsg, method string) bool {
	urlfmt := fmt.Sprintf("%%s/%s/%s/%s", Rversion, Rbuckets, bucketFrom)
	smap4bcast := &Smap{}
	p.smap.copyLocked(smap4bcast)
	f := func(si *daemonInfo, _ []byte, err error, status int) {
		if err != nil {
			glog.Errorf("Target %s failed to rename local bucket %s => %s, err: %v (%d)",
				si.DaemonID, bucketFrom, bucketTo, err, status)
		}
	}
	msg.Value = lbmap4ren
	jsbytes, err := json.Marshal(msg)
	assert(err == nil, err)
	p.broadcast(urlfmt, method, jsbytes, smap4bcast, f, ctx.config.Timeout.Default)

	p.lbmap.lock()
	defer p.lbmap.unlock()

	p.lbmap.del(bucketFrom)
	p.lbmap.add(bucketTo)
	lbpathname := filepath.Join(p.confdir, lbname)
	if err := LocalSave(lbpathname, p.lbmap); err != nil {
		glog.Errorf("Failed to store lbmap %s, err: %v", lbpathname, err)
		return false
	}
	return true
}

func (p *proxyrunner) getbucketnames(w http.ResponseWriter, r *http.Request, bucketspec string) {
	q := r.URL.Query()
	localonly, _ := parsebool(q.Get(URLParamLocal))
	if localonly {
		bucketnames := &BucketNames{Cloud: []string{}, Local: make([]string, 0, 64)}
		for bucket := range p.lbmap.LBmap {
			bucketnames.Local = append(bucketnames.Local, bucket)
		}
		jsbytes, err := json.Marshal(bucketnames)
		assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "getbucketnames?local=true")
		return
	}

	var si *daemonInfo
	for _, si = range p.smap.Smap {
		break
	}
	url := si.DirectURL + "/" + Rversion + "/" + Rbuckets + "/" + bucketspec
	outjson, err, errstr, status := p.call(si, url, r.Method, nil)
	if err != nil {
		p.invalmsghdlr(w, r, errstr)
		p.kalive.onerr(err, status)
	} else {
		p.writeJSON(w, r, outjson, "getbucketnames")
	}
	return
}

// For cached = false goes to the Cloud, otherwise returns locally cached files
func (p *proxyrunner) targetListBucket(bucket string, dinfo *daemonInfo,
	reqBody []byte, islocal bool, cached bool) (response *bucketResp, err error) {
	url := fmt.Sprintf("%s/%s/%s/%s?%s=%v&%s=%v", dinfo.DirectURL, Rversion,
		Rbuckets, bucket, URLParamLocal, islocal, URLParamCached, cached)
	outjson, err, _, status := p.call(dinfo, url, http.MethodGet, reqBody, ctx.config.Timeout.Default)
	if err != nil {
		p.kalive.onerr(err, status)
	}

	response = &bucketResp{
		outjson: outjson,
		err:     err,
		id:      dinfo.DaemonID,
	}
	return response, err
}

// Receives info about locally cached files from targets in batches
// and merges with existing list of cloud files
func (p *proxyrunner) consumeCachedList(bmap map[string]*BucketEntry,
	dataCh chan *localFilePage, errch chan error, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	for rb := range dataCh {
		if rb.err != nil {
			if errch != nil {
				errch <- rb.err
			}
			glog.Errorf("Failed to get information about file in DFC cache: %v", rb.err)
			return
		}
		if rb.entries == nil || len(rb.entries) == 0 {
			continue
		}

		for _, newEntry := range rb.entries {
			nm := newEntry.Name
			if entry, ok := bmap[nm]; ok {
				entry.IsCached = true
				entry.Atime = newEntry.Atime
			}
		}
	}
}

// Request list of all cached files from a target.
// The target returns its list in batches `pageSize` length
func (p *proxyrunner) generateCachedList(bucket string, daemon *daemonInfo,
	dataCh chan *localFilePage, wg *sync.WaitGroup, origmsg *GetMsg) {
	if wg != nil {
		defer wg.Done()
	}
	var msg GetMsg
	copyStruct(&msg, origmsg)
	msg.GetPageSize = internalPageSize
	for {
		// re-marshall with an updated PageMarker
		listmsgjson, err := json.Marshal(&msg)
		assert(err == nil, err)
		resp, err := p.targetListBucket(bucket, daemon, listmsgjson, false /* islocal */, true /* cachedObjects */)
		if err != nil {
			if dataCh != nil {
				dataCh <- &localFilePage{
					id:  daemon.DaemonID,
					err: err,
				}
			}
			glog.Errorf("Failed to get information about cached objects on target %v: %v", daemon.DaemonID, err)
			return
		}

		if resp.outjson == nil || len(resp.outjson) == 0 {
			return
		}

		entries := BucketList{Entries: make([]*BucketEntry, 0, 128)}
		if err := json.Unmarshal(resp.outjson, &entries); err != nil {
			if dataCh != nil {
				dataCh <- &localFilePage{
					id:  daemon.DaemonID,
					err: err,
				}
			}
			glog.Errorf("Failed to unmarshall cached objects list from target %v: %v", daemon.DaemonID, err)
			return
		}

		msg.GetPageMarker = entries.PageMarker
		if dataCh != nil {
			dataCh <- &localFilePage{
				err:     nil,
				id:      daemon.DaemonID,
				marker:  entries.PageMarker,
				entries: entries.Entries,
			}
		}

		// empty PageMarker means that there are no more files to
		// return. So, the loop can be interrupted
		if len(entries.Entries) == 0 || entries.PageMarker == "" {
			break
		}
	}
}

// Get list of cached files from all targets and update the list
// of files from cloud with local metadata (iscached, atime etc)
func (p *proxyrunner) collectCachedFileList(bucket string, fileList *BucketList, getmsgjson []byte) (err error) {
	reqParams := &GetMsg{}
	err = json.Unmarshal(getmsgjson, reqParams)
	if err != nil {
		return
	}

	bucketMap := make(map[string]*BucketEntry, initialBucketListSize)
	for _, entry := range fileList.Entries {
		bucketMap[entry.Name] = entry
	}

	dataCh := make(chan *localFilePage, p.smap.count())
	errch := make(chan error, 1)
	wgConsumer := &sync.WaitGroup{}
	wgConsumer.Add(1)
	go p.consumeCachedList(bucketMap, dataCh, errch, wgConsumer)

	// since cached file page marker is not compatible with any cloud
	// marker, it should be empty for the first call
	reqParams.GetPageMarker = ""

	wg := &sync.WaitGroup{}
	for _, daemon := range p.smap.Smap {
		wg.Add(1)
		go p.generateCachedList(bucket, daemon, dataCh, wg, reqParams)
	}
	wg.Wait()
	close(dataCh)
	wgConsumer.Wait()

	select {
	case err = <-errch:
		return
	default:
	}

	fileList.Entries = make([]*BucketEntry, 0, len(bucketMap))
	for _, entry := range bucketMap {
		fileList.Entries = append(fileList.Entries, entry)
	}

	// sort the result to be consistent with other API
	ifLess := func(i, j int) bool {
		return fileList.Entries[i].Name < fileList.Entries[j].Name
	}
	sort.Slice(fileList.Entries, ifLess)

	return
}

func (p *proxyrunner) getLocalBucketObjects(bucket string, listmsgjson []byte) (allentries *BucketList, err error) {
	type targetReply struct {
		resp *bucketResp
		err  error
	}
	const (
		islocal    = true
		cachedObjs = false
	)
	msg := &GetMsg{}
	if err = json.Unmarshal(listmsgjson, msg); err != nil {
		return
	}
	pageSize := DefaultPageSize
	if msg.GetPageSize != 0 {
		pageSize = msg.GetPageSize
	}

	if pageSize > MaxPageSize {
		glog.Warningf("Page size(%d) for local bucket %s exceeds the limit(%d)", msg.GetPageSize, bucket, MaxPageSize)
	}

	chresult := make(chan *targetReply, len(p.smap.Smap))
	wg := &sync.WaitGroup{}

	targetCallFn := func(si *daemonInfo) {
		defer wg.Done()
		resp, err := p.targetListBucket(bucket, si, listmsgjson, islocal, cachedObjs)
		chresult <- &targetReply{resp, err}
	}

	for _, si := range p.smap.Smap {
		wg.Add(1)
		go targetCallFn(si)
	}
	wg.Wait()
	close(chresult)

	// combine results
	allentries = &BucketList{Entries: make([]*BucketEntry, 0, pageSize)}
	for r := range chresult {
		if r.err != nil {
			err = r.err
			return
		}

		if r.resp.outjson == nil || len(r.resp.outjson) == 0 {
			continue
		}

		bucketList := &BucketList{Entries: make([]*BucketEntry, 0, pageSize)}
		if err = json.Unmarshal(r.resp.outjson, &bucketList); err != nil {
			return
		}

		if len(bucketList.Entries) == 0 {
			continue
		}

		allentries.Entries = append(allentries.Entries, bucketList.Entries...)
	}

	// return the list always sorted in alphabetical order
	entryLess := func(i, j int) bool {
		return allentries.Entries[i].Name < allentries.Entries[j].Name
	}
	sort.Slice(allentries.Entries, entryLess)

	// shrink the result to `pageSize` entries. If the page is full than
	// mark the result incomplete by setting PageMarker
	if len(allentries.Entries) >= pageSize {
		for i := pageSize; i < len(allentries.Entries); i++ {
			allentries.Entries[i] = nil
		}

		allentries.Entries = allentries.Entries[:pageSize]
		allentries.PageMarker = allentries.Entries[pageSize-1].Name
	}

	return allentries, nil
}

func (p *proxyrunner) getCloudBucketObjects(bucket string, listmsgjson []byte) (allentries *BucketList, err error) {
	const (
		islocal       = false
		cachedObjects = false
	)
	var resp *bucketResp
	allentries = &BucketList{Entries: make([]*BucketEntry, 0, initialBucketListSize)}
	msg := GetMsg{}
	err = json.Unmarshal(listmsgjson, &msg)
	if err != nil {
		return
	}
	if msg.GetPageSize > MaxPageSize {
		glog.Warningf("Page size(%d) for cloud bucket %s exceeds the limit(%d)", msg.GetPageSize, bucket, MaxPageSize)
	}

	// first, get the cloud object list from a random target
	for _, si := range p.smap.Smap {
		resp, err = p.targetListBucket(bucket, si, listmsgjson, islocal, cachedObjects)
		if err != nil {
			return
		}
		break
	}

	if resp.outjson == nil || len(resp.outjson) == 0 {
		return
	}
	if err = json.Unmarshal(resp.outjson, &allentries); err != nil {
		return
	}
	if len(allentries.Entries) == 0 {
		return
	}

	if strings.Contains(msg.GetProps, GetPropsAtime) ||
		strings.Contains(msg.GetProps, GetPropsIsCached) {
		// Now add local properties to the cloud objects
		// The call replaces allentries.Entries with new values
		err = p.collectCachedFileList(bucket, allentries, listmsgjson)
	}
	return
}

// Local bucket:
//   - reads object list from all targets, combines, sorts and returns the
//     first pageSize objects
// Cloud bucket:
//   - selects a random target to read the list of objects from cloud
//   - if iscached or atime property is requested it does extra steps:
//      * get list of cached files info from all targets
//      * updates the list of objects from the cloud with cached info
//   - returns the list
func (p *proxyrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string) (pagemarker string, ok bool) {
	var allentries *BucketList
	listmsgjson, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s := fmt.Sprintf("listbucket: Failed to read %s request, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("listbucket: Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		p.invalmsghdlr(w, r, s)
		return
	}

	if p.islocalBucket(bucket) {
		allentries, err = p.getLocalBucketObjects(bucket, listmsgjson)
	} else {
		allentries, err = p.getCloudBucketObjects(bucket, listmsgjson)
	}
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	jsbytes, err := json.Marshal(allentries)
	assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "listbucket")
	pagemarker = allentries.PageMarker
	return
}

// receiveDrop reads until EOF and uses dummy writer (ReadToNull)
func (p *proxyrunner) receiveDrop(w http.ResponseWriter, r *http.Request, redirecturl string) {
	if glog.V(3) {
		glog.Infof("GET redirect URL %q", redirecturl)
	}
	newr, err := http.Get(redirecturl)
	if err != nil {
		glog.Errorf("Failed to GET redirect URL %q, err: %v", redirecturl, err)
		return
	}
	defer newr.Body.Close()

	bufreader := bufio.NewReader(newr.Body)
	bytes, err := ReadToNull(bufreader)
	if err != nil {
		glog.Errorf("Failed to copy data to http, URL %q, err: %v", redirecturl, err)
		return
	}
	if glog.V(3) {
		glog.Infof("Received and discarded %q (size %.2f MB)", redirecturl, float64(bytes)/MiB)
	}
}

func (p *proxyrunner) deleteLocalBucket(w http.ResponseWriter, r *http.Request, lbucket string) {
	if !p.checkPrimaryProxy("delete local bucket", w, r) {
		return
	}

	if !p.islocalBucket(lbucket) {
		p.invalmsghdlr(w, r, fmt.Sprintf("Cannot delete non-local bucket %s", lbucket))
		return
	}

	p.lbmap.lock()
	defer p.lbmap.unlock()
	if !p.lbmap.del(lbucket) {
		s := fmt.Sprintf("Local bucket %s does not exist, nothing to remove", lbucket)
		p.invalmsghdlr(w, r, s)
		return
	}
	p.synclbmap(w, r)
}

// synclbmap requires the caller to lock p.lbmap
func (p *proxyrunner) synclbmap(w http.ResponseWriter, r *http.Request) {
	lbpathname := filepath.Join(p.confdir, lbname)
	if err := LocalSave(lbpathname, p.lbmap); err != nil {
		s := fmt.Sprintf("Failed to store localbucket config %s, err: %v", lbpathname, err)
		p.invalmsghdlr(w, r, s)
		return
	}
	go p.synchronizeMaps(0, "", nil)
}

// syncsmap requires the caller to lock p.smap
func (p *proxyrunner) syncsmap(w http.ResponseWriter, r *http.Request, newtarget *daemonInfo) {
	smappathname := filepath.Join(p.confdir, smapname)
	if ctx.config.TestFSP.Instance > 0 {
		instancedir := filepath.Join(ctx.config.Confdir, strconv.Itoa(ctx.config.TestFSP.Instance))
		smappathname = filepath.Join(instancedir, smapname)
	}
	if err := LocalSave(smappathname, p.smap); err != nil {
		s := fmt.Sprintf("Failed to store smap %s, err : %v", smappathname, err)
		p.invalmsghdlr(w, r, s)
		return
	}
	go p.synchronizeMaps(0, "", newtarget)
}

func (p *proxyrunner) filrename(w http.ResponseWriter, r *http.Request, msg *ActionMsg) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	lbucket, objname := apitems[0], strings.Join(apitems[1:], "/")
	p.lbmap.lock()
	if !p.islocalBucket(lbucket) {
		s := fmt.Sprintf("Rename/move is supported only for cache-only buckets (%s does not appear to be local)", lbucket)
		p.invalmsghdlr(w, r, s)
		p.lbmap.unlock()
		return
	}
	p.lbmap.unlock()

	si, errstr := hrwTarget(lbucket+"/"+objname, p.smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("RENAME %s %s/%s => %s", r.Method, lbucket, objname, si.DaemonID)
	}

	p.statsdC.Send("rename",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
	)

	p.statsif.add("numrename", 1)

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (GetMsg - see REST.go)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) actionlistrange(w http.ResponseWriter, r *http.Request, actionMsg *ActionMsg) {
	var (
		err    error
		method string
	)

	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket := apitems[0]
	islocal := p.islocalBucket(bucket)
	wait := false
	if jsmap, ok := actionMsg.Value.(map[string]interface{}); !ok {
		s := fmt.Sprintf("Failed to unmarshal JSMAP: Not a map[string]interface")
		p.invalmsghdlr(w, r, s)
		return
	} else if waitstr, ok := jsmap["wait"]; ok {
		if wait, ok = waitstr.(bool); !ok {
			s := fmt.Sprintf("Failed to read ListRangeMsgBase Wait: Not a bool")
			p.invalmsghdlr(w, r, s)
			return
		}
	}
	// Send json message to all
	jsonbytes, err := json.Marshal(actionMsg)
	assert(err == nil, err)

	switch actionMsg.Action {
	case ActEvict, ActDelete:
		method = http.MethodDelete
	case ActPrefetch:
		method = http.MethodPost
	default:
		s := fmt.Sprintf("Action unavailable for List/Range Operations: %s", actionMsg.Action)
		p.invalmsghdlr(w, r, s)
		return
	}

	wg := &sync.WaitGroup{}
	for _, si := range p.smap.Smap {
		wg.Add(1)
		go func(si *daemonInfo) {
			defer wg.Done()
			var (
				err     error
				errstr  string
				errcode int
				url     = fmt.Sprintf("%s/%s/%s/%s?%s=%t", si.DirectURL, Rversion, Rbuckets, bucket, URLParamLocal, islocal)
			)
			if wait {
				_, err, errstr, errcode = p.call(si, url, method, jsonbytes, 0)
			} else {
				_, err, errstr, errcode = p.call(si, url, method, jsonbytes)
			}
			if err != nil {
				s := fmt.Sprintf("Failed to execute List/Range request: %v (%d: %s)", err, errcode, errstr)
				p.invalmsghdlr(w, r, s)
				return
			}
		}(si)
	}
	wg.Wait()
}

//===========================
//
// control plane
//
//===========================

// "/"+Rversion+"/"+Rdaemon
func (p *proxyrunner) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpdaeget(w, r)
	case http.MethodPut:
		p.httpdaeput(w, r)
	default:
		invalhdlr(w, r)
	}
}

func (p *proxyrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	var msg GetMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.GetWhat {
	case GetWhatConfig:
		jsbytes, err := json.Marshal(ctx.config)
		assert(err == nil)
		p.writeJSON(w, r, jsbytes, "httpdaeget")
	case GetWhatSmap:
		jsbytes, err := json.Marshal(p.smap)
		assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget")
	case GetWhatSmapVote:
		_, xx := p.xactinp.findLocked(ActElection)
		vote := (xx != nil)
		msg := SmapVoteMsg{
			VoteInProgress: vote,
			Smap:           p.smap,
		}
		jsbytes, err := json.Marshal(msg)
		assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget")
	default:
		s := fmt.Sprintf("Unexpected GetMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}

	if len(apitems) > 0 {
		switch apitems[0] {
		case Rsynclb:
			p.httpdaeputLBMap(w, r, apitems)
			return
		case Rsyncsmap, Rebalance:
			p.httpdaeputSmap(w, r, apitems)
			return
		case Rproxy:
			p.httpdaesetprimaryproxy(w, r)
			return
		default:
		}
	}

	//
	// other PUT /daemon actions
	//
	var msg ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActSetConfig:
		var (
			value string
			ok    bool
		)
		if value, ok = msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: not a string"))
			return
		}
		switch msg.Name {
		case "loglevel", "stats_time", "passthru":
			if errstr := p.setconfig(msg.Name, value); errstr != "" {
				p.invalmsghdlr(w, r, errstr)
			}
		default:
			errstr := fmt.Sprintf("Invalid setconfig request: proxy does not support (updating) '%s'", msg.Name)
			p.invalmsghdlr(w, r, errstr)
		}
	case ActShutdown:
		q := r.URL.Query()
		force, _ := parsebool(q.Get(URLParamForce))
		if p.primary && !force {
			s := fmt.Sprintf("Cannot shutdown Primary Proxy without %s=true query parameter", URLParamForce)
			p.invalmsghdlr(w, r, s)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) httpdaeputSmap(w http.ResponseWriter, r *http.Request, apitems []string) {
	curversion := p.smap.Version
	var newsmap *Smap
	if p.readJSON(w, r, &newsmap) != nil {
		return
	}
	if curversion == newsmap.Version {
		return
	}
	if curversion > newsmap.Version {
		return
	}
	existentialQ := (newsmap.getProxy(p.si.DaemonID) != nil)
	assert(existentialQ)

	err := p.setPrimaryProxyAndSmap(newsmap)
	if err != nil {
		s := fmt.Sprintf("Failed to update Primary Proxy and Smap, err: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}
}

func (p *proxyrunner) httpdaeputLBMap(w http.ResponseWriter, r *http.Request, apitems []string) {
	if p.primary {
		p.invalmsghdlr(w, r, "Primary Proxy is the distributor of the lbmap, not the receiver")
		return
	}
	curversion := p.lbmap.Version
	newlbmap := &lbmap{LBmap: make(map[string]string)}
	if p.readJSON(w, r, newlbmap) != nil {
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
	p.lbmap = newlbmap

	if ctx.config.TestFSP.Instance > 0 {
		glog.Infof("Warning!")
		glog.Infof("Warning: proxy instance %d cannot store lbmap when the confdir %s is shared", ctx.config.TestFSP.Instance, p.confdir)
		glog.Infof("Warning!")
		return
	}
	lbpathname := filepath.Join(p.confdir, lbname)
	if err := LocalSave(lbpathname, p.lbmap); err != nil {
		glog.Errorf("Failed to store lbmap %s, err: %v", lbpathname, err)
	}
}

func (p *proxyrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	var (
		prepare bool
		err     error
	)
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rdaemon); apitems == nil {
		return
	}
	if p.primary {
		s := fmt.Sprint("Must use cluster handler to set primary proxy for the Primary Proxy")
		p.invalmsghdlr(w, r, s)
		return
	}

	proxyid := apitems[1]
	query := r.URL.Query()
	preparestr := query.Get(URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL Parameter: %v", URLParamPrepare, err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if p.si.DaemonID == proxyid {
		if !prepare {
			p.becomePrimaryProxy("" /* proxyIDToRemove */)
		}
		return
	}
	err = p.setPrimaryProxyLocked(proxyid, "" /* primaryToRemove */, prepare)
	if err != nil {
		s := fmt.Sprintf("Failed to set Primary Proxy to %v, err: %v", proxyid, err)
		p.invalmsghdlr(w, r, s)
		return
	}
}

func (p *proxyrunner) httpclusetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rcluster); apitems == nil {
		return
	}
	if !p.checkPrimaryProxy("set primary proxy", w, r) {
		return
	}

	proxyid := apitems[1]

	if proxyid == p.si.DaemonID {
		return
	}

	// 1st phase: Confirm that all proxies/targets are able to perform this primary proxy change.
	p.smap.lock()

	err := p.setPrimaryProxy(proxyid, "" /* primaryToRemove */, true)
	if err != nil {
		p.smap.unlock()
		s := fmt.Sprintf("Could not set primary proxy: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}
	smap4bcast := &Smap{}
	copyStruct(smap4bcast, p.smap)

	p.smap.unlock()

	urlfmt := fmt.Sprintf("%%s/%s/%s/%s/%s?%s=%t", Rversion, Rdaemon, Rproxy, proxyid, URLParamPrepare, true)
	method := http.MethodPut
	errch := make(chan error, smap4bcast.count()+smap4bcast.countProxies())
	f := func(si *daemonInfo, _ []byte, err error, status int) {
		if err != nil {
			errch <- fmt.Errorf("Error from %v in prepare phase of Set Primary Proxy: %v", si.DaemonID, err)
		}
	}
	p.broadcast(urlfmt, method, nil, smap4bcast, f, ctx.config.Timeout.VoteRequest)
	select {
	case err := <-errch:
		// If there are any proxies/targets that error during this phase, the change is not enacted.
		s := fmt.Sprintf("Could not set primary proxy: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	default:
	}

	// If phase 1 passed without error, broadcast phase 2:
	// After this point, errors will not result in any rollback.
	urlfmt = fmt.Sprintf("%%s/%s/%s/%s/%s?%s=%t", Rversion, Rdaemon, Rproxy, proxyid, URLParamPrepare, false)
	f = func(si *daemonInfo, _ []byte, err error, status int) {
		if err != nil {
			glog.Errorf("Error from %v in commit phase of Set Primary Proxy: %v", si.DaemonID, err)
		}
	}
	p.broadcast(urlfmt, method, nil, smap4bcast, f, ctx.config.Timeout.VoteRequest)

	p.smap.lock()
	defer p.smap.unlock()
	p.becomeNonPrimaryProxy()
	_ = p.setPrimaryProxy(proxyid, "" /* primaryToRemove */, false)
}

// handler for: "/"+Rversion+"/"+Rcluster
func (p *proxyrunner) clusterhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpcluget(w, r)
	case http.MethodPost:
		p.httpclupost(w, r)
	case http.MethodDelete:
		p.httpcludel(w, r)
		glog.Flush()
	case http.MethodPut:
		p.httpcluput(w, r)
		glog.Flush()
	default:
		invalhdlr(w, r)
	}
}

// handler for: "/"+Rversion+"/"+Rtokens
func (p *proxyrunner) tokenhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.httptokenpost(w, r)
	default:
		invalhdlr(w, r)
	}
}

// gets target info
func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	var msg GetMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.GetWhat {
	case GetWhatStats:
		getstatsmsg, err := json.Marshal(msg) // same message to all targets
		assert(err == nil, err)
		p.httpclugetstats(w, r, getstatsmsg)
	default:
		s := fmt.Sprintf("Unexpected GetMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

// FIXME: read-lock
func (p *proxyrunner) httpclugetstats(w http.ResponseWriter, r *http.Request, getstatsmsg []byte) {
	out := p.newClusterStats()
	for _, si := range p.smap.Smap {
		stats := &storstatsrunner{Capacity: make(map[string]*fscapacity)}
		out.Target[si.DaemonID] = stats
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
		outjson, err, errstr, status := p.call(si, url, r.Method, getstatsmsg)
		if err != nil {
			p.invalmsghdlr(w, r, errstr)
			p.kalive.onerr(err, status)
			return
		}
		if err = json.Unmarshal(outjson, stats); err != nil {
			p.invalmsghdlr(w, r, string(outjson))
			return
		}
	}
	rr := getproxystatsrunner()
	rr.Lock()
	out.Proxy = &rr.Core
	jsbytes, err := json.Marshal(out)
	rr.Unlock()
	assert(err == nil, err)
	p.writeJSON(w, r, jsbytes, "httpclugetstats")
}

// register|keepalive target
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		nsi       daemonInfo
		keepalive bool
		proxy     bool
	)

	if !p.checkPrimaryProxy("register", w, r) {
		return
	}

	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	if len(apitems) > 0 {
		keepalive = (apitems[0] == Rkeepalive)
		proxy = (apitems[0] == Rproxy)
		if proxy && len(apitems) > 1 {
			keepalive = (apitems[1] == Rkeepalive)
		}
	}
	if p.readJSON(w, r, &nsi) != nil {
		return
	}
	if net.ParseIP(nsi.NodeIPAddr) == nil {
		s := fmt.Sprintf("register target %s: invalid IP address %v", nsi.DaemonID, nsi.NodeIPAddr)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.statsdC.Send("post",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
	)
	p.statsif.add("numpost", 1)

	// local in-mem state updates under lock
	p.smap.lock()
	defer p.smap.unlock()

	var newtarget *daemonInfo
	if proxy {
		p.registerproxy(&nsi, keepalive)
	} else {
		newtarget = &nsi
		p.registertarget(&nsi, keepalive)
	}
	p.syncsmap(w, r, newtarget)
}

func (p *proxyrunner) shouldAddToSmap(nsi *daemonInfo, osi *daemonInfo, keepalive bool, kind string) bool {
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive %s %s: adding back to the cluster map", kind, nsi.DaemonID)
			return true
		}
		if osi.NodeIPAddr != nsi.NodeIPAddr || osi.DaemonPort != nsi.DaemonPort {
			glog.Warningf("register/keepalive %s %s: info changed - renewing", kind, nsi.DaemonID)
			return true
		}
		p.kalive.timestamp(nsi.DaemonID)
		return false
	}
	if osi != nil {
		if osi.NodeIPAddr == nsi.NodeIPAddr && osi.DaemonPort == nsi.DaemonPort && osi.DirectURL == nsi.DirectURL {
			glog.Infof("register %s %s: already done", kind, nsi.DaemonID)
			return false
		}
		glog.Warningf("register %s %s: renewing the registration %+v => %+v", kind, nsi.DaemonID, osi, nsi)
	}
	return true
}

func (p *proxyrunner) registerproxy(nsi *daemonInfo, keepalive bool) {
	pi := proxyInfo{
		daemonInfo: *nsi,
		Primary:    false, // this proxy is the primary, so the newly registered one cannot be
	}
	osi := p.smap.getProxy(nsi.DaemonID)
	var osidi *daemonInfo
	if osi != nil {
		osidi = &osi.daemonInfo

	}
	if !p.shouldAddToSmap(nsi, osidi, keepalive, "proxy") {
		return
	}
	p.smap.addProxy(&pi)
	if glog.V(3) {
		glog.Infof("register proxy %s (count %d)", nsi.DaemonID, p.smap.count())
	}
}

func (p *proxyrunner) registertarget(nsi *daemonInfo, keepalive bool) {
	osi := p.smap.get(nsi.DaemonID)
	if !p.shouldAddToSmap(nsi, osi, keepalive, "target") {
		return
	}
	// NOTE: getting used for hrw routing right away..
	p.smap.add(nsi)
	if glog.V(3) {
		glog.Infof("register target %s (count %d)", nsi.DaemonID, p.smap.count())
	}
}

// unregisters a target/proxy
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	if !p.checkPrimaryProxy("unregister", w, r) {
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 6)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rcluster); apitems == nil {
		return
	}
	if apitems[0] != Rdaemon {
		s := fmt.Sprintf("Invalid API element: %s (expecting %s)", apitems[0], Rdaemon)
		p.invalmsghdlr(w, r, s)
		return
	}
	sid := apitems[1]
	proxy := false
	if sid == Rproxy {
		proxy = true
		sid = apitems[2]
	}
	p.smap.lock()
	defer p.smap.unlock()
	if !proxy && p.smap.get(sid) == nil {
		glog.Errorf("Unknown target %s", sid)
		return
	}
	if proxy && p.smap.getProxy(sid) == nil {
		glog.Errorf("Unknown proxy %s", sid)
		return
	}
	if proxy {
		p.smap.delProxy(sid)
	} else {
		p.smap.del(sid)
	}
	//
	// TODO: startup -- leave --
	//
	if glog.V(3) {
		if proxy {
			glog.Infof("Unregistered proxy {%s} (count %d)", sid, p.smap.countProxies())
		} else {
			glog.Infof("Unregistered target {%s} (count %d)", sid, p.smap.count())
		}
	}
	p.syncsmap(w, r, nil)
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": "rebalance"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
// '{"action": "setconfig"}' /v1/cluster => (proxy) =>
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}

	if len(apitems) > 0 && apitems[0] == Rproxy {
		p.httpclusetprimaryproxy(w, r)
		return
	}

	var msg ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: Not a string"))
		} else if errstr := p.setconfig(msg.Name, value); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
		} else {
			msgbytes, err := json.Marshal(msg) // same message -> all targets
			assert(err == nil, err)
			// Broadcast is not used here, because these changes should only be propogated to targets.
			for _, si := range p.smap.Smap {
				url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
				if _, err, errstr, status := p.call(si, url, http.MethodPut, msgbytes); err != nil {
					p.invalmsghdlr(w, r, fmt.Sprintf("%s (%s = %s) failed, err: %s", msg.Action, msg.Name, value, errstr))
					p.kalive.onerr(err, status)
					break
				}

			}
		}
	case ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		msgbytes, err := json.Marshal(msg) // same message -> all targets
		assert(err == nil, err)

		urlfmt := fmt.Sprintf("%%s/%s/%s", Rversion, Rdaemon)
		callback := func(_ *daemonInfo, _ []byte, _ error, _ int) {}
		p.smap.lock()
		smap4bcast := &Smap{}
		copyStruct(smap4bcast, p.smap)
		p.smap.unlock()

		p.broadcast(urlfmt, http.MethodPut, msgbytes, smap4bcast, callback)
		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case ActSyncSmap:
		fallthrough
	case ActRebalance:
		if !p.checkPrimaryProxy("initiate rebalance", w, r) {
			return
		}
		go p.synchronizeMaps(0, msg.Action, nil)

	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

//========================
//
// delayed broadcasts
//
//========================
func (p *proxyrunner) synchronizeMaps(ntargets int, action string, newtarget *daemonInfo) {
	if !p.primary {
		glog.Errorf("Only the primary proxy can synchronize maps")
		return
	}
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&p.syncmapinp, 0, aval) {
		if glog.V(4) {
			glog.Infof("synchronizeMaps is already running")
		}
		return
	}
	defer atomic.CompareAndSwapInt64(&p.syncmapinp, aval, 0)
	if ntargets > 0 { // starting up
		time.Sleep(syncmapldelay)
	}
	if p.mapsAlreadyInSync() {
		return
	}
	retrycnt := 0
	for ; retrycnt < maxmapsyncretries; retrycnt++ {
		if ntargets > 0 { // if provided, use ntargets as a hint
			ntargetsCur := p.smap.countLocked()
			if ntargetsCur >= ntargets {
				glog.Infof("Reached the expected number %d (%d) of target registrations",
					ntargets, ntargetsCur)
				glog.Flush()
			} else {
				time.Sleep(syncmapldelay)
				continue
			}
		}
		// change in the cluster map warrants broadcasting of every other config that
		// must be shared across the cluster; the opposite it not true though
		p.httpfilputLB(newtarget)
		if action == Rebalance {
			p.httpcluputSmap(Rebalance, nil) // REST API
		} else if p.smap.syncversion != p.smap.versionLocked() {
			if ntargets > 0 {
				p.httpcluputSmap(Rsyncsmap, nil)
			} else {
				p.httpcluputSmap(Rebalance, newtarget)
			}
		}
		if p.mapsAlreadyInSync() {
			break
		}
		if retrycnt < maxmapsyncretries-1 {
			time.Sleep(syncmapsdelay)
		}
	}
	if retrycnt == maxmapsyncretries {
		glog.Errorf("Critical: Smap v(%d, %d) and lbmap v(%d, %d) out-of-sync cluster-wide",
			p.smap.syncversion, p.smap.Version, p.lbmap.syncversion, p.lbmap.Version)
	} else {
		glog.Infof("Smap (v%d) and lbmap (v%d) in-sync cluster-wide", p.smap.syncversion, p.lbmap.syncversion)
	}
}

func (p *proxyrunner) mapsAlreadyInSync() bool {
	p.smap.lock()
	defer p.smap.unlock()
	p.lbmap.lock()
	defer p.lbmap.unlock()
	if p.lbmap.version() == p.lbmap.syncversion && p.smap.version() == p.smap.syncversion {
		if glog.V(4) {
			glog.Infof("Smap (v%d) and lbmap (v%d) are already in sync cluster-wide",
				p.smap.syncversion, p.lbmap.syncversion)
		}
		return true
	}
	if glog.V(3) {
		glog.Infof("Smap v(%d, %d) and lbmap v(%d, %d) are yet to be synchronized",
			p.smap.syncversion, p.smap.version(), p.lbmap.syncversion, p.lbmap.version())
	}
	return false
}

func (c *mapsBcastContext) callback(si *daemonInfo, _ []byte, err error, status int) {
	if err != nil {
		glog.Errorf("Failed to %s %s, err: %v", c.action, si.DaemonID, err)
		if IsErrConnectionRefused(err) {
			c.Lock()
			c.retry = true
			c.Unlock()
			if si.DaemonID == c.newtargetid {
				glog.Infof("Retrying %s newtarget=%s, err: %v", c.action, si.DaemonID, err)
			} else {
				glog.Warningf("Retrying %s target=%s, err: %v", c.action, si.DaemonID, err)
			}
		} else {
			c.p.kalive.onerr(err, status)
		}
	}
}

func (p *proxyrunner) httpcluputSmap(action string, newtarget *daemonInfo) {
	var newtargetid string
	if newtarget != nil {
		newtargetid = newtarget.DaemonID
	}

	urlfmt := fmt.Sprintf("%%s/%s/%s/%s?%s=%s", Rversion, Rdaemon, action, URLParamNewTargetID, newtargetid)
	if glog.V(3) {
		glog.Infoln(urlfmt[2:])
	}

	p.smap.lock()
	jsbytes, err := json.Marshal(p.smap)
	smap4bcast := &Smap{}
	copyStruct(smap4bcast, p.smap)
	syncversion := p.smap.version()
	p.smap.unlock()

	assert(err == nil, err)

	c := &mapsBcastContext{p: p, action: action, newtargetid: newtargetid}
	p.broadcast(urlfmt, http.MethodPut, jsbytes, smap4bcast, c.callback)

	if !c.retry {
		p.smap.lock()
		p.smap.syncversion = syncversion
		p.smap.unlock()
	}
	glog.Infof("new Smap syncversion %d", p.smap.syncversion)
}

func (p *proxyrunner) httpfilputLB(newtarget *daemonInfo) {
	var newtargetid string
	if newtarget != nil {
		newtargetid = newtarget.DaemonID
	}
	p.lbmap.lock()
	jsbytes, err := json.Marshal(p.lbmap)
	assert(err == nil, err)
	syncversion := p.lbmap.version()
	p.lbmap.unlock()

	urlfmt := fmt.Sprintf("%%s/%s/%s/%s", Rversion, Rdaemon, Rsynclb)
	if glog.V(3) {
		glog.Infoln(urlfmt[2:], string(jsbytes))
	}

	p.smap.lock()
	smap4bcast := &Smap{}
	copyStruct(smap4bcast, p.smap)
	p.smap.unlock()

	c := &mapsBcastContext{p: p, action: Rsynclb, newtargetid: newtargetid}
	p.broadcast(urlfmt, http.MethodPut, jsbytes, smap4bcast, c.callback)

	if !c.retry {
		p.lbmap.lock()
		p.lbmap.syncversion = syncversion
		p.lbmap.unlock()
	}
	glog.Infof("new lbmap syncversion %d", p.lbmap.syncversion)
}

//===================
//
//===================
// FIXME: move to httpcommon
func (p *proxyrunner) islocalBucket(bucket string) bool {
	_, ok := p.lbmap.LBmap[bucket]
	return ok
}

func (p *proxyrunner) checkPrimaryProxy(action string, w http.ResponseWriter, r *http.Request) bool {
	if !p.primary {
		if p.proxysi != nil {
			w.Header().Add(HeaderPrimaryProxyURL, p.proxysi.DirectURL)
			w.Header().Add(HeaderPrimaryProxyID, p.proxysi.DaemonID)
		}
		s := fmt.Sprintf("Cannot %s from non-primary proxy %v", action, p.si.DaemonID)
		p.invalmsghdlr(w, r, s)
		return false
	}
	return true
}

/* Broadcasts jsbytes using the given method to all targets and proxies in the cluster.
The URL for each proxy is created with urlfmt, which should contain one %s representing
the direct url of any given node.

Sending to each node happens in parallel, and callback will be called with the result of each one.

The caller must lock p.smap.
*/
func (p *proxyrunner) broadcast(urlfmt, method string, jsbytes []byte, smap *Smap,
	callback func(*daemonInfo, []byte, error, int), timeout ...time.Duration) {
	wg := &sync.WaitGroup{}
	for _, si := range smap.Smap {
		wg.Add(1)
		go func(si *daemonInfo) {
			defer wg.Done()
			url := fmt.Sprintf(urlfmt, si.DirectURL)
			r, err, _, status := p.call(si, url, method, jsbytes, timeout...)
			callback(si, r, err, status)
		}(si)
	}
	for _, si := range smap.Pmap {
		// Don't broadcast to self
		if si.DaemonID != p.si.DaemonID {
			wg.Add(1)
			go func(si *proxyInfo) {
				defer wg.Done()
				url := fmt.Sprintf(urlfmt, si.DirectURL)
				r, err, _, status := p.call(&si.daemonInfo, url, method, jsbytes, timeout...)
				callback(&si.daemonInfo, r, err, status)
			}(si)
		}
	}
	wg.Wait()
}

// update list of valid tokens
func (p *proxyrunner) httptokenpost(w http.ResponseWriter, r *http.Request) {
	var (
		tokenList = &TokenList{}
	)

	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rtokens); apitems == nil {
		return
	}

	if err := p.readJSON(w, r, tokenList); err != nil {
		s := fmt.Sprintf("Invalid token list: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	newAuth, err := newAuthList(tokenList)
	if err != nil {
		s := fmt.Sprintf("Invalid token list: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.authn.Lock()
	defer p.authn.Unlock()
	p.authn.tokens = newAuth
}

// Read a token from request header and validates it
// Header format:
//		'Authorization: Bearer <token>'
func (p *proxyrunner) validateToken(r *http.Request) (*authRec, error) {
	s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 || s[0] != tokenStart {
		return nil, fmt.Errorf("Invalid request")
	}

	if p.authn == nil {
		return nil, fmt.Errorf("Invalid credentials")
	}

	auth, err := p.authn.validateToken(s[1])
	if err != nil {
		glog.Errorf("Invalid token: %v", err)
		return nil, fmt.Errorf("Invalid token")
	}

	return auth, nil
}

// A wrapper to check any request before delegating the request to real handler
// If authentication is disabled, it does nothing.
// If authentication is enabled, it looks for token in request header and
// makes sure that it is valid
func (p *proxyrunner) checkHTTPAuth(h http.HandlerFunc) http.HandlerFunc {
	wrappedFunc := func(w http.ResponseWriter, r *http.Request) {
		var (
			auth *authRec
			err  error
		)

		if ctx.config.Auth.Enabled {
			if auth, err = p.validateToken(r); err != nil {
				glog.Error(err)
				p.invalmsghdlr(w, r, "Not authorized", http.StatusUnauthorized)
				return
			}
			if glog.V(3) {
				glog.Infof("Logged as %s", auth.userID)
			}
		}

		h.ServeHTTP(w, r)
	}

	return wrappedFunc
}

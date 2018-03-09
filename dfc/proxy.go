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
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
)

const (
	syncmapsdelay = time.Second * 3
)

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
	syncmapinp  int64
}

// start proxy runner
func (p *proxyrunner) run() error {
	p.httprunner.init(getproxystats())
	ctx.smap.ProxySI = p.si
	p.httprunner.kalive = getproxykalive()

	p.xactinp = newxactinp()
	// local (aka cache-only) buckets
	p.lbmap = &lbmap{LBmap: make(map[string]string)}
	lbpathname := p.confdir + "/" + ctx.config.LBConf
	p.lbmap.lock()
	if localLoad(lbpathname, p.lbmap) != nil {
		// create empty
		p.lbmap.Version = 1
		if err := localSave(lbpathname, p.lbmap); err != nil {
			glog.Fatalf("FATAL: cannot store localbucket config, err: %v", err)
		}
	}
	p.lbmap.unlock()

	// startup: sync local buckets and cluster map when the latter stabilizes
	go p.synchronizeMaps(clivars.ntargets, "")

	//
	// REST API: register proxy handlers and start listening
	//
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", p.filehdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon, p.daemonhdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster, p.clusterhdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster+"/", p.clusterhdlr) // FIXME
	p.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Proxy %s is ready", p.si.DaemonID)
	glog.Flush()
	p.starttime = time.Now()

	return p.httprunner.run()
}

// stop gracefully
func (p *proxyrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", p.name, err)
	p.xactinp.abortAll()
	//
	// give targets a limited time to unregister
	//
	version := ctx.smap.versionLocked()
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		v := ctx.smap.versionLocked()
		if version != v {
			version = v
			time.Sleep(time.Second)
			continue
		}
		break
	}
	p.httprunner.stop(err)
}

//==============
//
// http handlers
//
//==============

// handler for: "/"+Rversion+"/"+Rfiles+"/"
func (p *proxyrunner) filehdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpfilget(w, r)
	case http.MethodPut:
		fallthrough
	case http.MethodDelete:
		p.httpfilputdelete(w, r)
	case http.MethodPost:
		p.actionLocalBucket(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
}

// e.g.: GET /v1/files/bucket/object
func (p *proxyrunner) httpfilget(w http.ResponseWriter, r *http.Request) {
	if ctx.smap.count() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname := apitems[0], ""
	if len(apitems) > 1 {
		objname = apitems[1]
	}
	if strings.Contains(bucket, "/") {
		s := fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		p.invalmsghdlr(w, r, s)
		return
	}
	var si *daemonInfo
	if len(objname) == 0 {
		p.statsif.add("numlist", 1)
		// local bucket - proxy does the job
		if p.islocalBucket(bucket) {
			p.listbucket(w, r, bucket)
			return
		}
		// cloud bucket - any (random) target will do
		for _, si = range ctx.smap.Smap { // see the spec for "map iteration order"
			break
		}
	} else { // CH target selection to execute GET bucket/objname
		p.statsif.add("numget", 1)
		si = hrwTarget(bucket+"/"+objname, ctx.smap)
	}
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s (%s)", r.URL.Path, si.DirectURL, r.Method)
	}
	if !ctx.config.Proxy.Passthru && len(objname) > 0 {
		glog.Infof("passthru=false: proxy initiates the GET %s/%s", bucket, objname)
		_ = p.receiveDrop(w, r, redirecturl) // ignore error, proceed to http redirect
	}
	if len(objname) != 0 {
		http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
	} else {
		// NOTE:
		//       code 307 is the only way to http-redirect with the
		//       original JSON payload (GetMsg - see REST.go)
		http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
	}
}

// FIXME: crude..
func (p *proxyrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string) {
	var allentries = BucketList{Entries: make([]*BucketEntry, 0, initialBucketListSize)}
	listmsgjson, err := ioutil.ReadAll(r.Body)
	errclose := r.Body.Close()
	assert(errclose == nil)

	// FIXME: re-un-marshaling here and elsewhere
	for _, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rfiles + "/" + bucket
		outjson, err, errstr, status := p.call(si, url, r.Method, listmsgjson) // forward as is
		if err != nil {
			p.invalmsghdlr(w, r, errstr)
			p.kalive.onerr(err, status)
			return
		}
		entries := BucketList{Entries: make([]*BucketEntry, 0, 128)}
		if err = json.Unmarshal(outjson, &entries); err != nil {
			p.invalmsghdlr(w, r, string(outjson))
			return
		}
		if len(entries.Entries) == 0 {
			continue
		}
		// FIXME: variadic may not scale well
		allentries.Entries = append(allentries.Entries, entries.Entries...)
	}
	jsbytes, err := json.Marshal(&allentries)
	assert(err == nil, err)
	_ = p.writeJSON(w, r, jsbytes, "listbucket")
}

// receiveDrop reads until EOF and uses dummy writer (ReadToNull)
func (p *proxyrunner) receiveDrop(w http.ResponseWriter, r *http.Request, redirecturl string) error {
	if glog.V(3) {
		glog.Infof("GET redirect URL %q", redirecturl)
	}
	newr, err := http.Get(redirecturl)
	if err != nil {
		glog.Errorf("Failed to GET redirect URL %q, err: %v", redirecturl, err)
		return err
	}
	defer func() {
		if newr != nil {
			err = newr.Body.Close()
		}
	}()
	bufreader := bufio.NewReader(newr.Body)
	bytes, err := ReadToNull(bufreader)
	if err != nil {
		glog.Errorf("Failed to copy data to http, URL %q, err: %v", redirecturl, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		p.statsif.add("numerr", 1)
		return err
	}
	if glog.V(3) {
		glog.Infof("Received and discarded %q (size %.2f MB)", redirecturl, float64(bytes)/1000/1000)
	}
	return err
}

// PUT bucket/file
// DELETE bucket/file
// DELETE lbucket
func (p *proxyrunner) httpfilputdelete(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket := apitems[0]
	if len(apitems) == 1 {
		if p.islocalBucket(bucket) {
			p.actionLocalBucket(w, r)
			return
		}
		p.invalmsghdlr(w, r, "Cannot "+r.Method+" non-local bucket")
		return
	}
	//
	// FIXME: add protection agaist putting into non-existing local bucket
	//
	objname := strings.Join(apitems[1:], "/")
	if glog.V(3) {
		glog.Infof("%s %s/%s", r.Method, bucket, objname)
	}
	si := hrwTarget(bucket+"/"+objname, ctx.smap)
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s (%s)", r.URL.Path, si.DirectURL, r.Method)
	}
	if r.Method == http.MethodDelete {
		p.statsif.add("numdelete", 1)
	} else {
		p.statsif.add("numput", 1)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

// { action } "/"+Rversion+"/"+Rfiles + "/" + localbucket
func (p *proxyrunner) actionLocalBucket(w http.ResponseWriter, r *http.Request) {
	var msg ActionMsg
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	lbucket := apitems[0]

	if r.Method == http.MethodDelete {
		p.lbmap.lock()
		if !p.lbmap.del(lbucket) {
			s := fmt.Sprintf("Local bucket %s does not exist, nothing to remove", lbucket)
			p.invalmsghdlr(w, r, s)
			p.lbmap.unlock()
			return
		}
		goto synclbmap
	}
	if r.Method != http.MethodPost {
		p.invalmsghdlr(w, r, "Unexpected method "+r.Method+" in action-local-bucket")
		return
	}
	// from here on we are executing POST {action} lbucket
	if strings.Contains(lbucket, "/") {
		s := fmt.Sprintf("Invalid local bucket name %s (contains '/')", lbucket)
		p.invalmsghdlr(w, r, s)
		return
	}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActCreateLB:
		p.lbmap.lock()
		if !p.lbmap.add(lbucket) {
			s := fmt.Sprintf("Local bucket %s already exists", lbucket)
			p.invalmsghdlr(w, r, s)
			p.lbmap.unlock()
			return
		}
	case ActSyncLB:
		p.lbmap.lock()
	case ActRename:
		p.filrename(w, r, &msg)
		return
	case ActPrefetch:
		p.filprefetch(w, r, &msg)
		return
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
		return
	}
synclbmap:
	lbpathname := p.confdir + "/" + ctx.config.LBConf
	if err := localSave(lbpathname, p.lbmap); err != nil {
		s := fmt.Sprintf("Failed to store localbucket config %s, err: %v", lbpathname, err)
		p.invalmsghdlr(w, r, s)
		p.lbmap.unlock()
		return
	}
	p.lbmap.unlock()

	go p.synchronizeMaps(0, "")
}

func (p *proxyrunner) filrename(w http.ResponseWriter, r *http.Request, msg *ActionMsg) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rfiles); apitems == nil {
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

	si := hrwTarget(lbucket+"/"+objname, ctx.smap)
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s (rename)", r.URL.Path, si.DirectURL)
	}
	p.statsif.add("numrename", 1)
	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (GetMsg - see REST.go)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) filprefetch(w http.ResponseWriter, r *http.Request, actionMsg *ActionMsg) {
	var (
		err error
	)

	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket := apitems[0]
	wait := false
	if actionMsg.Action == ActPrefetch {
		if jsmap, ok := actionMsg.Value.(map[string]interface{}); !ok {
			p.statsif.add("numerr", 1)
			glog.Errorf("Failed to unmarshal JSMAP: Not a map[string]interface")
			return
		} else if w, ok := jsmap["wait"]; ok {
			if wait, ok = w.(bool); !ok {
				p.statsif.add("numerr", 1)
				glog.Errorf("Failed to read PrefetchMsgBase Wait: Not a bool")
				return
			}
		}
	}
	// Send json message to all
	jsonbytes, err := json.Marshal(actionMsg)
	if err != nil {
		p.statsif.add("numerr", 1)
		glog.Errorf("Failed to re-marshal Prefetch JSON: %v", err)
		return
	}
	wg := &sync.WaitGroup{}
	for _, si := range ctx.smap.Smap {
		wg.Add(1)
		go func(si *daemonInfo) {
			defer wg.Done()
			var (
				err     error
				errstr  string
				errcode int
				url     = si.DirectURL + "/" + Rversion + "/" + Rfiles + "/" + bucket
			)
			if wait {
				_, err, errstr, errcode = p.call(si, url, http.MethodPost, jsonbytes, 0)
			} else {
				_, err, errstr, errcode = p.call(si, url, http.MethodPost, jsonbytes)
			}
			if err != nil {
				p.statsif.add("numerr", 1)
				glog.Errorf("Failed to execute Prefetch request: %v (%d: %s)", err, errcode, errstr)
				return
			}
		}(si)
	}
	wg.Wait()
	glog.Infoln("Completed sending Prefetch ActionMsg to all targets")
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
		_ = p.writeJSON(w, r, jsbytes, "httpdaeget")
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
	//
	// other PUT /daemon actions
	//
	var msg ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: Not a string"))
		} else if msg.Name != "stats_time" && msg.Name != "passthru" {
			p.invalmsghdlr(w, r, fmt.Sprintf("Invalid setconfig request: Proxy does not support this configuration variable: %s", msg.Name))
		} else if errstr := p.setconfig(msg.Name, value); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
		}
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
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
	case http.MethodPut:
		p.httpcluput(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
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
	case GetWhatSmap:
		jsbytes, err := json.Marshal(ctx.smap)
		assert(err == nil, err)
		_ = p.writeJSON(w, r, jsbytes, "httpcluget")
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
	out := newClusterStats()
	for _, si := range ctx.smap.Smap {
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
	_ = p.writeJSON(w, r, jsbytes, "httpclugetstats")
}

// register|keepalive target
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		osi       *daemonInfo
		nsi       daemonInfo
		keepalive bool
	)
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	if len(apitems) > 0 {
		keepalive = (apitems[0] == Rkeepalive)
	}
	if p.readJSON(w, r, &nsi) != nil {
		return
	}
	if net.ParseIP(nsi.NodeIPAddr) == nil {
		s := fmt.Sprintf("register target %s: invalid IP address %v", nsi.DaemonID, nsi.NodeIPAddr)
		p.invalmsghdlr(w, r, s)
		return
	}
	p.statsif.add("numpost", 1)
	ctx.smap.lock()
	osi = ctx.smap.get(nsi.DaemonID)
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive target %s: adding back to the cluster map", nsi.DaemonID)
			goto add
		}
		if osi.NodeIPAddr != nsi.NodeIPAddr || osi.DaemonPort != nsi.DaemonPort {
			glog.Warningf("register/keepalive target %s: info changed - renewing", nsi.DaemonID)
			goto add
		}
		ctx.smap.unlock()
		p.kalive.timestamp(nsi.DaemonID)
		return
	}
	if osi != nil {
		if osi.NodeIPAddr == nsi.NodeIPAddr && osi.DaemonPort == nsi.DaemonPort && osi.DirectURL == nsi.DirectURL {
			glog.Infof("register target %s: already done", nsi.DaemonID)
		} else {
			glog.Errorf("register target %s: renewing the registration %+v => %+v", nsi.DaemonID, osi, nsi)
		}
		// fall through
	}
add:
	ctx.smap.add(&nsi)
	ctx.smap.unlock()
	if glog.V(3) {
		glog.Infof("register target %s (count %d)", nsi.DaemonID, ctx.smap.count())
	}
	go p.synchronizeMaps(0, "")
}

// unregisters a target
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rcluster); apitems == nil {
		return
	}
	if apitems[0] != Rdaemon {
		s := fmt.Sprintf("Invalid API element: %s (expecting %s)", apitems[0], Rdaemon)
		p.invalmsghdlr(w, r, s)
		return
	}
	sid := apitems[1]
	ctx.smap.lock()
	if ctx.smap.get(sid) == nil {
		glog.Errorf("Unknown target %s", sid)
		ctx.smap.unlock()
		return
	}
	ctx.smap.del(sid)
	ctx.smap.unlock()
	//
	// TODO: startup -- leave --
	//
	if glog.V(3) {
		glog.Infof("Unregistered target {%s} (count %d)", sid, ctx.smap.count())
	}
	go p.synchronizeMaps(0, "")
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
			for _, si := range ctx.smap.Smap {
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
		for _, si := range ctx.smap.Smap {
			url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
			glog.Infof("%s: %s", msg.Action, url)
			p.call(si, url, http.MethodPut, msgbytes) // ignore errors
		}
		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case ActSyncSmap:
		fallthrough
	case ActRebalance:
		go p.synchronizeMaps(0, msg.Action)

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
// TODO: proxy.stop() must terminate this routine
func (p *proxyrunner) synchronizeMaps(ntargets int, action string) {
	aval := time.Now().Unix()
	startingUp := ntargets > 0

	if !atomic.CompareAndSwapInt64(&p.syncmapinp, 0, aval) {
		glog.Infof("synchronizeMaps is already running")
		return
	}
	defer atomic.CompareAndSwapInt64(&p.syncmapinp, aval, 0)
	if startingUp {
		time.Sleep(syncmapsdelay)
	}
	ctx.smap.lock()
	p.lbmap.lock()
	lbversion := p.lbmap.version()
	smapversion := ctx.smap.version()
	ntargets_cur := ctx.smap.count()
	delay := syncmapsdelay
	if lbversion == p.lbmap.syncversion && smapversion == ctx.smap.syncversion {
		glog.Infof("Smap (v%d) and lbmap (v%d) are already in sync with the targets",
			smapversion, lbversion)
		p.lbmap.unlock()
		ctx.smap.unlock()
		return
	}
	p.lbmap.unlock()
	ctx.smap.unlock()
	time.Sleep(time.Second)
	for {
		lbv := p.lbmap.versionLocked()
		if lbversion != lbv {
			lbversion = lbv
			time.Sleep(delay)
			continue
		}
		smv := ctx.smap.versionLocked()
		if smapversion != smv {
			smapversion = smv
			// if provided, use ntargets as a hint
			if startingUp {
				ctx.smap.lock()
				ntargets_cur = ctx.smap.count()
				ctx.smap.unlock()
				if ntargets_cur >= ntargets {
					glog.Infof("Reached the expected number %d (%d) of target registrations",
						ntargets, ntargets_cur)
					glog.Flush()
				}
			} else {
				time.Sleep(delay)
				continue
			}
		}
		// finally:
		// change in the cluster map warrants the broadcast of every other config that
		// must be shared across the cluster;
		// the opposite it not true though, that's why the check below
		p.httpfilput_lb()
		if action == Rebalance {
			p.httpcluput_smap(Rebalance) // REST cmd
		} else if ctx.smap.syncversion != smapversion {
			if startingUp {
				p.httpcluput_smap(Rsyncsmap)
			} else {
				p.httpcluput_smap(Rebalance) // NOTE: auto-rebalance
			}
		}
		break
	}
	ctx.smap.lock()
	p.lbmap.lock()
	ctx.smap.syncversion = smapversion
	p.lbmap.syncversion = lbversion
	p.lbmap.unlock()
	ctx.smap.unlock()
	glog.Infof("Smap (v%d) and lbmap (v%d) are now in sync with the targets", smapversion, lbversion)
}

func (p *proxyrunner) httpcluput_smap(action string) {
	method := http.MethodPut
	assert(action == Rebalance || action == Rsyncsmap)
	ctx.smap.lock()
	jsbytes, err := json.Marshal(ctx.smap)
	ctx.smap.unlock()
	assert(err == nil, err)
	for _, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon + "/" + action
		glog.Infof("%s: %s", action, url)
		if _, err, errstr, status := p.call(si, url, method, jsbytes); errstr != "" {
			p.kalive.onerr(err, status)
			return
		}
	}
}

func (p *proxyrunner) httpfilput_lb() {
	p.lbmap.lock()
	jsbytes, err := json.Marshal(p.lbmap)
	assert(err == nil, err)
	p.lbmap.unlock()

	for _, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon + "/" + Rsynclb
		glog.Infof("%s: %+v", url, p.lbmap)
		if _, err, _, status := p.call(si, url, http.MethodPut, jsbytes); err != nil {
			p.kalive.onerr(err, status)
			return
		}
	}
}

//===================
//
//===================
// FIXME: move to httpcommon
func (p *proxyrunner) islocalBucket(bucket string) bool {
	_, ok := p.lbmap.LBmap[bucket]
	return ok
}

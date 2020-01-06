// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

const maxRetrySeconds = 5

// Background:
// 	- Each proxy/gateway stores a local copy of the cluster map (Smap)
// 	- Each Smap instance is versioned; the versioning is monotonic (increasing)
// 	- Only the primary (leader) proxy distributes Smap updates to all other clustered nodes
// 	- Bootstrap sequence includes /steps/ intended to resolve all the usual conflicts that may arise.
func (p *proxyrunner) bootstrap() {
	const fmd = "%s: discovered %s (primary=%s): %s local %s (primary=%s)"
	var (
		found, smap       *smapX
		url               string
		pname             = p.si.Name()
		config            = cmn.GCO.Get()
		secondary, loaded bool
	)
	// 1: load a local copy and try to utilize it for discovery
	smap = newSmap()
	smap.Pmap[p.si.DaemonID] = p.si
	if err := p.smapowner.load(smap, config); err == nil {
		loaded = true
		found = p.bootstrapSmap(smap)
	}
	// 2: having local Smap, plus, maybe, discovered max-version Smap, plus env "AIS_PRIMARYPROXY"
	//    we figure out if this proxy *may be* the primary and proceed to start it up as such
	//    until and if there's more evidence that points to the contrary.
	if found != nil {
		if smap.version() == found.version() {
			// do nothing
		} else if smap.version() > found.version() {
			glog.Infof(fmd, pname, found, found.ProxySI.DaemonID, "merging", smap, smap.ProxySI.DaemonID)
			found.merge(smap)
		} else {
			glog.Infof(fmd, pname, found, found.ProxySI.DaemonID, "overriding", smap, smap.ProxySI.DaemonID)
			smap = found
		}
	}
	// 3: decide whether this starting-up proxy is primary
	if loaded {
		var disagreement = found != nil && smap.ProxySI.DaemonID != found.ProxySI.DaemonID
		if disagreement {
			// NOTE: potential split-brain down the road
			glog.Warningf("%s: discovered Smap has a different primary %s", pname, found.ProxySI.Name())
		}
		secondary = !smap.isPrimary(p.si)
		if secondary {
			url = smap.ProxySI.IntraControlNet.DirectURL
			// TODO if disagreement: url2 = found.ProxySI.IntraControlNet.DirectURL - and use it
		} else if disagreement {
			glog.Warningf("%s: proceeding as primary anyway...", pname)
		}
	} else {
		secondary = os.Getenv("AIS_PRIMARYPROXY") == ""
		if secondary {
			url = config.Proxy.PrimaryURL // NOTE: PublicNet, not IntraControlNet
		} else {
			smap.ProxySI = p.si
		}
	}
	// 4: join cluster as secondary
	if secondary {
		glog.Infof("%s: starting up as non-primary, joining via %s", pname, url)
		p.secondaryStartup(url)
		return
	}
	// 5: keep starting up as a primary - for now
	glog.Infof("%s: assuming the primary role for now, starting up...", pname)
	go p.primaryStartup(smap, clivars.ntargets)
}

func (p *proxyrunner) bootstrapSmap(smap *smapX) (found *smapX) {
	if smap.CountTargets() == 0 && smap.CountProxies() <= 1 {
		return
	}
	pname := p.si.Name()
	q := url.Values{}
	glog.Infof("%s: fast discovery based on %s", pname, smap.pp())
	q.Add(cmn.URLParamWhat, cmn.GetWhatSmapVote)
	url := cmn.URLPath(cmn.Version, cmn.Daemon)
	results := p.bcastGet(bcastArgs{req: cmn.ReqArgs{Path: url, Query: q}, smap: smap, to: cluster.AllNodes})
	for res := range results {
		if res.err != nil {
			continue
		}
		svm := SmapVoteMsg{}
		if err := jsoniter.Unmarshal(res.outjson, &svm); err != nil {
			continue
		}
		if svm.Smap == nil || svm.Smap.version() == 0 {
			continue
		}
		if svm.VoteInProgress {
			found = nil // unusable
			break
		}
		if found == nil || svm.Smap.version() > found.version() {
			found = svm.Smap
			glog.Infof("%s: found %s from %s", pname, found, res.si)
		}
	}
	return
}

// no change of mind when on the "secondary" track
func (p *proxyrunner) secondaryStartup(getSmapURL string) {
	var (
		config = cmn.GCO.Get()
		query  = url.Values{}
	)
	query.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	req := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   getSmapURL,
		Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
		Query:  query,
	}
	// get Smap
	f := func() {
		var (
			res  callResult
			args = callArgs{
				si:      p.si,
				req:     req,
				timeout: cmn.DefaultTimeout,
			}
		)
		for i := 0; i < maxRetrySeconds; i++ {
			res = p.call(args)
			if res.err != nil {
				if cmn.IsErrConnectionRefused(res.err) || res.status == http.StatusRequestTimeout {
					glog.Errorf("%s: get Smap from primary %s - retrying...", p.si.Name(), getSmapURL)
					time.Sleep(config.Timeout.CplaneOperation)
					continue
				}
			}
			break
		}
		if res.err != nil {
			glog.Fatalf("FATAL: error getting Smap from primary %s: %v", getSmapURL, res.err)
		}
		smap := &smapX{}
		err := jsoniter.Unmarshal(res.outjson, smap)
		cmn.AssertNoErr(err)
		if !smap.isValid() {
			glog.Fatalf("FATAL: invalid Smap at startup/registration: %s", smap.pp())
		}
		p.smapowner.put(smap) // put Smap
	}

	// get Smap -- wait some -- use the Smap to register self
	f()
	if err := p.registerWithRetry(); err != nil {
		glog.Fatalf("FATAL: %v", err)
	}
	time.Sleep(time.Second)
	f()

	p.smapowner.Lock()
	smap := p.smapowner.get()
	if !smap.isPresent(p.si) {
		glog.Fatalf("FATAL: %s failed to register self - not present in the %s", p.si.Name(), smap.pp())
	}
	if err := p.smapowner.persist(smap); err != nil {
		glog.Fatalf("FATAL: %s", err)
	}
	p.smapowner.Unlock()
	p.startedUp.Store(true) // joined as non-primary and started up
}

// proxy/gateway that is, potentially, the leader of the cluster
// waits a configured time for other nodes to join,
// discoveris cluster-wide metadata, and resolve remaining conflicts
func (p *proxyrunner) primaryStartup(loadedSmap *smapX, ntargets int) {
	const (
		metaction1 = "early-start-have-registrations"
		metaction2 = "primary-started-up"
	)
	var (
		smap                     *smapX
		emptySmap                = newSmap()
		ver                      = loadedSmap.version()
		haveRegistratons, merged bool
	)
	p.smapowner.Lock()
	emptySmap.Pmap[p.si.DaemonID] = p.si
	emptySmap.ProxySI = p.si
	p.smapowner.put(emptySmap)
	p.smapowner.Unlock() // starting up with an empty Smap version = 0

	// startup: accept registrations aka joins
	p.startup(ntargets)

	p.smapowner.Lock()
	smap = p.smapowner.get()
	if !smap.isPrimary(p.si) {
		p.smapowner.Unlock()
		glog.Infof("%s: change of mind #1: registering with %s", p.si.Name(), smap.ProxySI.IntraControlNet.DirectURL)
		p.secondaryStartup(smap.ProxySI.IntraControlNet.DirectURL)
		return
	}
	if smap.version() > 0 {
		cmn.Assert(smap.CountTargets() > 0 || smap.CountProxies() > 1)
		haveRegistratons = true
		v := smap.version()
		smap.Smap.Version = ver
		if !loadedSmap.Equals(&smap.Smap) {
			smap.Smap.Version = v
			loadedSmap.merge(smap)
			smap.Smap.Version = cmn.MaxI64(v, ver) + 1
			p.smapowner.put(smap)
			merged = true
		}
	}
	if !merged {
		p.smapowner.put(loadedSmap)
		smap = loadedSmap
	}
	p.smapowner.Unlock()

	if haveRegistratons {
		if merged {
			glog.Infof("%s: merged loaded %s", p.si.Name(), loadedSmap)
		}
		glog.Infof("%s: %s => %s", p.si.Name(), loadedSmap, smap.StringEx())
		bmd := p.bmdowner.get()
		msgInt := p.newActionMsgInternalStr(metaction1, smap, bmd)
		p.metasyncer.sync(true, revspair{smap, msgInt}, revspair{bmd, msgInt})
	} else {
		glog.Infof("%s: no registrations yet", p.si.Name())
	}

	// discover cluster-wide metadata and resolve remaining conflicts, if any
	p.discoverMeta(haveRegistratons)
	smap = p.smapowner.get()

	if !smap.isPrimary(p.si) {
		glog.Infof("%s: change of mind #2: registering with %s", p.si.Name(), smap.ProxySI.IntraControlNet.DirectURL)
		p.secondaryStartup(smap.ProxySI.IntraControlNet.DirectURL)
		return
	}

	if err := p.smapowner.persist(p.smapowner.get()); err != nil {
		glog.Fatalf("FATAL: %s", err)
	}
	p.bmdowner.Lock()
	bmd := p.bmdowner.get()
	// create BMD(new origin)
	if bmd.Version == 0 {
		clone := bmd.clone()
		clone.Version = 1
		clone.Origin = newBMDorigin()
		p.bmdowner.put(clone)
		bmd = clone
	}
	p.bmdowner.Unlock()

	msgInt := p.newActionMsgInternalStr(metaction2, smap, bmd)
	p.setGlobRebID(smap, msgInt, false /*set*/)
	p.metasyncer.sync(false, revspair{smap, msgInt}, revspair{bmd, msgInt})

	glog.Infof("%s: primary/cluster startup complete, %s", p.si.Name(), smap.StringEx())
	p.startedUp.Store(true) // started up as primary
}

func (p *proxyrunner) startup(ntargets int) {
	started := time.Now()
	for time.Since(started) < cmn.GCO.Get().Timeout.Startup {
		smap := p.smapowner.get()
		if !smap.isPrimary(p.si) {
			break
		}
		nt := smap.CountTargets()
		if nt >= ntargets && ntargets > 0 {
			glog.Infof("%s: reached the specified ntargets %d (curr=%d)", p.si.Name(), ntargets, nt)
			return
		}
		time.Sleep(time.Second)
	}
	nt := p.smapowner.get().CountTargets()
	if nt > 0 {
		glog.Warningf("%s: timed-out waiting for %d ntargets (curr=%d)", p.si.Name(), ntargets, nt)
	}
}

// the final major step in the primary startup sequence:
// discover cluster-wide metadata and resolve remaining conflicts
func (p *proxyrunner) discoverMeta(haveRegistratons bool) {
	var (
		pname    = p.si.Name()
		now      = time.Now()
		deadline = now.Add(cmn.GCO.Get().Timeout.Startup)
	)
	maxVerSmap, bucketmd := p.meta(deadline)
	if bucketmd != nil {
		p.bmdowner.Lock()
		if p.bmdowner.get().version() < bucketmd.version() {
			p.bmdowner.put(bucketmd)
		}
		p.bmdowner.Unlock()
	}
	if maxVerSmap == nil || maxVerSmap.version() == 0 {
		glog.Infof("%s: Smap discovery: none discovered", pname)
		return
	}
	smap := p.smapowner.get()
	if !haveRegistratons {
		// with no "live" registrations during p.startup() we use discovered Smap
		glog.Infof("%s: overriding local/merged Smap with the discovered %s", pname, maxVerSmap.pp())
		p.smapowner.put(maxVerSmap)
		return
	}
	// check for split-brain
	if maxVerSmap.ProxySI != nil && maxVerSmap.ProxySI.DaemonID != p.si.DaemonID {
		glog.Errorf("%s: split-brain (local/merged %s, primary=%s) vs (discovered %s, primary=%s)",
			pname, smap, smap.ProxySI.Name(), maxVerSmap, maxVerSmap.ProxySI.Name())
		glog.Flush()
	}
	if !smap.Equals(&maxVerSmap.Smap) {
		glog.Infof("%s %s: merging discovered %s", pname, smap, maxVerSmap.StringEx())
		p.smapowner.Lock()
		clone := p.smapowner.get().clone()
		maxVerSmap.merge(clone)
		clone.Version = cmn.MaxI64(clone.version(), maxVerSmap.version()) + 1
		p.smapowner.put(clone)
		p.smapowner.Unlock()
		glog.Infof("%s: merged %s", pname, clone.pp())
	}
}

func (p *proxyrunner) meta(deadline time.Time) (*smapX, *bucketMD) {
	var (
		maxVerSmap *smapX
		maxVerBMD  *bucketMD
		results    chan callResult
		origin     int64
		err        error
		resmap     map[*cluster.Snode]*bucketMD
		bcastSmap  = p.smapowner.get().clone()
		q          = url.Values{}
		config     = cmn.GCO.Get()
		keeptrying = true
		slowp      = false
	)
	resmap = make(map[*cluster.Snode]*bucketMD, bcastSmap.CountTargets()+bcastSmap.CountProxies())
	q.Add(cmn.URLParamWhat, cmn.GetWhatSmapVote)
	for keeptrying && time.Now().Before(deadline) {
		url := cmn.URLPath(cmn.Version, cmn.Daemon)
		results = p.bcastTo(bcastArgs{
			req:  cmn.ReqArgs{Path: url, Query: q},
			smap: bcastSmap,
			to:   cluster.AllNodes,
		})
		maxVerBMD, origin, keeptrying, slowp = nil, 0, false, false
		for k := range resmap {
			delete(resmap, k)
		}
		for res := range results {
			if res.err != nil {
				keeptrying = true
				continue
			}
			svm := SmapVoteMsg{}
			if err = jsoniter.Unmarshal(res.outjson, &svm); err != nil {
				glog.Errorf("unexpected unmarshal-error: %v", err)
				keeptrying = true
				continue
			}
			if svm.BucketMD != nil && svm.BucketMD.version() > 0 {
				if maxVerBMD == nil { // 1. init
					origin, maxVerBMD = svm.BucketMD.Origin, svm.BucketMD
				} else if origin != svm.BucketMD.Origin { // 2. slow path
					slowp = true
				} else if !slowp && maxVerBMD.Version < svm.BucketMD.Version { // 3. fast path max(version)
					maxVerBMD = svm.BucketMD
				}
			}
			if svm.Smap == nil {
				keeptrying = true
				continue
			}
			if maxVerSmap == nil || svm.Smap.version() > maxVerSmap.version() {
				maxVerSmap = svm.Smap
				for id, v := range maxVerSmap.Tmap {
					bcastSmap.Tmap[id] = v
				}
				for id, v := range maxVerSmap.Pmap {
					bcastSmap.Pmap[id] = v
				}
			}
			if svm.VoteInProgress {
				var s string
				if svm.Smap.ProxySI != nil {
					s = " of the current one " + svm.Smap.ProxySI.DaemonID
				}
				glog.Warningf("%s: starting up as primary(?) during reelection%s", p.si.Name(), s)
				maxVerSmap, maxVerBMD = nil, nil // zero-out as unusable
				keeptrying = true
				time.Sleep(config.Timeout.CplaneOperation)
				break
			}
			if svm.BucketMD != nil && svm.BucketMD.version() > 0 {
				resmap[res.si] = svm.BucketMD
			}
		}
		time.Sleep(config.Timeout.CplaneOperation)
	}
	if slowp {
		if maxVerBMD, err = resolveBMDorigin(resmap); err != nil {
			if _, split := err.(*errBmdOriginSplit); split {
				glog.Fatalf("FATAL: %v", err)
			}
			if !errors.Is(err, errNoBMD) {
				glog.Error(err.Error())
			}
		}
	}
	return maxVerSmap, maxVerBMD
}

func (p *proxyrunner) registerWithRetry() error {
	if status, err := p.register(false, cmn.DefaultTimeout); err != nil {
		if cmn.IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
			glog.Warningf("%s: retrying registration...", p.si.Name())
			time.Sleep(cmn.GCO.Get().Timeout.CplaneOperation)
			if _, err = p.register(false, cmn.DefaultTimeout); err != nil {
				glog.Errorf("%s: failed the 2nd attempt to register, err: %v", p.si.Name(), err)
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

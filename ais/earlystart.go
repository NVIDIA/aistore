// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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
// 	- The proxy's bootstrap sequence includes the following 3 main steps (below)
// 	  and is intended to resolve all the usual conflicts that typically arise
//	  in this type scenarios
func (p *proxyrunner) bootstrap() {
	var (
		found, smap    *smapX
		guessAmPrimary bool
		config         = cmn.GCO.Get()
		getSmapURL     = config.Proxy.PrimaryURL // NOTE: PublicNet, not IntraControlNet
		tout           = config.Timeout.CplaneOperation
		q              = url.Values{}
	)
	// step 1: load a local copy of the cluster map and
	//         try to use it for discovery of the current one
	smap = newSmap()
	if err := cmn.LocalLoad(filepath.Join(config.Confdir, cmn.SmapBackupFile), smap); err == nil {
		if smap.CountTargets() > 0 || smap.CountProxies() > 1 {
			glog.Infof("%s: fast discovery based on %s", p.si.Name(), smap.pp())
			q.Add(cmn.URLParamWhat, cmn.GetWhatSmapVote)
			url := cmn.URLPath(cmn.Version, cmn.Daemon)
			res := p.broadcastTo(url, q, http.MethodGet, nil, smap, tout, cmn.NetworkIntraControl, cluster.AllNodes)
			for re := range res {
				if re.err != nil {
					continue
				}
				svm := SmapVoteMsg{}
				if err := jsoniter.Unmarshal(re.outjson, &svm); err != nil {
					continue
				}
				if svm.Smap == nil || svm.Smap.version() == 0 {
					continue
				}
				if svm.VoteInProgress {
					found = nil // unusable
					break
				}
				if found == nil {
					found = svm.Smap
					glog.Infof("%s: found Smap v%d from %s", p.si.Name(), found.version(), re.si)
				} else if svm.Smap.version() > found.version() {
					found = svm.Smap
					glog.Infof("%s: found Smap v%d from %s", p.si.Name(), found.version(), re.si)
				}
			}
		}
	}
	// step 2: the information at this point includes (up to) 3 different pieces:
	// 	   local Smap +	discovered max-versioned Smap + environment setting "AIS_PRIMARYPROXY"
	// 	   Based on all of the above, we figure out if this proxy *may be* the primary,
	// 	   and if it may, proceed to start it up as such - until and if there's more evidence
	// 	   that points to the contrary
	if found != nil {
		if smap.version() > found.version() {
			glog.Infof("%s: discovered Smap v%d (primary=%s): merging => local v%d (primary=%s)",
				p.si.Name(), found.version(), found.ProxySI.DaemonID, smap.version(), smap.ProxySI.DaemonID)
			found.merge(smap)
			getSmapURL = p.resolvePrimary(smap.ProxySI, found.ProxySI, smap)
			if getSmapURL == "" {
				// discovered Smap is of no use - discard it
				glog.Warningf("%s: no good primary candidate, proceeding with an empty Smap", p.si.Name())
				smap = newSmap()
				getSmapURL = config.Proxy.PrimaryURL // NOTE: PublicNet, not IntraControlNet
			}
		} else {
			glog.Infof("%s: discovered Smap v%d (primary=%s): overriding local v%d (primary=%s)",
				p.si.Name(), found.version(), found.ProxySI.DaemonID, smap.version(), smap.ProxySI.DaemonID)
			smap = found
			getSmapURL = smap.ProxySI.IntraControlNet.DirectURL
		}
		smap.Pmap[p.si.DaemonID] = p.si
		guessAmPrimary = smap.isPrimary(p.si)
		// environment is a clue, not a prescription: discovered Smap outweighs
		if os.Getenv("AIS_PRIMARYPROXY") != "" && !guessAmPrimary {
			glog.Warningf("%s: Smap v%d (primary=%s): disregarding the environment setting primary=%s (self)",
				p.si.Name(), smap.version(), smap.ProxySI.DaemonID, p.si.DaemonID)
		}
	} else if os.Getenv("AIS_PRIMARYPROXY") != "" { // environment rules!
		smap.Pmap[p.si.DaemonID] = p.si
		smap.ProxySI = p.si
		guessAmPrimary = true
		glog.Infof("%s: initializing empty Smap, primary=self", p.si.Name())
	} else {
		smap.Pmap[p.si.DaemonID] = p.si
		glog.Infof("%s: initializing empty Smap, non-primary", p.si.Name())
	}

	// step 3: join as a non-primary, or
	// 	   keep starting up as a primary
	if !guessAmPrimary {
		cmn.AssertMsg(getSmapURL != p.si.PublicNet.DirectURL &&
			getSmapURL != p.si.IntraControlNet.DirectURL, getSmapURL)
		glog.Infof("%s: starting up as non-primary, joining => %s", p.si.Name(), getSmapURL)
		p.secondaryStartup(getSmapURL)
		return
	}
	glog.Infof("%s: assuming the primary role for now, starting up...", p.si.Name())
	go p.primaryStartup(smap, clivars.ntargets)
}

func (p *proxyrunner) resolvePrimary(myPrimary, otherPrimary *cluster.Snode, smap *smapX) (getSmapURL string) {
	var (
		newSmap *smapX
		errstr  string
	)
	getSmapURL = myPrimary.IntraControlNet.DirectURL
	newSmap, errstr = p.smapFromURL(getSmapURL)
	if errstr == "" && newSmap.ProxySI.DaemonID == myPrimary.DaemonID {
		return
	}
	getSmapURL = otherPrimary.IntraControlNet.DirectURL
	newSmap, errstr = p.smapFromURL(getSmapURL)
	if errstr == "" && newSmap.ProxySI.DaemonID == otherPrimary.DaemonID {
		smap.ProxySI = newSmap.ProxySI
		glog.Infof("%s: change of mind #3: primary is %s (not %s)", p.si.Name(), otherPrimary.DaemonID, myPrimary.DaemonID)
		return
	}
	return ""
}

// no change of mind when on the "secondary" track
func (p *proxyrunner) secondaryStartup(getSmapURL string) {
	var (
		config = cmn.GCO.Get()
		query  = url.Values{}
	)
	query.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	req := reqArgs{
		method: http.MethodGet,
		base:   getSmapURL,
		path:   cmn.URLPath(cmn.Version, cmn.Daemon),
		query:  query,
	}
	// get Smap
	f := func() {
		var (
			res  callResult
			args = callArgs{
				si:      p.si,
				req:     req,
				timeout: defaultTimeout,
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
	if errstr := p.smapowner.persist(smap, true /*saveSmap*/); errstr != "" {
		glog.Fatalf("FATAL: %s", errstr)
	}
	p.smapowner.Unlock()
	p.startedUp.Store(true) // joined as non-primary and started up
}

// proxy/gateway that is, potentially, the leader of the cluster executes steps:
// 	- (i)    initialize empty Smap
// 	- (ii)   wait a configured time for other nodes to join
// 	- (iii)  merge the Smap containing newly joined nodes with the guessSmap
// 		 that was previously discovered/merged
// 	- (iiii) discover cluster-wide metadata, and resolve remaining conflicts
func (p *proxyrunner) primaryStartup(guessSmap *smapX, ntargets int) {
	const (
		metaction1 = "early-start-have-registrations"
		metaction2 = "primary-started-up"
	)
	// (i) initialize empty Smap
	cmn.Assert(p.smapowner.get() == nil)
	p.smapowner.Lock()
	startupSmap := newSmap()
	startupSmap.Pmap[p.si.DaemonID] = p.si
	startupSmap.ProxySI = p.si
	p.smapowner.put(startupSmap)
	p.smapowner.Unlock() // starting up with an empty Smap version = 0

	// (ii) give it some time for other nodes to join the cluster
	p.startup(ntargets)

	smap := p.smapowner.get()
	if !smap.isPrimary(p.si) {
		glog.Infof("%s: change of mind #1: registering with %s", p.si.Name(), smap.ProxySI.IntraControlNet.DirectURL)
		p.secondaryStartup(smap.ProxySI.IntraControlNet.DirectURL)
		return
	}
	// (iii) merge with the previously discovered/merged - but only if there were new registrations
	haveRegistratons := false
	p.smapowner.Lock()
	if smap.version() > 0 {
		cmn.Assert(smap.CountTargets() > 0 || smap.CountProxies() > 1)
		haveRegistratons = true
		guessSmap.merge(smap)
		p.smapowner.put(smap)
	} else { // otherwise, use the previously discovered/merged Smap
		p.smapowner.put(guessSmap)
	}
	p.smapowner.Unlock()

	smap = p.smapowner.get()
	if haveRegistratons {
		glog.Infof("%s: merged local Smap (%d/%d)", p.si.Name(), smap.CountTargets(), smap.CountProxies())
		bmd := p.bmdowner.get()
		msgInt := p.newActionMsgInternalStr(metaction1, smap, bmd)
		p.metasyncer.sync(true, revspair{smap, msgInt}, revspair{bmd, msgInt})
	} else {
		glog.Infof("%s: no registrations yet", p.si.Name())
	}

	// (iiii) discover cluster-wide metadata and resolve remaining conflicts, if any
	p.discoverMeta(haveRegistratons)
	smap = p.smapowner.get()

	if !smap.isPrimary(p.si) {
		glog.Infof("%s: change of mind #2: registering with %s", p.si.Name(), smap.ProxySI.IntraControlNet.DirectURL)
		p.secondaryStartup(smap.ProxySI.IntraControlNet.DirectURL)
		return
	}

	if s := p.smapowner.persist(p.smapowner.get(), true); s != "" {
		glog.Fatalf("FATAL: %s", s)
	}
	bmd := p.bmdowner.get()
	msgInt := p.newActionMsgInternalStr(metaction2, smap, bmd)
	p.metasyncer.sync(false, revspair{smap, msgInt}, revspair{bmd, msgInt})
	glog.Infof("%s: primary/cluster startup complete, Smap v%d, ntargets %d", p.si.Name(), smap.version(), smap.CountTargets())
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
			glog.Infof("%s: reached the expected %d/%d target registrations", p.si.Name(), ntargets, nt)
			return
		}
		time.Sleep(time.Second)
	}
	nt := p.smapowner.get().CountTargets()
	if nt > 0 {
		glog.Warningf("%s: timed out waiting for %d/%d target registrations", p.si.Name(), ntargets, nt)
	}
}

// the final major step in the primary startup sequence:
// discover cluster-wide metadata and resolve remaining conflicts
func (p *proxyrunner) discoverMeta(haveRegistratons bool) {
	var (
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
		glog.Infof("%s: Smap discovery: none discovered", p.si.Name())
		return
	}
	smap := p.smapowner.get()
	// use the discovered Smap if there were no (live) registrations during the startup()
	if !haveRegistratons {
		glog.Infof("%s: overriding local/merged Smap with the discovered %s", p.si.Name(), maxVerSmap.pp())
		p.smapowner.put(maxVerSmap)
		return
	}
	// check for split-brain
	if maxVerSmap.ProxySI != nil && maxVerSmap.ProxySI.DaemonID != p.si.DaemonID {
		glog.Errorf("%s: split-brain (local/merged v%d, primary=%s) vs (discovered v%d, primary=%s)",
			p.si.Name(), smap.version(), smap.ProxySI.DaemonID, maxVerSmap.version(), maxVerSmap.ProxySI.DaemonID)
		glog.Flush()
	}
	// merge the discovered (max-version) Smap and the local/current one
	// that was constructed from scratch via node-joins
	glog.Infof("%s: merging discovered Smap v%d (%d, %d)", p.si.Name(),
		maxVerSmap.version(), maxVerSmap.CountTargets(), maxVerSmap.CountProxies())

	p.smapowner.Lock()
	clone := p.smapowner.get().clone()
	maxVerSmap.merge(clone)
	clone.Version++
	if clone.version() < maxVerSmap.version() {
		clone.Version = maxVerSmap.version() + 1
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()
	glog.Infof("%s: merged %s", p.si.Name(), clone.pp())
}

func (p *proxyrunner) meta(deadline time.Time) (*smapX, *bucketMD) {
	var (
		maxVerBucketMD *bucketMD
		maxVersionSmap *smapX
		bcastSmap      = p.smapowner.get().clone()
		q              = url.Values{}
		config         = cmn.GCO.Get()
		tout           = config.Timeout.CplaneOperation
		keeptrying     = true
	)
	q.Add(cmn.URLParamWhat, cmn.GetWhatSmapVote)
	for keeptrying && time.Now().Before(deadline) {
		url := cmn.URLPath(cmn.Version, cmn.Daemon)
		res := p.broadcastTo(url, q, http.MethodGet, nil, bcastSmap, tout, cmn.NetworkIntraControl, cluster.AllNodes)
		keeptrying = false
		for re := range res {
			if re.err != nil {
				keeptrying = true
				continue
			}
			svm := SmapVoteMsg{}
			if err := jsoniter.Unmarshal(re.outjson, &svm); err != nil {
				glog.Errorf("Unexpected unmarshal-error: %v", err)
				keeptrying = true
				continue
			}
			if svm.BucketMD != nil && svm.BucketMD.version() > 0 {
				if maxVerBucketMD == nil || svm.BucketMD.version() > maxVerBucketMD.version() {
					maxVerBucketMD = svm.BucketMD
				}
			}
			if svm.Smap == nil {
				keeptrying = true
				continue
			}
			if maxVersionSmap == nil || svm.Smap.version() > maxVersionSmap.version() {
				maxVersionSmap = svm.Smap
				for id, v := range maxVersionSmap.Tmap {
					bcastSmap.Tmap[id] = v
				}
				for id, v := range maxVersionSmap.Pmap {
					bcastSmap.Pmap[id] = v
				}
			}
			if svm.VoteInProgress {
				var s string
				if svm.Smap.ProxySI != nil {
					s = " of the current one " + svm.Smap.ProxySI.DaemonID
				}
				glog.Warningf("%s: starting up as primary(?) during reelection%s", p.si.Name(), s)
				maxVersionSmap, maxVerBucketMD = nil, nil // zero-out as unusable
				keeptrying = true
				time.Sleep(config.Timeout.CplaneOperation)
				break
			}
		}
		time.Sleep(config.Timeout.CplaneOperation)
	}
	return maxVersionSmap, maxVerBucketMD
}

func (p *proxyrunner) registerWithRetry() error {
	if status, err := p.register(false, defaultTimeout); err != nil {
		if cmn.IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
			glog.Warningf("%s: retrying registration...", p.si.Name())
			time.Sleep(cmn.GCO.Get().Timeout.CplaneOperation)
			if _, err = p.register(false, defaultTimeout); err != nil {
				glog.Errorf("%s: failed the 2nd attempt to register, err: %v", p.si.Name(), err)
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// Background:
// 	- Each proxy/gateway stores a local copy of the cluster map (Smap)
// 	- Each Smap instance is versioned; the versioning is monotonic (increasing)
// 	- Only the primary (leader) proxy distributes Smap updates to all other clustered nodes
// 	- Bootstrap sequence includes /steps/ intended to resolve all the usual conflicts that may arise.
func (p *proxyrunner) bootstrap() {
	var (
		smap            *smapX
		config          = cmn.GCO.Get()
		pid, primaryURL string
		primary, loaded bool
	)
	// 1: load a local copy and try to utilize it for discovery
	smap = newSmap()
	if err := p.owner.smap.load(smap, config); err == nil {
		loaded = true
		if smap.CountTargets() == 0 {
			loaded = false
		} else if err := p.checkPresenceNetChange(smap); err != nil {
			glog.Error(err)
			loaded = false
		}
	}
	// 2. make the preliminary/primary decision
	if !loaded {
		smap = nil
	}
	pid, primary = p.determineRole(smap, loaded)

	// 3. primary?
	if primary {
		if pid != "" { // takes precedence over everything else
			cmn.Assert(pid == p.si.ID())
		} else if loaded {
			var smapMaxVer int64
			// double-check
			if smapMaxVer, primaryURL = p.bcastHealth(smap); smapMaxVer > smap.version() {
				glog.Infof("%s: cannot assume the primary role with older %s < v%d", p.si, smap, smapMaxVer)
				primary = false
			}
		}
	}

	// 4.1: start as primary
	if primary {
		glog.Infof("%s: assuming primary role for now, starting up...", p.si)
		go p.primaryStartup(smap, config, daemon.cli.ntargets)
		return
	}

	// 4.2: otherwise, join as non-primary
	glog.Infof("%s: starting up as non-primary", p.si)
	err := p.secondaryStartup(smap, primaryURL)
	if err != nil {
		if loaded {
			maxVerSmap, _ := p.uncoverMeta(smap)
			if maxVerSmap != nil && maxVerSmap.ProxySI != nil {
				glog.Infof("%s: second attempt  - joining via %s...", p.si, maxVerSmap)
				err = p.secondaryStartup(maxVerSmap)
			}
		}
	}
	if err != nil {
		cmn.ExitLogf("FATAL: %s (non-primary) failed to join cluster, err: %v", p.si, err)
	}
}

// - make the *primary* decision taking into account both environment and
//   loaded Smap, if exists
// - handle AIS_PRIMARY_ID (TODO: target)
// - see also "change of mind"
func (p *proxyrunner) determineRole(smap *smapX, loaded bool) (pid string, primary bool) {
	tag := "no Smap, "
	if loaded {
		smap.Pmap[p.si.ID()] = p.si
		tag = smap.StringEx() + ", "
	}
	// parse env
	envP := struct {
		pid     string
		primary bool
	}{
		pid:     os.Getenv(cmn.EnvVars.PrimaryID),
		primary: cmn.IsParseBool(os.Getenv(cmn.EnvVars.IsPrimary)),
	}

	if envP.pid != "" && envP.primary && p.si.ID() != envP.pid {
		cmn.ExitLogf(
			"FATAL: %s: invalid combination of %s=true & %s=%s",
			p.si, cmn.EnvVars.IsPrimary, cmn.EnvVars.PrimaryID, envP.pid,
		)
	}
	glog.Infof("%s: %sprimary-env=%+v", p.si, tag, envP)

	if loaded && envP.pid != "" {
		primary := smap.GetProxy(envP.pid)
		if primary == nil {
			glog.Errorf(
				"%s: ignoring %s=%s - not found in the loaded %s",
				p.si, cmn.EnvVars.IsPrimary, envP.pid, smap,
			)
			envP.pid = ""
		} else if smap.ProxySI.ID() != envP.pid {
			glog.Warningf(
				"%s: new %s=%s, previous %s",
				p.si, cmn.EnvVars.PrimaryID, envP.pid, smap.ProxySI,
			)
			smap.ProxySI = primary
		}
	}
	if envP.pid != "" {
		primary = envP.pid == p.si.ID()
		pid = envP.pid
	} else if loaded {
		primary = smap.isPrimary(p.si)
	} else {
		primary = envP.primary
	}
	return
}

// join cluster
// no change of mind when on the "secondary" track
func (p *proxyrunner) secondaryStartup(smap *smapX, primaryURLs ...string) error {
	if smap == nil {
		smap = newSmap()
	} else if smap.ProxySI.ID() == p.si.ID() {
		glog.Infof("%s: zeroing-out primary=self in %s", p.si, smap)
		smap.ProxySI = nil
	}
	p.owner.smap.put(smap)
	if err := p.withRetry(p.joinCluster, "join", true /* backoff */, primaryURLs...); err != nil {
		glog.Errorf("%s failed to join, err: %v", p.si, err)
		return err
	}

	p.markNodeStarted()

	go func() {
		p.pollClusterStarted(cmn.GCO.Get().Timeout.CplaneOperation)
		p.markClusterStarted()
	}()

	glog.Infof("%s: joined as non-primary, %s", p.si, smap.StringEx())
	return nil
}

// Proxy/gateway that is, potentially, the leader of the cluster.
// It waits a configured time for other nodes to join,
// discovers cluster-wide metadata, and resolve remaining conflicts.
func (p *proxyrunner) primaryStartup(loadedSmap *smapX, config *cmn.Config, ntargets int) {
	const (
		metaction1 = "early-start-have-registrations"
		metaction2 = "primary-started-up"
	)
	var (
		smap             = newSmap()
		haveRegistratons bool
	)

	// 1: init Smap to accept reg-s
	p.owner.smap.Lock()
	smap.Pmap[p.si.ID()] = p.si
	smap.ProxySI = p.si
	p.owner.smap.put(smap)
	p.owner.smap.Unlock()

	p.markNodeStarted()

	if !daemon.cli.skipStartup {
		maxVerSmap := p.acceptRegistrations(smap, loadedSmap, config, ntargets)
		if maxVerSmap != nil {
			maxVerSmap.Pmap[p.si.ID()] = p.si
			p.owner.smap.put(maxVerSmap)
			glog.Infof("%s: change-of-mind #1: registering with %s(%s)",
				p.si, maxVerSmap.ProxySI.ID(), maxVerSmap.ProxySI.IntraControlNet.DirectURL)
			if err := p.secondaryStartup(maxVerSmap); err != nil {
				cmn.ExitLogf("FATAL: %v", err)
			}
			return
		}
	}

	smap = p.owner.smap.get()
	haveRegistratons = smap.CountTargets() > 0 || smap.CountProxies() > 1

	// 2: merging local => boot
	if haveRegistratons {
		var added int
		if loadedSmap != nil {
			added, _ = smap.merge(loadedSmap, true /*override (IP, port) duplicates*/)
			p.owner.smap.Lock()
			smap = loadedSmap
			if added > 0 {
				smap.Version = smap.Version + int64(added) + 1
			}
			p.owner.smap.put(smap)
			p.owner.smap.Unlock()
		}
		glog.Infof("%s: initial %s, curr %s, added=%d", p.si, loadedSmap, smap.StringEx(), added)
		bmd := p.owner.bmd.get()
		msg := p.newAisMsgStr(metaction1, smap, bmd)
		wg := p.metasyncer.sync(revsPair{smap, msg}, revsPair{bmd, msg})
		wg.Wait()
	} else {
		glog.Infof("%s: no registrations yet", p.si)
		if loadedSmap != nil {
			glog.Infof("%s: keep going w/ local %s", p.si, loadedSmap.StringEx())
			p.owner.smap.Lock()
			smap = loadedSmap
			p.owner.smap.put(smap)
			p.owner.smap.Unlock()
		}
	}

	// 3: discover cluster meta and resolve remaining conflicts, if any
	p.discoverMeta(smap)

	// 4: still primary?
	p.owner.smap.Lock()
	smap = p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		p.owner.smap.Unlock()
		glog.Infof("%s: registering with %s", p.si, smap.ProxySI.NameEx())
		if err := p.secondaryStartup(smap); err != nil {
			cmn.ExitLogf("FATAL: %v", err)
		}
		return
	}

	// 5:  persist and finalize w/ sync + BMD
	if smap.UUID == "" {
		if !daemon.cli.skipStartup && smap.CountTargets() == 0 {
			cmn.ExitLogf("FATAL: %s cannot create a new cluster with no targets, %s", p.si, smap)
		}
		clone := smap.clone()
		clone.UUID, clone.CreationTime = newClusterUUID() // new uuid
		clone.Version++
		p.owner.smap.put(clone)
		smap = clone
	}
	if err := p.owner.smap.persist(smap); err != nil {
		cmn.ExitLogf("FATAL: %s (primary), err: %v", p.si, err)
	}
	p.owner.smap.Unlock()

	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if bmd.Version == 0 {
		clone := bmd.clone()
		clone.Version = 1 // init BMD
		clone.UUID = smap.UUID
		p.owner.bmd.put(clone)
		bmd = clone
	}
	p.owner.bmd.Unlock()

	msg := p.newAisMsgStr(metaction2, smap, bmd)
	_ = p.metasyncer.sync(revsPair{smap, msg}, revsPair{bmd, msg})

	// 6: started up as primary
	glog.Infof("%s: primary/cluster startup complete, %s", p.si, smap.StringEx())
	p.markClusterStarted()
}

func (p *proxyrunner) acceptRegistrations(smap, loadedSmap *smapX, config *cmn.Config, ntargets int) (maxVerSmap *smapX) {
	const (
		// Number of iteration to consider cluster quiescent.
		quiescentIter = 4
	)

	var (
		deadlineTimer     = time.NewTimer(config.Timeout.Startup)
		sleepTicker       = time.NewTicker(config.Timeout.MaxKeepalive)
		checkClusterTimer = time.NewTimer(3 * config.Timeout.MaxKeepalive)
		definedTargetCnt  = ntargets > 0
	)

	defer func() {
		deadlineTimer.Stop()
		sleepTicker.Stop()
		checkClusterTimer.Stop()
	}()

MainLoop:
	for iter := 0; iter < quiescentIter; {
		select {
		case <-deadlineTimer.C:
			break MainLoop
		case <-sleepTicker.C:
			iter++
			break
		case <-checkClusterTimer.C:
			// Check whether the cluster has moved on (but check only once).
			if loadedSmap == nil || loadedSmap.CountTargets() == 0 {
				break
			}
			if maxVerSmap := p.bcastMaxVerBestEffort(loadedSmap); maxVerSmap != nil {
				return maxVerSmap
			}
		}

		prevTargetCnt := smap.CountTargets()
		smap = p.owner.smap.get()
		if !smap.isPrimary(p.si) {
			break
		}
		targetCnt := smap.CountTargets()
		if targetCnt > prevTargetCnt || (definedTargetCnt && targetCnt < ntargets) {
			// Reset the counter in case there are new targets or we wait for
			// targets but we still don't have enough of them.
			iter = 0
		}
	}
	targetCnt := p.owner.smap.get().CountTargets()
	if definedTargetCnt {
		if targetCnt >= ntargets {
			glog.Infof("%s: reached expected %d targets (registered: %d)", p.si, ntargets, targetCnt)
		} else {
			glog.Warningf("%s: timed-out waiting for %d targets (registered: %d)", p.si, ntargets, targetCnt)
		}
	} else {
		glog.Infof("%s: registered %d new targets", p.si, targetCnt)
	}
	return
}

// the final major step in the primary startup sequence:
// discover cluster-wide metadata and resolve remaining conflicts
func (p *proxyrunner) discoverMeta(smap *smapX) {
	var (
		maxVerSmap, maxVerBMD = p.uncoverMeta(smap)
	)
	if maxVerBMD != nil {
		p.owner.bmd.Lock()
		bmd := p.owner.bmd.get()
		if bmd == nil || bmd.version() < maxVerBMD.version() {
			p.owner.bmd.put(maxVerBMD)
		}
		p.owner.bmd.Unlock()
	}
	if maxVerSmap == nil || maxVerSmap.version() == 0 {
		glog.Infof("%s: no max-ver Smaps", p.si)
		return
	}
	glog.Infof("%s: local %s max-ver %s", p.si, smap.StringEx(), maxVerSmap.StringEx())
	sameUUID, sameVersion, eq := smap.Compare(&maxVerSmap.Smap)
	if !sameUUID {
		// FATAL: cluster integrity error (cie)
		cmn.ExitLogf("%s: split-brain uuid [%s %s] vs [%s %s]",
			ciError(10), p.si, smap.StringEx(), maxVerSmap.ProxySI, maxVerSmap.StringEx())
	}
	if eq && sameVersion {
		return
	}
	if maxVerSmap.ProxySI != nil && maxVerSmap.ProxySI.ID() != p.si.ID() {
		if maxVerSmap.version() > smap.version() {
			glog.Infof("%s: change-of-mind #2 %s <= max-ver %s", p.si, smap.StringEx(), maxVerSmap.StringEx())
			maxVerSmap.Pmap[p.si.ID()] = p.si
			p.owner.smap.put(maxVerSmap)
			return
		}
		// FATAL: cluster integrity error (cie)
		cmn.ExitLogf("%s: split-brain local [%s %s] vs [%s %s]",
			ciError(20), p.si, smap.StringEx(), maxVerSmap.ProxySI, maxVerSmap.StringEx())
	}
	p.owner.smap.Lock()
	clone := p.owner.smap.get().clone()
	if !eq {
		_, err := maxVerSmap.merge(clone, false /*err if detected (IP, port) duplicates*/)
		if err != nil {
			cmn.ExitLogf("%s: %v vs [%s: %s]", p.si, err, maxVerSmap.ProxySI, maxVerSmap.StringEx())
		}
	}
	clone.Version = cmn.MaxI64(clone.version(), maxVerSmap.version()) + 1
	p.owner.smap.put(clone)
	p.owner.smap.Unlock()
	glog.Infof("%s: merged %s", p.si, clone.pp())
}

func (p *proxyrunner) uncoverMeta(bcastSmap *smapX) (maxVerSmap *smapX, maxVerBMD *bucketMD) {
	var (
		err         error
		suuid       string
		config      = cmn.GCO.Get()
		now         = time.Now()
		deadline    = now.Add(config.Timeout.Startup)
		q           = url.Values{}
		url         = cmn.URLPath(cmn.Version, cmn.Daemon)
		l           = bcastSmap.CountTargets() + bcastSmap.CountProxies()
		bmds        = make(map[*cluster.Snode]*bucketMD, l)
		smaps       = make(map[*cluster.Snode]*smapX, l)
		done, slowp bool
	)
	q.Add(cmn.URLParamWhat, cmn.GetWhatSmapVote)
	args := bcastArgs{req: cmn.ReqArgs{Path: url, Query: q}, smap: bcastSmap, to: cluster.AllNodes}
	for {
		last := time.Now().After(deadline)
		maxVerSmap, maxVerBMD, done, slowp = p.bcastMaxVer(args, bmds, smaps)
		if done || last {
			break
		}
		time.Sleep(config.Timeout.CplaneOperation)
	}
	if slowp {
		if maxVerBMD, err = resolveUUIDBMD(bmds); err != nil {
			if _, split := err.(*errBmdUUIDSplit); split {
				cmn.ExitLogf("FATAL: %s (primary), err: %v", p.si, err) // cluster integrity error
			}
			glog.Error(err.Error())
		}
		for si, smap := range smaps {
			if !si.IsTarget() {
				continue
			}
			if suuid == "" {
				suuid = smap.UUID
				if suuid != "" {
					glog.Infof("%s: set Smap uuid = %s(%s)", p.si, si, suuid)
				}
			} else if suuid != smap.UUID && smap.UUID != "" {
				// FATAL: cluster integrity error (cie)
				cmn.ExitLogf("%s: split-brain [%s %s] vs [%s %s]",
					ciError(30), p.si, suuid, si, smap.UUID)
			}
		}
		for _, smap := range smaps {
			if smap.UUID != suuid {
				continue
			}
			if maxVerSmap == nil {
				maxVerSmap = smap
			} else if maxVerSmap.version() < smap.version() {
				maxVerSmap = smap
			}
		}
	}
	return
}

func (p *proxyrunner) bcastMaxVer(args bcastArgs, bmds map[*cluster.Snode]*bucketMD,
	smaps map[*cluster.Snode]*smapX) (maxVerSmap *smapX, maxVerBMD *bucketMD, done, slowp bool) {
	var (
		results          chan callResult
		borigin, sorigin string
		err              error
	)
	done = true
	results = p.bcastTo(args)
	for k := range bmds {
		delete(bmds, k)
	}
	for k := range smaps {
		delete(smaps, k)
	}
	for res := range results {
		if res.err != nil {
			done = false
			continue
		}
		svm := SmapVoteMsg{}
		if err = jsoniter.Unmarshal(res.outjson, &svm); err != nil {
			glog.Errorf("unexpected unmarshal-error: %v", err)
			done = false
			continue
		}
		if svm.BucketMD != nil && svm.BucketMD.version() > 0 {
			if maxVerBMD == nil { // 1. init
				borigin, maxVerBMD = svm.BucketMD.UUID, svm.BucketMD
			} else if borigin != "" && borigin != svm.BucketMD.UUID { // 2. slow path
				slowp = true
			} else if !slowp && maxVerBMD.Version < svm.BucketMD.Version { // 3. fast path max(version)
				maxVerBMD = svm.BucketMD
				borigin = svm.BucketMD.UUID
			}
		}
		if svm.Smap != nil && svm.VoteInProgress {
			var s string
			if svm.Smap.ProxySI != nil {
				s = " of the current one " + svm.Smap.ProxySI.ID()
			}
			glog.Warningf("%s: starting up as primary(?) during reelection%s", p.si, s)
			maxVerSmap, maxVerBMD = nil, nil // zero-out as unusable
			done = false
			break
		}
		if svm.Smap != nil && svm.Smap.version() > 0 {
			if maxVerSmap == nil { // 1. init
				sorigin, maxVerSmap = svm.Smap.UUID, svm.Smap
			} else if sorigin != "" && sorigin != svm.Smap.UUID { // 2. slow path
				slowp = true
			} else if !slowp && maxVerSmap.Version < svm.Smap.Version { // 3. fast path max(version)
				maxVerSmap = svm.Smap
				sorigin = svm.Smap.UUID
			}
		}
		if bmds != nil && svm.BucketMD != nil && svm.BucketMD.version() > 0 {
			bmds[res.si] = svm.BucketMD
		}
		if smaps != nil && svm.Smap != nil && svm.Smap.version() > 0 {
			smaps[res.si] = svm.Smap
		}
	}
	return
}

func (p *proxyrunner) bcastMaxVerBestEffort(smap *smapX) *smapX {
	var (
		q    = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmapVote}}
		args = bcastArgs{
			req:  cmn.ReqArgs{Path: cmn.URLPath(cmn.Version, cmn.Daemon), Query: q},
			smap: smap,
			to:   cluster.AllNodes,
		}
		maxVerSmap, _, _, slowp = p.bcastMaxVer(args, nil, nil)
	)
	if maxVerSmap != nil && !slowp {
		if maxVerSmap.UUID == smap.UUID && maxVerSmap.version() > smap.version() {
			if maxVerSmap.ProxySI != nil && maxVerSmap.ProxySI.ID() != p.si.ID() {
				glog.Infof("%s: my %s is older than max-ver %s",
					p.si, smap.StringEx(), maxVerSmap.StringEx())
				return maxVerSmap
			}
		}
	}
	return nil
}

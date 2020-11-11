// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

// Background:
// 	- Each proxy/gateway stores a local copy of the cluster map (Smap)
// 	- Each Smap instance is versioned; the versioning is monotonic (increasing)
// 	- Only the primary (leader) proxy distributes Smap updates to all other clustered nodes
// 	- Bootstrap sequence includes /steps/ intended to resolve all the usual conflicts that may arise.
func (p *proxyrunner) bootstrap() {
	var (
		config          = cmn.GCO.Get()
		pid, primaryURL string
		primary         bool
	)

	// 1: load a local copy and try to utilize it for discovery
	smap, reliable := p.tryLoadSmap()
	if !reliable {
		smap = nil
	}
	// 2. make the preliminary/primary decision
	pid, primary = p.determineRole(smap)

	// 3. primary?
	if primary {
		if pid != "" { // takes precedence over everything else
			cmn.Assert(pid == p.si.ID())
		} else if reliable {
			var smapMaxVer int64
			// double-check
			if smapMaxVer, primaryURL = p.bcastHealth(smap); smapMaxVer > smap.version() {
				glog.Infof("%s: cannot assume the primary role with older %s < v%d",
					p.si, smap, smapMaxVer)
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
		if reliable {
			maxVerSmap, _ := p.uncoverMeta(smap)
			if maxVerSmap != nil && maxVerSmap.Primary != nil {
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
func (p *proxyrunner) determineRole(loadedSmap *smapX) (pid string, primary bool) {
	tag := "no Smap, "
	if loadedSmap != nil {
		loadedSmap.Pmap[p.si.ID()] = p.si
		tag = loadedSmap.StringEx() + ", "
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

	if loadedSmap != nil && envP.pid != "" {
		primary := loadedSmap.GetProxy(envP.pid)
		if primary == nil {
			glog.Errorf(
				"%s: ignoring %s=%s - not found in the loaded %s",
				p.si, cmn.EnvVars.IsPrimary, envP.pid, loadedSmap,
			)
			envP.pid = ""
		} else if loadedSmap.Primary.ID() != envP.pid {
			glog.Warningf(
				"%s: new %s=%s, previous %s",
				p.si, cmn.EnvVars.PrimaryID, envP.pid, loadedSmap.Primary,
			)
			loadedSmap.Primary = primary
		}
	}
	if envP.pid != "" {
		primary = envP.pid == p.si.ID()
		pid = envP.pid
	} else if loadedSmap != nil {
		primary = loadedSmap.isPrimary(p.si)
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
	} else if smap.Primary.ID() == p.si.ID() {
		glog.Infof("%s: zeroing-out primary=self in %s", p.si, smap)
		smap.Primary = nil
	}
	p.owner.smap.put(smap)
	if err := p.withRetry(p.joinCluster, "join", true /* backoff */, primaryURLs...); err != nil {
		glog.Errorf("%s failed to join, err: %v", p.si, err)
		return err
	}

	p.markNodeStarted()

	go func() {
		p.pollClusterStarted(cmn.GCO.Get().Timeout.CplaneOperation)
		glog.Infof("%s: non-primary & cluster startup complete, %s", p.si, smap.StringEx())
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
	si := p.si.Clone()
	smap.Primary = si
	smap.Pmap[p.si.ID()] = si
	p.owner.smap.put(smap)
	p.owner.smap.Unlock()

	p.markNodeStarted()

	if !daemon.cli.skipStartup {
		maxVerSmap := p.acceptRegistrations(smap, loadedSmap, config, ntargets)
		if maxVerSmap != nil {
			maxVerSmap.Pmap[p.si.ID()] = p.si
			p.owner.smap.put(maxVerSmap)
			glog.Infof("%s: change-of-mind #1: registering with %s(%s)",
				p.si, maxVerSmap.Primary.ID(), maxVerSmap.Primary.IntraControlNet.DirectURL)
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
		glog.Infof("%s: registering with %s", p.si, smap.Primary.NameEx())
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
	// try to start with a fully staffed IC
	if l := smap.ICCount(); l < smap.DefaultICSize() {
		clone := smap.clone()
		clone.staffIC()
		if l != clone.ICCount() {
			clone.Version++
			smap = clone
			p.owner.smap.put(smap)
		}
	}
	if err := p.owner.smap.persist(smap); err != nil {
		cmn.ExitLogf("FATAL: %s (primary), err: %v", p.si, err)
	}
	p.owner.smap.Unlock()

	// 6: started up as primary
	ctx := &bmdModifier{
		pre:   p._startupBMDPre,
		final: p._startupBMDFinal,
		smap:  smap,
		msg:   &cmn.ActionMsg{Value: metaction2},
	}
	p.owner.bmd.modify(ctx)
	glog.Infof("%s: primary & cluster startup complete", p.si)

	p.markClusterStarted()
}

func (p *proxyrunner) _startupBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	if clone.Version == 0 {
		clone.Version = 1 // init BMD
		clone.UUID = ctx.smap.UUID
	}
	return nil
}

func (p *proxyrunner) _startupBMDFinal(ctx *bmdModifier, clone *bucketMD) {
	msg := p.newAisMsg(ctx.msg, ctx.smap, clone)
	// CAS returns `true` if it has swapped, so it a one-liner for:
	// doRebalance=rebalance.Load(); rebalance.Store(false)
	doRebalance := p.owner.rmd.rebalance.CAS(true, false)
	var wg *sync.WaitGroup
	if doRebalance && cmn.GCO.Get().Rebalance.Enabled {
		glog.Infof("rebalance did not finish, restarting...")
		msg.Action = cmn.ActRebalance
		ctx := &rmdModifier{
			smap: ctx.smap,
			pre:  func(ctx *rmdModifier, clone *rebMD) { clone.inc() },
		}
		rmd := p.owner.rmd.modify(ctx)
		wg = p.metasyncer.sync(revsPair{ctx.smap, msg}, revsPair{clone, msg}, revsPair{rmd, msg})
	} else {
		wg = p.metasyncer.sync(revsPair{ctx.smap, msg}, revsPair{clone, msg})
	}
	glog.Infof("%s: metasync %s, %s", p.si, ctx.smap.StringEx(), clone.StringEx())
	wg.Wait()
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
	maxVerSmap, maxVerBMD := p.uncoverMeta(smap)
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
	smapUUID, sameUUID, sameVersion, eq := smap.Compare(&maxVerSmap.Smap)
	if !sameUUID {
		// FATAL: cluster integrity error (cie)
		cmn.ExitLogf("%s: split-brain uuid [%s %s] vs [%s %s]",
			ciError(10), p.si, smap.StringEx(), maxVerSmap.Primary, maxVerSmap.StringEx())
	}
	if eq && sameVersion {
		return
	}
	if maxVerSmap.Primary != nil && maxVerSmap.Primary.ID() != p.si.ID() {
		if maxVerSmap.version() > smap.version() {
			glog.Infof("%s: change-of-mind #2 %s <= max-ver %s", p.si, smap.StringEx(), maxVerSmap.StringEx())
			maxVerSmap.Pmap[p.si.ID()] = p.si
			p.owner.smap.put(maxVerSmap)
			return
		}
		// FATAL: cluster integrity error (cie)
		cmn.ExitLogf("%s: split-brain local [%s %s] vs [%s %s]",
			ciError(20), p.si, smap.StringEx(), maxVerSmap.Primary, maxVerSmap.StringEx())
	}
	p.owner.smap.Lock()
	clone := p.owner.smap.get().clone()
	if !eq {
		_, err := maxVerSmap.merge(clone, false /*err if detected (IP, port) duplicates*/)
		if err != nil {
			cmn.ExitLogf("%s: %v vs [%s: %s]", p.si, err, maxVerSmap.Primary, maxVerSmap.StringEx())
		}
	} else {
		clone.UUID = smapUUID
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
		l           = bcastSmap.Count()
		bmds        = make(map[*cluster.Snode]*bucketMD, l)
		smaps       = make(map[*cluster.Snode]*smapX, l)
		done, slowp bool
	)
	for {
		last := time.Now().After(deadline)
		maxVerSmap, maxVerBMD, done, slowp = p.bcastMaxVer(bcastSmap, bmds, smaps)
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

func (p *proxyrunner) bcastMaxVer(bcastSmap *smapX, bmds map[*cluster.Snode]*bucketMD,
	smaps map[*cluster.Snode]*smapX) (maxVerSmap *smapX, maxVerBMD *bucketMD, done, slowp bool) {
	var (
		borigin, sorigin string

		args = bcastArgs{
			req: cmn.ReqArgs{
				Path:  cmn.JoinWords(cmn.Version, cmn.Daemon),
				Query: url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmapVote}},
			},
			smap: bcastSmap,
			to:   cluster.AllNodes,
			fv:   func() interface{} { return &SmapVoteMsg{} },
		}
		results = p.bcastToGroup(args)
	)
	done = true
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
		svm := res.v.(*SmapVoteMsg)
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
			if svm.Smap.Primary != nil {
				s = " of the current one " + svm.Smap.Primary.ID()
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
	maxVerSmap, _, _, slowp := p.bcastMaxVer(smap, nil, nil)
	if maxVerSmap != nil && !slowp {
		if maxVerSmap.UUID == smap.UUID && maxVerSmap.version() > smap.version() {
			if maxVerSmap.Primary != nil && maxVerSmap.Primary.ID() != p.si.ID() {
				glog.Infof("%s: my %s is older than max-ver %s",
					p.si, smap.StringEx(), maxVerSmap.StringEx())
				return maxVerSmap
			}
		}
	}
	return nil
}

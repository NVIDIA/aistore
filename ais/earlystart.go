// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const minPidConfirmations = 3

const (
	metaction1 = "early-start-have-registrations"
	metaction2 = "primary-started-up"
	metaction3 = "primary-startup-resume-rebalance"
)

type (
	bmds  map[*cluster.Snode]*bucketMD
	smaps map[*cluster.Snode]*smapX
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
		cii             *clusterInfo
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
			var cnt int // confirmation count
			// double-check
			if cii, cnt = p.bcastHealth(smap); cii.Smap.Version > smap.version() {
				if cii.Smap.Primary.ID != p.si.ID() || cnt < minPidConfirmations {
					glog.Warningf("%s: cannot assume the primary role: local %s < v%d(%s, cnt=%d)",
						p.si, smap, cii.Smap.Version, cii.Smap.Primary.ID, cnt)
					primary = false
					primaryURL = cii.Smap.Primary.PubURL
				} else {
					glog.Warningf("%s: proceeding as primary even though local %s < v%d(%s, cnt=%d)",
						p.si, smap, cii.Smap.Version, cii.Smap.Primary.ID, cnt)
				}
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
			svm := p.uncoverMeta(smap)
			if svm.Smap != nil && svm.Smap.Primary != nil {
				glog.Infof("%s: second attempt  - joining via %s...", p.si, svm.Smap)
				err = p.secondaryStartup(svm.Smap)
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
	if status, err := p.joinCluster(primaryURLs...); err != nil {
		glog.Errorf("%s failed to join cluster (status: %d, err: %v)", p.si, status, err)
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
	var (
		smap          = newSmap()
		uuid, created string
		haveJoins     bool
	)

	// 1: init Smap to accept reg-s
	p.owner.smap.Lock()
	si := p.si.Clone()
	smap.Primary = si
	smap.addProxy(si)
	p.owner.smap.put(smap)
	p.owner.smap.Unlock()

	p.markNodeStarted()

	if !daemon.cli.skipStartup {
		maxVerSmap := p.acceptRegistrations(smap, loadedSmap, config, ntargets)
		if maxVerSmap != nil {
			if _, err := maxVerSmap.IsDuplicate(p.si); err != nil {
				cmn.ExitLogf("FATAL: %v", err)
			}
			maxVerSmap.Pmap[p.si.ID()] = p.si
			p.owner.smap.put(maxVerSmap)
			glog.Infof("%s: change-of-mind #1: registering with %s(%s)",
				p.si, maxVerSmap.Primary.ID(), maxVerSmap.Primary.URL(cmn.NetworkIntraControl))
			if err := p.secondaryStartup(maxVerSmap); err != nil {
				cmn.ExitLogf("FATAL: %v", err)
			}
			return
		}
	}

	smap = p.owner.smap.get()
	haveJoins = smap.CountTargets() > 0 || smap.CountProxies() > 1

	// 2: merging local => boot
	if haveJoins {
		p.owner.smap.Lock()
		var (
			added int
			clone = p.owner.smap.get().clone()
		)
		if loadedSmap != nil {
			added, _ = clone.merge(loadedSmap, true /*override (IP, port) duplicates*/)
			clone = loadedSmap
			if added > 0 {
				clone.Version = clone.Version + int64(added) + 1
			}
		}
		glog.Infof("%s: initial %s, curr %s, added=%d", p.si, loadedSmap, clone.StringEx(), added)
		if clone.UUID == "" {
			uuid, created = p.discoverClusterUUID()
			clone.UUID, clone.CreationTime = uuid, created
		}
		smap = clone
		p.owner.smap.put(clone)
		p.owner.smap.Unlock()

		var (
			bmd = p.owner.bmd.get()
			msg = p.newAisMsgStr(metaction1, smap, bmd)
		)
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
		if uuid == "" {
			clone.UUID, clone.CreationTime = newClusterUUID()
		} else {
			clone.UUID, clone.CreationTime = uuid, created
		}
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

	// 6. initialize BMD
	bmd := p.owner.bmd.get().clone()
	if bmd.Version == 0 {
		bmd.Version = 1 // init BMD
		bmd.UUID = smap.UUID
		p.owner.bmd.put(bmd)
	}

	// 6.5. mark RMD as starting up to prevent joining targets from triggering rebalance
	ok := p.owner.rmd.startup.CAS(false, true)
	debug.Assert(ok)

	// 7. initialize configMD
	conf := p.ensureConfPrimaryURL()
	if conf.Version == 0 {
		conf.Version = 1
		p.owner.conf.persist(conf)
	}

	// 8. metasync (smap & bmd) and startup as primary
	// TODO: add config revs
	var (
		aisMsg = p.newAisMsgStr(metaction2, smap, bmd)
		pairs  = []revsPair{{smap, aisMsg}, {bmd, aisMsg}}
	)
	wg := p.metasyncer.sync(pairs...)
	wg.Wait()
	p.markClusterStarted()
	glog.Infof("%s: primary & cluster startup complete (%s, %s)", p.si, smap.StringEx(), bmd.StringEx())

	// Clear regpool
	p.reg.mtx.Lock()
	p.reg.pool = nil
	p.reg.mtx.Unlock()

	// 9. resume rebalance if needed
	if config.Rebalance.Enabled {
		p.resumeReb(smap, config)
	}
	p.owner.rmd.startup.Store(false)
}

// resume rebalance if needed
func (p *proxyrunner) resumeReb(smap *smapX, config *cmn.Config) {
	var (
		ver     = smap.version()
		nojoins = config.Timeout.MaxHostBusy // initial quiet time with no new joins
		sleep   = cmn.CalcProbeFreq(nojoins)
	)
	debug.AssertNoErr(smap.validate())
until:
	// until (last-Smap-update + nojoins)
	for elapsed := time.Duration(0); elapsed < nojoins; {
		time.Sleep(sleep)
		elapsed += sleep
		smap = p.owner.smap.get()
		if !smap.IsPrimary(p.si) {
			debug.AssertNoErr(newErrNotPrimary(p.si, smap))
			return
		}
		if smap.version() != ver {
			debug.Assert(smap.version() > ver)
			elapsed, nojoins = 0, cmn.MinDuration(nojoins+sleep, config.Timeout.Startup)
			ver = smap.version()
		}
	}
	debug.AssertNoErr(smap.validate())
	//
	// NOTE: under Smap lock to serialize vs node joins (see httpclupost)
	//
	p.owner.smap.Lock()
	if !p.owner.rmd.rebalance.CAS(true, false) {
		p.owner.smap.Unlock() // nothing to do
		return
	}
	smap = p.owner.smap.get()
	if smap.version() != ver {
		p.owner.smap.Unlock()
		goto until
	}
	if smap.CountActiveTargets() < 2 {
		err := &errNotEnoughTargets{p.si, smap, 2}
		cmn.ExitLogf("%s: cannot resume global rebalance - %v", p.si, err)
	}
	var (
		msg    = &cmn.ActionMsg{Action: cmn.ActRebalance, Value: metaction3}
		aisMsg = p.newAisMsg(msg, smap, nil)
		ctx    = &rmdModifier{
			pre: func(_ *rmdModifier, clone *rebMD) { clone.Version += 100 },
		}
	)
	rmd := p.owner.rmd.modify(ctx)
	wg := p.metasyncer.sync(revsPair{rmd, aisMsg})
	p.owner.rmd.startup.Store(false)
	p.owner.smap.Unlock()

	wg.Wait()
	glog.Errorf("%s: resuming global rebalance (%s, %s)", p.si, smap.StringEx(), rmd.String())
}

// maxVerSmap != nil iff there's a primary change _and_ the cluster has moved on
func (p *proxyrunner) acceptRegistrations(smap, loadedSmap *smapX, config *cmn.Config,
	ntargets int) (maxVerSmap *smapX) {
	const quiescentIter = 4 // Number of iterations to consider the cluster quiescent.
	var (
		deadlineTime         = config.Timeout.Startup
		checkClusterInterval = deadlineTime / quiescentIter
		sleepDuration        = checkClusterInterval / 5

		definedTargetCnt = ntargets > 0
		doClusterCheck   = loadedSmap != nil && loadedSmap.CountTargets() != 0
	)
	for wait, iter := time.Duration(0), 0; wait < deadlineTime && iter < quiescentIter; wait += sleepDuration {
		time.Sleep(sleepDuration)
		// Check the cluster Smap only once at max.
		if doClusterCheck && wait >= checkClusterInterval {
			if bcastSmap := p.bcastMaxVerBestEffort(loadedSmap); bcastSmap != nil {
				maxVerSmap = bcastSmap
				return
			}
			doClusterCheck = false
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
		} else {
			iter++
		}
	}

	targetCnt := p.owner.smap.get().CountTargets()
	if definedTargetCnt {
		if targetCnt >= ntargets {
			glog.Infof("%s: reached expected %d targets (registered: %d)", p.si, ntargets, targetCnt)
		} else {
			glog.Warningf("%s: timed-out waiting for %d targets (registered: %d)",
				p.si, ntargets, targetCnt)
		}
	} else {
		glog.Infof("%s: registered %d new targets", p.si, targetCnt)
	}
	return
}

// the final major step in the primary startup sequence:
// discover cluster-wide metadata and resolve remaining conflicts
func (p *proxyrunner) discoverMeta(smap *smapX) {
	svm := p.uncoverMeta(smap)
	if svm.BMD != nil {
		p.owner.bmd.Lock()
		bmd := p.owner.bmd.get()
		if bmd == nil || bmd.version() < svm.BMD.version() {
			glog.Infof("%s: override local %s with %s", p.si, bmd, svm.BMD)
			p.owner.bmd.put(svm.BMD)
		}
		p.owner.bmd.Unlock()
	}
	if svm.RMD != nil {
		p.owner.rmd.Lock()
		rmd := p.owner.rmd.get()
		if rmd == nil || rmd.version() < svm.RMD.version() {
			glog.Infof("%s: override local %s with %s", p.si, rmd, svm.RMD)
			p.owner.rmd.put(svm.RMD)
		}
		p.owner.rmd.Unlock()
	}
	if svm.Smap == nil || svm.Smap.version() == 0 {
		glog.Infof("%s: no max-ver Smaps", p.si)
		return
	}
	glog.Infof("%s: local %s max-ver %s", p.si, smap.StringEx(), svm.Smap.StringEx())
	smapUUID, sameUUID, sameVersion, eq := smap.Compare(&svm.Smap.Smap)
	if !sameUUID {
		// FATAL: cluster integrity error (cie)
		cmn.ExitLogf("%s: split-brain uuid [%s %s] vs %s", ciError(10), p.si, smap.StringEx(), svm.Smap.StringEx())
	}
	if eq && sameVersion {
		return
	}
	if svm.Smap.Primary != nil && svm.Smap.Primary.ID() != p.si.ID() {
		if svm.Smap.version() > smap.version() {
			if dupNode, err := svm.Smap.IsDuplicate(p.si); err != nil {
				if !svm.Smap.IsPrimary(dupNode) {
					cmn.ExitLogf("FATAL: %v", err)
				}
				// If the primary in max-ver Smap version and current node only differ by `DaemonID`,
				// overwrite the proxy entry with current `Snode` and proceed to merging Smap.
				// TODO: Add validation to ensure `dupNode` and `p.si` only differ in `DaemonID`.
				svm.Smap.Primary = p.si
				svm.Smap.delProxy(dupNode.ID())
				svm.Smap.Pmap[p.si.ID()] = p.si
				goto merge
			}
			glog.Infof("%s: change-of-mind #2 %s <= max-ver %s", p.si, smap.StringEx(), svm.Smap.StringEx())
			svm.Smap.Pmap[p.si.ID()] = p.si
			p.owner.smap.put(svm.Smap)
			return
		}
		// FATAL: cluster integrity error (cie)
		cmn.ExitLogf("%s: split-brain local [%s %s] vs %s", ciError(20), p.si, smap.StringEx(), svm.Smap.StringEx())
	}
merge:
	p.owner.smap.Lock()
	clone := p.owner.smap.get().clone()
	if !eq {
		glog.Infof("%s: merge local %s <== %s", p.si, clone, svm.Smap)
		_, err := svm.Smap.merge(clone, false /*err if detected (IP, port) duplicates*/)
		if err != nil {
			cmn.ExitLogf("%s: %v vs %s", p.si, err, svm.Smap.StringEx())
		}
	} else {
		clone.UUID = smapUUID
	}
	clone.Version = cmn.MaxI64(clone.version(), svm.Smap.version()) + 1
	p.owner.smap.put(clone)
	p.owner.smap.Unlock()
	glog.Infof("%s: merged %s", p.si, clone.pp())
}

func (p *proxyrunner) uncoverMeta(bcastSmap *smapX) (svm SmapVoteMsg) {
	var (
		err         error
		suuid       string
		config      = cmn.GCO.Get()
		now         = time.Now()
		deadline    = now.Add(config.Timeout.Startup)
		l           = bcastSmap.Count()
		bmds        = make(bmds, l)
		smaps       = make(smaps, l)
		done, slowp bool
	)
	for {
		if daemon.stopping.Load() {
			svm.Smap = nil
			return
		}
		last := time.Now().After(deadline)
		svm, done, slowp = p.bcastMaxVer(bcastSmap, bmds, smaps)
		if done || last {
			break
		}
		time.Sleep(config.Timeout.CplaneOperation)
	}
	if !slowp {
		return
	}
	glog.Infof("%s: slow path...", p.si)
	if svm.BMD, err = resolveUUIDBMD(bmds); err != nil {
		if _, split := err.(*errBmdUUIDSplit); split {
			cmn.ExitLogf("FATAL: %s (primary), err: %v", p.si, err) // cluster integrity error
		}
		glog.Error(err)
	}
	for si, smap := range smaps {
		if !si.IsTarget() {
			continue
		}
		if !cmn.IsValidUUID(smap.UUID) {
			continue
		}
		if suuid == "" {
			suuid = smap.UUID
			if suuid != "" {
				glog.Infof("%s: set Smap UUID = %s(%s)", p.si, si, suuid)
			}
		} else if suuid != smap.UUID {
			// FATAL: cluster integrity error (cie)
			cmn.ExitLogf("%s: split-brain [%s %s] vs [%s %s]", ciError(30), p.si, suuid, si, smap.UUID)
		}
	}
	for _, smap := range smaps {
		if smap.UUID != suuid {
			continue
		}
		if svm.Smap == nil {
			svm.Smap = smap
		} else if svm.Smap.version() < smap.version() {
			svm.Smap = smap
		}
	}
	return
}

func (p *proxyrunner) bcastMaxVer(bcastSmap *smapX, bmds bmds, smaps smaps) (out SmapVoteMsg, done, slowp bool) {
	var (
		borigin, sorigin string
		args             = allocBcastArgs()
	)
	args.req = cmn.ReqArgs{
		Path:  cmn.URLPathDaemon.S,
		Query: url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmapVote}},
	}
	args.smap = bcastSmap
	args.to = cluster.AllNodes
	args.fv = func() interface{} { return &SmapVoteMsg{} }
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	done = true
	for k := range bmds {
		delete(bmds, k)
	}
	for k := range smaps {
		delete(smaps, k)
	}
	for _, res := range results {
		if res.err != nil {
			done = false
			continue
		}
		svm, ok := res.v.(*SmapVoteMsg)
		debug.Assert(ok)
		if svm.BMD != nil && svm.BMD.version() > 0 {
			if out.BMD == nil { // 1. init
				borigin, out.BMD = svm.BMD.UUID, svm.BMD
			} else if borigin != "" && borigin != svm.BMD.UUID { // 2. slow path
				slowp = true
			} else if !slowp && out.BMD.Version < svm.BMD.Version { // 3. fast path max(version)
				out.BMD = svm.BMD
				borigin = svm.BMD.UUID
			}
		}
		if svm.RMD != nil && svm.RMD.version() > 0 {
			if out.RMD == nil { // 1. init
				out.RMD = svm.RMD
			} else if !slowp && out.RMD.Version < svm.RMD.Version { // 3. fast path max(version)
				out.RMD = svm.RMD
			}
		}
		if svm.Smap != nil && svm.VoteInProgress {
			var s string
			if svm.Smap.Primary != nil {
				s = " of the current one " + svm.Smap.Primary.ID()
			}
			glog.Warningf("%s: starting up as primary(?) during reelection%s", p.si, s)
			out.Smap, out.BMD, out.RMD = nil, nil, nil // zero-out as unusable
			done = false
			break
		}
		if svm.Smap != nil && svm.Smap.version() > 0 {
			if out.Smap == nil { // 1. init
				sorigin, out.Smap = svm.Smap.UUID, svm.Smap
			} else if sorigin != "" && sorigin != svm.Smap.UUID { // 2. slow path
				slowp = true
			} else if !slowp && out.Smap.Version < svm.Smap.Version { // 3. fast path max(version)
				out.Smap = svm.Smap
				sorigin = svm.Smap.UUID
			}
		}
		if bmds != nil && svm.BMD != nil && svm.BMD.version() > 0 {
			bmds[res.si] = svm.BMD
		}
		if smaps != nil && svm.Smap != nil && svm.Smap.version() > 0 {
			smaps[res.si] = svm.Smap
		}
	}
	freeCallResults(results)
	return
}

func (p *proxyrunner) bcastMaxVerBestEffort(smap *smapX) *smapX {
	svm, _, slowp := p.bcastMaxVer(smap, nil, nil)
	if svm.Smap != nil && !slowp {
		if svm.Smap.UUID == smap.UUID && svm.Smap.version() > smap.version() && svm.Smap.validate() == nil {
			if svm.Smap.Primary.ID() != p.si.ID() {
				glog.Warningf("%s: primary change whereby local %s is older than max-ver %s",
					p.si, smap.StringEx(), svm.Smap.StringEx())
				return svm.Smap
			}
		}
	}
	return nil
}

func (p *proxyrunner) discoverClusterUUID() (uuid, created string) {
	p.reg.mtx.RLock()
	defer p.reg.mtx.RUnlock()
	if len(p.reg.pool) == 0 {
		return newClusterUUID()
	}
	var maxCnt int
	counter := make(map[string]int) // UUID => count
	for _, regReq := range p.reg.pool {
		if regReq.Smap == nil || !cmn.IsValidUUID(regReq.Smap.UUID) {
			continue
		}
		counter[regReq.Smap.UUID]++
		if maxCnt < counter[regReq.Smap.UUID] {
			maxCnt = counter[regReq.Smap.UUID]
			uuid, created = regReq.Smap.UUID, regReq.Smap.CreationTime
		}
	}

	if len(counter) > 1 {
		var uuids string
		for id, cnt := range counter {
			uuids += id + "(cnt-" + strconv.Itoa(cnt) + ") vs "
		}
		uuids = strings.TrimRight(uuids, "vs ")
		glog.Errorf("%s: smap UUIDs don't match %s", ciError(10), uuids)
	}

	if (maxCnt > 0 && len(counter) == 1) || maxCnt > minPidConfirmations {
		glog.Infof("%s: rediscovered cluster UUID %s at conf. count %d", p.si, uuid, maxCnt)
		return
	}
	return newClusterUUID()
}

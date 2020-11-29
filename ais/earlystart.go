// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const minPidConfirmations = 3

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
		config                = cmn.GCO.Get()
		pid                   string
		primaryURL, primaryID string
		primary               bool
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
			var (
				smapMaxVer int64
				cnt        int // confirmation count
			)
			// double-check
			if smapMaxVer, primaryURL, primaryID, cnt = p.bcastHealth(smap); smapMaxVer > smap.version() {
				if primaryID != p.si.ID() || cnt < minPidConfirmations {
					glog.Warningf("%s: cannot assume the primary role: local %s < v%d(%s, cnt=%d)",
						p.si, smap, smapMaxVer, primaryID, cnt)
					primary = false
				} else {
					glog.Warningf("%s: proceeding as primary even though local %s < v%d(%s, cnt=%d)",
						p.si, smap, smapMaxVer, primaryID, cnt)
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
		uuid, created    string
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
				p.si, maxVerSmap.Primary.ID(), maxVerSmap.Primary.URL(cmn.NetworkIntraControl))
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
		if smap.UUID == "" {
			uuid, created = newClusterUUID() // new uuid
			smap.UUID, smap.CreationTime = uuid, created
		}
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

	// 7. metasync and startup as primary
	var (
		aisMsg = p.newAisMsg(&cmn.ActionMsg{Value: metaction2}, smap, bmd)
		pairs  = []revsPair{{smap, aisMsg}, {bmd, aisMsg}}
	)

	// 8. bump up RMD (to take over) and resume rebalance if needed
	err := p.canStartRebalance()
	if err == nil && p.owner.rmd.rebalance.CAS(true, false) {
		glog.Infof("rebalance did not finish, restarting...")
		aisMsg.Action = cmn.ActRebalance
		ctx := &rmdModifier{
			pre: func(_ *rmdModifier, clone *rebMD) { clone.Version += 100 },
		}
		rmd := p.owner.rmd.modify(ctx)
		pairs = append(pairs, revsPair{rmd, aisMsg})
	}

	wg := p.metasyncer.sync(pairs...)
	wg.Wait()
	glog.Infof("%s: metasync %s, %s", p.si, smap.StringEx(), bmd.StringEx())

	glog.Infof("%s: primary & cluster startup complete", p.si)
	p.markClusterStarted()
}

// maxVerSmap != nil iff there's a primary change _and_ the cluster has moved on
func (p *proxyrunner) acceptRegistrations(smap, loadedSmap *smapX, config *cmn.Config, ntargets int) (maxVerSmap *smapX) {
	const (
		// Number of iteration to consider the cluster quiescent.
		quiescentIter = 4
	)
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
			glog.Infof("%s: change-of-mind #2 %s <= max-ver %s", p.si, smap.StringEx(), svm.Smap.StringEx())
			svm.Smap.Pmap[p.si.ID()] = p.si
			p.owner.smap.put(svm.Smap)
			return
		}
		// FATAL: cluster integrity error (cie)
		cmn.ExitLogf("%s: split-brain local [%s %s] vs %s", ciError(20), p.si, smap.StringEx(), svm.Smap.StringEx())
	}
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

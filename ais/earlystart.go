// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

const maxVerConfirmations = 3 // NOTE: minimum number of max-ver confirmations required to make the decision

const (
	metaction1 = "early-start-have-registrations"
	metaction2 = "primary-started-up"
	metaction3 = "primary-startup-resume-rebalance"
)

type (
	bmds  map[*meta.Snode]*bucketMD
	smaps map[*meta.Snode]*smapX
)

// Background:
//   - Each proxy/gateway stores a local copy of the cluster map (Smap)
//   - Each Smap instance is versioned; the versioning is monotonic (increasing)
//   - Only the primary (leader) proxy distributes Smap updates to all other clustered nodes
//   - Bootstrap sequence includes /steps/ intended to resolve all the usual conflicts that may arise.
func (p *proxy) bootstrap() {
	var (
		config          = cmn.GCO.Get()
		pid, primaryURL string
		cii             *clusterInfo
		primary         bool
	)

	// 1: load a local copy and try to utilize it for discovery
	smap, reliable := p.loadSmap()
	if !reliable {
		smap = nil
	} else {
		nlog.Infoln(p.String()+": loaded", smap.StringEx())
	}

	// 2. make the preliminary/primary decision
	pid, primary = p.determineRole(smap)

	// 3. primary?
	if primary {
		if pid != "" { // takes precedence over everything else
			cos.Assert(pid == p.SID())
		} else if reliable {
			var cnt int // confirmation count
			// double-check
			if cii, cnt = p.bcastHealth(smap, true /*checkAll*/); cii != nil && cii.Smap.Version > smap.version() {
				if cii.Smap.Primary.ID != p.SID() || cnt < maxVerConfirmations {
					nlog.Warningf("%s: cannot assume the primary role: local %s < v%d(%s, cnt=%d)",
						p.si, smap, cii.Smap.Version, cii.Smap.Primary.ID, cnt)
					primary = false
					primaryURL = cii.Smap.Primary.PubURL
				} else {
					nlog.Warningf("%s: proceeding as primary even though local %s < v%d(%s, cnt=%d)",
						p.si, smap, cii.Smap.Version, cii.Smap.Primary.ID, cnt)
				}
			}
		}
	}

	// 4.1: start as primary
	if primary {
		nlog.Infoln(p.String() + ": assuming primary role for now, starting up")
		go p.primaryStartup(smap, config, daemon.cli.primary.ntargets)
		return
	}

	// 4.2: otherwise, join as non-primary
	nlog.Infoln(p.String() + ": starting up as non-primary")
	err := p.secondaryStartup(smap, primaryURL)
	if err != nil {
		if reliable {
			svm := p.uncoverMeta(smap)
			if svm.Smap != nil && svm.Smap.Primary != nil {
				nlog.Infoln(p.String()+": second attempt - joining via", svm.Smap.String())
				err = p.secondaryStartup(svm.Smap)
			}
		}
	}
	if err != nil {
		cos.ExitLog(p.String(), "(non-primary) failed to join:", err)
	}
}

//   - make the *primary* decision taking into account both environment and
//     loaded Smap, if exists
//   - handle AIS_PRIMARY_ID (TODO: target)
//   - see also "change of mind"
func (p *proxy) determineRole(loadedSmap *smapX) (pid string, primary bool) {
	tag := "no Smap, "
	if loadedSmap != nil {
		loadedSmap.Pmap[p.SID()] = p.si
		tag = loadedSmap.StringEx() + ", "
	}
	// parse env
	envP := struct {
		pid     string
		primary bool
	}{
		pid:     os.Getenv(env.AIS.PrimaryID),
		primary: cos.IsParseBool(os.Getenv(env.AIS.IsPrimary)),
	}

	if envP.pid != "" && envP.primary && p.SID() != envP.pid {
		cos.ExitLogf("%s: invalid combination of %s=true & %s=%s", p, env.AIS.IsPrimary, env.AIS.PrimaryID, envP.pid)
	}
	nlog.Infof("%s: %sprimary-env=%+v", p, tag, envP)

	if loadedSmap != nil && envP.pid != "" {
		primary := loadedSmap.GetProxy(envP.pid)
		if primary == nil {
			nlog.Errorf(
				"%s: ignoring %s=%s - not found in the loaded %s",
				p.si, env.AIS.IsPrimary, envP.pid, loadedSmap,
			)
			envP.pid = ""
		} else if loadedSmap.Primary.ID() != envP.pid {
			nlog.Warningf(
				"%s: new %s=%s, previous %s",
				p.si, env.AIS.PrimaryID, envP.pid, loadedSmap.Primary,
			)
			loadedSmap.Primary = primary
		}
	}
	if envP.pid != "" {
		primary = envP.pid == p.SID()
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
func (p *proxy) secondaryStartup(smap *smapX, primaryURLs ...string) error {
	if smap == nil {
		smap = newSmap()
	} else if smap.Primary.ID() == p.SID() {
		nlog.Infof("%s: zeroing-out primary=self in %s", p, smap)
		smap.Primary = nil
	}
	p.owner.smap.put(smap)
	if status, err := p.joinCluster(apc.ActSelfJoinProxy, primaryURLs...); err != nil {
		nlog.Errorf("%s failed to join cluster: %v(%d)", p, err, status)
		return err
	}

	p.markNodeStarted()

	go func() {
		config := cmn.GCO.Get()
		cii := p.pollClusterStarted(config, smap.Primary)
		if daemon.stopping.Load() {
			return
		}
		if cii != nil {
			primary := cii.Smap.Primary
			if status, err := p.joinCluster(apc.ActSelfJoinProxy, primary.CtrlURL, primary.PubURL); err != nil {
				nlog.Errorf("%s failed to rejoin cluster: %v(%d)", p, err, status)
				return
			}
		}
		p.markClusterStarted()
	}()

	nlog.Infoln(p.String()+": joined as non-primary,", smap.StringEx())
	return nil
}

// Proxy/gateway that is, potentially, the leader of the cluster.
// It waits a configured time for other nodes to join,
// discovers cluster-wide metadata, and resolve remaining conflicts.
func (p *proxy) primaryStartup(loadedSmap *smapX, config *cmn.Config, ntargets int) {
	var (
		smap          = newSmap()
		uuid, created string
		haveJoins     bool
	)

	// 1: init Smap to accept reg-s
	p.owner.smap.mu.Lock()
	si := p.si.Clone()
	smap.Primary = si
	smap.addProxy(si)
	if loadedSmap != nil {
		smap.UUID = loadedSmap.UUID
	}
	p.owner.smap.put(smap)
	p.owner.smap.mu.Unlock()

	p.markNodeStarted()

	if !daemon.cli.primary.skipStartup {
		maxVerSmap := p.acceptRegistrations(smap, loadedSmap, config, ntargets)
		if maxVerSmap != nil {
			if _, err := maxVerSmap.IsDupNet(p.si); err != nil {
				cos.ExitLogf("%s: %v", cmn.BadSmapPrefix, err)
			}
			maxVerSmap.Pmap[p.SID()] = p.si
			p.owner.smap.put(maxVerSmap)
			nlog.Infof("%s: change-of-mind #1: registering with %s(%s)",
				p.si, maxVerSmap.Primary.ID(), maxVerSmap.Primary.URL(cmn.NetIntraControl))
			if err := p.secondaryStartup(maxVerSmap); err != nil {
				cos.ExitLogf("%s: %v", cmn.BadSmapPrefix, err)
			}
			return
		}
	}

	smap = p.owner.smap.get()
	haveJoins = smap.CountTargets() > 0 || smap.CountProxies() > 1

	// 2: merging local => boot
	if haveJoins {
		var (
			before, after cluMeta
			added         int
		)
		p.owner.smap.mu.Lock()
		clone := p.owner.smap.get().clone()
		if loadedSmap != nil {
			// TODO [feature]: when node re-joins with different network settings (see prxclu)
			added, _ = clone.merge(loadedSmap, true /*override (IP, port) duplicates*/)
			clone = loadedSmap
			if added > 0 {
				clone.Version = clone.Version + int64(added) + 1
			}
		}
		// NOTE: use regpool to try to upgrade all the four revs: Smap, BMD, RMD, and global Config
		before.Smap, before.BMD, before.RMD, before.EtlMD = clone, p.owner.bmd.get(), p.owner.rmd.get(), p.owner.etl.get()
		before.Config, _ = p.owner.config.get()

		p.reg.mu.RLock()
		smap = p.regpoolMaxVer(&before, &after)
		p.reg.mu.RUnlock()

		uuid, created = smap.UUID, smap.CreationTime
		p.owner.smap.put(smap)
		p.owner.smap.mu.Unlock()

		msg := p.newAmsgStr(metaction1, after.BMD)
		wg := p.metasyncer.sync(revsPair{smap, msg}, revsPair{after.BMD, msg})

		// before and after
		nlog.Infof("%s: loaded %s, merged %s, added %d", p, loadedSmap, before.Smap.StringEx(), added)
		nlog.Infof("before: %s, %s, %s, %s", before.BMD.StringEx(), before.RMD, before.Config, before.EtlMD)
		nlog.Infof("after: %s, %s, %s, %s, %s", smap.StringEx(), after.BMD.StringEx(), after.RMD, after.Config, after.EtlMD)
		wg.Wait()
	} else {
		nlog.Infoln(p.String() + ": no registrations yet")
		if loadedSmap != nil {
			nlog.Infoln(p.String()+": keep going w/ local", loadedSmap.StringEx())
			p.owner.smap.mu.Lock()
			smap = loadedSmap
			p.owner.smap.put(smap)
			p.owner.smap.mu.Unlock()
		}
	}

	// 3: discover cluster meta and resolve remaining conflicts, if any
	p.discoverMeta(smap)

	// 4: still primary?
	p.owner.smap.mu.Lock()
	smap = p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		p.owner.smap.mu.Unlock()
		nlog.Infoln(p.String()+": registering with primary", smap.Primary.StringEx())
		if err := p.secondaryStartup(smap); err != nil {
			cos.ExitLog(err)
		}
		return
	}

	// 5:  persist and finalize w/ sync + BMD
	if smap.UUID == "" {
		if !daemon.cli.primary.skipStartup && smap.CountTargets() == 0 {
			cos.ExitLog(p.String(), "cannot create cluster with no targets,", smap.StringEx())
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
	if l := smap.ICCount(); l < meta.DfltCountIC {
		clone := smap.clone()
		clone.staffIC()
		if l != clone.ICCount() {
			clone.Version++
			smap = clone
			p.owner.smap.put(smap)
		}
	}
	if err := p.owner.smap.persist(smap); err != nil {
		cos.ExitLog(p.String(), "(primary):", err)
	}
	p.owner.smap.mu.Unlock()

	// 6. initialize BMD
	bmd := p.owner.bmd.get().clone()
	if bmd.Version == 0 {
		bmd.Version = 1 // init BMD
		bmd.UUID = smap.UUID
		if err := p.owner.bmd.putPersist(bmd, nil); err != nil {
			cos.ExitLog(err)
		}
	}

	// 7. mark RMD as starting up to prevent joining targets from triggering rebalance
	ok := p.owner.rmd.starting.CAS(false, true)
	debug.Assert(ok)

	// 8. initialize etl
	etlMD := p.owner.etl.get().clone()
	if etlMD.Version > 0 {
		if err := p.owner.etl.putPersist(etlMD, nil); err != nil {
			nlog.Errorf("%s: failed to persist etl metadata, err %v - proceeding anyway...", p, err)
		}
	}

	// 9. cluster config: load existing _or_ initialize brand new v1
	cluConfig, err := p._cluConfig(smap.UUID)
	if err != nil {
		cos.ExitLog(err)
	}

	// 10. metasync (smap, config, etl & bmd) and startup as primary
	smap = p.owner.smap.get()
	var (
		aisMsg = p.newAmsgStr(metaction2, bmd)
		pairs  = []revsPair{{smap, aisMsg}, {bmd, aisMsg}, {cluConfig, aisMsg}}
	)
	wg := p.metasyncer.sync(pairs...)
	wg.Wait()
	p.markClusterStarted()
	nlog.Infoln(p.String(), "primary: cluster started up:", smap.StringEx(), bmd.StringEx())

	if etlMD.Version > 0 {
		_ = p.metasyncer.sync(revsPair{etlMD, aisMsg})
	}

	// 11. Clear regpool
	p.reg.mu.Lock()
	p.reg.pool = p.reg.pool[:0]
	p.reg.pool = nil
	p.reg.mu.Unlock()

	// 12. resume rebalance if needed
	if config.Rebalance.Enabled {
		p.resumeReb(smap, config)
	}
	p.owner.rmd.starting.Store(false)
}

func (p *proxy) _cluConfig(uuid string) (config *globalConfig, err error) {
	if p.owner.config.version() > 0 {
		return p.owner.config.get()
	}
	// Create version 1 and set primary URL.
	config, err = p.owner.config.modify(&configModifier{
		pre: func(ctx *configModifier, clone *globalConfig) (updated bool, err error) {
			debug.Assert(clone.version() == 0)
			clone.Proxy.PrimaryURL = p.si.URL(cmn.NetPublic)
			clone.UUID = uuid
			updated = true
			return
		},
	})
	return
}

// [cluster startup]: resume rebalance if `interrupted`
func (p *proxy) resumeReb(smap *smapX, config *cmn.Config) {
	debug.AssertNoErr(smap.validate())
	ver := smap.version()

	// initial quiet time
	nojoins := config.Timeout.MaxKeepalive.D()
	if p.owner.rmd.interrupted.Load() {
		nojoins = config.Timeout.MaxHostBusy.D()
	}
	sleep := cos.ProbingFrequency(nojoins)
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
			elapsed = 0
			nojoins = min(nojoins+sleep, config.Timeout.Startup.D())
			if p.owner.rmd.interrupted.Load() {
				nojoins = max(nojoins+sleep, config.Timeout.MaxHostBusy.D())
			}
			ver = smap.version()
		}
	}

	// NOTE: the remaining section under lock to serialize node joins (`httpclupost`)

	p.owner.smap.mu.Lock()
	if !p.owner.rmd.interrupted.CAS(true, false) {
		p.owner.smap.mu.Unlock() // nothing to do
		return
	}
	smap = p.owner.smap.get()
	if smap.version() != ver {
		p.owner.smap.mu.Unlock()
		goto until // repeat
	}

	// TODO: skip when there's a single target (benign)

	// do
	var (
		msg    = &apc.ActMsg{Action: apc.ActRebalance, Value: metaction3}
		aisMsg = p.newAmsg(msg, nil)
		ctx    = &rmdModifier{
			pre: func(_ *rmdModifier, clone *rebMD) { clone.Version += 100 },
		}
	)
	rmd, err := p.owner.rmd.modify(ctx)
	if err != nil {
		cos.ExitLog(err)
	}
	// NOTE: cannot rmdModifier.listen - p.notifs may not be initialized yet

	wg := p.metasyncer.sync(revsPair{rmd, aisMsg})

	p.owner.rmd.starting.Store(false) // done
	p.owner.smap.mu.Unlock()

	wg.Wait()
	nlog.Errorln("Warning: resumed global rebalance", ctx.rebID, smap.StringEx(), rmd.String())
}

// maxVerSmap != nil iff there's a primary change _and_ the cluster has moved on
func (p *proxy) acceptRegistrations(smap, loadedSmap *smapX, config *cmn.Config,
	ntargets int) (maxVerSmap *smapX) {
	const quiescentIter = 4 // Number of iterations to consider the cluster quiescent.
	var (
		deadlineTime         = config.Timeout.Startup.D()
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
		s := "target" + cos.Plural(ntargets)
		if targetCnt >= ntargets {
			nlog.Infof("%s: reached expected membership of %d %s (joined: %d)", p, ntargets, s, targetCnt)
		} else {
			nlog.Warningf("%s: timed out waiting for %d %s (joined: %d)", p, ntargets, s, targetCnt)
		}
	} else {
		s := "target" + cos.Plural(targetCnt)
		nlog.Infof("%s: joined %d new %s", p, targetCnt, s)
	}
	return
}

// the final major step in the primary startup sequence:
// discover cluster-wide metadata and resolve remaining conflicts
func (p *proxy) discoverMeta(smap *smapX) {
	svm := p.uncoverMeta(smap)
	if svm.BMD != nil {
		p.owner.bmd.Lock()
		bmd := p.owner.bmd.get()
		if bmd == nil || bmd.version() < svm.BMD.version() {
			nlog.Infoln(p.String()+"override local", bmd.String(), "with", svm.BMD.String())
			if err := p.owner.bmd.putPersist(svm.BMD, nil); err != nil {
				cos.ExitLog(err)
			}
		}
		p.owner.bmd.Unlock()
	}
	if svm.RMD != nil {
		p.owner.rmd.Lock()
		rmd := p.owner.rmd.get()
		if rmd == nil || rmd.version() < svm.RMD.version() {
			nlog.Infoln(p.String()+"override local", rmd.String(), "with", svm.RMD.String())
			p.owner.rmd.put(svm.RMD)
		}
		p.owner.rmd.Unlock()
	}

	if svm.Config != nil && svm.Config.UUID != "" {
		p.owner.config.Lock()
		config := cmn.GCO.Get()
		if config.Version < svm.Config.version() {
			if !cos.IsValidUUID(svm.Config.UUID) {
				debug.Assert(false, svm.Config.String())
				cos.ExitLogf("%s: invalid config UUID: %s", p, svm.Config)
			}
			if cos.IsValidUUID(config.UUID) && config.UUID != svm.Config.UUID {
				nlog.Errorf("Warning: configs have different UUIDs: (%s, %s) vs %s - proceeding anyway",
					p, config, svm.Config)
			} else {
				nlog.Infoln(p.String(), "override local", config.String(), "with", svm.Config.String())
			}
			cmn.GCO.Update(&svm.Config.ClusterConfig)
		}
		p.owner.config.Unlock()
	}

	if svm.Smap == nil || svm.Smap.version() == 0 {
		nlog.Infoln(p.String() + ": no max-ver Smaps")
		return
	}
	nlog.Infoln(p.String(), "local", smap.StringEx(), "max-ver", svm.Smap.StringEx())
	smapUUID, sameUUID, sameVersion, eq := smap.Compare(&svm.Smap.Smap)
	if !sameUUID {
		// FATAL: cluster integrity error (cie)
		cos.ExitLogf("%s: split-brain uuid [%s %s] vs %s", ciError(10), p, smap.StringEx(), svm.Smap.StringEx())
	}
	if eq && sameVersion {
		return
	}
	if svm.Smap.Primary != nil && svm.Smap.Primary.ID() != p.SID() {
		if svm.Smap.version() > smap.version() {
			if dupNode, err := svm.Smap.IsDupNet(p.si); err != nil {
				if !svm.Smap.IsPrimary(dupNode) {
					cos.ExitLog(err)
				}
				// If the primary in max-ver Smap version and current node only differ by `DaemonID`,
				// overwrite the proxy entry with current `Snode` and proceed to merging Smap.
				// TODO: Add validation to ensure `dupNode` and `p.si` only differ in `DaemonID`.
				svm.Smap.Primary = p.si
				svm.Smap.delProxy(dupNode.ID())
				svm.Smap.Pmap[p.SID()] = p.si
				goto merge
			}
			nlog.Infof("%s: change-of-mind #2 %s <= max-ver %s", p, smap.StringEx(), svm.Smap.StringEx())
			svm.Smap.Pmap[p.SID()] = p.si
			p.owner.smap.put(svm.Smap)
			return
		}
		// FATAL: cluster integrity error (cie)
		cos.ExitLogf("%s: split-brain local [%s %s] vs %s", ciError(20), p, smap.StringEx(), svm.Smap.StringEx())
	}
merge:
	p.owner.smap.mu.Lock()
	clone := p.owner.smap.get().clone()
	if !eq {
		nlog.Infof("%s: merge local %s <== %s", p, clone, svm.Smap)
		_, err := svm.Smap.merge(clone, false /*err if detected (IP, port) duplicates*/)
		if err != nil {
			cos.ExitLogf("%s: %v vs %s", p, err, svm.Smap.StringEx())
		}
	} else {
		clone.UUID = smapUUID
	}
	clone.Version = max(clone.version(), svm.Smap.version()) + 1
	p.owner.smap.put(clone)
	p.owner.smap.mu.Unlock()
	nlog.Infof("%s: merged %s", p, clone.pp())
}

func (p *proxy) uncoverMeta(bcastSmap *smapX) (svm cluMeta) {
	var (
		err         error
		suuid       string
		config      = cmn.GCO.Get()
		now         = time.Now()
		deadline    = now.Add(config.Timeout.Startup.D())
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
		time.Sleep(config.Timeout.CplaneOperation.D())
	}
	if !slowp {
		return
	}
	nlog.Infoln(p.String(), "(primary) slow path...")
	if svm.BMD, err = resolveUUIDBMD(bmds); err != nil {
		if _, split := err.(*errBmdUUIDSplit); split {
			cos.ExitLog(p.String(), "(primary), err:", err) // cluster integrity error
		}
		nlog.Errorln(err)
	}
	for si, smap := range smaps {
		if !si.IsTarget() {
			continue
		}
		if !cos.IsValidUUID(smap.UUID) {
			continue
		}
		if suuid == "" {
			suuid = smap.UUID
			if suuid != "" {
				nlog.Infof("%s: set Smap UUID = %s(%s)", p, si, suuid)
			}
		} else if suuid != smap.UUID {
			// FATAL: cluster integrity error (cie)
			cos.ExitLogf("%s: split-brain [%s %s] vs [%s %s]", ciError(30), p, suuid, si, smap.UUID)
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

func (p *proxy) bcastMaxVer(bcastSmap *smapX, bmds bmds, smaps smaps) (out cluMeta, done, slowp bool) {
	var (
		borigin, sorigin string
		args             = allocBcArgs()
	)
	args.req = cmn.HreqArgs{
		Path:  apc.URLPathDae.S,
		Query: url.Values{apc.QparamWhat: []string{apc.WhatSmapVote}},
	}
	args.smap = bcastSmap
	args.to = core.AllNodes
	args.cresv = cresCM{} // -> cluMeta
	results := p.bcastGroup(args)
	freeBcArgs(args)
	done = true

	clear(bmds)
	clear(smaps)

	for _, res := range results {
		if res.err != nil {
			done = false
			continue
		}
		svm, ok := res.v.(*cluMeta)
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
		if svm.Config != nil && svm.Config.version() > 0 {
			if out.Config == nil { // 1. init
				out.Config = svm.Config
			} else if !slowp && out.Config.version() < svm.Config.version() { // 3. fast path max(version)
				out.Config = svm.Config
			}
		}

		// TODO: maxver of EtlMD

		if svm.Smap != nil && svm.VoteInProgress {
			var s string
			if svm.Smap.Primary != nil {
				s = " of the current one " + svm.Smap.Primary.ID()
			}
			nlog.Warningln(p.String(), "starting up as primary(?) during reelection"+s)
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
	freeBcastRes(results)
	return
}

func (p *proxy) bcastMaxVerBestEffort(smap *smapX) *smapX {
	svm, _, slowp := p.bcastMaxVer(smap, nil, nil)
	if svm.Smap != nil && !slowp {
		if svm.Smap.UUID == smap.UUID && svm.Smap.version() > smap.version() && svm.Smap.validate() == nil {
			if svm.Smap.Primary.ID() != p.SID() {
				nlog.Warningln(p.String(), "detected primary change, whereby local", smap.StringEx(),
					"is older than max-ver", svm.Smap.StringEx())
				return svm.Smap
			}
		}
	}
	return nil
}

func (p *proxy) regpoolMaxVer(before, after *cluMeta) (smap *smapX) {
	*after = *before
	if len(p.reg.pool) == 0 {
		goto ret
	}
	for _, regReq := range p.reg.pool {
		if regReq.Smap != nil && regReq.Smap.version() > 0 && cos.IsValidUUID(regReq.Smap.UUID) {
			if after.Smap != nil && after.Smap.version() > 0 {
				if cos.IsValidUUID(after.Smap.UUID) && after.Smap.UUID != regReq.Smap.UUID {
					cos.ExitLogf("%s: Smap UUIDs don't match: [%s %s] vs %s", ciError(10),
						p, after.Smap.StringEx(), regReq.Smap.StringEx())
				}
			}
			if after.Smap == nil || after.Smap.version() < regReq.Smap.version() {
				after.Smap = regReq.Smap
			}
		}
		if regReq.BMD != nil && regReq.BMD.version() > 0 && cos.IsValidUUID(regReq.BMD.UUID) {
			if after.BMD != nil && after.BMD.version() > 0 {
				if cos.IsValidUUID(after.BMD.UUID) && after.BMD.UUID != regReq.BMD.UUID {
					cos.ExitLogf("%s: BMD UUIDs don't match: [%s %s] vs %s", ciError(10),
						p.si, after.BMD.StringEx(), regReq.BMD.StringEx())
				}
			}
			if after.BMD == nil || after.BMD.version() < regReq.BMD.version() {
				after.BMD = regReq.BMD
			}
		}
		if regReq.RMD != nil && regReq.RMD.version() > 0 {
			if after.RMD == nil || after.RMD.version() < regReq.RMD.version() {
				after.RMD = regReq.RMD
			}
		}
		if regReq.Config != nil && regReq.Config.version() > 0 && cos.IsValidUUID(regReq.Config.UUID) {
			if after.Config != nil && after.Config.version() > 0 {
				if cos.IsValidUUID(after.Config.UUID) && after.Config.UUID != regReq.Config.UUID {
					cos.ExitLogf("%s: Global Config UUIDs don't match: [%s %s] vs %s", ciError(10),
						p.si, after.Config, regReq.Config)
				}
			}
			if after.Config == nil || after.Config.version() < regReq.Config.version() {
				after.Config = regReq.Config
			}
		}
	}
	if after.BMD != before.BMD {
		if err := p.owner.bmd.putPersist(after.BMD, nil); err != nil {
			cos.ExitLog(err)
		}
	}
	if after.RMD != before.RMD {
		p.owner.rmd.put(after.RMD)
	}
	if after.Config != before.Config {
		var err error
		after.Config, err = p.owner.config.modify(&configModifier{
			pre: func(ctx *configModifier, clone *globalConfig) (updated bool, err error) {
				*clone = *after.Config
				updated = true
				return
			},
		})
		if err != nil {
			cos.ExitLog(err)
		}
	}

ret:
	if after.Smap.version() == 0 || !cos.IsValidUUID(after.Smap.UUID) {
		after.Smap.UUID, after.Smap.CreationTime = newClusterUUID()
		nlog.Infoln(p.String(), "new cluster UUID:", after.Smap.UUID)
		return after.Smap
	}
	if before.Smap == after.Smap {
		return after.Smap
	}

	debug.Assert(before.Smap.version() < after.Smap.version())
	// not interfering with elections
	if after.VoteInProgress {
		nlog.Errorln(p.String()+" primary differ:", before.Smap.StringEx(), "vs. newer", after.Smap.StringEx(), "(voting = YES)")
		before.Smap.UUID, before.Smap.CreationTime = after.Smap.UUID, after.Smap.CreationTime
		return before.Smap
	}
	// trusting & taking over (have joins)
	var err error
	nlog.Warningf("%s: primary differ: %s vs. newer %s - taking over as PRIMARY...", p,
		before.Smap.StringEx(), after.Smap.StringEx())
	clone := after.Smap.clone()
	clone.Primary = p.si
	clone.Pmap[p.SID()] = p.si
	clone.Version += 100
	after.Config, err = p.owner.config.modify(&configModifier{
		pre: func(ctx *configModifier, clone *globalConfig) (updated bool, err error) {
			clone.Proxy.PrimaryURL = p.si.URL(cmn.NetPublic)
			clone.Version++
			updated = true
			return
		},
	})
	if err != nil {
		cos.ExitLog(err)
	}
	return clone
}

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
//
// extended action aka xaction
//
package ais

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/mirror"
)

type (
	xactions struct {
		sync.Mutex
		v       []cmn.Xact
		nextid  int64
		cleanup bool
	}
	xactRebBase struct {
		cmn.XactBase
		runnerCnt int
		confirmCh chan struct{}
	}
	xactGlobalReb struct {
		xactRebBase
		smapVersion int64 // smap version on which this rebalance has started
	}
	xactLocalReb struct {
		xactRebBase
	}
	xactLRU struct {
		cmn.XactBase
	}
	xactPrefetch struct {
		cmn.XactBase
	}
	xactEvictDelete struct {
		cmn.XactBase
	}
	xactElection struct {
		cmn.XactBase
		proxyrunner *proxyrunner
		vr          *VoteRecord
	}
	xactRechecksum struct {
		cmn.XactBase
		bucket string
	}
)

func makeXactRebBase(id int64, rebType int, runnerCnt int) xactRebBase {
	kind := ""
	switch rebType {
	case localRebType:
		kind = cmn.ActLocalReb
	case globalRebType:
		kind = cmn.ActGlobalReb
	default:
		cmn.AssertMsg(false, fmt.Sprintf("unknown rebalance type: %d", rebType))
	}

	return xactRebBase{
		XactBase:  *cmn.NewXactBase(id, kind),
		runnerCnt: runnerCnt,
		confirmCh: make(chan struct{}, runnerCnt),
	}
}

//===================
//
// xactions
//

func newXs() *xactions {
	xs := &xactions{v: make([]cmn.Xact, 0, 32)}
	xs.nextid = time.Now().UTC().UnixNano() & 0xffff
	return xs
}

func (xs *xactions) uniqueid() int64   { xs.nextid++; return xs.nextid } // under lock
func (xs *xactions) add(xact cmn.Xact) { xs.v = append(xs.v, xact) }     // ditto

func (xs *xactions) findU(kind string) (xact cmn.Xact) {
	if xs.cleanup {
		// TODO: keep longer, e.g. until creation of same kind
		xs._cleanup()
	}
	for _, x := range xs.v {
		if x.Finished() {
			xs.cleanup = true
			continue
		}
		if x.Kind() == kind {
			xact = x
			return
		}
	}
	return
}

func (xs *xactions) _cleanup() {
	for i := len(xs.v) - 1; i >= 0; i-- {
		x := xs.v[i]
		if x.Finished() {
			xs.delAt(i)
		}
	}
	xs.cleanup = false
}

func (xs *xactions) findL(kind string) (idx int, xact cmn.Xact) {
	xs.Lock()
	xact = xs.findU(kind)
	xs.Unlock()
	return
}

func (xs *xactions) selectL(kind string) (matching []cmn.Xact) {
	xs.Lock()
	for _, x := range xs.v {
		if x.Kind() == kind || path.Dir(x.Kind()) == kind {
			if matching == nil {
				matching = make([]cmn.Xact, 0, 4)
			}
			matching = append(matching, x) // NOTE: may include Finished()
		}
	}
	xs.Unlock()
	return
}

func (xs *xactions) delAt(k int) {
	l := len(xs.v)
	if k < l-1 {
		copy(xs.v[k:], xs.v[k+1:])
	}
	xs.v[l-1] = nil
	xs.v = xs.v[:l-1]
}

func (xs *xactions) renewGlobalReb(smapVersion int64, runnerCnt int) *xactRebBase {
	xs.Lock()
	xx := xs.findU(cmn.ActGlobalReb)
	if xx != nil {
		xGlobalReb := xx.(*xactGlobalReb)
		if xGlobalReb.smapVersion > smapVersion {
			glog.Errorf("(reb: %s) %d version is greater than smapVersion %d", xGlobalReb, xGlobalReb.smapVersion, smapVersion)
			xs.Unlock()
			return nil
		}
		if xGlobalReb.smapVersion == smapVersion {
			glog.Infof("%s already running, nothing to do", xGlobalReb)
			xs.Unlock()
			return nil
		}
		xGlobalReb.Abort()
		for i := 0; i < xGlobalReb.runnerCnt; i++ {
			<-xGlobalReb.confirmCh
		}
		close(xGlobalReb.confirmCh)
	}
	id := xs.uniqueid()
	xGlobalReb := &xactGlobalReb{
		xactRebBase: makeXactRebBase(id, globalRebType, runnerCnt),
		smapVersion: smapVersion,
	}
	xs.add(xGlobalReb)
	xs.Unlock()
	return &xGlobalReb.xactRebBase
}

func (xs *xactions) globalRebStatus() (aborted, running bool) {
	pmarker := persistentMarker(globalRebType)
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	xs.Lock()
	xx := xs.findU(cmn.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactGlobalReb)
		if !xreb.Finished() {
			running = true
		}
	}
	xs.Unlock()
	return
}

func (xs *xactions) localRebStatus() (aborted, running bool) {
	pmarker := persistentMarker(localRebType)
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	xs.Lock()
	xx := xs.findU(cmn.ActLocalReb)
	if xx != nil {
		xreb := xx.(*xactLocalReb)
		if !xreb.Finished() {
			running = true
		}
	}
	xs.Unlock()
	return
}

func (xs *xactions) renewLocalReb(runnerCnt int) *xactRebBase {
	xs.Lock()
	xx := xs.findU(cmn.ActLocalReb)
	if xx != nil {
		xLocalReb := xx.(*xactLocalReb)
		xLocalReb.Abort()
		for i := 0; i < xLocalReb.runnerCnt; i++ {
			<-xLocalReb.confirmCh
		}
		close(xLocalReb.confirmCh)
	}
	id := xs.uniqueid()
	xLocalReb := &xactLocalReb{
		xactRebBase: makeXactRebBase(id, localRebType, runnerCnt),
	}
	xs.add(xLocalReb)
	xs.Unlock()
	return &xLocalReb.xactRebBase
}

func (xs *xactions) renewLRU() *xactLRU {
	xs.Lock()
	xx := xs.findU(cmn.ActLRU)
	if xx != nil {
		xlru := xx.(*xactLRU)
		glog.Infof("%s already running, nothing to do", xlru)
		xs.Unlock()
		return nil
	}
	id := xs.uniqueid()
	xlru := &xactLRU{XactBase: *cmn.NewXactBase(id, cmn.ActLRU)}
	xs.add(xlru)
	xs.Unlock()
	return xlru
}

func (xs *xactions) renewElection(p *proxyrunner, vr *VoteRecord) *xactElection {
	xs.Lock()
	xx := xs.findU(cmn.ActElection)
	if xx != nil {
		xele := xx.(*xactElection)
		glog.Infof("%s already running, nothing to do", xele)
		xs.Unlock()
		return nil
	}
	id := xs.uniqueid()
	xele := &xactElection{
		XactBase:    *cmn.NewXactBase(id, cmn.ActElection),
		proxyrunner: p,
		vr:          vr,
	}
	xs.add(xele)
	xs.Unlock()
	return xele
}

func (xs *xactions) newEvictDelete(evict bool) *xactEvictDelete {
	xs.Lock()
	defer xs.Unlock()

	xact := cmn.ActDelete
	if evict {
		xact = cmn.ActEvictObjects
	}

	id := xs.uniqueid()
	xdel := &xactEvictDelete{XactBase: *cmn.NewXactBase(id, xact)}
	xs.add(xdel)
	return xdel
}

func (xs *xactions) renewPrefetch() *xactPrefetch {
	xs.Lock()
	defer xs.Unlock()
	xx := xs.findU(cmn.ActPrefetch)
	if xx != nil {
		xpre := xx.(*xactPrefetch)
		glog.Infof("%s already running, nothing to do", xpre)
		return nil
	}
	id := xs.uniqueid()
	xpre := &xactPrefetch{XactBase: *cmn.NewXactBase(id, cmn.ActPrefetch)}
	xs.add(xpre)
	return xpre
}

func (xs *xactions) renewRechecksum(bucket string) *xactRechecksum {
	kind := path.Join(cmn.ActRechecksum, bucket)
	xs.Lock()
	xx := xs.findU(kind)
	if xx != nil {
		xrcksum := xx.(*xactRechecksum)
		xs.Unlock()
		glog.Infof("%s already running for bucket %s, nothing to do", xrcksum, bucket)
		return nil
	}
	id := xs.uniqueid()
	xrcksum := &xactRechecksum{XactBase: *cmn.NewXactBase(id, kind), bucket: bucket}
	xs.add(xrcksum)
	xs.Unlock()
	return xrcksum
}

func (xs *xactions) renewPutCopies(lom *cluster.LOM, t *targetrunner) (xcopy *mirror.XactCopy) {
	kindput := path.Join(cmn.ActPutCopies, lom.Bucket)
	xs.Lock()
	xx := xs.findU(kindput)
	if xx != nil {
		xcopy = xx.(*mirror.XactCopy)
		xcopy.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
		xs.Unlock()
		return
	}
	kinderase := path.Join(cmn.ActEraseCopies, lom.Bucket)
	xx = xs.findU(kinderase)
	if xx != nil {
		xerase := xx.(*mirror.XactErase)
		if !xerase.Finished() {
			glog.Errorf("cannot start '%s' xaction when %s is running", cmn.ActPutCopies, xx)
			xs.Unlock()
			return nil
		}
	}
	// construct new
	id := xs.uniqueid()
	base := cmn.NewXactDemandBase(id, kindput, lom.Bucket)
	slab := gmem2.SelectSlab2(cmn.MiB) // FIXME: estimate
	xcopy = &mirror.XactCopy{
		XactDemandBase: *base,
		Slab:           slab,
		Mirror:         *lom.MirrorConf,
		T:              t,
		Namelocker:     t.rtnamemap,
		BckIsLocal:     lom.BckIsLocal,
	}
	if err := xcopy.InitAndRun(); err != nil {
		glog.Errorln(err)
		xcopy = nil
	} else {
		xs.add(xcopy)
	}
	xs.Unlock()
	return
}

func (xs *xactions) abortPutCopies(bucket string) {
	kindput := path.Join(cmn.ActPutCopies, bucket)
	xs.Lock()
	xx := xs.findU(kindput)
	if xx != nil {
		xx.Abort()
	}
	xs.Unlock()
}

func (xs *xactions) renewEraseCopies(bucket string, t *targetrunner, bckIsLocal bool) {
	kinderase := path.Join(cmn.ActEraseCopies, bucket)
	xs.Lock()
	xx := xs.findU(kinderase)
	if xx != nil && !xx.Finished() {
		glog.Infof("nothing to do: %s", xx)
		xs.Unlock()
		return
	}
	id := xs.uniqueid()
	base := cmn.NewXactBase(id, kinderase, bucket)
	xerase := &mirror.XactErase{
		XactBase:   *base,
		T:          t,
		Namelocker: t.rtnamemap,
		BckIsLocal: bckIsLocal,
	}
	xs.add(xerase)
	go xerase.Run()
	xs.Unlock()
}

// PutCopies, EraseCopies and EC as those are currently the only bucket-specific xaction we may have
func (xs *xactions) abortBucketSpecific(bucket string) {
	var (
		bucketSpecific = []string{cmn.ActPutCopies, cmn.ActEraseCopies, cmn.ActEC}
		wg             = &sync.WaitGroup{}
	)
	for _, act := range bucketSpecific {
		k := path.Join(act, bucket)
		wg.Add(1)
		go func(kind string, wg *sync.WaitGroup) {
			defer wg.Done()
			xs.Lock()
			xx := xs.findU(kind)
			xs.Unlock()
			if xx == nil {
				return
			}
			xx.Abort()
			for i := 0; i < 5; i++ {
				time.Sleep(time.Millisecond * 500)
				if xx.Finished() {
					return
				}
			}
			glog.Errorf("%s: timed-out waiting for termination", xx)
		}(k, wg)
	}
	wg.Wait()
}

func (xs *xactions) renewDownloader(t *targetrunner) (xdl *downloader.Downloader, err error) {
	kind := cmn.Download
	xs.Lock()
	xx := xs.findU(kind)
	if xx != nil {
		xdl = xx.(*downloader.Downloader)
		xdl.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
		xs.Unlock()
		return
	}
	id := xs.uniqueid()
	xdl, err = downloader.NewDownloader(t, t.statsif, fs.Mountpaths, id, kind)
	if err != nil {
		return nil, err
	}
	xs.add(xdl)
	go xdl.Run()
	xs.Unlock()
	return
}

func (xs *xactions) abortAll() (sleep bool) {
	xs.Lock()
	for _, xact := range xs.v {
		if !xact.Finished() {
			xact.Abort()
			sleep = true
		}
	}
	xs.Unlock()
	return
}

func (xs *xactions) renewEC(bucket string) *ec.XactEC {
	kind := path.Join(cmn.ActEC, bucket)
	xs.Lock()
	xx := xs.findU(kind)
	if xx != nil {
		xec := xx.(*ec.XactEC)
		xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
		glog.Infof("%s already running, nothing to do", xec)
		xs.Unlock()
		return xec
	}

	id := xs.uniqueid()
	xec := ECM.newXact(bucket)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, kind, bucket)
	go xec.Run()
	xs.add(xec)
	xs.Unlock()
	return xec
}

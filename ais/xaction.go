// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
//
// extended action aka xaction
//
package ais

import (
	"os"
	"path"
	"path/filepath"
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
	xactRebalance struct {
		cmn.XactBase
		curversion int64
		runnerCnt  int
		confirmCh  chan struct{}
	}
	xactLocalRebalance struct {
		cmn.XactBase
		runnerCnt int
		confirmCh chan struct{}
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

func (xs *xactions) renewRebalance(curversion int64, runnerCnt int) *xactRebalance {
	xs.Lock()
	xx := xs.findU(cmn.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactRebalance)
		if xreb.curversion > curversion {
			glog.Errorf("%s version is greater than curversion %d", xreb, curversion)
			xs.Unlock()
			return nil
		}
		if xreb.curversion == curversion {
			glog.Infof("%s already running, nothing to do", xreb)
			xs.Unlock()
			return nil
		}
		xreb.Abort()
		for i := 0; i < xreb.runnerCnt; i++ {
			<-xreb.confirmCh
		}
		close(xreb.confirmCh)
	}
	id := xs.uniqueid()
	xreb := &xactRebalance{
		XactBase:   *cmn.NewXactBase(id, cmn.ActGlobalReb),
		curversion: curversion,
		runnerCnt:  runnerCnt,
		confirmCh:  make(chan struct{}, runnerCnt),
	}
	xs.add(xreb)
	xs.Unlock()
	return xreb
}

// persistent mark indicating rebalancing in progress
func (xs *xactions) rebalanceInProgress() (pmarker string) {
	return filepath.Join(cmn.GCO.Get().Confdir, cmn.RebalanceMarker)
}

// persistent mark indicating rebalancing in progress
func (xs *xactions) localRebalanceInProgress() (pmarker string) {
	return filepath.Join(cmn.GCO.Get().Confdir, cmn.LocalRebalanceMarker)
}

func (xs *xactions) isAbortedOrRunningRebalance() (aborted, running bool) {
	pmarker := xs.rebalanceInProgress()
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	xs.Lock()
	xx := xs.findU(cmn.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactRebalance)
		if !xreb.Finished() {
			running = true
		}
	}
	xs.Unlock()
	return
}

func (xs *xactions) isAbortedOrRunningLocalRebalance() (aborted, running bool) {
	pmarker := xs.localRebalanceInProgress()
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	xs.Lock()
	xx := xs.findU(cmn.ActLocalReb)
	if xx != nil {
		running = true
	}
	xs.Unlock()
	return
}

func (xs *xactions) renewLocalRebalance(runnerCnt int) *xactLocalRebalance {
	xs.Lock()
	xx := xs.findU(cmn.ActLocalReb)
	if xx != nil {
		xLocalReb := xx.(*xactLocalRebalance)
		xLocalReb.Abort()
		for i := 0; i < xLocalReb.runnerCnt; i++ {
			<-xLocalReb.confirmCh
		}
		close(xLocalReb.confirmCh)
	}
	id := xs.uniqueid()
	xLocalReb := &xactLocalRebalance{
		XactBase:  *cmn.NewXactBase(id, cmn.ActLocalReb),
		runnerCnt: runnerCnt,
		confirmCh: make(chan struct{}, runnerCnt),
	}
	xs.add(xLocalReb)
	xs.Unlock()
	return xLocalReb
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
		Mirror:         *lom.Mirror,
		T:              t,
		Namelocker:     t.rtnamemap,
		Bislocal:       lom.Bislocal,
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

func (xs *xactions) renewEraseCopies(bucket string, t *targetrunner, islocal bool) {
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
		Bislocal:   islocal,
	}
	xs.add(xerase)
	go xerase.Run()
	xs.Unlock()
}

// PutCopies and EraseCopies as those are currently the only bucket-specific xaction we may have
func (xs *xactions) abortBucketSpecific(bucket string) {
	xs.Lock()
	defer xs.Unlock()
	var (
		bucketSpecific = []string{cmn.ActPutCopies, cmn.ActEraseCopies}
		wg             = &sync.WaitGroup{}
	)
	for _, act := range bucketSpecific {
		k := path.Join(act, bucket)
		wg.Add(1)
		go func(kind string, wg *sync.WaitGroup) {
			defer wg.Done()
			xx := xs.findU(kind)
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
		wg.Wait()
	}
}

func (xs *xactions) renewDownloader(t *targetrunner, bucket string) (xdl *downloader.Downloader) {
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
	xdl = downloader.NewDownloader(t, t.statsif, fs.Mountpaths, id, kind, bucket)
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

func (xs *xactions) renewEC() *ec.XactEC {
	kind := cmn.ActEC
	xs.Lock()
	xx := xs.findU(cmn.ActEC)
	if xx != nil {
		xec := xx.(*ec.XactEC)
		xec.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
		glog.Infof("%s already running, nothing to do", xec)
		xs.Unlock()
		return xec
	}

	id := xs.uniqueid()
	xec := ECM.newXact()
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, kind, "" /* all buckets */)
	go xec.Run()
	xs.add(xec)
	xs.Unlock()
	return xec
}

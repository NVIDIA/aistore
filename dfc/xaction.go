// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
//
// extended action aka xaction
//
package dfc

import (
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/mirror"
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
		xact = cmn.ActEvict
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

func (xs *xactions) renewPutLR(bucket string, bislocal bool) (xputlr *mirror.XactPut) {
	kind := path.Join(cmn.ActPutLR, bucket)
	xs.Lock()
	xx := xs.findU(kind)
	if xx != nil {
		xputlr = xx.(*mirror.XactPut)
		xputlr.Renew() // to reduce (but not totally eliminate) the race btw self-termination and renewal
		xs.Unlock()
		return
	}
	id := xs.uniqueid()
	xputlr = &mirror.XactPut{XactDemandBase: *cmn.NewXactDemandBase(id, kind), Bucket: bucket, Bislocal: bislocal}
	xs.add(xputlr)
	xputlr.Run()
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

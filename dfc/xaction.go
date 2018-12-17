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
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
)

type (
	xactInProgress struct {
		xactinp []cmn.XactInterface
		lock    *sync.Mutex
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
// xactInProgress
//

func newxactinp() *xactInProgress {
	q := make([]cmn.XactInterface, 4)
	qq := &xactInProgress{xactinp: q[0:0]}
	qq.nextid = time.Now().UTC().UnixNano() & 0xffff
	qq.lock = &sync.Mutex{}
	return qq
}

func (q *xactInProgress) uniqueid() int64            { return atomic.AddInt64(&q.nextid, 1) }
func (q *xactInProgress) add(xact cmn.XactInterface) { q.xactinp = append(q.xactinp, xact) }

func (q *xactInProgress) findU(kind string) (idx int, xact cmn.XactInterface) {
	if q.cleanup {
		// TODO: keep longer, e.g. until creation of same kind
		q._cleanup()
	}
	idx = -1
	for i, x := range q.xactinp {
		if x.Finished() {
			q.cleanup = true
			continue
		}
		if x.Kind() == kind {
			idx, xact = i, x
			return
		}
	}
	return
}

func (q *xactInProgress) _cleanup() {
	for i := len(q.xactinp) - 1; i >= 0; i-- {
		x := q.xactinp[i]
		if x.Finished() {
			q.delAt(i)
		}
	}
	q.cleanup = false
}

func (q *xactInProgress) findL(kind string) (idx int, xact cmn.XactInterface) {
	q.lock.Lock()
	idx, xact = q.findU(kind)
	q.lock.Unlock()
	return
}

func (q *xactInProgress) delAt(k int) {
	l := len(q.xactinp)
	if k < l-1 {
		copy(q.xactinp[k:], q.xactinp[k+1:])
	}
	q.xactinp[l-1] = nil
	q.xactinp = q.xactinp[:l-1]
}

func (q *xactInProgress) renewRebalance(curversion int64, runnerCnt int) *xactRebalance {
	q.lock.Lock()
	_, xx := q.findU(cmn.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactRebalance)
		if xreb.curversion > curversion {
			glog.Errorf("%s version is greater than curversion %d", xreb, curversion)
			q.lock.Unlock()
			return nil
		}
		if xreb.curversion == curversion {
			glog.Infof("%s already running, nothing to do", xreb)
			q.lock.Unlock()
			return nil
		}
		xreb.Abort()
		for i := 0; i < xreb.runnerCnt; i++ {
			<-xreb.confirmCh
		}
		close(xreb.confirmCh)
	}
	id := q.uniqueid()
	xreb := &xactRebalance{
		XactBase:   *cmn.NewXactBase(id, cmn.ActGlobalReb),
		curversion: curversion,
		runnerCnt:  runnerCnt,
		confirmCh:  make(chan struct{}, runnerCnt),
	}
	q.add(xreb)
	q.lock.Unlock()
	return xreb
}

// persistent mark indicating rebalancing in progress
func (q *xactInProgress) rebalanceInProgress() (pmarker string) {
	return filepath.Join(cmn.GCO.Get().Confdir, cmn.RebalanceMarker)
}

// persistent mark indicating rebalancing in progress
func (q *xactInProgress) localRebalanceInProgress() (pmarker string) {
	return filepath.Join(cmn.GCO.Get().Confdir, cmn.LocalRebalanceMarker)
}

func (q *xactInProgress) isAbortedOrRunningRebalance() (aborted, running bool) {
	pmarker := q.rebalanceInProgress()
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	q.lock.Lock()
	_, xx := q.findU(cmn.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactRebalance)
		if !xreb.Finished() {
			running = true
		}
	}
	q.lock.Unlock()
	return
}

func (q *xactInProgress) isAbortedOrRunningLocalRebalance() (aborted, running bool) {
	pmarker := q.localRebalanceInProgress()
	_, err := os.Stat(pmarker)
	if err == nil {
		aborted = true
	}

	q.lock.Lock()
	_, xx := q.findU(cmn.ActLocalReb)
	if xx != nil {
		running = true
	}
	q.lock.Unlock()
	return
}

func (q *xactInProgress) renewLocalRebalance(runnerCnt int) *xactLocalRebalance {
	q.lock.Lock()
	_, xx := q.findU(cmn.ActLocalReb)
	if xx != nil {
		xLocalReb := xx.(*xactLocalRebalance)
		xLocalReb.Abort()
		for i := 0; i < xLocalReb.runnerCnt; i++ {
			<-xLocalReb.confirmCh
		}
		close(xLocalReb.confirmCh)
	}
	id := q.uniqueid()
	xLocalReb := &xactLocalRebalance{
		XactBase:  *cmn.NewXactBase(id, cmn.ActLocalReb),
		runnerCnt: runnerCnt,
		confirmCh: make(chan struct{}, runnerCnt),
	}
	q.add(xLocalReb)
	q.lock.Unlock()
	return xLocalReb
}

func (q *xactInProgress) renewLRU() *xactLRU {
	q.lock.Lock()
	_, xx := q.findU(cmn.ActLRU)
	if xx != nil {
		xlru := xx.(*xactLRU)
		glog.Infof("%s already running, nothing to do", xlru)
		q.lock.Unlock()
		return nil
	}
	id := q.uniqueid()
	xlru := &xactLRU{XactBase: *cmn.NewXactBase(id, cmn.ActLRU)}
	q.add(xlru)
	q.lock.Unlock()
	return xlru
}

func (q *xactInProgress) renewElection(p *proxyrunner, vr *VoteRecord) *xactElection {
	q.lock.Lock()
	_, xx := q.findU(cmn.ActElection)
	if xx != nil {
		xele := xx.(*xactElection)
		glog.Infof("%s already running, nothing to do", xele)
		q.lock.Unlock()
		return nil
	}
	id := q.uniqueid()
	xele := &xactElection{
		XactBase:    *cmn.NewXactBase(id, cmn.ActElection),
		proxyrunner: p,
		vr:          vr,
	}
	q.add(xele)
	q.lock.Unlock()
	return xele
}

func (q *xactInProgress) newEvictDelete(evict bool) *xactEvictDelete {
	q.lock.Lock()
	defer q.lock.Unlock()

	xact := cmn.ActDelete
	if evict {
		xact = cmn.ActEvict
	}

	id := q.uniqueid()
	xdel := &xactEvictDelete{XactBase: *cmn.NewXactBase(id, xact)}
	q.add(xdel)
	return xdel
}

func (q *xactInProgress) renewPrefetch() *xactPrefetch {
	q.lock.Lock()
	defer q.lock.Unlock()
	_, xx := q.findU(cmn.ActPrefetch)
	if xx != nil {
		xpre := xx.(*xactPrefetch)
		glog.Infof("%s already running, nothing to do", xpre)
		return nil
	}
	id := q.uniqueid()
	xpre := &xactPrefetch{XactBase: *cmn.NewXactBase(id, cmn.ActPrefetch)}
	q.add(xpre)
	return xpre
}

func (q *xactInProgress) renewRechecksum(bucket string) *xactRechecksum {
	kind := path.Join(cmn.ActRechecksum, bucket)
	q.lock.Lock()
	_, xx := q.findU(kind)
	if xx != nil {
		xrcksum := xx.(*xactRechecksum)
		q.lock.Unlock()
		glog.Infof("%s already running for bucket %s, nothing to do", xrcksum, bucket)
		return nil
	}
	id := q.uniqueid()
	xrcksum := &xactRechecksum{XactBase: *cmn.NewXactBase(id, kind), bucket: bucket}
	q.add(xrcksum)
	q.lock.Unlock()
	return xrcksum
}

func (q *xactInProgress) abortAll() (sleep bool) {
	q.lock.Lock()
	for _, xact := range q.xactinp {
		if !xact.Finished() {
			xact.Abort()
			sleep = true
		}
	}
	q.lock.Unlock()
	return
}

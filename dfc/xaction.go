/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
//
// extended action aka xaction
//
package dfc

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
)

const timeStampFormat = "15:04:05.000000"

type xactInProgress struct {
	xactinp []cmn.XactInterface
	lock    *sync.Mutex
}

type xactRebalance struct {
	cmn.XactBase
	curversion   int64
	targetrunner *targetrunner
	runnerCnt    int
	confirmCh    chan struct{}
}

type xactLocalRebalance struct {
	cmn.XactBase
	targetRunner *targetrunner
	runnerCnt    int
	confirmCh    chan struct{}
}

type xactLRU struct {
	cmn.XactBase
	targetrunner *targetrunner
}

type xactElection struct {
	cmn.XactBase
	proxyrunner *proxyrunner
	vr          *VoteRecord
}

type xactRechecksum struct {
	cmn.XactBase
	targetrunner *targetrunner
	bucket       string
}

//===================
//
// xactInProgress
//
//===================

func newxactinp() *xactInProgress {
	q := make([]cmn.XactInterface, 4)
	qq := &xactInProgress{xactinp: q[0:0]}
	qq.lock = &sync.Mutex{}
	return qq
}

func (q *xactInProgress) uniqueid() int64 {
	id := time.Now().UTC().UnixNano() & 0xffff
	for i := 0; i < 10; i++ {
		if _, x := q.findU(id); x == nil {
			return id
		}
		id = (time.Now().UTC().UnixNano() + id) & 0xffff
	}
	cmn.Assert(false)
	return 0
}

func (q *xactInProgress) add(xact cmn.XactInterface) {
	q.xactinp = append(q.xactinp, xact)
}

func (q *xactInProgress) findU(by interface{}) (idx int, xact cmn.XactInterface) {
	var id int64
	var kind string
	switch by.(type) {
	case int64:
		id = by.(int64)
	case string:
		kind = by.(string)
	default:
		cmn.Assert(false, fmt.Sprintf("unexpected find() arg: %#v", by))
	}
	for i, xact := range q.xactinp {
		if id != 0 && xact.ID() == id {
			return i, xact
		}
		if kind != "" && xact.Kind() == kind {
			return i, xact
		}
	}
	return -1, nil
}

func (q *xactInProgress) findL(by interface{}) (idx int, xact cmn.XactInterface) {
	q.lock.Lock()
	idx, xact = q.findU(by)
	q.lock.Unlock()
	return
}

func (q *xactInProgress) findUAll(kind string) []cmn.XactInterface {
	xacts := make([]cmn.XactInterface, 0)
	for _, xact := range q.xactinp {
		if xact.Kind() == kind {
			xacts = append(xacts, xact)
		}
	}
	return xacts
}

func (q *xactInProgress) del(by interface{}) {
	q.lock.Lock()
	k, xact := q.findU(by)
	if xact == nil {
		glog.Errorf("Failed to find xact by %#v", by)
		q.lock.Unlock()
		return
	}
	l := len(q.xactinp)
	if k < l-1 {
		copy(q.xactinp[k:], q.xactinp[k+1:])
	}
	q.xactinp[l-1] = nil
	q.xactinp = q.xactinp[:l-1]
	q.lock.Unlock()
}

func (q *xactInProgress) renewRebalance(curversion int64, t *targetrunner, runnerCnt int) *xactRebalance {
	q.lock.Lock()
	_, xx := q.findU(cmn.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactRebalance)
		if !xreb.Finished() {
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
			xreb.abort()
			for i := 0; i < xreb.runnerCnt; i++ {
				<-xreb.confirmCh
			}
			close(xreb.confirmCh)
		}
	}
	id := q.uniqueid()
	xreb := &xactRebalance{
		XactBase:     *cmn.NewXactBase(id, cmn.ActGlobalReb),
		curversion:   curversion,
		targetrunner: t,
		runnerCnt:    runnerCnt,
		confirmCh:    make(chan struct{}, runnerCnt),
	}
	q.add(xreb)
	q.lock.Unlock()
	return xreb
}

// persistent mark indicating rebalancing in progress
func (q *xactInProgress) rebalanceInProgress() (pmarker string) {
	return filepath.Join(ctx.config.Confdir, rebinpname)
}

// persistent mark indicating rebalancing in progress
func (q *xactInProgress) localRebalanceInProgress() (pmarker string) {
	return filepath.Join(ctx.config.Confdir, reblocinpname)
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
		xreb := xx.(*xactLocalRebalance)
		if !xreb.Finished() {
			running = true
		}
	}
	q.lock.Unlock()
	return
}

func (q *xactInProgress) renewLocalRebalance(t *targetrunner, runnerCnt int) *xactLocalRebalance {
	q.lock.Lock()
	_, xx := q.findU(cmn.ActLocalReb)
	if xx != nil {
		xLocalReb := xx.(*xactLocalRebalance)
		if !xLocalReb.Finished() {
			xLocalReb.abort()
			for i := 0; i < xLocalReb.runnerCnt; i++ {
				<-xLocalReb.confirmCh
			}
			close(xLocalReb.confirmCh)
		}
	}
	id := q.uniqueid()
	xLocalReb := &xactLocalRebalance{
		XactBase:     *cmn.NewXactBase(id, cmn.ActLocalReb),
		targetRunner: t,
		runnerCnt:    runnerCnt,
		confirmCh:    make(chan struct{}, runnerCnt),
	}
	q.add(xLocalReb)
	q.lock.Unlock()
	return xLocalReb
}

func (q *xactInProgress) renewLRU(t *targetrunner) *xactLRU {
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
	xlru.targetrunner = t
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

func (q *xactInProgress) renewRechecksum(t *targetrunner, bucket string) *xactRechecksum {
	q.lock.Lock()
	defer q.lock.Unlock()

	for _, xx := range q.findUAll(cmn.ActRechecksum) {
		xrcksum := xx.(*xactRechecksum)
		if xrcksum.bucket == bucket {
			glog.Infof("%s already running for bucket %s, nothing to do", xrcksum, bucket)
			return nil
		}
	}
	id := q.uniqueid()
	xrcksum := &xactRechecksum{
		XactBase:     *cmn.NewXactBase(id, cmn.ActRechecksum),
		targetrunner: t,
		bucket:       bucket,
	}
	q.add(xrcksum)
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

//===================
//
// xactLRU
//
//===================
func (xact *xactLRU) String() string {
	if !xact.Finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.Kind(), xact.ID(), xact.StartTime().Format(timeStampFormat))
	}
	d := xact.EndTime().Sub(xact.StartTime())
	return fmt.Sprintf("xaction %s:%d %v finished %v (duration %v)", xact.Kind(), xact.ID(),
		xact.StartTime().Format(timeStampFormat), xact.EndTime().Format(timeStampFormat), d)
}

//===================
//
// xactRebalance
//
//===================
func (xact *xactRebalance) String() string {
	if !xact.Finished() {
		return fmt.Sprintf("xaction %s:%d v%d started %v", xact.Kind(), xact.ID(), xact.curversion, xact.StartTime().Format(timeStampFormat))
	}
	d := xact.EndTime().Sub(xact.StartTime())
	return fmt.Sprintf("xaction %s:%d v%d started %v finished %v (duration %v)",
		xact.Kind(), xact.ID(), xact.curversion, xact.StartTime().Format(timeStampFormat), xact.EndTime().Format(timeStampFormat), d)
}

func (xact *xactRebalance) abort() {
	xact.XactBase.Abort()
	glog.Infof("ABORT: " + xact.String())
}

//===================
//
// xactLocalRebalance
//
//===================
func (xact *xactLocalRebalance) String() string {
	if !xact.Finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.Kind(), xact.ID(), xact.StartTime().Format(timeStampFormat))
	}
	d := xact.EndTime().Sub(xact.StartTime())
	return fmt.Sprintf("xaction %s:%d started %v finished %v (duration %v)",
		xact.Kind(), xact.ID(), xact.StartTime().Format(timeStampFormat), xact.EndTime().Format(timeStampFormat), d)
}

func (xact *xactLocalRebalance) abort() {
	xact.XactBase.Abort()
	glog.Infof("ABORT: " + xact.String())
}

//==============
//
// xactElection
//
//==============
func (xact *xactElection) String() string {
	if !xact.Finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.Kind(), xact.ID(), xact.StartTime().Format(timeStampFormat))
	}
	d := xact.EndTime().Sub(xact.StartTime())
	return fmt.Sprintf("xaction %s:%d started %v finished %v (duration %v)", xact.Kind(), xact.ID(),
		xact.StartTime().Format(timeStampFormat), xact.EndTime().Format(timeStampFormat), d)
}

func (xact *xactElection) abort() {
	xact.XactBase.Abort()
	glog.Infof("ABORT: " + xact.String())
}

//===================
//
// xactRechecksum
//
//===================
func (xact *xactRechecksum) String() string {
	if !xact.Finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.Kind(), xact.ID(), xact.StartTime().Format(timeStampFormat))
	}
	d := xact.EndTime().Sub(xact.StartTime())
	return fmt.Sprintf("xaction %s:%d started %v finished %v (duration %v)", xact.Kind(), xact.ID(),
		xact.StartTime().Format(timeStampFormat), xact.EndTime().Format(timeStampFormat), d)
}

func (xact *xactRechecksum) abort() {
	xact.XactBase.Abort()
	glog.Infof("ABORT: " + xact.String())
}

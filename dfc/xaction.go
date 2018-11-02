/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
//
// xaction: Extended Action aka Transaction
//
package dfc

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
)

const timeStampFormat = "15:04:05.000000"

type xactInterface interface {
	getid() int64
	getkind() string
	getStartTime() time.Time
	getEndTime() time.Time
	tostring() string
	abort()
	finished() bool
}

type xactInProgress struct {
	xactinp []xactInterface
	lock    *sync.Mutex
}

type xactBase struct {
	id    int64
	stime time.Time
	etime time.Time
	kind  string
	abrt  chan struct{}
}

type xactRebalance struct {
	xactBase
	curversion   int64
	targetrunner *targetrunner
	runnerCnt    int
	confirmCh    chan struct{}
}

type xactLocalRebalance struct {
	xactBase
	targetRunner *targetrunner
	runnerCnt    int
	confirmCh    chan struct{}
}

type xactLRU struct {
	xactBase
	targetrunner *targetrunner
}

type xactElection struct {
	xactBase
	proxyrunner *proxyrunner
	vr          *VoteRecord
}

type xactRechecksum struct {
	xactBase
	targetrunner *targetrunner
	bucket       string
}

//===================
//
// xactBase
//
//===================
func newxactBase(id int64, kind string) *xactBase {
	return &xactBase{id: id, stime: time.Now(), kind: kind, abrt: make(chan struct{}, 1)}
}

func (xact *xactBase) getid() int64 {
	return xact.id
}

func (xact *xactBase) getkind() string {
	return xact.kind
}

func (xact *xactBase) getStartTime() time.Time {
	return xact.stime
}

func (xact *xactBase) getEndTime() time.Time {
	return xact.etime
}

func (xact *xactBase) tostring() string { common.Assert(false, "must be implemented"); return "" }

func (xact *xactBase) abort() {
	xact.etime = time.Now()
	xact.abrt <- struct{}{}
	close(xact.abrt)
}

func (xact *xactBase) finished() bool {
	return !xact.etime.IsZero()
}

//===================
//
// xactInProgress
//
//===================

func newxactinp() *xactInProgress {
	q := make([]xactInterface, 4)
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
	common.Assert(false)
	return 0
}

func (q *xactInProgress) add(xact xactInterface) {
	q.xactinp = append(q.xactinp, xact)
}

func (q *xactInProgress) findU(by interface{}) (idx int, xact xactInterface) {
	var id int64
	var kind string
	switch by.(type) {
	case int64:
		id = by.(int64)
	case string:
		kind = by.(string)
	default:
		common.Assert(false, fmt.Sprintf("unexpected find() arg: %#v", by))
	}
	for i, xact := range q.xactinp {
		if id != 0 && xact.getid() == id {
			return i, xact
		}
		if kind != "" && xact.getkind() == kind {
			return i, xact
		}
	}
	return -1, nil
}

func (q *xactInProgress) findL(by interface{}) (idx int, xact xactInterface) {
	q.lock.Lock()
	idx, xact = q.findU(by)
	q.lock.Unlock()
	return
}

func (q *xactInProgress) findUAll(kind string) []xactInterface {
	xacts := make([]xactInterface, 0)
	for _, xact := range q.xactinp {
		if xact.getkind() == kind {
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
	_, xx := q.findU(api.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactRebalance)
		if !xreb.finished() {
			if xreb.curversion > curversion {
				glog.Errorf("%s version is greater than curversion %d", xreb.tostring(), curversion)
				q.lock.Unlock()
				return nil
			}
			if xreb.curversion == curversion {
				glog.Infof("%s already running, nothing to do", xreb.tostring())
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
		xactBase:     *newxactBase(id, api.ActGlobalReb),
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
	_, xx := q.findU(api.ActGlobalReb)
	if xx != nil {
		xreb := xx.(*xactRebalance)
		if !xreb.finished() {
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
	_, xx := q.findU(api.ActLocalReb)
	if xx != nil {
		xreb := xx.(*xactLocalRebalance)
		if !xreb.finished() {
			running = true
		}
	}
	q.lock.Unlock()
	return
}

func (q *xactInProgress) renewLocalRebalance(t *targetrunner, runnerCnt int) *xactLocalRebalance {
	q.lock.Lock()
	_, xx := q.findU(api.ActLocalReb)
	if xx != nil {
		xLocalReb := xx.(*xactLocalRebalance)
		if !xLocalReb.finished() {
			xLocalReb.abort()
			for i := 0; i < xLocalReb.runnerCnt; i++ {
				<-xLocalReb.confirmCh
			}
			close(xLocalReb.confirmCh)
		}
	}
	id := q.uniqueid()
	xLocalReb := &xactLocalRebalance{
		xactBase:     *newxactBase(id, api.ActLocalReb),
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
	_, xx := q.findU(api.ActLRU)
	if xx != nil {
		xlru := xx.(*xactLRU)
		glog.Infof("%s already running, nothing to do", xlru.tostring())
		q.lock.Unlock()
		return nil
	}
	id := q.uniqueid()
	xlru := &xactLRU{xactBase: *newxactBase(id, api.ActLRU)}
	xlru.targetrunner = t
	q.add(xlru)
	q.lock.Unlock()
	return xlru
}

func (q *xactInProgress) renewElection(p *proxyrunner, vr *VoteRecord) *xactElection {
	q.lock.Lock()
	_, xx := q.findU(api.ActElection)
	if xx != nil {
		xele := xx.(*xactElection)
		glog.Infof("%s already running, nothing to do", xele.tostring())
		q.lock.Unlock()
		return nil
	}
	id := q.uniqueid()
	xele := &xactElection{
		xactBase:    *newxactBase(id, api.ActElection),
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

	for _, xx := range q.findUAll(api.ActRechecksum) {
		xrcksum := xx.(*xactRechecksum)
		if xrcksum.bucket == bucket {
			glog.Infof("%s already running for bucket %s, nothing to do", xrcksum.tostring(), bucket)
			return nil
		}
	}
	id := q.uniqueid()
	xrcksum := &xactRechecksum{
		xactBase:     *newxactBase(id, api.ActRechecksum),
		targetrunner: t,
		bucket:       bucket,
	}
	q.add(xrcksum)
	return xrcksum
}

func (q *xactInProgress) abortAll() (sleep bool) {
	q.lock.Lock()
	for _, xact := range q.xactinp {
		if !xact.finished() {
			xact.abort()
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
func (xact *xactLRU) tostring() string {
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.kind, xact.id, xact.stime.Format(timeStampFormat))
	}
	d := xact.etime.Sub(xact.stime)
	return fmt.Sprintf("xaction %s:%d %v finished %v (duration %v)", xact.kind, xact.id,
		xact.stime.Format(timeStampFormat), xact.etime.Format(timeStampFormat), d)
}

//===================
//
// xactRebalance
//
//===================
func (xact *xactRebalance) tostring() string {
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d v%d started %v", xact.kind, xact.id, xact.curversion, xact.stime.Format(timeStampFormat))
	}
	d := xact.etime.Sub(xact.stime)
	return fmt.Sprintf("xaction %s:%d v%d started %v finished %v (duration %v)",
		xact.kind, xact.id, xact.curversion, xact.stime.Format(timeStampFormat), xact.etime.Format(timeStampFormat), d)
}

func (xact *xactRebalance) abort() {
	xact.xactBase.abort()
	glog.Infof("ABORT: " + xact.tostring())
}

//===================
//
// xactLocalRebalance
//
//===================
func (xact *xactLocalRebalance) tostring() string {
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.kind, xact.id, xact.stime.Format(timeStampFormat))
	}
	d := xact.etime.Sub(xact.stime)
	return fmt.Sprintf("xaction %s:%d started %v finished %v (duration %v)",
		xact.kind, xact.id, xact.stime.Format(timeStampFormat), xact.etime.Format(timeStampFormat), d)
}

func (xact *xactLocalRebalance) abort() {
	xact.xactBase.abort()
	glog.Infof("ABORT: " + xact.tostring())
}

//==============
//
// xactElection
//
//==============
func (xact *xactElection) tostring() string {
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.kind, xact.id, xact.stime.Format(timeStampFormat))
	}
	d := xact.etime.Sub(xact.stime)
	return fmt.Sprintf("xaction %s:%d started %v finished %v (duration %v)", xact.kind, xact.id,
		xact.stime.Format(timeStampFormat), xact.etime.Format(timeStampFormat), d)
}

func (xact *xactElection) abort() {
	xact.xactBase.abort()
	glog.Infof("ABORT: " + xact.tostring())
}

//===================
//
// xactRechecksum
//
//===================
func (xact *xactRechecksum) tostring() string {
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.kind, xact.id, xact.stime.Format(timeStampFormat))
	}
	d := xact.etime.Sub(xact.stime)
	return fmt.Sprintf("xaction %s:%d started %v finished %v (duration %v)", xact.kind, xact.id,
		xact.stime.Format(timeStampFormat), xact.etime.Format(timeStampFormat), d)
}

func (xact *xactRechecksum) abort() {
	xact.xactBase.abort()
	glog.Infof("ABORT: " + xact.tostring())
}

// Package atime tracks object access times in the system while providing a number of performance enhancements.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package atime

import (
	"os"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/ios"
)

// ================================ Summary ===============================================
//
// The atime (access time) module provides atime.Runner - a long running task with the
// purpose of updating object access times. The work is performed on a per local
// filesystem bases, via mpathAtimeRunners (children). The atime.Runner's main responsibility
// is to dispatch requests to the corresponding mpathAtimeRunner instance.
//
// API exposed to the rest of the code includes the following operations:
//
//   * Run      - to run
//   * Stop     - to stop
//   * Touch    - to request an access time update for a specified object
//   * Atime    - to request the most recent access time of a given object
// The Touch and Atime requests are added to the request queue
// and then are dispatched to the mpathAtimeRunner for a given filesystem.
//
// Note: atime.Runner assumes that object in question either belongs to a
// bucket that has LRU enabled or LRU is enabled through the global config when bucket properties
// are not present. Thus, it is the responsibility of the caller to ensure
// that LRU is enabled. Although this check is not necessary for the Atime method (a zero-valued
// Response will be returned because it will not exist in any mpathAtimeRunner's atimemap),
// it is recommended to do this check.
//
// The remaining operations are private to the atime.Runner and used only internally.
//
// Each mpathAtimeRunner, which corresponds to a mountpath, has an access time map (in memory)
// that keeps track of object access times. Every so often atime.Runner
// calls mpathAtimeRunners to flush access time maps.  Access times get flushed to
// the disk when the number of stored access times reaches a certain threshold and when:
//   * disk utilization is low, or
//   * access time map is filled over a certain point (watermark)
// This way, the atime.Runner and mpathAtimeRunner operation will impact the
// datapath as little as possible.  As such, atime.Runner can be thought of as an
// extension of the LRU, or any alternative
// caching mechanism that can be implemented in the future.
//
// The reason behind the existence of this module is the 'noatime' mounting option;
// if a file system has been mounted with this option, reading accesses to the
// file system will no longer result in an update to the atime information associated
// with the file, which eliminates the need to make writes to the file system for files
// that are simply being read, resulting in noticeable performance improvements.
// Inside DFC cluster, this option is always set, so DFC implements its own access time
// updating.
//
// ================================ Summary ===============================================

//================================= Constants ==============================================
const (
	setChSize                = 256
	mpathRunnersMapSize      = 8
	atimeCacheFlushThreshold = 4 * 1024
	atimeLWM                 = 60
	atimeHWM                 = 80
)

const (
	atimeTouch = "touch"
	atimeGet   = "get"
)

//================================= Global Variables ==========================================
// atimeSyncTime is used to determine how often flushes occur.
var atimeSyncTime = time.Minute * 3

//
// API types
//
type (
	// atime.Runner gets and sets access times for a given object identified by its fqn.
	// atime.Runner implements the fsprunner interface where each mountpath has its own
	// mpathAtimeRunner that manages requests on a per local filesystem basis.
	// atime.Runner will also periodically call its mpathAtimeRunners
	// to flush files (read description above).
	Runner struct {
		cmn.Named
		requestCh    chan *atimeRequest // Requests for file access times or set access times
		stopCh       chan struct{}      // Control channel for stopping
		mpathReqCh   chan fs.ChangeReq
		mpathRunners map[string]*mpathAtimeRunner // mpath -> mpathAtimeRunner
		mountpaths   *fs.MountedFS
		riostat      *ios.IostatRunner
	}
	// The Response object is used to return the access time of
	// an object in the atimemap and whether it actually existed in
	// the atimemap of the mpathAtimeRunner it belongs to.
	Response struct {
		Ok         bool
		AccessTime time.Time
	}
)

//
// private types
//
type (
	// Each mpathAtimeRunner corresponds to a mpath. All atime request for any file belonging
	// to this mpath are handled by mpathAtimeRunner. This includes requests for getting,
	// setting and flushing atimes.
	mpathAtimeRunner struct {
		mpath    string
		fs       string
		stopCh   chan struct{}        // Control channel for stopping
		atimemap map[string]time.Time // maps fqn:atime key-value pairs
		getCh    chan *atimeRequest   // Requests for file access times
		setCh    chan *atimeRequest   // Requests to set access times
		flushCh  chan int             // Request to flush the file system
		riostat  *ios.IostatRunner
	}

	// Each request to atime.Runner via its API is encapsulated in an
	// atimeRequest object. The responseCh is used to ensure each atime request gets its
	// corresponding response.
	// The accessTime field is used by Touch to set the atime of the requested object.
	// The mpath field is used by atime.Runner to determine which mpathAtimeRunner to
	// dispatch the request to.
	atimeRequest struct {
		fqn         string
		accessTime  time.Time
		responseCh  chan *Response
		mpath       string
		requestType string
	}
)

/*
 * implements fs.PathRunner interface
 */
var _ fs.PathRunner = &Runner{}

func (r *Runner) ReqAddMountpath(mpath string)     { r.mpathReqCh <- fs.MountpathAdd(mpath) }
func (r *Runner) ReqRemoveMountpath(mpath string)  { r.mpathReqCh <- fs.MountpathRem(mpath) }
func (r *Runner) ReqEnableMountpath(mpath string)  {}
func (r *Runner) ReqDisableMountpath(mpath string) {}

//================================ atime.Runner ==========================================

func NewRunner(mountpaths *fs.MountedFS, riostat *ios.IostatRunner) (r *Runner) {
	return &Runner{
		stopCh:       make(chan struct{}, 4),
		mpathReqCh:   make(chan fs.ChangeReq, 1),
		mpathRunners: make(map[string]*mpathAtimeRunner, mpathRunnersMapSize),
		mountpaths:   mountpaths,
		requestCh:    make(chan *atimeRequest),
		riostat:      riostat,
	}
}

func (r *Runner) init() {
	availablePaths, disabledPaths := r.mountpaths.Get()
	for mpath := range availablePaths {
		r.addMpathAtimeRunner(mpath)
	}
	for mpath := range disabledPaths {
		r.addMpathAtimeRunner(mpath)
	}
}

// Run initiates the work of the receiving atime.Runner
func (r *Runner) Run() error {
	glog.Infof("Starting %s", r.Getname())
	ticker := time.NewTicker(atimeSyncTime)
	r.init()
	for {
		select {
		case <-ticker.C:
			for _, runner := range r.mpathRunners {
				runner.flush()
			}
		case mpathRequest := <-r.mpathReqCh:
			switch mpathRequest.Action {
			case fs.Add:
				r.addMpathAtimeRunner(mpathRequest.Path)
			case fs.Remove:
				r.removeMpathAtimeRunner(mpathRequest.Path)
			}
		case request := <-r.requestCh:
			mpathRunner, ok := r.mpathRunners[request.mpath]
			if ok {
				if request.requestType == atimeTouch {
					mpathRunner.setCh <- request
				} else {
					mpathRunner.getCh <- request
				}
			} else if request.requestType == atimeGet {
				// invalid mpath so return a nil time for atime request
				request.responseCh <- &Response{AccessTime: time.Time{}, Ok: false}
			}
		case <-r.stopCh:
			ticker.Stop() // NOTE: not flushing cached atimes
			for _, runner := range r.mpathRunners {
				runner.stop()
			}
			return nil
		}
	}
}

// Stop terminates the atime.Runner
func (r *Runner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// touch requests an access time update for a given object to the current
// time. touch additionally allows the caller to set the access time of an object
// to a set time using the variadic function parameter setTime.
// Note this method should only be called on objects belonging to buckets that have
// LRU Enabled.
func (r *Runner) Touch(fqn string, setTime ...time.Time) {
	mpathInfo, _ := r.mountpaths.Path2MpathInfo(fqn)
	if mpathInfo == nil {
		return
	}
	var t time.Time
	if len(setTime) == 1 {
		t = setTime[0]
	} else {
		t = time.Now()
	}
	mpath := mpathInfo.Path
	request := &atimeRequest{
		accessTime:  t,
		fqn:         fqn,
		mpath:       mpath,
		requestType: atimeTouch,
	}
	r.requestCh <- request
}

// atime requests the most recent access time of a given file.
// Note the atime method returns a channel. The caller of the function should
// block until it can receive from the channel an Response object, which
// indicates that the request has been fully processed. Note: caller of this
// method does not necessarily need to check if the bucket the object belongs
// to has LRU Enabled (a zero-valued Response will be returned)
// Note, the caller can optionally provide a customRespCh where the response will
// be written to. This reduces channel creation if atime is called repeatedly.
// Example usage:
//     Response := <-atimer.atime("/tmp/fqn123")
//     accessTime, ok := Response.AccessTime, Response.Ok
func (r *Runner) Atime(fqn string, customRespCh ...chan *Response) (responseCh chan *Response) {
	if len(customRespCh) == 1 {
		responseCh = customRespCh[0]
	} else {
		responseCh = make(chan *Response, 1)
	}
	var mpath string
	if mpathInfo, _ := r.mountpaths.Path2MpathInfo(fqn); mpathInfo != nil {
		mpath = mpathInfo.Path
		request := &atimeRequest{
			responseCh:  responseCh,
			fqn:         fqn,
			mpath:       mpath,
			requestType: atimeGet,
		}
		r.requestCh <- request
		return request.responseCh
	}

	// No mpath exists for the file
	responseCh <- &Response{AccessTime: time.Time{}, Ok: false}
	return responseCh
}

// convenience method to obtain atime from the (atime) cache or the file itself,
// and format accordingly
func (r *Runner) FormatAtime(fqn string, respCh chan *Response, useCache bool, format ...string) (atimestr string, atime time.Time, err error) {
	var (
		atimeResp *Response
		finfo     os.FileInfo
		ok        bool
	)
	if useCache {
		if respCh != nil {
			atimeResp = <-r.Atime(fqn, respCh)
		} else {
			atimeResp = <-r.Atime(fqn)
		}
		atime, ok = atimeResp.AccessTime, atimeResp.Ok
	}
	if !ok {
		finfo, err = os.Stat(fqn)
		if err == nil {
			atime, mtime, _ := ios.GetAmTimes(finfo)
			if mtime.After(atime) {
				atime = mtime
			}
		}
	}
	if err == nil {
		if len(format) > 0 {
			atimestr = atime.Format(format[0])
		} else {
			atimestr = atime.Format(cmn.RFC822)
		}
	}
	return
}

//
// private methods
//

func (r *Runner) addMpathAtimeRunner(mpath string) {
	if _, ok := r.mpathRunners[mpath]; ok {
		glog.Warningf("Attempt to add already existing mountpath %q", mpath)
		return
	}
	mpathInfo, _ := r.mountpaths.Path2MpathInfo(mpath)
	if mpathInfo == nil {
		glog.Errorf("Attempt to add mountpath %q with no corresponding filesystem", mpath)
		return
	}

	r.mpathRunners[mpath] = r.newMpathAtimeRunner(mpath, mpathInfo.FileSystem, r.riostat)
	go r.mpathRunners[mpath].run()
}

func (r *Runner) removeMpathAtimeRunner(mpath string) {
	mpathRunner, ok := r.mpathRunners[mpath]
	if !ok {
		glog.Errorf("Invalid mountpath %q", mpath)
		return
	}
	mpathRunner.stop()
	delete(r.mpathRunners, mpath)
}

//================================= mpathAtimeRunner ===========================================

func (r *Runner) newMpathAtimeRunner(mpath, fs string, riostat *ios.IostatRunner) *mpathAtimeRunner {
	return &mpathAtimeRunner{
		mpath:    mpath,
		fs:       fs,
		stopCh:   make(chan struct{}, 1),
		atimemap: make(map[string]time.Time),
		getCh:    make(chan *atimeRequest),
		setCh:    make(chan *atimeRequest, setChSize),
		flushCh:  make(chan int),
		riostat:  riostat,
	}
}

func (m *mpathAtimeRunner) run() {
	for {
		select {
		case request := <-m.getCh:
			accessTime, ok := m.atimemap[request.fqn]
			request.responseCh <- &Response{ok, accessTime}
		case request := <-m.setCh:
			m.atimemap[request.fqn] = request.accessTime
		case numToFlush := <-m.flushCh:
			m.handleFlush(numToFlush)
		case <-m.stopCh:
			return
		}
	}
}

func (m *mpathAtimeRunner) stop() {
	glog.Infof("Stopping mpathAtimeRunner for mpath: %s", m.mpath)
	m.stopCh <- struct{}{}
	close(m.stopCh)
}

// getNumberItemsToFlush estimates the number of timestamps that must be flushed
// the atime map, by taking into account the max utilitization of the corresponding
// local mpath (or, more exactly, the corresponding local mpath's disks).
func (m *mpathAtimeRunner) getNumberItemsToFlush() (n int) {
	atimeMapSize := len(m.atimemap)
	if atimeMapSize <= atimeCacheFlushThreshold {
		return
	}
	max := cmn.GCO.Get().LRU.AtimeCacheMax
	filling := cmn.MinU64(100, uint64(atimeMapSize)*100/max)

	maxDiskUtil := float32(-1)
	util, ok := m.riostat.MaxUtilFS(m.fs)
	if ok {
		maxDiskUtil = util
	}

	switch {
	case maxDiskUtil >= 0 && maxDiskUtil < 50: // disk is idle so we can utilize it a bit
		n = atimeMapSize / 4
	case filling == 100: // max capacity reached - flushing a great number of items is required
		n = atimeMapSize / 2
	case filling > atimeHWM: // atime map capacity at high watermark
		n = atimeMapSize / 4
	case filling > atimeLWM: // low watermark => weighted formula
		f := float64(filling-atimeLWM) / float64(atimeHWM-atimeLWM) * float64(atimeMapSize)
		n = int(f) / 4
	}

	return
}

// Flush accepts an optional paramter to set the number of items to flush for testing purposes.
func (m *mpathAtimeRunner) flush(numToFlush ...int) {
	n := 0
	if len(numToFlush) == 1 {
		n = numToFlush[0]
	}
	m.flushCh <- n
}

// handleFlush tries to change access and modification time for at most n files in
// the atime map, and removes them from the map.
func (m *mpathAtimeRunner) handleFlush(n int) {
	var (
		i     int
		mtime time.Time
	)
	if n == 0 {
		n = m.getNumberItemsToFlush()
	}
	if n <= 0 {
		return
	}
	for fqn, atime := range m.atimemap {
		finfo, err := os.Stat(fqn)
		if err != nil {
			if os.IsNotExist(err) {
				delete(m.atimemap, fqn)
				i++
			} else {
				glog.Warningf("failing to touch %s, err: %v", fqn, err)
			}
			goto cont
		}
		mtime = finfo.ModTime()
		if err = os.Chtimes(fqn, atime, mtime); err != nil {
			if os.IsNotExist(err) {
				delete(m.atimemap, fqn)
				i++
			} else {
				glog.Warningf("can't touch %s, err: %v", fqn, err) // FIXME: carry on forever?
			}
		} else {
			delete(m.atimemap, fqn)
			i++
			if glog.V(4) {
				glog.Infof("touch %s at %v", fqn, atime)
			}
		}
	cont:
		if i >= n {
			break
		}
	}
}

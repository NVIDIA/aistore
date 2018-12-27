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
// filesystem bases, via joggers (children). The atime.Runner's main responsibility
// is to dispatch requests to the corresponding jogger instance.
//
// API exposed to the rest of the code includes the following operations:
//
//   * Run      - to run
//   * Stop     - to stop
//   * Touch    - to request an access time update for a specified object
//   * Atime    - to request the most recent access time of a given object
// The Touch and Atime requests are added to the request queue
// and then are dispatched to the jogger for a given filesystem.
//
// Note: atime.Runner assumes that object in question either belongs to a
// bucket that has LRU enabled or LRU is enabled through the global config when bucket properties
// are not present. Thus, it is the responsibility of the caller to ensure
// that LRU is enabled. Although this check is not necessary for the Atime method (a zero-valued
// Response will be returned because it will not exist in any jogger's atimemap),
// it is recommended to do this check.
//
// The remaining operations are private to the atime.Runner and used only internally.
//
// Each jogger, which corresponds to a mountpath, has an access time map (in memory)
// that keeps track of object access times. Every so often atime.Runner
// calls joggers to flush access time maps.  Access times get flushed to
// the disk when the number of stored access times reaches a certain threshold and when:
//   * disk utilization is low, or
//   * access time map is filled over a certain point (watermark)
// This way, the atime.Runner and jogger operation will impact the
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
	chanCap = 256
	LowWM   = 60
	HighWM  = 80
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
	// jogger that manages requests on a per local filesystem basis.
	// atime.Runner will also periodically call its joggers
	// to flush files (read description above).
	Runner struct {
		cmn.NamedID
		requestCh  chan *atimeRequest // Requests for file access times or set access times
		stopCh     chan struct{}      // Control channel for stopping
		mpathReqCh chan fs.ChangeReq
		joggers    map[string]*jogger // mpath -> jogger
		mountpaths *fs.MountedFS
		riostat    *ios.IostatRunner
	}
	// The Response object is used to return the access time of
	// an object in the atimemap and whether it actually existed in
	// the atimemap of the jogger it belongs to.
	Response struct {
		Ok         bool
		AccessTime time.Time
	}
)

//
// private types
//
type (
	// jogger handles a given specific mountpath/* as far as getting, setting, and flushing atimes
	jogger struct {
		mpathInfo *fs.MountpathInfo
		stopCh    chan struct{}        // Control channel for stopping
		atimemap  map[string]time.Time // maps fqn:atime key-value pairs
		getCh     chan *atimeRequest   // Requests for file access times
		setCh     chan *atimeRequest   // Requests to set access times
		flushCh   chan struct{}        // Request to flush atimes
		riostat   *ios.IostatRunner
	}

	// Each request to atime.Runner via its API is encapsulated in an
	// atimeRequest object. The responseCh is used to ensure each atime request gets its
	// corresponding response.
	// The accessTime field is used by Touch to set the atime of the requested object.
	// The mpath field is used by atime.Runner to determine which jogger to
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
		stopCh:     make(chan struct{}, 4),
		mpathReqCh: make(chan fs.ChangeReq, 1),
		joggers:    make(map[string]*jogger, 8),
		mountpaths: mountpaths,
		requestCh:  make(chan *atimeRequest),
		riostat:    riostat,
	}
}

func (r *Runner) init() {
	availablePaths, disabledPaths := r.mountpaths.Get()
	for mpath := range availablePaths {
		r.addJogger(mpath)
	}
	for mpath := range disabledPaths {
		r.addJogger(mpath)
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
			for _, jogger := range r.joggers {
				jogger.flushCh <- struct{}{}
			}
		case mpathRequest := <-r.mpathReqCh:
			switch mpathRequest.Action {
			case fs.Add:
				r.addJogger(mpathRequest.Path)
			case fs.Remove:
				r.removeJogger(mpathRequest.Path)
			}
		case request := <-r.requestCh:
			jogger, ok := r.joggers[request.mpath]
			if ok {
				if request.requestType == atimeTouch {
					jogger.setCh <- request
				} else {
					jogger.getCh <- request
				}
			} else if request.requestType == atimeGet {
				// invalid mpath so return a nil time for atime request
				request.responseCh <- &Response{AccessTime: time.Time{}, Ok: false}
			}
		case <-r.stopCh:
			ticker.Stop() // NOTE: not flushing cached atimes
			for _, jogger := range r.joggers {
				jogger.stop()
			}
			return nil
		}
	}
}

// Stop terminates atime.Runner
func (r *Runner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// Touch requests an access time update for a given object. If not specified,
// the current time will be used. Note LRU must be enabled on the corresponding
// bucket.
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

func (r *Runner) addJogger(mpath string) {
	if _, ok := r.joggers[mpath]; ok {
		glog.Warningf("Attempt to add already existing mountpath %q", mpath)
		return
	}
	mpathInfo, _ := r.mountpaths.Path2MpathInfo(mpath)
	if mpathInfo == nil {
		glog.Errorf("Attempt to add mountpath %q with no corresponding filesystem", mpath)
		return
	}
	jogger := r.newJogger(mpathInfo, r.riostat)
	r.joggers[mpath] = jogger
	go jogger.jog()
}

func (r *Runner) removeJogger(mpath string) {
	jogger, ok := r.joggers[mpath]
	if !ok {
		glog.Errorf("Invalid mountpath %q", mpath)
		return
	}
	jogger.stop()
	delete(r.joggers, mpath)
}

//================================= jogger ===========================================

func (r *Runner) newJogger(mpathInfo *fs.MountpathInfo, riostat *ios.IostatRunner) *jogger {
	return &jogger{
		mpathInfo: mpathInfo,
		stopCh:    make(chan struct{}, 1),
		atimemap:  make(map[string]time.Time),
		getCh:     make(chan *atimeRequest),
		setCh:     make(chan *atimeRequest, chanCap),
		flushCh:   make(chan struct{}, 16),
		riostat:   riostat,
	}
}

func (j *jogger) jog() {
	for {
		select {
		case request := <-j.getCh:
			accessTime, ok := j.atimemap[request.fqn]
			request.responseCh <- &Response{ok, accessTime}
		case request := <-j.setCh:
			j.atimemap[request.fqn] = request.accessTime
		case <-j.flushCh:
			j.flushAtimes()
		case <-j.stopCh:
			return
		}
	}
}

func (j *jogger) stop() {
	glog.Infof("Stopping jogger [%s]", j.mpathInfo.Path)
	j.stopCh <- struct{}{}
	close(j.stopCh)
}

// num2flush estimates the number of timestamps that must be flushed
func (j *jogger) num2flush() (n int) {
	config := cmn.GCO.Get()
	maxlen := cmn.MaxI64(config.LRU.AtimeCacheMax, 1)
	lowlen := maxlen * LowWM / 100
	curlen := int64(len(j.atimemap))
	curpct := curlen * 100 / maxlen
	f := cmn.Ratio(HighWM, LowWM, curpct)
	n = int(float32(curlen-lowlen) * f)

	// TODO: handle the idle case in the slow path as part of the _TBD_ refactoring
	if n == 0 && j.mpathInfo.IsIdle(config) {
		n = int(curlen / 4)
	}
	return
}

// flush stores computed number of access times and removes the corresponding entries from the map.
func (j *jogger) flushAtimes() {
	var (
		mtime time.Time
		i     int
		n     = j.num2flush()
	)
	for fqn, atime := range j.atimemap {
		finfo, err := os.Stat(fqn)
		if err != nil {
			if !os.IsNotExist(err) {
				glog.Errorf("%s not-not-exists, fstat err: %v", fqn, err)
			}
		} else {
			mtime = finfo.ModTime()
			if err = os.Chtimes(fqn, atime, mtime); err != nil {
				glog.Errorf("Failed to touch %s, err: %v", fqn, err)
			}
		}
		delete(j.atimemap, fqn)
		i++
		if i >= n {
			break
		}
	}
}

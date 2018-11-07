/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"fmt"
	"os"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

// ================================ Summary ===============================================
//
// The atime (access time) module implements an atimerunner - a long running task with the
// purpose of updating object (file) access times. atimerunner implements the fsprunner
// interface. Each file system has a corresponding mountpath, which has an associated
// mpathAtimeRunner. atimrunner dispatches requests to the mpathAtimeRunner pertaining to the
// mountpath the object in question belongs to.
// For the mountpath definition, see fs/mountfs.go
//
// The atimerunner API exposed to the rest of the code includes the following operations:
//   * run      - to initiate the work of the receiving atimerunner
//   * stop     - to stop the receiving atimerunner
//   * touch    - to request an access time update for a given file
//   * atime    - to request the most recent access time of a given file
// The touch and atime requests are added to the atimerunner's request queue
// and then are dispatched to the mpathAtimeRunner pertaining to the mpath the corresponding
// file belongs to.
//
// Note: atimerunner assumes that any file in question either belongs to a
// bucket that has LRU enabled or LRU is enabled through the global config when bucket properties
// are not present. Thus, it is the responsibility of the caller of touch to ensure
// that LRU is enabled. Although this check is not necessary for the atime method (a zero-valued
// atimeResponse will be returned because it will not exist in any mpathAtimeRunner's atimemap),
// it is recommended to do this check for the file.
//
// All other operations are private to the atimerunner and used only internally!
//
// Each mpathAtimeRunner, which corresponds to a mount path, has an access time map (in memory)
// that keeps track of object access times belonging to that mpath. atimerunner will
// periodically call each mpathAtimeRunner to flush the data in the access time map to the disk.
// Access times get flushed to the disk when the number of stored access times has reached
// a certain flush threshold and when:
//   * disk utilization is low, or
//   * access time map is filled over a certain point (watermark)
// This way, the atimerunner and mpathAtimeRunner will impact the datapath as little as possible.
// As such, atimerunner can be thought of as an extension of the LRU, or any alternative
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
	atimeAddMountpath    = "addmpath"
	atimeRemoveMountpath = "removempath"
	atimeTouch           = "touch"
	atimeGet             = "get"
)

//================================= Global Variables ==========================================
// atimeSyncTime is used to determine how often flushes occur.
var atimeSyncTime = time.Minute * 3

//================================= Type Definitions ==========================================
// atimerunner is used to get and set atimes for a given fqn.
// atimerunner implements the fsprunner interface, and each mpath has its own
// mpathAtimeRunner that manages requests for setting and getting atimes for fqns
// belonging to that mpath.
// atimerunner will also periodically call its mpathAtimeRunners that it manages
// to flush files (read description above).
type atimerunner struct {
	cmn.Named
	requestCh    chan *atimeRequest // Requests for file access times or set access times
	stopCh       chan struct{}      // Control channel for stopping
	mpathReqCh   chan mpathReq
	mpathRunners map[string]*mpathAtimeRunner // mpath -> mpathAtimeRunner
	mountpaths   *fs.MountedFS
	t            *targetrunner
}

// Each mpathAtimeRunner corresponds to a mpath. All atime request for any file belonging
// to this mpath are handled by mpathAtimeRunner. This includes requests for getting,
// setting and flushing atimes.
type mpathAtimeRunner struct {
	mpath    string
	fs       string
	stopCh   chan struct{}        // Control channel for stopping
	atimemap map[string]time.Time // maps fqn:atime key-value pairs
	getCh    chan *atimeRequest   // Requests for file access times
	setCh    chan *atimeRequest   // Requests to set access times
	flushCh  chan int             // Request to flush the file system
}

// Each request to atimerunner via its api (for touch and atime) is encapsulated in an
// atimeRequest object. The responseCh is used to ensure each atime request gets its
// corresponding response through an atimeResponse (request-response pattern).
// The accessTime field is used by touch to set the atime of the requested object to a specified time.
// The mpath field is used by atimerunner to determine which mpathAtimeRunner to
// dispatch the request to.
type atimeRequest struct {
	fqn         string
	accessTime  time.Time
	responseCh  chan *atimeResponse
	mpath       string
	requestType string
}

// The atimeResponse object is used to return the access time of
// an object in the atimemap and whether it actually existed in
// the atimemap of the mpathAtimeRunner it belongs to.
type atimeResponse struct {
	ok         bool
	accessTime time.Time
}

//================================ atimerunner ==========================================

func newAtimeRunner(t *targetrunner, mountpaths *fs.MountedFS) (r *atimerunner) {
	return &atimerunner{
		stopCh:       make(chan struct{}, 4),
		mpathReqCh:   make(chan mpathReq, 1),
		mpathRunners: make(map[string]*mpathAtimeRunner, mpathRunnersMapSize),
		mountpaths:   mountpaths,
		requestCh:    make(chan *atimeRequest),
		t:            t,
	}
}

func (r *atimerunner) init() {
	availablePaths, disabledPaths := r.mountpaths.Mountpaths()
	for mpath := range availablePaths {
		r.addMpathAtimeRunner(mpath)
	}
	for mpath := range disabledPaths {
		r.addMpathAtimeRunner(mpath)
	}
}

// run initiates the work of the receiving atimerunner
func (r *atimerunner) Run() error {
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
			switch mpathRequest.action {
			case atimeAddMountpath:
				r.addMpathAtimeRunner(mpathRequest.mpath)
			case atimeRemoveMountpath:
				r.removeMpathAtimeRunner(mpathRequest.mpath)
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
				request.responseCh <- &atimeResponse{accessTime: time.Time{}, ok: false}
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

// stop aborts the receiving atimerunner
func (r *atimerunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// touch requests an access time update for a given object to the current
// time. touch additionally allows the caller to set the access time of an object
// to a set time using the variadic function parameter setTime.
// Note this method should only be called on objects belonging to buckets that have
// LRU Enabled.
func (r *atimerunner) touch(fqn string, setTime ...time.Time) {
	mpathInfo, _ := path2mpathInfo(fqn)
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
// block until it can receive from the channel an atimeResponse object, which
// indicates that the request has been fully processed. Note: caller of this
// method does not necessarily need to check if the bucket the object belongs
// to has LRU Enabled (a zero-valued atimeResponse will be returned)
// Note, the caller can optionally provide a customRespCh where the response will
// be written to. This reduces channel creation if atime is called repeatedly.
// Example usage:
//     atimeResponse := <-atimer.atime("/tmp/fqn123")
//     accessTime, ok := atimeResponse.accessTime, atimeResponse.ok
func (r *atimerunner) atime(fqn string, customRespCh ...chan *atimeResponse) (responseCh chan *atimeResponse) {
	if len(customRespCh) == 1 {
		responseCh = customRespCh[0]
	} else {
		responseCh = make(chan *atimeResponse, 1)
	}
	var mpath string
	if mpathInfo, _ := path2mpathInfo(fqn); mpathInfo != nil {
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
	responseCh <- &atimeResponse{accessTime: time.Time{}, ok: false}
	return responseCh
}

func (r *atimerunner) addMpathAtimeRunner(mpath string) {
	if _, ok := r.mpathRunners[mpath]; ok {
		glog.Warningf("Attempted to add already existing mpath: %q", mpath)
		return
	}
	mpathInfo, _ := path2mpathInfo(mpath)
	if mpathInfo == nil {
		cmn.Assert(false, fmt.Sprintf("Attempted to add a mpath %q with no corresponding filesystem", mpath))
	}

	r.mpathRunners[mpath] = r.newMpathAtimeRunner(mpath, mpathInfo.FileSystem)
	go r.mpathRunners[mpath].run()
}

func (r *atimerunner) removeMpathAtimeRunner(mpath string) {
	mpathRunner, ok := r.mpathRunners[mpath]
	cmn.Assert(ok, "Mountpath unregister handler for replication called with invalid mountpath")
	mpathRunner.stop()
	delete(r.mpathRunners, mpath)
}

/*
 * fsprunner methods
 */

func (r *atimerunner) reqAddMountpath(mpath string) {
	r.mpathReqCh <- mpathReq{action: atimeAddMountpath, mpath: mpath}
}

func (r *atimerunner) reqRemoveMountpath(mpath string) {
	r.mpathReqCh <- mpathReq{action: atimeRemoveMountpath, mpath: mpath}
}

func (r *atimerunner) reqEnableMountpath(mpath string) {
	return
}

func (r *atimerunner) reqDisableMountpath(mpath string) {
	return
}

//================================= mpathAtimeRunner ===========================================

func (r *atimerunner) newMpathAtimeRunner(mpath, fs string) *mpathAtimeRunner {
	return &mpathAtimeRunner{
		mpath:    mpath,
		fs:       fs,
		stopCh:   make(chan struct{}, 1),
		atimemap: make(map[string]time.Time),
		getCh:    make(chan *atimeRequest),
		setCh:    make(chan *atimeRequest, setChSize),
		flushCh:  make(chan int),
	}
}

func (m *mpathAtimeRunner) run() {
	for {
		select {
		case request := <-m.getCh:
			accessTime, ok := m.atimemap[request.fqn]
			request.responseCh <- &atimeResponse{ok, accessTime}
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

	filling := cmn.MinU64(100, uint64(atimeMapSize)*100/ctx.config.LRU.AtimeCacheMax)
	riostat := getiostatrunner()

	maxDiskUtil := float32(-1)
	util, ok := riostat.maxUtilFS(m.fs)
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

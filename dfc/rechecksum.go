/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/fs"
)

type recksumctx struct {
	xrcksum *xactRechecksum
	t       *targetrunner
	fs      string
	thrctx  throttleContext
}

// TODO:
// 1) support for adding targets in the middle of re-checksumming
// 2) querying state/status of rechecksumming
// 3) losing mountpath in the middle of re-checksumming

// runRechecksumBucket traverses all objects in a bucket
func (t *targetrunner) runRechecksumBucket(bucket string) {
	// check if re-checksumming of a given bucket is currently being performed
	xrcksum := t.xactinp.renewRechecksum(t, bucket)
	if xrcksum == nil {
		return
	}

	// re-checksum every object in a given bucket
	glog.Infof("Re-checksum: %s started: bucket: %s", xrcksum.tostring(), bucket)
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	wg := &sync.WaitGroup{}
	for _, mpathInfo := range availablePaths {
		wg.Add(1)
		go func(mpathInfo *fs.MountpathInfo) {
			t.oneRechecksumBucket(mpathInfo, makePathLocal(mpathInfo.Path), xrcksum)
			wg.Done()
		}(mpathInfo)
	}
	wg.Wait()
	for _, mpathInfo := range availablePaths {
		wg.Add(1)
		go func(mpathInfo *fs.MountpathInfo) {
			t.oneRechecksumBucket(mpathInfo, makePathCloud(mpathInfo.Path), xrcksum)
			wg.Done()
		}(mpathInfo)
	}
	wg.Wait()

	// finish up
	xrcksum.etime = time.Now()
	glog.Infoln(xrcksum.tostring())
	t.xactinp.del(xrcksum.id)
}

func (t *targetrunner) oneRechecksumBucket(mpathInfo *fs.MountpathInfo, bucketDir string, xrcksum *xactRechecksum) {
	rcksctx := &recksumctx{
		xrcksum: xrcksum,
		t:       t,
		fs:      mpathInfo.FileSystem,
	}
	if err := filepath.Walk(bucketDir, rcksctx.walkFunc); err != nil {
		glog.Errorf("failed to traverse %q, error: %v", bucketDir, err)
	}
}

func (rcksctx *recksumctx) walkFunc(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		glog.Errorf("rechecksum walk function callback invoked with error: %v", err)
		return err
	}
	if osfi.IsDir() {
		return nil
	}
	if isWork, isOld := rcksctx.t.isworkfile(fqn); isWork && !isOld {
		return nil
	}

	rcksctx.thrctx.throttle(rcksctx.newRechecksumThrottleParams())

	// stop traversing if xaction is aborted
	select {
	case <-rcksctx.xrcksum.abrt:
		glog.Infof("%s aborted, exiting rechecksum walk function", rcksctx.xrcksum.tostring())
		glog.Flush()
		return errors.New("rechecksumming aborted") // returning error stops bucket directory traversal
	case <-time.After(time.Millisecond):
		break
	}

	file, err := os.Open(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			glog.Infof("Warning (file evicted?): %s", fqn)
			return nil
		}
		glog.Warningf("failed to open %q, error: %v", fqn, err)
		rcksctx.t.fshc(err, fqn) // call filesystem health checker on file
		return err
	}
	defer file.Close()

	xxHashBinary, errstr := Getxattr(fqn, api.XattrXXHashVal)
	if xxHashBinary != nil && errstr != "" {
		// checksum already there, no need to compute a new one
		return nil
	} else if errstr != "" {
		ioerr := errors.New(errstr)
		glog.Warningf("failed to get attribute %s for file %s, error: %v", api.XattrXXHashVal, fqn, ioerr)
		rcksctx.t.fshc(ioerr, fqn)
		return ioerr
	}

	buf, slab := gmem2.AllocFromSlab2(osfi.Size())
	xxHashVal, errstr := common.ComputeXXHash(file, buf)
	slab.Free(buf)
	if errstr != "" {
		glog.Warningf("failed to calculate hash on %s, error: %s", fqn, errstr)
		return errors.New(errstr)
	}
	if errstr = Setxattr(fqn, api.XattrXXHashVal, []byte(xxHashVal)); errstr != "" {
		ioerr := errors.New(errstr)
		glog.Warningf("failed to set attribute %s for file %s, error: %v", api.XattrXXHashVal, fqn, ioerr)
		rcksctx.t.fshc(ioerr, fqn)
		return ioerr
	}
	return nil
}

func (rcksctx *recksumctx) newRechecksumThrottleParams() *throttleParams {
	return &throttleParams{
		throttle: onDiskUtil,
		fs:       rcksctx.fs,
	}
}

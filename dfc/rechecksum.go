// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/fs"
)

type recksumctx struct {
	xrcksum *xactRechecksum
	t       *targetrunner
	fs      string
}

// TODO: Smap and mpath changes, fspath runner, progress bar, and more...

// runRechecksumBucket traverses all objects in a bucket
func (t *targetrunner) runRechecksumBucket(bucket string) {
	// check if re-checksumming of a given bucket is currently being performed
	xrcksum := t.xactions.renewRechecksum(bucket)
	if xrcksum == nil {
		return
	}

	// re-checksum every object in a given bucket
	glog.Infof("Re-checksum: %s started: bucket: %s", xrcksum, bucket)
	availablePaths, _ := fs.Mountpaths.Get()
	for contentType, contentResolver := range fs.CSM.RegisteredContentTypes {
		if !contentResolver.PermToProcess() { // FIXME: PermToRechecksum?
			continue
		}

		wg := &sync.WaitGroup{}
		for _, mpathInfo := range availablePaths {
			wg.Add(1)
			go func(mpathInfo *fs.MountpathInfo) {
				t.oneRechecksumBucket(mpathInfo, fs.Mountpaths.MakePathLocal(mpathInfo.Path, contentType), xrcksum)
				wg.Done()
			}(mpathInfo)
		}
		wg.Wait()
		for _, mpathInfo := range availablePaths {
			wg.Add(1)
			go func(mpathInfo *fs.MountpathInfo) {
				t.oneRechecksumBucket(mpathInfo, fs.Mountpaths.MakePathCloud(mpathInfo.Path, contentType), xrcksum)
				wg.Done()
			}(mpathInfo)
		}
		wg.Wait()
	}

	// finish up
	xrcksum.EndTime(time.Now())
}

func (t *targetrunner) oneRechecksumBucket(mpathInfo *fs.MountpathInfo, bucketDir string, xrcksum *xactRechecksum) {
	rcksctx := &recksumctx{
		xrcksum: xrcksum,
		t:       t,
		fs:      mpathInfo.FileSystem,
	}

	if err := filepath.Walk(bucketDir, rcksctx.walk); err != nil {
		glog.Errorf("failed to traverse %q, error: %v", bucketDir, err)
	}
}

func (rcksctx *recksumctx) walk(fqn string, osfi os.FileInfo, err error) error {
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
	if _, isOld := fs.CSM.PermToEvict(fqn); isOld {
		return nil
	}

	// FIXME: throttle self - see lru

	// stop traversing if xaction is aborted
	select {
	case <-rcksctx.xrcksum.ChanAbort():
		return errors.New("rechecksumming aborted") // returning error stops bucket directory traversal
	case <-time.After(time.Millisecond):
		break
	}

	// TODO: future scrubber to |cluster.LomCksumPresentRecomp, and scrub
	lom := &cluster.LOM{Fqn: fqn, Size: osfi.Size()}
	_ = lom.Fill(cluster.LomCksum | cluster.LomCksumMissingRecomp)
	return nil
}

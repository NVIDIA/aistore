// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/glog"
)

func (t *targetrunner) runRebalance() {
	xreb := t.xactinp.renewRebalance(t.smap.Version, t)
	if xreb == nil {
		return
	}
	glog.Infoln(xreb.tostring())
	for mpath := range ctx.mountpaths {
		aborted := t.oneRebalance(mpath+"/"+ctx.config.CloudBuckets, xreb)
		if aborted {
			break
		}
		aborted = t.oneRebalance(mpath+"/"+ctx.config.LocalBuckets, xreb)
		if aborted {
			break
		}
	}
	xreb.etime = time.Now()
	glog.Infoln(xreb.tostring())
	t.xactinp.del(xreb.id)
}

func (t *targetrunner) oneRebalance(mpath string, xreb *xactRebalance) bool {
	if err := filepath.Walk(mpath, xreb.rewalkf); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping mpath %q traversal: %s", mpath, s)
		} else {
			glog.Errorf("Failed to traverse mpath %q, err: %v", mpath, err)
		}
		return true
	}
	return false
}

// the walking callback is execited by the LRU xaction
// (notice the receiver)
func (xreb *xactRebalance) rewalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("rewalkf callback invoked with err: %v", err)
		return err
	}
	// skip system files and directories
	if osfi.Mode().IsDir() {
		return nil
	}
	// abort?
	select {
	case <-xreb.abrt:
		s := fmt.Sprintf("%s aborted, exiting rewalkf", xreb.tostring())
		glog.Infoln(s)
		glog.Flush()
		return errors.New(s)
	case <-time.After(time.Millisecond):
		break
	}
	if xreb.finished() {
		return fmt.Errorf("%s aborted - exiting rewalkf", xreb.tostring())
	}
	// rebalance this fobject maybe
	t := xreb.targetrunner
	bucket, objname, ok := t.fqn2bckobj(fqn)
	if !ok {
		return fmt.Errorf("Warning: cannot rebalance (%q => bucket=%s,object=%s) - fspath config changed?",
			fqn, bucket, objname)

	}
	si, errstr := hrwTarget(bucket+"/"+objname, t.smap)
	if errstr != "" {
		return fmt.Errorf(errstr)
	}
	if si.DaemonID != t.si.DaemonID {
		glog.Infof("rebalancing [%s %s] %s => %s", bucket, objname, t.si.DaemonID, si.DaemonID)
		if s := xreb.targetrunner.sendfile(http.MethodPut, bucket, objname, si, osfi.Size(), ""); s != "" {
			glog.Infof("Failed to rebalance [%s %s]: %s", bucket, objname, s)
		} else {
			// FIXME: TODO: delay the removal or (even) rely on the LRU
			if err := os.Remove(fqn); err != nil {
				glog.Errorf("Failed to delete the file %s that has moved, err: %v", fqn, err)
			}
		}
	}
	return nil
}

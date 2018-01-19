/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"errors"
	"fmt"
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
	glog.Infof("%s started", xreb.tostring())
	for _, mountpath := range ctx.mountpaths {
		aborted := t.oneRebalance(mountpath.Path, xreb)
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
	if strings.HasPrefix(osfi.Name(), ".") || osfi.Mode().IsDir() {
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
		return errors.New(fmt.Sprintf("%s aborted - exiting rewalkf", xreb.tostring()))
	}

	// rebalance this fobject maybe
	t := xreb.targetrunner
	mpath, bucket, objname := t.splitfqn(fqn)
	si := hrwTarget(bucket+"/"+objname, t.smap)
	if si.DaemonID != t.si.DaemonID {
		glog.Infof("[%s %s %s] must be rebalanced from %s to %s", mpath, bucket, objname, t.si.DaemonID, si.DaemonID)
		glog.Flush()
		return nil
	}
	return nil
}

/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/glog"
)

func (t *targetrunner) runRebalance() {
	xact := t.xactinp.renewRebalance(t.smap.Version)
	if xact == nil {
		return
	}
	glog.Infof("%s started", xact.tostring())
	for _, mountpath := range ctx.mountpaths {
		aborted := t.oneRebalance(mountpath.Path, xact)
		if aborted {
			break
		}
	}
	xact.etime = time.Now()
	glog.Infof("%s finished", xact.tostring())
	t.xactinp.del(xact)
}

func (t *targetrunner) oneRebalance(mpath string, xact *xactRebalance) bool {
	if err := filepath.Walk(mpath, t.rewalkf); err != nil {
		glog.Errorf("Failed to traverse mpath %q, err: %v", mpath, err)
		return true
	}
	return false
}

func (t *targetrunner) rewalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("walkfunc callback invoked with err: %v", err)
		return err
	}
	// skip system files and directories
	if strings.HasPrefix(osfi.Name(), ".") || osfi.Mode().IsDir() {
		return nil
	}
	mpath, bucket, objname := t.splitfqn(fqn)
	si := hrwTarget(bucket+"/"+objname, t.smap)
	if si.DaemonID != t.si.DaemonID {
		glog.Infoln("[%s %s %s] must be rebalanced from %s to %s", mpath, bucket, objname, t.si.DaemonID, si.DaemonID)
	}
	return nil
}

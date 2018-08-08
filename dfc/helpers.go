/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

// enableMountpath enables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func enableMountpath(mountpath string) (enabled, exists bool) {
	enabled, exists = ctx.mountpaths.EnableMountpath(mountpath)
	if !enabled || !exists {
		return enabled, exists
	}

	t := gettarget()
	getiostatrunner().updateFSDisks()
	t.runRebalance(t.smapowner.get(), "")
	glog.Infof("Reenabled mountpath %s", mountpath)

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	if len(availablePaths) == 1 {
		if err := t.enable(); err != nil {
			glog.Errorf("The mountpath was enabled but registering target failed: %v", err)
		}
	}

	return enabled, exists
}

// disableMountpath disables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func disableMountpath(mountpath string) (disabled, exists bool) {
	disabled, exists = ctx.mountpaths.DisableMountpath(mountpath)
	if !disabled || !exists {
		return disabled, exists
	}

	getiostatrunner().updateFSDisks()
	glog.Infof("Disabled mountpath %s", mountpath)

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	if len(availablePaths) > 0 {
		return disabled, exists
	}

	glog.Warningf("The last available mountpath was disabled. Unregistering the target")
	t := gettarget()
	if err := t.disable(); err != nil {
		glog.Errorf("Failed to unregister target %s, error: %v", t.si.DaemonID, err)
	}

	return disabled, exists
}

// addMountpath adds mountpath and notifies necessary runners about the change
// if mountpath was actually added.
func addMountpath(mountpath string) error {
	err := ctx.mountpaths.AddMountpath(mountpath)
	if err != nil {
		return err
	}

	t := gettarget()
	getfshealthchecker().RequestAddMountpath(mountpath)
	getiostatrunner().updateFSDisks()
	t.runRebalance(t.smapowner.get(), "")
	glog.Infof("Added mountpath %s", mountpath)

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	if len(availablePaths) == 1 {
		if err := t.enable(); err != nil {
			glog.Errorf("The mountpath was added but registering target failed: %v", err)
		}
	}

	return nil
}

// removeMountpath removes mountpath and notifies necessary runners about the
// change if mountpath was actually removed.
func removeMountpath(mountpath string) error {
	err := ctx.mountpaths.RemoveMountpath(mountpath)
	if err != nil {
		return err
	}

	getfshealthchecker().RequestRemoveMountpath(mountpath)
	getiostatrunner().updateFSDisks()
	glog.Infof("Removed mountpath %s", mountpath)

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	if len(availablePaths) == 0 {
		t := gettarget()
		if err := t.disable(); err != nil {
			glog.Errorf("The last mountpath was removed but unregistering target %s failed: %v", t.si.DaemonID, err)
		}
	}

	return nil
}

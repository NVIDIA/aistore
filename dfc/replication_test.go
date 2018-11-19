/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/fs"
)

const (
	fakeDaemonID = "fakeDaemonId"
)

func TestReplicationRunnerStop(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS()
	tr := newFakeTargetRunner()
	rr := newReplicationRunner(tr, fs.Mountpaths)
	go rr.Run()

	rr.Stop(fmt.Errorf("Testing replicationRunner.stop"))

	waitCh := make(chan struct{})
	go func() {
		rr.reqSendReplica("fakeDirectURL", "fakeFqn", false, replicationPolicySync)
		waitCh <- struct{}{}
	}()

	select {
	case <-waitCh:
		t.Error("Replication runner did not stop")
	case <-time.After(50 * time.Millisecond):
		break
	}
}

func TestReplicationSendNonExistingObject(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS()
	cleanMountpaths()

	tr := newFakeTargetRunner()
	rr := newReplicationRunner(tr, fs.Mountpaths)
	go rr.Run()

	err := rr.reqSendReplica("fakeDirectURL", "fakeFqn", false, replicationPolicySync)
	if err == nil {
		t.Error("Send operation should fail on non-existing file")
	}

	rr.Stop(fmt.Errorf("Testing replicationRunner.sendReplica sync"))
}

// newFakeTargetRunner returns a fake targetrunner initialized for replication tests
func newFakeTargetRunner() *targetrunner {
	t := &targetrunner{}
	t.si = newSnode(fakeDaemonID, httpProto, &net.TCPAddr{}, &net.TCPAddr{}, &net.TCPAddr{})
	return t
}

func cleanMountpaths() {
	availableMountpaths, disabledMountpaths := fs.Mountpaths.Get()
	for _, mpathInfo := range availableMountpaths {
		fs.Mountpaths.Remove(mpathInfo.Path)
	}
	for _, mpathInfo := range disabledMountpaths {
		fs.Mountpaths.Remove(mpathInfo.Path)
	}
}

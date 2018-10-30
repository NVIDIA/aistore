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
	ctx.mountpaths = fs.NewMountedFS(ctx.config.CloudBuckets, ctx.config.LocalBuckets)
	tr := newFakeTargetRunner()
	rr := newReplicationRunner(tr, ctx.mountpaths)
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
	ctx.mountpaths = fs.NewMountedFS(ctx.config.CloudBuckets, ctx.config.LocalBuckets)
	cleanMountpaths()

	tr := newFakeTargetRunner()
	rr := newReplicationRunner(tr, ctx.mountpaths)
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

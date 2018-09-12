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

func TestReplicationRunnerStop(t *testing.T) {
	tr := newFakeTargetrunner()
	rr := newReplicationRunner(tr)
	go rr.run()

	rr.stop(fmt.Errorf("Testing replicationRunner.stop"))

	waitCh := make(chan struct{})
	go func() {
		rr.sendReplica("fakeDirectURL", "fakeFqn", false, replicationPolicySync)
		waitCh <- struct{}{}
	}()

	select {
	case <-waitCh:
		t.Error("Replication runner did not stop")
	case <-time.After(50 * time.Millisecond):
		break
	}
}

func TestReplicationSendNonExistingFile(t *testing.T) {
	ctx.mountpaths = fs.NewMountedFS()
	cleanMountpaths()

	tr := newFakeTargetrunner()
	rr := newReplicationRunner(tr)
	go rr.run()

	err := rr.sendReplica("fakeDirectURL", "fakeFqn", false, replicationPolicySync)
	if err == nil {
		t.Error("Send operation should fail on non-existing file")
	}

	rr.stop(fmt.Errorf("Testing replicationRunner.sendReplica sync"))
}

// newFakeTargetrunner returns a fake targetrunner initialized for replication tests
func newFakeTargetrunner() *targetrunner {
	t := &targetrunner{}
	t.si = newDaemonInfo("fake_id", httpProto, &net.TCPAddr{}, &net.TCPAddr{}, &net.TCPAddr{})
	return t
}

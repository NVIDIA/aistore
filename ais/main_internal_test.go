// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"flag"
	"net/http"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
)

var (
	mockTarget *target

	// interface guard
	_ http.ResponseWriter = (*discardRW)(nil)
)

func TestMain(m *testing.M) {
	flag.Parse()

	// file system
	cos.CreateDir(testMountpath)
	defer os.RemoveAll(testMountpath)
	fs.NewTestMFS(nil)

	// config
	config := cmn.GCO.BeginUpdate()
	config.HostNet.Hostname = "localhost"
	config.HostNet.Port = 8080
	config.HostNet.HostnameIntraControl = "localhost"
	config.HostNet.PortIntraControl = 9080
	config.Log.Level = "3"
	debug.AssertNoErr(config.HostNet.Validate(config))
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)

	co := newConfigOwner(config)

	// target
	t := newTarget(co)
	t.initPhase1(config)
	tid, _ := initTID(config)
	t.si.Init(tid, apc.Target, nil /*verifying key*/)

	fs.AddTestMpath(testMountpath, t.SID())

	t.htrun.initPhase2(config)
	t.ups.t = t

	t.statsT = mock.NewStatsTracker()
	core.Tinit(t, config, false)

	bck := meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)
	bmd := newBucketMD()
	bmd.add(bck, &cmn.Bprops{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumNone,
		},
	})
	t.owner.bmd.putPersist(bmd, nil)
	fs.CreateBucket(bck.Bucket(), false /*nilbmd*/)

	t.owner.smap.put(newSmap()) // Note: required by ais/txn_internal_test.go

	mockTarget = t
	m.Run()
}

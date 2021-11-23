// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestSmoke(t *testing.T) {
	objSizes := [3]uint64{3 * cos.KiB, 19 * cos.KiB, 77 * cos.KiB}

	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		if bck.IsCloud() && bck.RemoteBck().Provider == cmn.ProviderGoogle {
			t.Skip("GCP fails intermittently when overloaded with requests - skipping")
		}
		for _, objSize := range objSizes {
			name := fmt.Sprintf("size:%s", cos.B2S(int64(objSize), 0))
			t.Run(name, func(t *testing.T) {
				m := ioContext{
					t:        t,
					bck:      bck.Bck,
					num:      100,
					fileSize: objSize,
					prefix:   "smoke/obj-",
				}

				if !testing.Short() {
					m.num = 1000
				}

				m.initWithCleanup()

				m.puts()
				m.gets()
				m.del()
			})
		}
	})
}

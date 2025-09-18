// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

func TestSmoke(t *testing.T) { _wrd(t, 0) }
func TestFntl(t *testing.T)  { _wrd(t, 280) }

func _wrd(t *testing.T, nameLen int) {
	objSizes := [...]uint64{3 * cos.KiB, 19 * cos.KiB, 77 * cos.KiB}

	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		for _, chunked := range []bool{false, true} {
			for _, objSize := range objSizes {
				tname := "size:" + cos.ToSizeIEC(int64(objSize), 0)
				m := ioContext{
					t:        t,
					bck:      bck.Clone(),
					num:      100,
					nameLen:  nameLen,
					fileSize: objSize,
					prefix:   "smoke/obj-",
				}
				if chunked {
					m.chunksConf = &ioCtxChunksConf{
						numChunks: 10,
						multipart: true,
					}
					tname += "/chunked"
				}
				t.Run(tname, func(_ *testing.T) {
					if bck.IsAIS() || bck.IsRemoteAIS() {
						m.num = 1000
					}

					m.init(true /*cleanup*/)

					m.puts()
					m.gets(nil, false)
					m.del()
				})
			}
		}
	})
}

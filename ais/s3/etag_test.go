// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */

package s3 //nolint:testpackage // We use private functions here...

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var _ = Describe("ETag", func() {
	Describe("entryToS3 ETag behavior", func() {
		It("Should use custom ETag when available", func() {
			entry := &cmn.LsoEnt{
				Name:     "test-object",
				Checksum: "d41d8cd98f00b204e9800998ecf8427e",
				Custom:   cmn.CustomMD2S(cos.StrKVs{cmn.ETag: `"existing-etag"`}),
			}

			objInfo := entryToS3(entry, &apc.LsoMsg{})
			Expect(objInfo.ETag).To(Equal(`"existing-etag"`))
		})

		It("Should fallback to checksum when no existing ETag", func() {
			entry := &cmn.LsoEnt{
				Name:     "test-object",
				Checksum: "d41d8cd98f00b204e9800998ecf8427e",
				Custom:   "",
			}

			objInfo := entryToS3(entry, &apc.LsoMsg{})
			Expect(objInfo.ETag).To(Equal(`d41d8cd98f00b204e9800998ecf8427e`))
		})
	})
})

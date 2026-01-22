// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mirror_test

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mirror", func() {
	const (
		testDir = "/tmp/mirror-test_q/"

		testBucketName = "TEST_LOCAL_MIRROR_BUCKET"
		mpath          = testDir + "mirrortest_mpath/111"
		mpath2         = testDir + "mirrortest_mpath/222"

		testObjectName = "mirrortestobj.ext"
		testObjectSize = 1234
	)

	_ = cos.CreateDir(mpath)
	_ = cos.CreateDir(mpath2)

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.TestNew(nil)
	_, _ = fs.Add(mpath, "daeID")
	_, _ = fs.Add(mpath2, "daeID")

	var (
		props = &cmn.Bprops{
			Cksum:  cmn.CksumConf{Type: cos.ChecksumCesXxh},
			LRU:    cmn.LRUConf{Enabled: true},
			Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
			BID:    1,
		}
		bck             = meta.Bck{Name: testBucketName, Provider: apc.AIS, Ns: cmn.NsGlobal, Props: props}
		bmdMock         = mock.NewBaseBownerMock(&bck)
		mi              = fs.Mountpath{Path: mpath}
		mi2             = fs.Mountpath{Path: mpath2}
		bucketPath      = mi.MakePathCT(bck.Bucket(), fs.ObjCT)
		defaultObjFQN   = mi.MakePathFQN(bck.Bucket(), fs.ObjCT, testObjectName)
		expectedCopyFQN = mi2.MakePathFQN(bck.Bucket(), fs.ObjCT, testObjectName)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(mpath)
		_ = cos.CreateDir(mpath2)
		_ = mock.NewTarget(bmdMock)
	})

	AfterEach(func() {
		_ = os.RemoveAll(testDir)
	})

	// NOTE:
	// the test creates copies; there's a built-in assumption that `mi` will be
	// the HRW mountpath,
	// while `mi2` will not (and, therefore, can be used to place the copy).
	// Ultimately, this depends on the specific HRW hash; adding
	// Expect(lom.IsHRW()).To(BeTrue()) to catch that sooner.

	Describe("copyTo", func() {
		It("should copy correctly object and set xattrs", func() {
			createTestFile(bucketPath, testObjectName, testObjectSize)
			lom := newBasicLom(defaultObjFQN)
			Expect(lom.IsHRW()).To(BeTrue())
			lom.SetSize(testObjectSize)
			lom.SetAtimeUnix(time.Now().UnixNano())
			Expect(lom.Persist()).NotTo(HaveOccurred())
			Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())

			// Make copy
			lom.Lock(true)
			defer lom.Unlock(true)
			copyFQN := mi2.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
			clone, err := lom.Copy2FQN(copyFQN, nil)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(expectedCopyFQN).To(BeARegularFile())
			Expect(clone.Lsize(true)).To(BeEquivalentTo(testObjectSize))

			// Check copy set
			Expect(clone.IsCopy()).To(BeTrue())
			Expect(clone.NumCopies()).To(Equal(2))
			Expect(clone.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			/*
			 * Reload default LOM and copyLOM
			 */

			// Check reloaded default LOM
			newLOM := newBasicLom(defaultObjFQN)
			Expect(newLOM.IsHRW()).To(BeTrue())
			Expect(newLOM.Load(false, true)).ShouldNot(HaveOccurred())
			Expect(newLOM.IsCopy()).To(BeFalse())
			Expect(newLOM.HasCopies()).To(BeTrue())
			Expect(newLOM.NumCopies()).To(Equal(2))
			Expect(newLOM.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			// Check reloaded copyLOM
			copyLOM := newBasicLom(expectedCopyFQN)
			Expect(copyLOM.Load(false, false)).ShouldNot(HaveOccurred())
			copyCksum, err := copyLOM.ComputeSetCksum(true)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(copyCksum.Value()).To(Equal(newLOM.Checksum().Value()))
			Expect(copyLOM.IsHRW()).To(BeFalse())
			Expect(copyLOM.IsCopy()).To(BeTrue())
			Expect(copyLOM.HasCopies()).To(BeTrue())
		})

		It("should correctly copy chunked mirror object", func() {
			// Create chunked object
			lom := createChunkedMirrorLOM(defaultObjFQN, 2)
			Expect(lom.IsHRW()).To(BeTrue())
			Expect(lom.IsChunked()).To(BeTrue())

			// Make copy
			lom.Lock(true)
			defer lom.Unlock(true)
			copyFQN := mi2.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
			clone, err := lom.Copy2FQN(copyFQN, nil)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(expectedCopyFQN).To(BeARegularFile())

			// Verify chunked copy
			Expect(clone.IsChunked()).To(BeTrue())
			Expect(clone.IsCopy()).To(BeTrue())

			// Verify chunks were copied
			srcUfest, err := core.NewUfest("", lom, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(srcUfest.LoadCompleted(lom)).NotTo(HaveOccurred())

			dstUfest, err := core.NewUfest("", clone, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(dstUfest.LoadCompleted(clone)).NotTo(HaveOccurred())

			Expect(srcUfest.Count()).To(Equal(dstUfest.Count()))

			// Verify individual chunks were copied correctly
			for i := 1; i <= srcUfest.Count(); i++ {
				srcChunk, err := srcUfest.GetChunk(i)
				Expect(err).NotTo(HaveOccurred())
				dstChunk, err := dstUfest.GetChunk(i)
				Expect(err).NotTo(HaveOccurred())
				Expect(srcChunk).NotTo(BeNil())
				Expect(dstChunk).NotTo(BeNil())
				Expect(srcChunk.Path()).To(BeARegularFile())
				Expect(dstChunk.Path()).To(BeARegularFile())
			}

			// Final validation: Compare full object content using lom.Open() readers
			srcReader, err := lom.Open()
			Expect(err).NotTo(HaveOccurred())
			defer srcReader.Close()

			dstReader, err := clone.Open()
			Expect(err).NotTo(HaveOccurred())
			defer dstReader.Close()

			// Read and compare entire content
			srcContent, err := io.ReadAll(srcReader)
			Expect(err).NotTo(HaveOccurred())
			dstContent, err := io.ReadAll(dstReader)
			Expect(err).NotTo(HaveOccurred())

			Expect(len(srcContent)).To(Equal(len(dstContent)))
			Expect(srcContent).To(Equal(dstContent))
		})
	})
})

func createTestFile(filePath, objName string, size int64) {
	err := cos.CreateDir(filePath)
	Expect(err).ShouldNot(HaveOccurred())

	r, err := readers.New(&readers.Arg{
		Path:      filePath,
		Name:      objName,
		Type:      readers.File,
		Size:      size,
		CksumType: cos.ChecksumNone,
	})

	Expect(err).ShouldNot(HaveOccurred())
	Expect(r.Close()).ShouldNot(HaveOccurred())
}

func newBasicLom(fqn string) *core.LOM {
	lom := &core.LOM{}
	err := lom.InitFQN(fqn, nil)
	Expect(err).NotTo(HaveOccurred())
	lom.UncacheUnless()
	return lom
}

// createChunkedMirrorLOM helper for creating chunked mirror objects
func createChunkedMirrorLOM(fqn string, numChunks int) *core.LOM {
	const chunkSize = 20 * cos.KiB

	lom := &core.LOM{}
	err := lom.InitFQN(fqn, nil)
	Expect(err).NotTo(HaveOccurred())

	totalSize := int64(numChunks * chunkSize)
	lom.SetSize(totalSize)
	lom.SetAtimeUnix(time.Now().UnixNano())

	// Create Ufest for chunked upload
	ufest, err := core.NewUfest("", lom, false)
	Expect(err).NotTo(HaveOccurred())

	// Create chunks
	for i := 1; i <= numChunks; i++ {
		chunk, err := ufest.NewChunk(i, lom)
		Expect(err).NotTo(HaveOccurred())

		// Create chunk file directly
		chunkFQN := chunk.Path()
		err = cos.CreateDir(filepath.Dir(chunkFQN))
		Expect(err).NotTo(HaveOccurred())

		r, err := readers.New(&readers.Arg{
			Path:      filepath.Dir(chunkFQN),
			Name:      filepath.Base(chunkFQN),
			Type:      readers.File,
			Size:      chunkSize,
			CksumType: cos.ChecksumNone,
		})

		Expect(err).ShouldNot(HaveOccurred())
		Expect(r.Close()).ShouldNot(HaveOccurred())

		err = ufest.Add(chunk, int64(chunkSize), int64(i))
		Expect(err).NotTo(HaveOccurred())
	}

	// Complete the Ufest - this handles chunked flag setting and persistence internally
	err = lom.CompleteUfest(ufest, false)
	Expect(err).NotTo(HaveOccurred())

	// Reload to pick up chunked flag
	lom.UncacheUnless()
	Expect(lom.Load(false, false)).NotTo(HaveOccurred())
	Expect(lom.IsChunked()).To(BeTrue())
	return lom
}

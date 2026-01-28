// Package integration_test provides integration tests for AIStore.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

// computeCRC32C computes CRC32C checksum of data
func computeCRC32C(data []byte) *cos.Cksum {
	h := cos.NewCksumHash(cos.ChecksumCRC32C)
	h.H.Write(data)
	h.Finalize()
	return &h.Cksum
}

// computeXXHash computes XXHash checksum of data
func computeXXHash(data []byte) *cos.Cksum {
	h := cos.NewCksumHash(cos.ChecksumOneXxh)
	h.H.Write(data)
	h.Finalize()
	return &h.Cksum
}

// generatePartData generates deterministic part data
func generatePartData(partNum, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((partNum*17 + i) % 256)
	}
	return data
}

// TestMultipartChecksumSequential tests that sequential uploads use streaming checksum
// (bucket-configured type) when parts arrive in order.
func TestMultipartChecksumSequential(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-mpu-cksum-sequential"

		partSize = 64 * cos.KiB
		numParts = 3
	)

	// Generate parts
	parts := make([][]byte, numParts)
	var expectedContent []byte
	for i := range parts {
		parts[i] = generatePartData(i+1, partSize)
		expectedContent = append(expectedContent, parts[i]...)
	}

	// Create bucket with XXHash checksum type
	bprops := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumOneXxh)},
	}
	tools.CreateBucket(t, proxyURL, bck, bprops, true /*cleanup*/)

	tlog.Logfln("sequential multipart upload with checksum validation: %s/%s", bck.Name, objName)

	// Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	// Upload parts SEQUENTIALLY (wait for each to complete)
	for i, data := range parts {
		partNum := i + 1
		putPartArgs := &api.PutPartArgs{
			PutArgs: api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    objName,
				Reader:     readers.NewBytes(data),
				Size:       uint64(len(data)),
			},
			UploadID:   uploadID,
			PartNumber: partNum,
		}
		err = api.UploadPart(putPartArgs)
		tassert.CheckFatal(t, err)
		tlog.Logfln("  uploaded part %d sequentially", partNum)
	}

	// Complete multipart upload
	partNumbers := make([]int, numParts)
	for i := range partNumbers {
		partNumbers[i] = i + 1
	}
	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
	tassert.CheckFatal(t, err)

	// Verify object attributes
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	objProps, err := api.HeadObject(baseParams, bck, objName, hargs)
	tassert.CheckFatal(t, err)

	actualCksum := objProps.ObjAttrs.Checksum()
	tlog.Logfln("  checksum type: %s, value: %s", actualCksum.Ty(), actualCksum.Val())

	// For sequential upload, we MUST get the bucket-configured checksum type (XXHash)
	// because parts arrived in order and streaming checksum should be used
	tassert.Fatalf(t, actualCksum.Ty() == cos.ChecksumOneXxh,
		"sequential upload should use streaming checksum (expected %s, got %s)", cos.ChecksumOneXxh, actualCksum.Ty())

	// Validate checksum value
	expectedCksum := computeXXHash(expectedContent)
	tassert.Fatalf(t, actualCksum.Val() == expectedCksum.Val(),
		"checksum value mismatch: expected %s, got %s", expectedCksum.Val(), actualCksum.Val())

	// Verify content via download with checksum validation
	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObjectWithValidation(baseParams, bck, objName, &getArgs)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, bytes.Equal(writer.Bytes(), expectedContent),
		"content mismatch after download")

	tlog.Logfln("sequential multipart upload checksum validation passed")
}

// TestMultipartChecksumParallel tests that parallel uploads fall back to CRC32C
// when parts arrive out of order.
func TestMultipartChecksumParallel(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-mpu-cksum-parallel"

		partSize = 64 * cos.KiB
		numParts = 20 // High parallelism to ensure out-of-order arrival
	)

	// Generate parts
	parts := make([][]byte, numParts)
	var expectedContent []byte
	for i := range parts {
		parts[i] = generatePartData(i+1, partSize)
		expectedContent = append(expectedContent, parts[i]...)
	}

	// Create bucket with XXHash checksum type
	bprops := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumOneXxh)},
	}
	tools.CreateBucket(t, proxyURL, bck, bprops, true /*cleanup*/)

	tlog.Logfln("parallel multipart upload with checksum validation: %s/%s (%d parts)", bck.Name, objName, numParts)

	// Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	// Upload ALL parts simultaneously to guarantee out-of-order arrival
	var wg sync.WaitGroup
	errCh := make(chan error, numParts)
	startCh := make(chan struct{}) // Barrier to start all goroutines at once

	for partNum := 1; partNum <= numParts; partNum++ {
		wg.Add(1)
		go func(pNum int, data []byte) {
			defer wg.Done()
			<-startCh // Wait for barrier

			putPartArgs := &api.PutPartArgs{
				PutArgs: api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    objName,
					Reader:     readers.NewBytes(data),
					Size:       uint64(len(data)),
				},
				UploadID:   uploadID,
				PartNumber: pNum,
			}
			if err := api.UploadPart(putPartArgs); err != nil {
				errCh <- err
			}
		}(partNum, parts[partNum-1])
	}

	close(startCh) // Release all goroutines simultaneously
	wg.Wait()
	close(errCh)

	for err := range errCh {
		tassert.CheckFatal(t, err)
	}

	// Complete multipart upload
	partNumbers := make([]int, numParts)
	for i := range partNumbers {
		partNumbers[i] = i + 1
	}
	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
	tassert.CheckFatal(t, err)

	// Verify object attributes
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	objProps, err := api.HeadObject(baseParams, bck, objName, hargs)
	tassert.CheckFatal(t, err)

	actualCksum := objProps.ObjAttrs.Checksum()
	tlog.Logfln("  checksum type: %s, value: %s", actualCksum.Ty(), actualCksum.Val())

	// Parallel uploads MUST fall back to CRC32C - this is the whole point of this test
	tassert.Fatalf(t, actualCksum.Ty() == cos.ChecksumCRC32C,
		"parallel upload must fall back to CRC32C, got %s (streaming checksum should have been abandoned)", actualCksum.Ty())

	// Validate CRC32C checksum value
	expectedCksum := computeCRC32C(expectedContent)
	tassert.Fatalf(t, actualCksum.Val() == expectedCksum.Val(),
		"CRC32C checksum value mismatch: expected %s, got %s", expectedCksum.Val(), actualCksum.Val())

	// Verify content
	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObjectWithValidation(baseParams, bck, objName, &getArgs)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, bytes.Equal(writer.Bytes(), expectedContent),
		"content mismatch after download")

	tlog.Logfln("parallel multipart upload correctly fell back to CRC32C checksum")
}

// TestMultipartChecksumSinglePart tests checksum for single-part multipart upload.
func TestMultipartChecksumSinglePart(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-mpu-cksum-single"

		partData = generatePartData(1, 128*cos.KiB)
	)

	// Create bucket with CRC32C checksum type
	bprops := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumCRC32C)},
	}
	tools.CreateBucket(t, proxyURL, bck, bprops, true /*cleanup*/)

	tlog.Logfln("single-part multipart upload: %s/%s", bck.Name, objName)

	// Create and complete single-part upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	putPartArgs := &api.PutPartArgs{
		PutArgs: api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Reader:     readers.NewBytes(partData),
			Size:       uint64(len(partData)),
		},
		UploadID:   uploadID,
		PartNumber: 1,
	}
	err = api.UploadPart(putPartArgs)
	tassert.CheckFatal(t, err)

	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, []int{1})
	tassert.CheckFatal(t, err)

	// Verify checksum
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	objProps, err := api.HeadObject(baseParams, bck, objName, hargs)
	tassert.CheckFatal(t, err)

	actualCksum := objProps.ObjAttrs.Checksum()
	tlog.Logfln("  checksum type: %s, value: %s", actualCksum.Ty(), actualCksum.Val())

	// Single part upload should use streaming checksum (bucket-configured type: CRC32C)
	tassert.Fatalf(t, actualCksum.Ty() == cos.ChecksumCRC32C,
		"single-part upload should use streaming checksum (expected %s, got %s)", cos.ChecksumCRC32C, actualCksum.Ty())

	// Validate CRC32C value
	expectedCksum := computeCRC32C(partData)
	tassert.Fatalf(t, actualCksum.Val() == expectedCksum.Val(),
		"checksum value mismatch: expected %s, got %s", expectedCksum.Val(), actualCksum.Val())

	// Validate via download
	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObjectWithValidation(baseParams, bck, objName, &getArgs)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, bytes.Equal(writer.Bytes(), partData),
		"content mismatch")

	tlog.Logfln("single-part checksum validation passed (type: %s)", actualCksum.Ty())
}

// TestMultipartChecksumLargeParts tests checksum with larger parts.
func TestMultipartChecksumLargeParts(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-mpu-cksum-large"

		// Use larger parts (5MB each)
		partSize = 5 * cos.MiB
		numParts = 3
	)

	// Create bucket with CRC32C checksum type
	bprops := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumCRC32C)},
	}
	tools.CreateBucket(t, proxyURL, bck, bprops, true /*cleanup*/)

	tlog.Logfln("large parts multipart upload: %s/%s (%d parts x %s)",
		bck.Name, objName, numParts, cos.ToSizeIEC(int64(partSize), 0))

	// Generate parts
	parts := make([][]byte, numParts)
	var expectedContent []byte
	for i := range parts {
		parts[i] = generatePartData(i+1, partSize)
		expectedContent = append(expectedContent, parts[i]...)
	}

	// Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	// Upload parts sequentially
	for i, data := range parts {
		partNum := i + 1
		putPartArgs := &api.PutPartArgs{
			PutArgs: api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    objName,
				Reader:     readers.NewBytes(data),
				Size:       uint64(len(data)),
			},
			UploadID:   uploadID,
			PartNumber: partNum,
		}
		err = api.UploadPart(putPartArgs)
		tassert.CheckFatal(t, err)
	}

	// Complete
	partNumbers := make([]int, numParts)
	for i := range partNumbers {
		partNumbers[i] = i + 1
	}
	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
	tassert.CheckFatal(t, err)

	// Verify checksum
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	objProps, err := api.HeadObject(baseParams, bck, objName, hargs)
	tassert.CheckFatal(t, err)

	actualCksum := objProps.ObjAttrs.Checksum()
	tlog.Logfln("  checksum type: %s, value: %s", actualCksum.Ty(), actualCksum.Val())

	// Sequential upload should use streaming checksum (bucket-configured type: CRC32C)
	tassert.Fatalf(t, actualCksum.Ty() == cos.ChecksumCRC32C,
		"large parts upload should use streaming checksum (expected %s, got %s)", cos.ChecksumCRC32C, actualCksum.Ty())

	// Validate CRC32C value
	expectedCksum := computeCRC32C(expectedContent)
	tassert.Fatalf(t, actualCksum.Val() == expectedCksum.Val(),
		"CRC32C mismatch: expected %s, got %s", expectedCksum.Val(), actualCksum.Val())

	// Verify content via download
	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObjectWithValidation(baseParams, bck, objName, &getArgs)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, bytes.Equal(writer.Bytes(), expectedContent),
		"content mismatch after download")

	tlog.Logfln("large parts checksum validation passed")
}

// TestMultipartChecksumManyParts tests checksum with many small parts.
func TestMultipartChecksumManyParts(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-mpu-cksum-many"

		partSize = 32 * cos.KiB
		numParts = 20
	)

	// Create bucket
	bprops := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumOneXxh)},
	}
	tools.CreateBucket(t, proxyURL, bck, bprops, true /*cleanup*/)

	tlog.Logfln("many parts multipart upload: %s/%s (%d parts)", bck.Name, objName, numParts)

	// Generate parts
	parts := make([][]byte, numParts)
	var expectedContent []byte
	for i := range parts {
		parts[i] = generatePartData(i+1, partSize)
		expectedContent = append(expectedContent, parts[i]...)
	}

	// Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	// Upload parts sequentially
	for i, data := range parts {
		partNum := i + 1
		putPartArgs := &api.PutPartArgs{
			PutArgs: api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    objName,
				Reader:     readers.NewBytes(data),
				Size:       uint64(len(data)),
			},
			UploadID:   uploadID,
			PartNumber: partNum,
		}
		err = api.UploadPart(putPartArgs)
		tassert.CheckFatal(t, err)
	}

	// Complete
	partNumbers := make([]int, numParts)
	for i := range partNumbers {
		partNumbers[i] = i + 1
	}
	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
	tassert.CheckFatal(t, err)

	// Verify checksum
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	objProps, err := api.HeadObject(baseParams, bck, objName, hargs)
	tassert.CheckFatal(t, err)

	actualCksum := objProps.ObjAttrs.Checksum()
	tlog.Logfln("  checksum type: %s, value: %s", actualCksum.Ty(), actualCksum.Val())

	// Sequential upload should use streaming checksum (bucket-configured type: XXHash)
	tassert.Fatalf(t, actualCksum.Ty() == cos.ChecksumOneXxh,
		"many parts upload should use streaming checksum (expected %s, got %s)", cos.ChecksumOneXxh, actualCksum.Ty())

	// Validate XXHash value
	expectedCksum := computeXXHash(expectedContent)
	tassert.Fatalf(t, actualCksum.Val() == expectedCksum.Val(),
		"checksum mismatch: expected %s, got %s", expectedCksum.Val(), actualCksum.Val())

	// Verify via download
	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObjectWithValidation(baseParams, bck, objName, &getArgs)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, bytes.Equal(writer.Bytes(), expectedContent),
		"content mismatch")

	tlog.Logfln("many parts checksum validation passed (type: %s)", actualCksum.Ty())
}

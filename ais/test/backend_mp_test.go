// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
)

const (
	tempObjPrefix         = "TestMPObj-"
	tempDirPrefix         = "TestMPDir-"
	nonMPBasenamePrefix   = "nonMP-"
	smallMPBasenamePrefix = "smallMP-"
	bigMPBasenamePrefix   = "bigMP-"
	srcFileSuffix         = "srcFile"
	objectSuffix          = "object"
	dstFileSuffix         = "dstFile"
	randomDevFilePath     = "/dev/urandom"
	writeFilePerm         = 0o600
)

func TestBackendMP(t *testing.T) {
	var (
		err               error
		bucket            = os.Getenv("BUCKET")
		bck               cmn.Bck
		proxyURL          = tools.RandomProxyURL(t)
		baseParams        = tools.BaseAPIParams(proxyURL)
		nonMPFileSize     int64
		smallMPFileSize   int64
		bigMPFileSize     int64
		nonMPSrcHashSum   []byte
		smallMPSrcHashSum []byte
		bigMPSrcHashSum   []byte
		nonMPDstHashSum   []byte
		smallMPDstHashSum []byte
		bigMPDstHashSum   []byte
		tempDirPath       string
		randomDevFile     *os.File
	)

	if bucket == "" {
		t.Skipf("BUCKET ENV missing... skipping...")
	}
	if bck, _, err = cmn.ParseBckObjectURI(bucket, cmn.ParseURIOpts{}); err != nil {
		t.Fatalf("cmn.ParseBckObjectURI(bucket, cmn.ParseURIOpts{}) failed: %v", err)
	}
	if bck.Provider != apc.OCI {
		t.Skipf("BUCKET .Provider (\"%s\") skipped", bck.Provider)
	}

	if nonMPFileSize, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_SIZE_NON_MP"), ""); err != nil {
		t.Skipf("OCI_TEST_FILE_SIZE_NON_MP ENV missing or invalid... skipping...")
	}
	if smallMPFileSize, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_SIZE_SMALL_MP"), ""); err != nil {
		t.Skipf("OCI_TEST_FILE_SIZE_SMALL_MP ENV missing or invalid... skipping...")
	}
	if bigMPFileSize, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_SIZE_BIG_MP"), ""); err != nil {
		t.Skipf("OCI_TEST_FILE_SIZE_BIG_MP ENV missing or invalid... skipping...")
	}

	tempDirPath, err = os.MkdirTemp("", tempDirPrefix)
	if err != nil {
		t.Fatalf("os.MkdirTemp(\"\", tempDirPrefix) failed: %v", err)
	}
	defer os.RemoveAll(tempDirPath)

	if randomDevFile, err = os.Open(randomDevFilePath); err != nil {
		t.Fatalf("os.Open(randomDevFilePath) failed: %v", err)
	}

	if nonMPSrcHashSum, err = testMPGenerateSrcFile(
		randomDevFile,
		nonMPFileSize,
		tempDirPath+"/"+nonMPBasenamePrefix+srcFileSuffix); err != nil {
		t.Fatal(err)
	}
	if smallMPSrcHashSum, err = testMPGenerateSrcFile(
		randomDevFile,
		smallMPFileSize,
		tempDirPath+"/"+smallMPBasenamePrefix+srcFileSuffix); err != nil {
		t.Fatal(err)
	}
	if bigMPSrcHashSum, err = testMPGenerateSrcFile(
		randomDevFile,
		bigMPFileSize,
		tempDirPath+"/"+bigMPBasenamePrefix+srcFileSuffix); err != nil {
		t.Fatal(err)
	}

	if err = randomDevFile.Close(); err != nil {
		t.Fatalf("randomDevFile.Close() failed: %v", err)
	}

	if err = testMPUploadDownloadCleanup(
		bck,
		baseParams,
		tempDirPath+"/"+nonMPBasenamePrefix+srcFileSuffix,
		tempDirPath+"/"+nonMPBasenamePrefix+dstFileSuffix); err != nil {
		t.Fatalf("%v [fileSize==%d]", err, nonMPFileSize)
	}
	if err = testMPUploadDownloadCleanup(
		bck,
		baseParams,
		tempDirPath+"/"+smallMPBasenamePrefix+srcFileSuffix,
		tempDirPath+"/"+smallMPBasenamePrefix+dstFileSuffix); err != nil {
		t.Fatalf("%v [fileSize==%d]", err, smallMPFileSize)
	}
	if err = testMPUploadDownloadCleanup(
		bck,
		baseParams,
		tempDirPath+"/"+bigMPBasenamePrefix+srcFileSuffix,
		tempDirPath+"/"+bigMPBasenamePrefix+dstFileSuffix); err != nil {
		t.Fatalf("%v [fileSize==%d]", err, bigMPFileSize)
	}

	if nonMPDstHashSum, err = testMPHashDstFile(
		tempDirPath + "/" + nonMPBasenamePrefix + dstFileSuffix); err != nil {
		t.Fatalf("%v [fileSize==%d]", err, nonMPFileSize)
	}
	if !bytes.Equal(nonMPSrcHashSum, nonMPDstHashSum) {
		t.Fatalf("bytes.Equal(nonMPSrcHashSum, nonMPDstHashSum) returned false")
	}
	if smallMPDstHashSum, err = testMPHashDstFile(
		tempDirPath + "/" + smallMPBasenamePrefix + dstFileSuffix); err != nil {
		t.Fatalf("%v [fileSize==%d]", err, smallMPFileSize)
	}
	if !bytes.Equal(smallMPSrcHashSum, smallMPDstHashSum) {
		t.Fatalf("bytes.Equal(smallMPSrcHashSum, smallMPDstHashSum) returned false")
	}
	if bigMPDstHashSum, err = testMPHashDstFile(
		tempDirPath + "/" + bigMPBasenamePrefix + dstFileSuffix); err != nil {
		t.Fatalf("%v [fileSize==%d]", err, bigMPFileSize)
	}
	if !bytes.Equal(bigMPSrcHashSum, bigMPDstHashSum) {
		t.Fatalf("bytes.Equal(bigMPSrcHashSum, bigMPDstHashSum) returned false")
	}
}

func testMPGenerateSrcFile(randomDevFile *os.File, fileSize int64, filePath string) (hashSum []byte, err error) {
	var (
		buf  []byte
		hash hash.Hash
		n    int
	)

	buf = make([]byte, fileSize)

	n, err = randomDevFile.Read(buf)
	if err != nil {
		err = fmt.Errorf("randomDevFile.Read(buf[len(%d)]) failed: %v", len(buf), err)
		return
	}
	if n != len(buf) {
		err = fmt.Errorf("randomDevFile.Read(buf[len(%d)]) returned unexpected byte count: %d", len(buf), n)
		return
	}

	hash = sha256.New()
	hash.Write(buf)
	hashSum = hash.Sum(nil)

	if err = os.WriteFile(filePath, buf, writeFilePerm); err != nil {
		err = fmt.Errorf("os.WriteFile(filePath, buf[len(%d)], writeFilePerm) failed: %v", len(buf), err)
		return
	}

	return
}

type testMPReaderStruct struct {
	filePath      string
	currentlyOpen bool
	file          *os.File
}

func (r *testMPReaderStruct) Open() (cos.ReadOpenCloser, error) {
	if r.currentlyOpen {
		// Ignore duplicate opens
		return r, nil
	}

	file, err := os.Open(r.filePath)
	if err == nil {
		r.currentlyOpen = true
		r.file = file
	}

	return r, err
}

func (r *testMPReaderStruct) Read(p []byte) (n int, err error) {
	if !r.currentlyOpen {
		return 0, errors.New("testMPReaderStruct not currently open")
	}

	return r.file.Read(p)
}

func (r *testMPReaderStruct) Close() error {
	if r.currentlyOpen {
		if err := r.file.Close(); err != nil {
			return err
		}
		r.currentlyOpen = false
	}

	// No error if we reach here (ignoring duplicate close's)
	return nil
}

type testMPWriterStruct struct {
	filePath string
	file     *os.File
}

func (w *testMPWriterStruct) Write(p []byte) (n int, err error) {
	return w.file.Write(p)
}

func testMPUploadDownloadCleanup(bck cmn.Bck, baseParams api.BaseParams, srcFilePath, dstFilePath string) (err error) {
	var (
		randomObjectName = fmt.Sprintf("%s%d", tempObjPrefix, rand.Int64())
		reader           = &testMPReaderStruct{filePath: srcFilePath}
		writer           = &testMPWriterStruct{filePath: dstFilePath}
	)

	if _, err = reader.Open(); err != nil {
		err = fmt.Errorf("reader.Open() failed: %v", err)
		return
	}

	if _, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    randomObjectName,
		Reader:     reader,
	}); err != nil {
		err = fmt.Errorf("api.PutObject() failed: %v", err)
		return
	}

	if err = reader.Close(); err != nil {
		err = fmt.Errorf("reader.Close() failed: %v", err)
		return
	}

	if writer.file, err = os.Create(writer.filePath); err != nil {
		err = fmt.Errorf("os.Create(writer.filePath) failed: %v", err)
		return
	}
	if _, err = api.GetObject(baseParams, bck, randomObjectName, &api.GetArgs{
		Writer: writer,
	}); err != nil {
		err = fmt.Errorf("api.GetObject(,,,) failed: %v", err)
		return
	}
	if err = writer.file.Close(); err != nil {
		err = fmt.Errorf("writer.file.Close() failed: %v", err)
		return
	}

	if err = api.DeleteObject(baseParams, bck, randomObjectName); err != nil {
		err = fmt.Errorf("api.DeleteObject(baseParams, bck, randomObjectName) failed: %v", err)
		return
	}

	return
}

func testMPHashDstFile(filePath string) (hashSum []byte, err error) {
	var (
		buf  []byte
		hash hash.Hash
	)

	if buf, err = os.ReadFile(filePath); err != nil {
		err = fmt.Errorf("os.ReadFile(filePath) failed: %v", err)
		return
	}

	hash = sha256.New()
	hash.Write(buf)
	hashSum = hash.Sum(nil)

	return
}

// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
)

const (
	testBackendMPFileNameSuffixLen   = 16
	testBackendMPFileContentBlockLen = 4 * cos.KiB
	testBackendMPObjectNameFormat    = "TestMPObj-%02X"
	testBackendMPRandomDevFilePath   = "/dev/urandom"
)

type testBackendMPGlobalsStruct struct {
	sync.WaitGroup
	sync.Mutex
	bck        cmn.Bck
	baseParams api.BaseParams
	workChan   chan int64
	errs       string
}

type testBackendMPFileStruct struct {
	sync.Mutex
	size    int
	name    string
	content []byte // len(.content) == testBackendMPFileContentBlockLen, repeated for entire .size
	pos     int
}

func testBackendMPFileNew(size int) (f *testBackendMPFileStruct, err error) {
	var (
		byteSlicePos          int
		n                     int
		nameSuffixAsByteSlice []byte
		randomDevFile         *os.File
	)

	if randomDevFile, err = os.Open(testBackendMPRandomDevFilePath); err != nil {
		err = fmt.Errorf("os.Open(testBackendMPRandomDevFilePath) failed: %v", err)
		return
	}

	nameSuffixAsByteSlice = make([]byte, testBackendMPFileNameSuffixLen)
	byteSlicePos = 0
	for byteSlicePos < testBackendMPFileNameSuffixLen {
		if n, err = randomDevFile.Read(nameSuffixAsByteSlice[byteSlicePos:]); err != nil {
			err = fmt.Errorf("randomDevFile.Read(nameSuffixAsByteSlice[byteSlicePos:]) failed: %v", err)
			return
		}
		byteSlicePos += n
	}

	f = &testBackendMPFileStruct{
		size:    size,
		name:    fmt.Sprintf(testBackendMPObjectNameFormat, nameSuffixAsByteSlice),
		content: make([]byte, testBackendMPFileContentBlockLen),
	}

	byteSlicePos = 0
	for byteSlicePos < testBackendMPFileContentBlockLen {
		if n, err = randomDevFile.Read(f.content[byteSlicePos:]); err != nil {
			err = fmt.Errorf("randomDevFile.Read(f.content[byteSlicePos:]) failed: %v", err)
			return
		}
		byteSlicePos += n
	}

	if err = randomDevFile.Close(); err != nil {
		err = fmt.Errorf("randomDevFile.Close() failed: %v", err)
		return
	}

	return
}

func (f *testBackendMPFileStruct) String() string {
	return f.name
}

func (f *testBackendMPFileStruct) Open() (cos.ReadOpenCloser, error) {
	f.Lock()
	defer f.Unlock()

	// Treat a (re)open as a file.Seek(offset=0,whence=os.SEEK_SET)
	f.pos = 0

	return f, nil
}

func (f *testBackendMPFileStruct) Read(p []byte) (n int, err error) {
	f.Lock()
	defer f.Unlock()

	n = f.size - f.pos
	if n == 0 {
		err = io.EOF
		return
	}
	if n > len(p) {
		n = len(p)
	}

	pPos := copy(p, f.content[(f.pos%testBackendMPFileContentBlockLen):])

	for pPos < n {
		cnt := copy(p[pPos:], f.content)
		pPos += cnt
	}

	f.pos += n

	return
}

func (f *testBackendMPFileStruct) Write(p []byte) (n int, err error) {
	var (
		cLim int
		cPos int
		pLim int
		pPos int
	)

	f.Lock()
	defer f.Unlock()

	n = f.size - f.pos
	switch {
	case len(p) < n:
		n = len(p)
	case len(p) == n:
		// n is perfect
	case len(p) > n:
		err = errors.New("attempt to write beyond EOF")
		return
	}

	cPos = f.pos % testBackendMPFileContentBlockLen

	for pPos < n {
		if (n - pPos) >= (testBackendMPFileContentBlockLen - cPos) {
			cLim = testBackendMPFileContentBlockLen
		} else {
			cLim = cPos + (n - pPos)
		}
		pLim = pPos + (cLim - cPos)
		if !bytes.Equal(p[pPos:pLim], f.content[cPos:cLim]) {
			err = errors.New("attempt to write mismatched data")
			return
		}

		cPos = 0 // Only changes cPos in 1st loop iteration... but here for clarity
		pPos = pLim
	}

	f.pos += n

	return
}

func (f *testBackendMPFileStruct) Close() error {
	f.Lock()
	defer f.Unlock()

	f.pos = 0

	return nil
}

func TestBackendMP(t *testing.T) {
	var (
		globals         *testBackendMPGlobalsStruct
		err             error
		bucket          = os.Getenv("BUCKET")
		nonMPFileSize   int64
		smallMPFileSize int64
		bigMPFileSize   int64
		totalFiles      int64
		parallelFiles   int64
		lastFileSize    int64
	)

	if bucket == "" {
		t.Skipf("BUCKET ENV missing... skipping...")
	}

	globals = &testBackendMPGlobalsStruct{}

	if globals.bck, _, err = cmn.ParseBckObjectURI(bucket, cmn.ParseURIOpts{}); err != nil {
		t.Fatalf("cmn.ParseBckObjectURI(bucket, cmn.ParseURIOpts{}) failed: %v", err)
	}
	if globals.bck.Provider != apc.OCI {
		t.Skipf("BUCKET .Provider (\"%s\") skipped", globals.bck.Provider)
	}

	globals.baseParams = tools.BaseAPIParams(tools.RandomProxyURL(t))

	nonMPFileSize, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_SIZE_NON_MP"), "")
	if (err != nil) || (nonMPFileSize <= 0) {
		t.Skipf("OCI_TEST_FILE_SIZE_NON_MP ENV missing or invalid... skipping...")
	}
	smallMPFileSize, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_SIZE_SMALL_MP"), "")
	if (err != nil) || (smallMPFileSize <= 0) {
		t.Skipf("OCI_TEST_FILE_SIZE_SMALL_MP ENV missing or invalid... skipping...")
	}
	bigMPFileSize, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_SIZE_BIG_MP"), "")
	if (err != nil) || (bigMPFileSize <= 0) {
		t.Skipf("OCI_TEST_FILE_SIZE_BIG_MP ENV missing or invalid... skipping...")
	}

	totalFiles, err = cos.ParseSize(os.Getenv("OCI_TEST_FILES_TOTAL"), "")
	if (err != nil) || (totalFiles <= 0) {
		t.Skipf("OCI_TEST_FILES_TOTAL ENV missing or invalid... skipping...")
	}
	parallelFiles, err = cos.ParseSize(os.Getenv("OCI_TEST_FILES_PARALLEL"), "")
	if (err != nil) || (parallelFiles <= 0) {
		t.Skipf("OCI_TEST_FILES_PARALLEL ENV missing or invalid... skipping...")
	}

	// Start off just running one worker for each size in parallel

	globals.workChan = make(chan int64, 3)

	globals.Add(1)
	go testMPUploadDownloadCleanupDispatcher(globals)

	globals.workChan <- nonMPFileSize
	globals.workChan <- smallMPFileSize
	globals.workChan <- bigMPFileSize

	close(globals.workChan)

	globals.Wait()

	if globals.errs != "" {
		t.Fatal(errors.New(globals.errs))
	}

	// Now perform the specified total # of workers with the specified parallelism

	globals.workChan = make(chan int64, parallelFiles)

	globals.Add(1)
	go testMPUploadDownloadCleanupDispatcher(globals)

	lastFileSize = bigMPFileSize

	for range totalFiles {
		switch lastFileSize {
		case nonMPFileSize:
			lastFileSize = smallMPFileSize
		case smallMPFileSize:
			lastFileSize = bigMPFileSize
		case bigMPFileSize:
			lastFileSize = nonMPFileSize
		}
		globals.workChan <- lastFileSize
	}

	close(globals.workChan)

	globals.Wait()

	if globals.errs != "" {
		t.Fatal(errors.New(globals.errs))
	}
}

func testMPUploadDownloadCleanupDispatcher(globals *testBackendMPGlobalsStruct) {
	var (
		size int64
	)

	for size = range globals.workChan {
		globals.Add(1)
		go testMPUploadDownloadCleanup(globals, size)
	}

	globals.Done()
}

func testMPUploadDownloadCleanup(globals *testBackendMPGlobalsStruct, size int64) {
	var (
		err               error
		testBackendMPFile *testBackendMPFileStruct
	)

	defer globals.Done()

	defer func() {
		if err != nil {
			globals.Lock()
			if globals.errs == "" {
				globals.errs = "Worker errors:"
			}
			globals.errs += "\n" + err.Error()
			globals.Unlock()
		}
	}()

	if testBackendMPFile, err = testBackendMPFileNew(int(size)); err != nil {
		err = fmt.Errorf("testBackendMPFileNew() failed: %v", err)
		return
	}

	defer func() {
		// Always clean-up... only updating returned err on api.DeleteObject() failure if err was nil
		deferErr := api.DeleteObject(globals.baseParams, globals.bck, testBackendMPFile.String())
		if (err == nil) && (deferErr != nil) {
			err = fmt.Errorf("api.DeleteObject(globals.baseParams, globals.bck, randomObjectName) failed: %v", deferErr)
		}
	}()

	if _, err = testBackendMPFile.Open(); err != nil {
		err = fmt.Errorf("testBackendMPFile.Open() [PUT] failed: %v", err)
		return
	}
	if _, err = api.PutObject(&api.PutArgs{
		BaseParams: globals.baseParams,
		Bck:        globals.bck,
		ObjName:    testBackendMPFile.String(),
		Reader:     testBackendMPFile,
	}); err != nil {
		err = fmt.Errorf("api.PutObject() failed: %v", err)
		return
	}
	if err = testBackendMPFile.Close(); err != nil {
		err = fmt.Errorf("reader.Close() [PUT] failed: %v", err)
		return
	}

	if _, err = testBackendMPFile.Open(); err != nil {
		err = fmt.Errorf("testBackendMPFile.Open() [GET] failed: %v", err)
		return
	}
	if _, err = api.GetObject(globals.baseParams, globals.bck, testBackendMPFile.String(), &api.GetArgs{
		Writer: testBackendMPFile,
	}); err != nil {
		err = fmt.Errorf("api.GetObject() failed: %v", err)
		return
	}
	if err = testBackendMPFile.Close(); err != nil {
		err = fmt.Errorf("reader.Close() [GET] failed: %v", err)
	}
}

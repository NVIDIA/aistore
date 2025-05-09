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
	"time"

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
	bck                 cmn.Bck
	baseParams          api.BaseParams
	workChan            chan struct{}
	errs                string
	putObjectRetryLimit int64
	putObjectRetryDelay time.Duration
	getObjectRetryLimit int64
	getObjectRetryDelay time.Duration
}

type testBackendMPFileStruct struct {
	sync.Mutex
	index   int
	size    int
	name    string
	content []byte // len(.content) == testBackendMPFileContentBlockLen, repeated for entire .size
	pos     int
}

func testBackendMPFileNew(index, size int) (f *testBackendMPFileStruct, err error) {
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
		index:   index,
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

	// Treat a (re-)open as a file.Seek(offset=0,whence=os.SEEK_SET)
	f.pos = 0

	return f, nil
}

func (f *testBackendMPFileStruct) Read(p []byte) (n int, err error) {
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
	case n == 0:
		err = io.EOF
		return
	case len(p) < n:
		n = len(p)
	case len(p) >= n:
		// n is more than sufficient to "read" the remainder of the content
	}

	pPos = 0

	cPos = f.pos % testBackendMPFileContentBlockLen

	for pPos < n {
		if (n - pPos) >= (testBackendMPFileContentBlockLen - cPos) {
			cLim = testBackendMPFileContentBlockLen
		} else {
			cLim = cPos + (n - pPos)
		}

		pLim = pPos + (cLim - cPos)

		_ = copy(p[pPos:pLim], f.content[cPos:cLim])

		cPos = 0 // Only changes cPos in 1st loop iteration... but here for clarity
		pPos = pLim
	}

	f.pos += n
	if f.pos == f.size {
		err = io.EOF
	}

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
		// n is sufficient to "write" the remainder of the expected content
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

func TestBackendMPFileReaderWriter(t *testing.T) {
	const (
		fileReaderWriterParallelStreams         = 1000
		fileReaderWriterStreamTotalSize         = 60000
		fileReaderWriterStreamReadSize          = 4096
		fileReaderWriterStreamWriteSize         = 8192
		fileReaderWriterStreamCloseAfterRead    = true
		fileReaderWriterStreamReOpenBeforeWrite = true
	)
	var (
		err       error
		errs      []error
		errsMutex sync.Mutex
		streams   []*testBackendMPFileStruct
		wg        sync.WaitGroup
	)

	errs = make([]error, 0)

	streams = make([]*testBackendMPFileStruct, fileReaderWriterParallelStreams)

	for index := range streams {
		streams[index], err = testBackendMPFileNew(index, fileReaderWriterStreamTotalSize)
		if err != nil {
			t.Fatalf("testBackendMPFileNew() failed: %v", err)
		}
		wg.Add(1)
		go func(stream *testBackendMPFileStruct) {
			var (
				buf      []byte
				contents []byte
				err      error
				n        int
				pos      int
				roc      cos.ReadOpenCloser
			)

			roc, err = stream.Open()
			if err != nil {
				errsMutex.Lock()
				errs = append(errs, fmt.Errorf("streams[%v].Open() failed: %v", stream.index, err))
				errsMutex.Unlock()
				wg.Done()
				return
			}
			if roc != stream {
				errsMutex.Lock()
				errs = append(errs, fmt.Errorf("streams[%v].Open() returned roc != stream", stream.index))
				errsMutex.Unlock()
				wg.Done()
				return
			}

			contents = make([]byte, 0, fileReaderWriterStreamTotalSize)

			pos = 0

			for {
				buf = make([]byte, fileReaderWriterStreamReadSize)
				n, err = stream.Read(buf)
				if err == nil {
					if n == 0 {
						errsMutex.Lock()
						errs = append(errs, fmt.Errorf("streams[%v].Read() @ pos: %v returned [n:0,err:nil]", stream.index, pos))
						errsMutex.Unlock()
						wg.Done()
						return
					}
				} else if err != io.EOF {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].Read() @ pos: %v failed: %v", stream.index, pos, err))
					errsMutex.Unlock()
					wg.Done()
					return
				}
				if n > fileReaderWriterStreamReadSize {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].Read() @ pos: %v returned [n:%v(>fileReaderWriterStreamReadSize:%v),err:nil]", stream.index, pos, n, fileReaderWriterStreamReadSize))
					errsMutex.Unlock()
					wg.Done()
					return
				}
				if n > 0 {
					contents = append(contents, buf[0:n]...)
					pos += n
				}
				if err == io.EOF {
					break
				}
			}

			if pos != fileReaderWriterStreamTotalSize {
				errsMutex.Lock()
				errs = append(errs, fmt.Errorf("streams[%v].Read() @ pos: %v(!=fileReaderWriterStreamTotalSize:%v) returned err: io.EOF", stream.index, pos, fileReaderWriterStreamTotalSize))
				errsMutex.Unlock()
				wg.Done()
				return
			}
			if len(contents) != fileReaderWriterStreamTotalSize {
				errsMutex.Lock()
				errs = append(errs, fmt.Errorf("streams[%v] len(contents): %v(!=fileReaderWriterStreamTotalSize:%v)", stream.index, len(contents), fileReaderWriterStreamTotalSize))
				errsMutex.Unlock()
				wg.Done()
				return
			}

			if fileReaderWriterStreamCloseAfterRead {
				err = stream.Close()
				if err != nil {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].Close() [After Read] failed: %v", stream.index, err))
					errsMutex.Unlock()
					wg.Done()
					return
				}
			}

			if fileReaderWriterStreamReOpenBeforeWrite {
				roc, err = stream.Open()
				if err != nil {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].(Re-)Open() failed: %v", stream.index, err))
					errsMutex.Unlock()
					wg.Done()
					return
				}
				if roc != stream {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].(Re-)Open() returned roc != sterams[i]", stream.index))
					errsMutex.Unlock()
					wg.Done()
					return
				}
			}

			pos = 0

			for {
				if (pos + fileReaderWriterStreamWriteSize) > fileReaderWriterStreamTotalSize {
					buf = make([]byte, fileReaderWriterStreamTotalSize-pos)
				} else {
					buf = make([]byte, fileReaderWriterStreamWriteSize)
				}
				_ = copy(buf, contents[pos:(pos+len(buf))])
				n, err = stream.Write(buf)
				if err != nil {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].Write() @ pos: %v failed: %v", stream.index, pos, err))
					errsMutex.Unlock()
					wg.Done()
					return
				}
				if n == 0 {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].Write() @ pos: %v returned [n:0,err:nil]", stream.index, pos))
					errsMutex.Unlock()
					wg.Done()
					return
				}
				if n > len(buf) {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].Write() @ pos: %v returned [n:%v(>len(buf):%v,err:nil]", stream.index, pos, n, len(buf)))
					errsMutex.Unlock()
					wg.Done()
					return
				}
				pos += n
				if pos > fileReaderWriterStreamTotalSize {
					errsMutex.Lock()
					errs = append(errs, fmt.Errorf("streams[%v].Write() @ pos: %v retuerned [n:%v,err:nil] taking pos to %v(>fileReaderWriterStreamTotalSize:%v)", stream.index, pos-n, n, pos, fileReaderWriterStreamTotalSize))
					errsMutex.Unlock()
					wg.Done()
					return
				}
				if pos == fileReaderWriterStreamTotalSize {
					break
				}
			}

			err = stream.Close()
			if err != nil {
				errsMutex.Lock()
				errs = append(errs, fmt.Errorf("streams[%v].Close() [After Write] failed: %v", stream.index, err))
				errsMutex.Unlock()
				wg.Done()
				return
			}

			wg.Done()
		}(streams[index])
	}

	wg.Wait()

	errsMutex.Lock()
	if len(errs) > 0 {
		if len(errs) == 1 {
			t.Fatalf("1 stream (of %v) failed [%v]", fileReaderWriterParallelStreams, errs[0])
		} else {
			t.Fatalf("%v stream(s) (of %v) failed", len(errs), fileReaderWriterParallelStreams)
		}
	}
	errsMutex.Unlock()
}

func TestBackendMPUploadDownload(t *testing.T) {
	var (
		bigMPFileSize         int64
		bucket                = os.Getenv("BUCKET")
		err                   error
		getObjectRetryDelayMS int64
		globals               *testBackendMPGlobalsStruct
		lastFileSize          int64
		nonMPFileSize         int64
		parallelFiles         int64
		putObjectRetryDelayMS int64
		smallMPFileSize       int64
		totalFiles            int64
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

	globals.putObjectRetryLimit, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_PUT_RETRY_LIMIT"), "")
	if (err != nil) || (globals.putObjectRetryLimit < 0) {
		t.Skipf("OCI_TEST_FILE_PUT_RETRY_LIMIT ENV missing or invallid... skipping...")
	}
	putObjectRetryDelayMS, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_PUT_RETRY_DELAY"), "")
	if (err != nil) || (putObjectRetryDelayMS < 0) {
		t.Skipf("OCI_TEST_FILE_PUT_RETRY_DELAY ENV missing or invallid... skipping...")
	}
	globals.putObjectRetryDelay = time.Duration(putObjectRetryDelayMS) * time.Millisecond
	globals.getObjectRetryLimit, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_GET_RETRY_LIMIT"), "")
	if (err != nil) || (globals.getObjectRetryLimit < 0) {
		t.Skipf("OCI_TEST_FILE_GET_RETRY_LIMIT ENV missing or invallid... skipping...")
	}
	getObjectRetryDelayMS, err = cos.ParseSize(os.Getenv("OCI_TEST_FILE_GET_RETRY_DELAY"), "")
	if (err != nil) || (getObjectRetryDelayMS < 0) {
		t.Skipf("OCI_TEST_FILE_GET_RETRY_DELAY ENV missing or invallid... skipping...")
	}
	globals.getObjectRetryDelay = time.Duration(getObjectRetryDelayMS) * time.Millisecond

	// Start off just running one worker for each size in parallel

	globals.workChan = make(chan struct{}, 3)

	globals.Add(3)
	go testMPUploadDownloadCleanup(globals, nonMPFileSize)
	go testMPUploadDownloadCleanup(globals, smallMPFileSize)
	go testMPUploadDownloadCleanup(globals, bigMPFileSize)

	globals.Wait()

	if globals.errs != "" {
		t.Fatal(errors.New(globals.errs))
	}

	// Now perform the specified total # of workers with the specified parallelism

	globals.workChan = make(chan struct{}, parallelFiles)

	globals.Add(int(totalFiles))

	lastFileSize = bigMPFileSize

	for range totalFiles {
		switch lastFileSize {
		case nonMPFileSize:
			lastFileSize = smallMPFileSize
		case smallMPFileSize:
			lastFileSize = nonMPFileSize
		case bigMPFileSize:
			lastFileSize = nonMPFileSize
		}

		go testMPUploadDownloadCleanup(globals, lastFileSize)
	}

	globals.Wait()

	if globals.errs != "" {
		t.Fatal(errors.New(globals.errs))
	}
}

func testMPUploadDownloadCleanup(globals *testBackendMPGlobalsStruct, size int64) {
	var (
		err               error
		getObjectRetries  int64
		objectProps       *cmn.ObjectProps
		putObjectRetries  int64
		testBackendMPFile *testBackendMPFileStruct
	)

	defer globals.Done()

	globals.workChan <- struct{}{}
	defer func() {
		<-globals.workChan
	}()

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

	if testBackendMPFile, err = testBackendMPFileNew(0, int(size)); err != nil {
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

putObjectRetry:

	if _, err = testBackendMPFile.Open(); err != nil {
		err = fmt.Errorf("testBackendMPFile.Open() [PUT] failed: %v", err)
		return
	}
	if _, err = api.PutObject(&api.PutArgs{
		Reader:     testBackendMPFile,
		BaseParams: globals.baseParams,
		Bck:        globals.bck,
		ObjName:    testBackendMPFile.String(),
		Size:       uint64(testBackendMPFile.size),
	}); err != nil {
		if putObjectRetries < globals.putObjectRetryLimit {
			putObjectRetries++
			time.Sleep(globals.putObjectRetryDelay)
			goto putObjectRetry
		}
		err = fmt.Errorf("api.PutObject() failed: %v", err)
		return
	}
	if err = testBackendMPFile.Close(); err != nil {
		err = fmt.Errorf("reader.Close() [PUT] failed: %v", err)
		return
	}

	if objectProps, err = api.HeadObject(globals.baseParams, globals.bck, testBackendMPFile.String(), api.HeadArgs{}); err != nil {
		err = fmt.Errorf("api.HeadObject() [cache hit case] failed: %v", err)
		return
	}
	if objectProps.Size != int64(testBackendMPFile.size) {
		err = fmt.Errorf("api.HeadObject() [cache hit case] returned .Size: %v but testBackendMPFile.size: %v", objectProps.Size, testBackendMPFile.size)
		return
	}

	if err = api.EvictObject(globals.baseParams, globals.bck, testBackendMPFile.String()); err != nil {
		err = fmt.Errorf("api.EvictObject() failed: %v", err)
		return
	}

	if objectProps, err = api.HeadObject(globals.baseParams, globals.bck, testBackendMPFile.String(), api.HeadArgs{}); err != nil {
		err = fmt.Errorf("api.HeadObject() [cache miss case] failed: %v", err)
		return
	}
	if objectProps.Size != int64(testBackendMPFile.size) {
		err = fmt.Errorf("api.HeadObject() [cache miss case] returned .Size: %v but testBackendMPFile.size: %v", objectProps.Size, testBackendMPFile.size)
		return
	}

getObjectRetry:

	if _, err = testBackendMPFile.Open(); err != nil {
		err = fmt.Errorf("testBackendMPFile.Open() [GET] failed: %v", err)
		return
	}
	if _, err = api.GetObject(globals.baseParams, globals.bck, testBackendMPFile.String(), &api.GetArgs{
		Writer: testBackendMPFile,
	}); err != nil {
		if getObjectRetries < globals.getObjectRetryLimit {
			getObjectRetries++
			time.Sleep(globals.getObjectRetryDelay)
			goto getObjectRetry
		}
		err = fmt.Errorf("api.GetObject() failed: %v", err)
		return
	}
	if err = testBackendMPFile.Close(); err != nil {
		err = fmt.Errorf("reader.Close() [GET] failed: %v", err)
	}
}

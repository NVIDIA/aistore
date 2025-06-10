// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

// [TODO]
// - get-batch() to return something other than .tar
// - multi-node (rm 'Skipf(max-targets)')

const (
	mossMissingPrefix = "missing-"
	mossMissingSuffix = ".nonexistent"
)

type mossConfig struct {
	archFormat    string // "" for plain objects, or archive.ExtTar, etc.
	continueOnErr bool   // GetBatch ContinueOnErr flag
	onlyObjName   bool   // GetBatch OnlyObjName flag
	withMissing   bool   // inject missing objects
	nested        bool   // subdirs in archives
	streaming     bool   // streaming GET (w/ no out-of-band api.MossResp metadata)
}

func (c *mossConfig) name() (s string) {
	s = "plain"
	if c.archFormat != "" {
		s = c.archFormat
	}
	if c.streaming {
		s += "/streaming"
	}
	if c.continueOnErr {
		s += "/continue-on-err"
	}
	if c.onlyObjName {
		s += "/only-objname"
	}
	if c.withMissing {
		s += "/with-missing"
	}
	if c.nested {
		s += "/nested"
	}
	return s
}

func TestMoss(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1}) // TODO -- FIXME: remove

	var (
		numPlainObjs = 500 // plain objects to create
		numArchives  = 400 // num shards to create
		numInArch    = 50  // files per archive (shard)
		fileSize     = 128 * cos.KiB
	)
	if testing.Short() {
		numPlainObjs = 50
		numArchives = 10
		numInArch = 10
		fileSize = cos.KiB
	}

	tests := []mossConfig{
		// (multi-part w/ out-of-band metadata; read objects) tests
		{archFormat: "", continueOnErr: false, onlyObjName: false},
		{archFormat: "", continueOnErr: true, onlyObjName: false},
		{archFormat: "", continueOnErr: true, onlyObjName: true},
		{archFormat: "", continueOnErr: true, onlyObjName: false, withMissing: true},

		// (multi-part w/ out-of-band metadata; read from archives (shards)) tests
		{archFormat: archive.ExtTar, continueOnErr: false, onlyObjName: false},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, withMissing: true},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: true, withMissing: true},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, nested: true},
		{archFormat: archive.ExtTgz, continueOnErr: true, onlyObjName: false, withMissing: true},
		{archFormat: archive.ExtZip, continueOnErr: false, onlyObjName: false, nested: true},

		// (streaming; read objects) tests
		{archFormat: "", streaming: true},
		{archFormat: "", continueOnErr: true, withMissing: true, streaming: true},
		{archFormat: "", continueOnErr: true, onlyObjName: true, withMissing: true, streaming: true},

		// (streaming, read from archives (shards)) tests
		{archFormat: archive.ExtTar, streaming: true},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, withMissing: true, streaming: true},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: true, withMissing: true, streaming: true},
		{archFormat: archive.ExtZip, continueOnErr: false, onlyObjName: false, nested: true, streaming: true},
	}

	for _, test := range tests {
		t.Run(test.name(), func(t *testing.T) {
			var (
				bck = cmn.Bck{Name: trand.String(15), Provider: apc.AIS}
				m   = ioContext{
					t:        t,
					bck:      bck,
					fileSize: uint64(fileSize),
					prefix:   "moss/",
					ordered:  true,
				}
				proxyURL = tools.RandomProxyURL(t)
			)

			tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
			m.init(false /*cleanup*/)

			if test.archFormat == "" {
				testMossPlainObjects(t, &m, &test, numPlainObjs)
			} else {
				testMossArchives(t, &m, &test, numArchives, numInArch)
			}
		})
	}
}

func testMossPlainObjects(t *testing.T, m *ioContext, test *mossConfig, numObjs int) {
	m.num = numObjs
	m.puts()

	// Build MossIn
	mossIn := make([]api.MossIn, 0, numObjs)
	for i := range numObjs {
		mossIn = append(mossIn, api.MossIn{
			ObjName: m.objNames[i],
		})
	}

	// Inject missing objects if requested
	if test.withMissing {
		originalEntries := mossIn
		mossIn = make([]api.MossIn, 0, len(originalEntries)+len(originalEntries)/3)

		for i, entry := range originalEntries {
			mossIn = append(mossIn, entry)
			if i%3 == 0 {
				mossIn = append(mossIn, api.MossIn{
					ObjName: mossMissingPrefix + trand.String(8),
				})
			}
		}
	}

	if test.streaming {
		testMossStreaming(t, m, test, mossIn)
	} else {
		testMossMultipart(t, m, test, mossIn)
	}
}

func testMossArchives(t *testing.T, m *ioContext, test *mossConfig, numArchives, numInArch int) {
	// Track actual files created in each archive for accurate retrieval
	type archiveInfo struct {
		name      string
		filePaths []string
	}

	// Collect all archive info
	archives := make([]archiveInfo, 0, numArchives)

	// Create archives
	var (
		tmpDir     = t.TempDir() // (with automatic cleanup)
		errCh      = make(chan error, numArchives)
		wg         = cos.NewLimitedWaitGroup(10, 0)
		archInfoCh = make(chan archiveInfo, numArchives)
	)
	for i := range numArchives {
		archName := fmt.Sprintf("archive%02d%s", i, test.archFormat)

		wg.Add(1)
		go func(archName string) {
			randomNames, err := _createMossArch(m, test, tmpDir, archName, numInArch)
			if err != nil {
				errCh <- err
			} else {
				archInfoCh <- archiveInfo{
					name:      archName,
					filePaths: randomNames,
				}
			}
			wg.Done()
		}(archName)
	}

	wg.Wait()
	close(archInfoCh)
	tassert.SelectErr(t, errCh, "create and upload archives", true)

	// Collect all archive info
	for info := range archInfoCh {
		archives = append(archives, info)
	}

	// Build api.MossIn entries
	var mossIn []api.MossIn
	for _, archInfo := range archives {
		// Add a few files from each archive
		numToRequest := min(len(archInfo.filePaths), max(numInArch/5, 3))
		for j := range numToRequest {
			mossIn = append(mossIn, api.MossIn{
				ObjName:  archInfo.name,
				ArchPath: archInfo.filePaths[j],
			})
		}
	}

	// Inject missing archive paths if requested
	if test.withMissing {
		for i := range mossIn {
			if i%3 == 0 {
				mossIn[i].ArchPath = trand.String(8) + mossMissingSuffix
			}
		}
	}

	if test.streaming {
		testMossStreaming(t, m, test, mossIn)
	} else {
		testMossMultipart(t, m, test, mossIn)
	}
}

func _createMossArch(m *ioContext, test *mossConfig, tmpDir, archName string, numInArch int) ([]string, error) {
	// Generate filenames for archive content
	randomNames := make([]string, 0, numInArch)
	for j := range numInArch {
		name := fmt.Sprintf("file%d.txt", j)
		if test.nested {
			// Use deterministic directory assignment
			dirs := []string{"a", "b", "c", "a/b", "a/c", "b/c"}
			dir := dirs[j%len(dirs)] // deterministic, not random
			name = dir + "/" + name
		}
		randomNames = append(randomNames, name)
	}

	// Create archive (shards) of a given format
	archPath := tmpDir + "/" + cos.GenTie() + test.archFormat
	err := tarch.CreateArchRandomFiles(
		archPath,
		tar.FormatUnknown, // tar format
		test.archFormat,   // file extension
		numInArch,         // file count
		int(m.fileSize),   // file size
		false,             // no duplication
		false,             // no random dir prefix
		nil,               // no record extensions
		randomNames,       // pregenerated filenames
	)
	if err != nil {
		return nil, err
	}

	// PUT
	reader, err := readers.NewExistingFile(archPath, cos.ChecksumNone)
	if err != nil {
		return nil, err
	}
	bp := tools.BaseAPIParams(m.proxyURL)
	putArgs := api.PutArgs{
		BaseParams: bp,
		Bck:        m.bck,
		ObjName:    archName,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	_, err = api.PutObject(&putArgs)
	return randomNames, err
}

func testMossMultipart(t *testing.T, m *ioContext, test *mossConfig, mossIn []api.MossIn) {
	type result struct {
		resp     api.MossResp
		err      error
		duration time.Duration
		tarSize  int
		tarData  []byte
	}

	// Concurrency settings
	numConcurrentCalls := 5
	if testing.Short() {
		numConcurrentCalls = 2
	}

	tlog.Logf("GetBatch: %d objects, %d concurrent calls, continueOnErr=%t, onlyObjName=%t, withMissing=%t\n",
		len(mossIn), numConcurrentCalls, test.continueOnErr, test.onlyObjName, test.withMissing)

	// Prepare api.MossReq
	req := &api.MossReq{
		In:            mossIn,
		ContinueOnErr: test.continueOnErr,
		OnlyObjName:   test.onlyObjName,
		StreamingGet:  false,
	}

	// Execute concurrent GetBatch calls
	var (
		results    = make([]result, numConcurrentCalls)
		wg         sync.WaitGroup
		baseParams = tools.BaseAPIParams(m.proxyURL)
		start      = mono.NanoTime()
	)
	for i := range numConcurrentCalls {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var (
				callStart = mono.NanoTime()
				buf       strings.Builder
			)
			resp, err := api.GetBatch(baseParams, m.bck, req, &buf)
			callDuration := mono.Since(callStart)

			tarData := []byte(buf.String())
			results[idx] = result{
				resp:     resp,
				err:      err,
				duration: callDuration,
				tarSize:  len(tarData),
				tarData:  tarData,
			}
		}(i)
	}

	wg.Wait()
	totalDuration := mono.Since(start)

	// Log latencies
	var (
		successCount, errorCount int
		totalTarSize             int64
		minDuration, maxDuration time.Duration
		sumDuration              time.Duration
	)
	minDuration = time.Hour // initialize to large value

	for i, result := range results {
		if result.err != nil {
			errorCount++
			tlog.Logf("  Call %d: ERROR in %v - %v\n", i, result.duration, result.err)
		} else {
			successCount++
			totalTarSize += int64(result.tarSize)
			sumDuration += result.duration
			if result.duration < minDuration {
				minDuration = result.duration
			}
			if result.duration > maxDuration {
				maxDuration = result.duration
			}
		}
	}

	avgDuration := sumDuration / time.Duration(successCount)
	tlog.Logf("GetBatch micro-bench: total=%v, min=%v, max=%v, avg=%v\n",
		totalDuration, minDuration, maxDuration, avgDuration)
	tlog.Logf("Success: %d/%d calls, Total TAR: %s\n",
		successCount, numConcurrentCalls, cos.ToSizeIEC(totalTarSize, 2))

	// Find first successful result for validation
	var firstSuccess *result
	for i := range results {
		if results[i].err == nil {
			firstSuccess = &results[i]
			break
		}
	}

	if test.withMissing && !test.continueOnErr {
		tassert.Errorf(t, errorCount > 0, "Expected GetBatch calls to fail but they succeeded")
		return
	}

	tassert.Fatalf(t, firstSuccess != nil, "Expected at least one successful call")

	tassert.Fatalf(t, len(firstSuccess.resp.Out) == len(mossIn),
		"Expected %d results, got %d", len(mossIn), len(firstSuccess.resp.Out))

	// Count success/error
	var objSuccessCount, objErrorCount int
	for i, objResult := range firstSuccess.resp.Out {
		if objResult.ErrMsg != "" {
			objErrorCount++
		} else {
			objSuccessCount++
			// Validate successful entries
			if test.onlyObjName {
				tassert.Errorf(t, objResult.ObjName != "", "Missing ObjName in result")
			}
			tassert.Errorf(t, objResult.Size > 0, "Expected positive size but got %d for %s",
				objResult.Size, mossIn[i].ObjName)
		}
	}

	// Validate error expectations
	if test.withMissing && test.continueOnErr {
		tassert.Fatalf(t, objErrorCount > 0, "Expected some object errors but got none")
		tassert.Fatalf(t, objSuccessCount > 0, "Expected some object successes but got none")
		tlog.Logf("Object results: %d success, %d errors\n", objSuccessCount, objErrorCount)
	} else {
		// No missing files expected - all should succeed
		for i, objResult := range firstSuccess.resp.Out {
			tassert.Errorf(t, objResult.ErrMsg == "", "Unexpected error for %s: %s",
				mossIn[i].ObjName, objResult.ErrMsg)
		}
	}

	// Always validate TAR contents for successful responses
	validateTarMultipart(t, req, firstSuccess.resp,
		bytes.NewReader(firstSuccess.tarData), len(firstSuccess.tarData))

	// Validate consistency across concurrent calls
	for i := 1; i < len(results); i++ {
		if results[i].err == nil {
			tassert.Errorf(t, len(results[i].resp.Out) == len(firstSuccess.resp.Out),
				"Inconsistent result count between concurrent calls")
			tassert.Errorf(t, results[i].tarSize == firstSuccess.tarSize,
				"Inconsistent TAR size between concurrent calls: %d vs %d",
				results[i].tarSize, firstSuccess.tarSize)
		}
	}
}

// Helper function to determine if a file is expected to be missing based on request patterns
func isMissingFile(mossIn *api.MossIn) bool {
	// Missing objects have "missing-" prefix
	if strings.HasPrefix(mossIn.ObjName, mossMissingPrefix) {
		return true
	}
	// Missing archive paths have ".nonexistent" suffix
	if mossIn.ArchPath != "" && strings.HasSuffix(mossIn.ArchPath, mossMissingSuffix) {
		return true
	}
	return false
}

// Enhanced TAR validation for multipart
func validateTarMultipart(t *testing.T, req *api.MossReq, resp api.MossResp, tarReader io.Reader, tarSize int) {
	tlog.Logln("Validating TAR contents: " + cos.ToSizeIEC(int64(tarSize), 2))

	// Parse TAR and collect all entries in order
	var (
		tr         = tar.NewReader(tarReader)
		tarEntries = make([]struct {
			name string
			size int64
			data []byte
		}, 0, len(req.In))
	)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, hdr.Typeflag == tar.TypeReg, "expecting only regular files, got: (%q, %c)", hdr.Name, hdr.Typeflag)

		// Read file data
		data, err := io.ReadAll(tr)
		tassert.CheckFatal(t, err)

		tarEntries = append(tarEntries, struct {
			name string
			size int64
			data []byte
		}{
			name: hdr.Name,
			size: hdr.Size,
			data: data,
		})
	}

	// Contract validation: same number of entries
	tassert.Fatalf(t, len(tarEntries) == len(resp.Out),
		"TAR entries (%d) != MossResp entries (%d)", len(tarEntries), len(resp.Out))

	// Contract validation: precise order - validate each position
	for i := range resp.Out {
		var (
			expectedName string
			mossOut      = &resp.Out[i]
		)
		// Calculate expected name based on onlyObjName setting
		if req.OnlyObjName {
			// When onlyObjName=true, TAR entries should contain only the object name
			expectedName = mossOut.ObjName
			if mossOut.ArchPath != "" {
				expectedName += "/" + mossOut.ArchPath
			}
		} else {
			// When onlyObjName=false, TAR entries should contain full bucket path
			expectedName = mossOut.Bucket + "/" + mossOut.ObjName
			if mossOut.ArchPath != "" {
				expectedName += "/" + mossOut.ArchPath
			}
		}

		// Determine missing status
		var (
			mossIn    = &req.In[i]
			isMissing = isMissingFile(mossIn)
		)
		if isMissing {
			expectedName = api.MissingFilesDirectory + "/" + expectedName
		}

		actualName := tarEntries[i].name

		// Enforce ordering contract: tarEntries[i] must match req.In[i]
		tassert.Errorf(t, actualName == expectedName,
			"Order violation at position %d: expected '%s', got '%s'", i, expectedName, actualName)

		// Additional validation based on file type
		if isMissing {
			// Missing files should have zero size in TAR
			tassert.Errorf(t, tarEntries[i].size == 0,
				"Missing file should have zero size, got %d for %s", tarEntries[i].size, actualName)
			// Missing files should have zero content length
			tassert.Errorf(t, len(tarEntries[i].data) == 0,
				"Missing file should have zero content, got %d bytes for %s", len(tarEntries[i].data), actualName)
		} else {
			// Valid files: validate size contract
			tassert.Errorf(t, mossOut.Size == tarEntries[i].size,
				"Size mismatch for %s: MossOut.Size=%d, TAR size=%d",
				mossOut.ObjName, mossOut.Size, tarEntries[i].size)

			// Validate actual file content length
			tassert.Errorf(t, len(tarEntries[i].data) == int(tarEntries[i].size),
				"TAR data length mismatch for %s: expected %d, got %d",
				mossOut.ObjName, tarEntries[i].size, len(tarEntries[i].data))

			// Size should be positive for valid files
			tassert.Errorf(t, mossOut.Size > 0, "Expected positive size but got %d", mossOut.Size)
		}
	}

	tlog.Logf("TAR validation passed: %d entries, correct order and naming\n", len(tarEntries))
}

func testMossStreaming(t *testing.T, m *ioContext, test *mossConfig, mossIn []api.MossIn) {
	type streamResult struct {
		err      error
		duration time.Duration
		tarSize  int
		tarData  []byte
	}

	// Concurrency settings - same as multipart
	numConcurrentCalls := 5
	if testing.Short() {
		numConcurrentCalls = 2
	}

	tlog.Logf("Streaming GetBatch: %d objects, %d concurrent calls, continueOnErr=%t, onlyObjName=%t, withMissing=%t\n",
		len(mossIn), numConcurrentCalls, test.continueOnErr, test.onlyObjName, test.withMissing)

	var (
		req = &api.MossReq{
			In:            mossIn,
			ContinueOnErr: test.continueOnErr,
			OnlyObjName:   test.onlyObjName,
			StreamingGet:  true,
		}
		baseParams = tools.BaseAPIParams(m.proxyURL)
		results    = make([]streamResult, numConcurrentCalls)
		wg         sync.WaitGroup
		start      = mono.NanoTime()
	)

	// Execute concurrent streaming GetBatch calls
	for i := range numConcurrentCalls {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var (
				callStart = mono.NanoTime()
				buf       bytes.Buffer
			)
			_, err := api.GetBatch(baseParams, m.bck, req, &buf)
			callDuration := mono.Since(callStart)

			tarData := buf.Bytes()
			results[idx] = streamResult{
				err:      err,
				duration: callDuration,
				tarSize:  len(tarData),
				tarData:  tarData,
			}
		}(i)
	}

	wg.Wait()
	totalDuration := mono.Since(start)

	// Log latencies (compare w/ multipart)
	var (
		successCount, errorCount int
		totalTarSize             int64
		minDuration, maxDuration time.Duration
		sumDuration              time.Duration
	)
	minDuration = time.Hour // initialize to large value

	for i, result := range results {
		if result.err != nil {
			errorCount++
			tlog.Logf("  Streaming Call %d: ERROR in %v - %v\n", i, result.duration, result.err)
		} else {
			successCount++
			totalTarSize += int64(result.tarSize)
			sumDuration += result.duration
			if result.duration < minDuration {
				minDuration = result.duration
			}
			if result.duration > maxDuration {
				maxDuration = result.duration
			}
		}
	}

	avgDuration := sumDuration / time.Duration(successCount)
	tlog.Logf("Streaming GetBatch micro-bench: total=%v, min=%v, max=%v, avg=%v\n",
		totalDuration, minDuration, maxDuration, avgDuration)
	tlog.Logf("Streaming Success: %d/%d calls, Total TAR: %s\n",
		successCount, numConcurrentCalls, cos.ToSizeIEC(totalTarSize, 2))

	// Validate results
	switch {
	case test.withMissing && !test.continueOnErr:
		// Should fail fast on first error
		tassert.Errorf(t, errorCount > 0, "Expected streaming GetBatch calls to fail but they succeeded")
	default:
		// Should succeed - validate first successful result
		var firstSuccess *streamResult
		for i := range results {
			if results[i].err == nil {
				firstSuccess = &results[i]
				break
			}
		}
		tassert.Fatalf(t, firstSuccess != nil, "Expected at least one successful streaming call")

		// Build expected entries for validation
		expected := make(map[string]bool, len(mossIn))
		for i := range mossIn {
			in := &mossIn[i]
			expectedName := req.NameInRespArch(&m.bck, i)
			if in.ArchPath != "" {
				expectedName += "/" + in.ArchPath
			}
			if req.ContinueOnErr {
				switch {
				case strings.HasPrefix(in.ObjName, mossMissingPrefix), strings.HasSuffix(in.ArchPath, mossMissingSuffix):
					expected[api.MissingFilesDirectory+"/"+expectedName] = true
				default:
					expected[expectedName] = true
				}
			} else {
				expected[expectedName] = true
			}
		}

		// Validate first successful TAR
		validateTarStreaming(t, expected, bytes.NewReader(firstSuccess.tarData))

		// Validate consistency across concurrent calls
		for i := 1; i < len(results); i++ {
			if results[i].err == nil {
				tassert.Errorf(t, results[i].tarSize == firstSuccess.tarSize,
					"Inconsistent TAR size between concurrent streaming calls: %d vs %d",
					results[i].tarSize, firstSuccess.tarSize)
			}
		}
	}
}

func validateTarStreaming(t *testing.T, expected map[string]bool, r io.Reader) {
	var (
		tr  = tar.NewReader(r)
		num = len(expected)
		cnt int
	)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, hdr.Typeflag == tar.TypeReg, "expecting only regular files, got: (%q, %c)", hdr.Name, hdr.Typeflag)

		_, _ = io.Copy(io.Discard, tr)
		if _, ok := expected[hdr.Name]; ok {
			delete(expected, hdr.Name)
			cnt++
		} else {
			t.Errorf("missing or mismatched TAR entry: %q", hdr.Name)
		}
	}

	if num != cnt {
		t.Errorf("entry count mismatch: want %d, got %d", num, cnt)
	} else {
		tlog.Logf("Streaming TAR validation passed: %d entries\n", cnt)
	}
}

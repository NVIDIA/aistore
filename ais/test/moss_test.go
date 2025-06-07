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
// - streaming
// - max-targets

type mossConfig struct {
	archFormat    string // "" for plain objects, or archive.ExtTar, etc.
	continueOnErr bool   // GetBatch ContinueOnErr flag
	onlyObjName   bool   // GetBatch OnlyObjName flag
	withMissing   bool   // inject missing objects
	nested        bool   // subdirs in archives
}

func (c *mossConfig) name() (s string) {
	s = "plain"
	if c.archFormat != "" {
		s = c.archFormat
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
		// Plain object tests
		{archFormat: "", continueOnErr: false, onlyObjName: false},
		{archFormat: "", continueOnErr: true, onlyObjName: false},
		{archFormat: "", continueOnErr: true, onlyObjName: true},
		{archFormat: "", continueOnErr: true, onlyObjName: false, withMissing: true},

		// Archive tests
		{archFormat: archive.ExtTar, continueOnErr: false, onlyObjName: false},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, withMissing: true},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: true, withMissing: true},
		{archFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, nested: true},
		{archFormat: archive.ExtTgz, continueOnErr: true, onlyObjName: false, withMissing: true},
		{archFormat: archive.ExtZip, continueOnErr: false, onlyObjName: false, nested: true},
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
	mossInEntries := make([]api.MossIn, 0, numObjs)
	for i := range numObjs {
		mossInEntries = append(mossInEntries, api.MossIn{
			ObjName: m.objNames[i],
		})
	}

	// Inject missing objects if requested
	if test.withMissing {
		originalEntries := mossInEntries
		mossInEntries = make([]api.MossIn, 0, len(originalEntries)+len(originalEntries)/3)

		for i, entry := range originalEntries {
			mossInEntries = append(mossInEntries, entry)
			if i%3 == 0 {
				mossInEntries = append(mossInEntries, api.MossIn{
					ObjName: "missing-" + trand.String(8),
				})
			}
		}
	}

	_testMoss(t, m, mossInEntries, test)
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

	// Build MossIn entries directly for archive contents
	var mossInEntries []api.MossIn
	for _, archInfo := range archives {
		// Add a few files from each archive
		numToRequest := min(len(archInfo.filePaths), max(numInArch/5, 3))
		for j := range numToRequest {
			mossInEntries = append(mossInEntries, api.MossIn{
				ObjName:  archInfo.name,
				ArchPath: archInfo.filePaths[j],
			})
		}
	}

	// Inject missing archive paths if requested
	if test.withMissing {
		for i := range mossInEntries {
			if i%3 == 0 {
				mossInEntries[i].ArchPath = trand.String(8) + ".nonexistent"
			}
		}
	}

	_testMoss(t, m, mossInEntries, test)
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

	// Create archive of a given format
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

func _testMoss(t *testing.T, m *ioContext, mossInEntries []api.MossIn, test *mossConfig) {
	type result struct {
		resp     api.MossResp
		err      error
		duration time.Duration
		tarSize  int
		tarData  []byte
	}

	// Concurrency settings
	numConcurrentCalls := 5 // Multiple concurrent GetBatch calls
	if testing.Short() {
		numConcurrentCalls = 2
	}

	tlog.Logf("GetBatch: %d objects, %d concurrent calls, continueOnErr=%t, onlyObjName=%t, withMissing=%t\n",
		len(mossInEntries), numConcurrentCalls, test.continueOnErr, test.onlyObjName, test.withMissing)

	// Prepare MossReq
	req := &api.MossReq{
		In:            mossInEntries,
		ContinueOnErr: test.continueOnErr,
		OnlyObjName:   test.onlyObjName,
		StreamingGet:  false,
	}

	// Execute multiple concurrent GetBatch calls
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

	// Log performance results
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
			// tlog.Logf("  Call %d: SUCCESS in %v, TAR size: %d bytes\n", i, result.duration, result.tarSize)
		}
	}

	avgDuration := sumDuration / time.Duration(successCount)
	tlog.Logf("GetBatch micro-bench: total=%v, min=%v, max=%v, avg=%v\n",
		totalDuration, minDuration, maxDuration, avgDuration)
	tlog.Logf("Success: %d/%d calls, Total TAR: %s\n",
		successCount, numConcurrentCalls, cos.ToSizeIEC(totalTarSize, 2))

	// Validate results - use the first successful result for detailed validation
	var firstSuccess *result
	for i := range results {
		if results[i].err == nil {
			firstSuccess = &results[i]
			break
		}
	}

	switch {
	case test.withMissing && test.continueOnErr:
		// Should succeed but have some failed entries
		tassert.Fatalf(t, firstSuccess != nil, "Expected at least one successful call")

		var objSuccessCount, objErrorCount int
		for _, objResult := range firstSuccess.resp.Out {
			if objResult.ErrMsg != "" {
				objErrorCount++
			} else {
				objSuccessCount++
			}
		}

		tassert.Fatalf(t, objErrorCount > 0, "Expected some object errors but got none")
		tassert.Fatalf(t, objSuccessCount > 0, "Expected some object successes but got none")
		tlog.Logf("Object results: %d success, %d errors\n", objSuccessCount, objErrorCount)
	case test.withMissing && !test.continueOnErr:
		// Should fail fast on first error
		tassert.Errorf(t, errorCount > 0, "Expected GetBatch calls to fail but they succeeded")
	default:
		// Should succeed completely
		tassert.Fatalf(t, firstSuccess != nil, "Expected successful calls but all failed")
		tassert.Fatalf(t, len(firstSuccess.resp.Out) == len(mossInEntries),
			"Expected %d results, got %d", len(mossInEntries), len(firstSuccess.resp.Out))

		for i, objResult := range firstSuccess.resp.Out {
			tassert.Errorf(t, objResult.ErrMsg == "", "Unexpected error for %s: %s",
				mossInEntries[i].ObjName, objResult.ErrMsg)

			if test.onlyObjName {
				tassert.Errorf(t, objResult.ObjName != "", "Missing ObjName in result")
			}

			tassert.Errorf(t, objResult.Size > 0, "Expected positive size but got %d", objResult.Size)
		}

		// Validate TAR contents against MossResp and the contract
		validateTARContents(t, m, req, firstSuccess.resp,
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
}

func validateTARContents(t *testing.T, m *ioContext, req *api.MossReq, resp api.MossResp, tarReader io.Reader, tarSize int) {
	tlog.Logln("Validating TAR contents: " + cos.ToSizeIEC(int64(tarSize), 2))

	// Define TAR entry structure
	type tarEntry struct {
		name string
		size int64
		data []byte
	}

	// Parse TAR and collect all entries
	var (
		tr         = tar.NewReader(tarReader)
		tarEntries = make([]tarEntry, 0, len(req.In))
	)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		tassert.CheckFatal(t, err)

		// Read file data
		data, err := io.ReadAll(tr)
		tassert.CheckFatal(t, err)

		tarEntries = append(tarEntries, tarEntry{
			name: header.Name,
			size: header.Size,
			data: data,
		})
	}

	// Contract validation: same number of entries
	tassert.Fatalf(t, len(tarEntries) == len(resp.Out),
		"TAR entries (%d) != MossResp entries (%d)", len(tarEntries), len(resp.Out))

	// Contract validation: precise order and naming convention
	for i, mossOut := range resp.Out {
		expectedName := buildExpectedTARName(m.bck, req, i)
		actualName := tarEntries[i].name

		// Handle missing files case
		if expectMissingFile(&mossOut) {
			expectedMissingName := api.MissingFilesDirectory + "/" + expectedName
			tassert.Errorf(t, actualName == expectedMissingName,
				"Missing file naming: expected '%s', got '%s'", expectedMissingName, actualName)
			tassert.Errorf(t, tarEntries[i].size == 0,
				"Missing file should have zero size, got %d", tarEntries[i].size)
		} else {
			tassert.Errorf(t, actualName == expectedName,
				"TAR naming: expected '%s', got '%s'", expectedName, actualName)

			// Contract validation: MossOut.Size == TAR entry size
			tassert.Errorf(t, mossOut.Size == tarEntries[i].size,
				"Size mismatch for %s: MossOut.Size=%d, TAR size=%d",
				mossOut.ObjName, mossOut.Size, tarEntries[i].size)

			// Validate actual file content (basic sanity check)
			tassert.Errorf(t, len(tarEntries[i].data) == int(tarEntries[i].size),
				"TAR data length mismatch for %s: expected %d, got %d",
				mossOut.ObjName, tarEntries[i].size, len(tarEntries[i].data))
		}
	}

	tlog.Logf("TAR validation passed: %d entries, correct order and naming\n", len(tarEntries))
}

func buildExpectedTARName(bck cmn.Bck, req *api.MossReq, index int) string {
	in := req.In[index]

	// Use the helper method from MossReq
	baseName := req.NameInRespArch(&bck, index)

	// Add archive path if present
	if in.ArchPath != "" {
		return baseName + "/" + in.ArchPath
	}
	return baseName
}

func expectMissingFile(mossOut *api.MossOut) bool {
	return mossOut.ErrMsg != "" && strings.Contains(mossOut.ErrMsg, "does not exist")
}

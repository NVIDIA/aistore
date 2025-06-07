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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

type mossConfig struct {
	name          string
	archFormat    string // "" for plain objects, or archive.ExtTar, etc.
	continueOnErr bool   // GetBatch ContinueOnErr flag
	onlyObjName   bool   // GetBatch OnlyObjName flag
	withMissing   bool   // inject missing objects
	nested        bool   // subdirs in archives
}

func TestMoss(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1}) // TODO -- FIXME: remove

	var (
		numPlainObjs = 500 // scale
		numArchives  = 20  // archives to create
		numInArch    = 50  // files per archive
		fileSize     = 4 * cos.KiB
	)
	if testing.Short() {
		numPlainObjs = 50
		numArchives = 3
		numInArch = 10
	}

	tests := []mossConfig{
		// Plain object tests
		{
			name:          "plain/continue=false/names=false",
			archFormat:    "",
			continueOnErr: false,
			onlyObjName:   false,
		},
		{
			name:          "plain/continue=true/names=false",
			archFormat:    "",
			continueOnErr: true,
			onlyObjName:   false,
		},
		{
			name:          "plain/continue=true/names=true",
			archFormat:    "",
			continueOnErr: true,
			onlyObjName:   true,
		},
		{
			name:          "plain/continue=true/with-missing",
			archFormat:    "",
			continueOnErr: true,
			onlyObjName:   false,
			withMissing:   true,
		},

		// Archive tests
		{
			name:          "tar/continue=false/names=false",
			archFormat:    archive.ExtTar,
			continueOnErr: false,
			onlyObjName:   false,
		},
		{
			name:          "tar/continue=true/names=true",
			archFormat:    archive.ExtTar,
			continueOnErr: true,
			onlyObjName:   true,
		},
		{
			name:          "tar/continue=true/nested",
			archFormat:    archive.ExtTar,
			continueOnErr: true,
			onlyObjName:   false,
			nested:        true,
		},
		{
			name:          "tgz/continue=true/with-missing",
			archFormat:    archive.ExtTgz,
			continueOnErr: true,
			onlyObjName:   false,
			withMissing:   true,
		},
		{
			name:          "zip/continue=false/nested",
			archFormat:    archive.ExtZip,
			continueOnErr: false,
			onlyObjName:   false,
			nested:        true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			m.init(true /*cleanup*/)

			if test.archFormat == "" {
				// Plain objects test
				testMossPlainObjects(t, &m, &test, numPlainObjs)
			} else {
				// Archive test
				testMossArchives(t, &m, &test, numArchives, numInArch)
			}
		})
	}
}

func testMossPlainObjects(t *testing.T, m *ioContext, test *mossConfig, numObjs int) {
	m.num = numObjs
	m.puts()

	// Build GetBatch request list (pre-allocate)
	objNames := make([]string, 0, numObjs)
	for i := range numObjs {
		objNames = append(objNames, m.objNames[i])
	}

	// Inject missing objects if requested
	if test.withMissing {
		// Add some non-existent objects every 10th position
		for i := 9; i < len(objNames); i += 10 {
			objNames[i] = "missing-" + trand.String(8)
		}
	}

	testGetBatchCore(t, m, objNames, test.continueOnErr, test.onlyObjName, test.withMissing)
}

func testMossArchives(t *testing.T, m *ioContext, test *mossConfig, numArchives, numInArch int) {
	const tmpDir = "/tmp"

	// Track actual files created in each archive for accurate retrieval
	type archiveInfo struct {
		name      string
		filePaths []string
	}

	// Collect all archive info (pre-allocate)
	archives := make([]archiveInfo, 0, numArchives)

	// Create archives similar to archive_test.go pattern
	errCh := make(chan error, numArchives)
	wg := cos.NewLimitedWaitGroup(10, 0)
	archInfoCh := make(chan archiveInfo, numArchives)

	for i := range numArchives {
		archName := fmt.Sprintf("archive%02d%s", i, test.archFormat)

		wg.Add(1)
		go func(archName string) {
			defer wg.Done()

			// Generate deterministic filenames for archive content
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

			// Create archive file
			archPath := tmpDir + "/" + cos.GenTie() + test.archFormat
			defer os.Remove(archPath)

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
				errCh <- err
				return
			}

			// Upload archive to cluster
			reader, err := readers.NewExistingFile(archPath, cos.ChecksumNone)
			if err != nil {
				errCh <- err
				return
			}

			tools.Put(m.proxyURL, m.bck, archName, reader, errCh)

			// Send back the archive info with actual file paths
			archInfoCh <- archiveInfo{
				name:      archName,
				filePaths: randomNames,
			}
		}(archName)
	}

	wg.Wait()
	close(archInfoCh)
	tassert.SelectErr(t, errCh, "create and upload archives", true)

	// Collect all archive info
	for info := range archInfoCh {
		archives = append(archives, info)
	}

	// Build GetBatch request list for archive contents using actual file paths
	var objNames []string
	for _, archInfo := range archives {
		// Add a few files from each archive
		numToRequest := min(len(archInfo.filePaths), max(numInArch/5, 3))
		for j := range numToRequest {
			// Use actual file paths that exist in the archive
			archPath := archInfo.filePaths[j]

			// Format: "archive.tar?archpath=path/to/file"
			objName := archInfo.name + "?archpath=" + archPath
			objNames = append(objNames, objName)
		}
	}

	// Inject missing archive paths if requested
	if test.withMissing {
		// Ensure we have SOME valid paths and SOME missing paths
		validCount := len(objNames)
		missingCount := max(1, validCount/5) // 20% missing

		// Only replace the last N entries with missing paths
		for i := validCount - missingCount; i < validCount; i++ {
			parts := strings.Split(objNames[i], "?archpath=")
			missingPath := "definitely-missing/" + trand.String(8) + ".txt"
			objNames[i] = parts[0] + "?archpath=" + missingPath
		}
	}

	testGetBatchCore(t, m, objNames, test.continueOnErr, test.onlyObjName, test.withMissing)
}

func testGetBatchCore(t *testing.T, m *ioContext, objNames []string, continueOnErr, onlyObjName, expectErrors bool) {
	batchSize := len(objNames)
	if !testing.Short() && batchSize > 100 {
		// Test with larger batches in non-short mode
		batchSize = min(batchSize, 200)
		objNames = objNames[:batchSize]
	}

	// Concurrency settings
	numConcurrentCalls := 5 // Multiple concurrent GetBatch calls
	if testing.Short() {
		numConcurrentCalls = 2
	}

	tlog.Logf("GetBatch: %d objects, %d concurrent calls, continueOnErr=%t, onlyObjName=%t, expectErrors=%t\n",
		batchSize, numConcurrentCalls, continueOnErr, onlyObjName, expectErrors)

	// Build MossReq with individual MossIn entries
	var mossInEntries []api.MossIn
	for _, objName := range objNames {
		// Handle archive paths: "archive.tar?archpath=path/to/file"
		parts := strings.Split(objName, "?archpath=")
		if len(parts) == 2 {
			// Archive file
			mossInEntries = append(mossInEntries, api.MossIn{
				ObjName:  parts[0],
				ArchPath: parts[1],
			})
		} else {
			// Plain object
			mossInEntries = append(mossInEntries, api.MossIn{
				ObjName: objName,
			})
		}
	}

	// Prepare MossReq template
	reqTemplate := &api.MossReq{
		In:            mossInEntries,
		ContinueOnErr: continueOnErr,
		OnlyObjName:   onlyObjName,
		StreamingGet:  false,
	}

	// Execute multiple concurrent GetBatch calls
	type result struct {
		resp     api.MossResp
		err      error
		duration time.Duration
		tarSize  int
		tarData  []byte
	}

	var (
		results    = make([]result, numConcurrentCalls)
		wg         sync.WaitGroup
		baseParams = tools.BaseAPIParams(m.proxyURL)
		start      = time.Now()
	)
	for i := range numConcurrentCalls {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var (
				callStart = time.Now()
				buf       strings.Builder
			)
			resp, err := api.GetBatch(baseParams, m.bck, reqTemplate, &buf)
			callDuration := time.Since(callStart)

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
	totalDuration := time.Since(start)

	// Log performance results
	var successCount, errorCount int
	var totalTarSize int64
	var minDuration, maxDuration, sumDuration time.Duration
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
			tlog.Logf("  Call %d: SUCCESS in %v, TAR size: %d bytes\n", i, result.duration, result.tarSize)
		}
	}

	avgDuration := sumDuration / time.Duration(successCount)
	tlog.Logf("GetBatch concurrency results: total=%v, min=%v, max=%v, avg=%v\n",
		totalDuration, minDuration, maxDuration, avgDuration)
	tlog.Logf("Success: %d/%d calls, Total TAR: %d bytes\n",
		successCount, numConcurrentCalls, totalTarSize)

	// Validate results - use the first successful result for detailed validation
	var firstSuccess *result
	for i := range results {
		if results[i].err == nil {
			firstSuccess = &results[i]
			break
		}
	}

	switch {
	case expectErrors && continueOnErr:
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
	case expectErrors && !continueOnErr:
		// Should fail fast on first error
		tassert.Errorf(t, errorCount > 0, "Expected GetBatch calls to fail but they succeeded")
	default:
		// Should succeed completely
		tassert.Fatalf(t, firstSuccess != nil, "Expected successful calls but all failed")
		tassert.Fatalf(t, len(firstSuccess.resp.Out) == len(objNames),
			"Expected %d results, got %d", len(objNames), len(firstSuccess.resp.Out))

		for i, objResult := range firstSuccess.resp.Out {
			tassert.Errorf(t, objResult.ErrMsg == "", "Unexpected error for %s: %s",
				objNames[i], objResult.ErrMsg)

			if onlyObjName {
				tassert.Errorf(t, objResult.ObjName != "", "Missing ObjName in result")
			}

			tassert.Errorf(t, objResult.Size > 0, "Expected positive size but got %d", objResult.Size)
		}

		// Validate TAR contents against MossResp and the contract
		validateTARContents(t, m, reqTemplate, firstSuccess.resp,
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
	tlog.Logf("Validating TAR contents: %d bytes\n", tarSize)

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

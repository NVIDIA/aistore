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
	inputFormat   string // AIStore to read (and serialize) plain objects or sharded files ("" defaults to plain)
	outputFormat  string // AIStore to return result as TAR or any other supported serialization format ("" defaults to TAR)
	continueOnErr bool   // GetBatch ContinueOnErr flag
	onlyObjName   bool   // GetBatch OnlyObjName flag
	withMissing   bool   // inject missing objects
	nested        bool   // subdirs in archives
	streaming     bool   // streaming GET (w/ no out-of-band apc.MossResp metadata)
}

func (c *mossConfig) name() (s string) {
	s = "plain"
	if c.inputFormat != "" {
		s = c.inputFormat
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
	if c.outputFormat != "" {
		s += "=>" + c.outputFormat
	}
	return s
}

// [NOTE]
// Compressed archives may vary slightly in size when identical requests are processed
// by different AIS targets due to compression: timing, buffer states, and metadata differences.
// Allow 1% tolerance for compressed TAR formats while requiring exact equality for the rest.
func equalSize(outputFormat string, size1, size2 int) bool {
	switch outputFormat {
	case archive.ExtTgz, archive.ExtTarGz, archive.ExtTarLz4:
		if size1 == size2 {
			return true
		}
		d := size1 - size2
		if d < 0 {
			d = -d
		}
		return d*100 < min(size1, size2)
	default:
		return size1 == size2
	}
}

func TestMoss(t *testing.T) {
	var (
		numPlainObjs = 300 // plain objects to create
		numArchives  = 100 // num shards to create
		numInArch    = 20  // files per archive (shard)
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
		{inputFormat: "", continueOnErr: false, onlyObjName: false},
		{inputFormat: "", continueOnErr: true, onlyObjName: false},
		{inputFormat: "", continueOnErr: true, onlyObjName: true},
		{inputFormat: "", continueOnErr: true, onlyObjName: false, withMissing: true},

		// (multi-part w/ out-of-band metadata; read from archives (shards)) tests
		{inputFormat: archive.ExtTar, continueOnErr: false, onlyObjName: false},
		{inputFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, withMissing: true},
		{inputFormat: archive.ExtTar, continueOnErr: true, onlyObjName: true, withMissing: true},
		{inputFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, nested: true},
		{inputFormat: archive.ExtTgz, continueOnErr: true, onlyObjName: false, withMissing: true},
		{inputFormat: archive.ExtZip, continueOnErr: false, onlyObjName: false, nested: true},

		// (multi-part; read plain objects and format output as something other than TAR)
		{inputFormat: "", outputFormat: archive.ExtTgz, continueOnErr: false, onlyObjName: false},
		{inputFormat: "", outputFormat: archive.ExtTgz, continueOnErr: true, onlyObjName: false, withMissing: true},
		{inputFormat: "", outputFormat: archive.ExtZip, continueOnErr: false, onlyObjName: true},
		{inputFormat: "", outputFormat: archive.ExtTarLz4, continueOnErr: true, onlyObjName: true, withMissing: true},

		// (multi-part; read archived files and format output as specified)
		{inputFormat: archive.ExtTar, outputFormat: archive.ExtTgz, continueOnErr: false, onlyObjName: false},
		{inputFormat: archive.ExtTgz, outputFormat: archive.ExtTgz, continueOnErr: true, onlyObjName: false, withMissing: true},
		{inputFormat: archive.ExtTar, outputFormat: archive.ExtZip, continueOnErr: false, onlyObjName: true},

		// (streaming; read objects) tests
		{inputFormat: "", streaming: true},
		{inputFormat: "", continueOnErr: true, withMissing: true, streaming: true},
		{inputFormat: "", continueOnErr: true, onlyObjName: true, withMissing: true, streaming: true},
		{inputFormat: archive.ExtTgz, continueOnErr: true, onlyObjName: false, withMissing: true, streaming: true},

		// (streaming, read from archives (shards)) tests
		{inputFormat: archive.ExtTar, streaming: true},
		{inputFormat: archive.ExtTar, continueOnErr: true, onlyObjName: false, withMissing: true, streaming: true},
		{inputFormat: archive.ExtTar, continueOnErr: true, onlyObjName: true, withMissing: true, streaming: true},
		{inputFormat: archive.ExtZip, continueOnErr: false, onlyObjName: false, nested: true, streaming: true},

		// (streaming; read plain objects and format output as something other than TAR)
		{inputFormat: "", outputFormat: archive.ExtTgz, streaming: true},
		{inputFormat: "", outputFormat: archive.ExtTarLz4, continueOnErr: true, withMissing: true, streaming: true},
		{inputFormat: "", outputFormat: archive.ExtZip, continueOnErr: true, onlyObjName: true, withMissing: true, streaming: true},

		// (streaming; read from shards and format output as ...)
		{inputFormat: archive.ExtTar, outputFormat: archive.ExtTgz, streaming: true},
		{inputFormat: archive.ExtTar, outputFormat: archive.ExtTarLz4, continueOnErr: true, withMissing: true, streaming: true},
		{inputFormat: archive.ExtTarLz4, outputFormat: archive.ExtZip, continueOnErr: true, onlyObjName: true, withMissing: true, streaming: true},
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
			// randomize file sizes
			if fileSize > 10 {
				m.fileSizeRange[0] = m.fileSize - m.fileSize/2
				m.fileSizeRange[1] = m.fileSize + m.fileSize/2
			}

			tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
			m.init(true /*cleanup*/)

			if test.inputFormat == "" {
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
	mossIn := make([]apc.MossIn, 0, numObjs)
	for i := range numObjs {
		mossIn = append(mossIn, apc.MossIn{
			ObjName: m.objNames[i],
			Opaque:  []byte(cos.GenTie()),
		})
	}

	// Inject missing objects if requested
	if test.withMissing {
		originalEntries := mossIn
		mossIn = make([]apc.MossIn, 0, len(originalEntries)+len(originalEntries)/3)

		for i, entry := range originalEntries {
			mossIn = append(mossIn, entry)
			if i%3 == 0 {
				mossIn = append(mossIn, apc.MossIn{
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
		archName := fmt.Sprintf("archive%02d%s", i, test.inputFormat)

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

	// Build apc.MossIn entries
	var mossIn []apc.MossIn
	for _, archInfo := range archives {
		// Add a few files from each archive
		numToRequest := min(len(archInfo.filePaths), max(numInArch/5, 3))
		for j := range numToRequest {
			mossIn = append(mossIn, apc.MossIn{
				ObjName:  archInfo.name,
				ArchPath: archInfo.filePaths[j],
				Opaque:   []byte(cos.GenTie()),
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
	archPath := tmpDir + "/" + cos.GenTie() + test.inputFormat
	err := tarch.CreateArchRandomFiles(
		archPath,
		tar.FormatUnknown, // tar format
		test.inputFormat,  // file extension
		numInArch,         // file count
		int(m.fileSize),   // file size
		nil,               // no record extensions
		randomNames,       // pregenerated filenames
		false,             // no duplication
		false,             // no random dir prefix
		false,             // not exact size
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

func testMossMultipart(t *testing.T, m *ioContext, test *mossConfig, mossIn []apc.MossIn) {
	type result struct {
		resp     apc.MossResp
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

	tlog.Logfln("GetBatch: %d objects, %d concurrent calls, continueOnErr=%t, onlyObjName=%t, withMissing=%t",
		len(mossIn), numConcurrentCalls, test.continueOnErr, test.onlyObjName, test.withMissing)

	// Prepare apc.MossReq
	req := &apc.MossReq{
		In:            mossIn,
		ContinueOnErr: test.continueOnErr,
		OnlyObjName:   test.onlyObjName,
		StreamingGet:  false,
		OutputFormat:  test.outputFormat,
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
			tlog.Logfln("  Call %d: ERROR in %v - %v", i, result.duration, result.err)
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

	tassert.Fatalf(t, successCount > 0, "Expected at least some GetBatch calls to succeed (multipart mode)")

	avgDuration := sumDuration / time.Duration(successCount)
	tlog.Logfln("GetBatch micro-bench: total=%v, min=%v, max=%v, avg=%v",
		totalDuration, minDuration, maxDuration, avgDuration)
	tlog.Logfln("Success: %d/%d calls, Total TAR: %s",
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
		tlog.Logfln("Object results: %d success, %d errors", objSuccessCount, objErrorCount)
	} else {
		// No missing files expected - all should succeed
		for i, objResult := range firstSuccess.resp.Out {
			tassert.Errorf(t, objResult.ErrMsg == "", "Unexpected error for %s: %s",
				mossIn[i].ObjName, objResult.ErrMsg)
		}
	}

	// Always validate TAR contents for successful responses
	validateTarMultipartWithArchive(t, req, firstSuccess.resp, bytes.NewReader(firstSuccess.tarData), len(firstSuccess.tarData))

	// Validate consistency across concurrent calls
	for i := 1; i < len(results); i++ {
		if results[i].err == nil {
			tassert.Errorf(t, len(results[i].resp.Out) == len(firstSuccess.resp.Out),
				"Inconsistent result count between concurrent calls")
			tassert.Errorf(t, equalSize(test.outputFormat, firstSuccess.tarSize, results[i].tarSize),
				"Inconsistent TAR size between concurrent calls: %d vs %d",
				results[i].tarSize, firstSuccess.tarSize)
		}
	}
}

// Helper function to determine if a file is expected to be missing based on request patterns
func isMissingFile(mossIn *apc.MossIn) bool {
	// Missing objects have "missing-" prefix
	if strings.HasPrefix(mossIn.ObjName, mossMissingPrefix) {
		return true
	}
	// Missing archive paths have ".nonexistent" suffix
	return mossIn.ArchPath != "" && strings.HasSuffix(mossIn.ArchPath, mossMissingSuffix)
}

func testMossStreaming(t *testing.T, m *ioContext, test *mossConfig, mossIn []apc.MossIn) {
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

	tlog.Logfln("Streaming GetBatch: %d objects, %d concurrent calls, continueOnErr=%t, onlyObjName=%t, withMissing=%t",
		len(mossIn), numConcurrentCalls, test.continueOnErr, test.onlyObjName, test.withMissing)

	var (
		req = &apc.MossReq{
			In:            mossIn,
			ContinueOnErr: test.continueOnErr,
			OnlyObjName:   test.onlyObjName,
			StreamingGet:  true,
			OutputFormat:  test.outputFormat,
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
			tlog.Logfln("  Streaming Call %d: ERROR in %v - %v", i, result.duration, result.err)
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

	tassert.Fatalf(t, successCount > 0, "Expected at least some GetBatch calls to succeed (streaming mode)")
	avgDuration := sumDuration / time.Duration(successCount)

	tlog.Logfln("Streaming GetBatch micro-bench: total=%v, min=%v, max=%v, avg=%v",
		totalDuration, minDuration, maxDuration, avgDuration)
	tlog.Logfln("Streaming Success: %d/%d calls, Total TAR: %s",
		successCount, numConcurrentCalls, cos.ToSizeIEC(totalTarSize, 2))

	// Validate results
	switch {
	case test.withMissing && !test.continueOnErr:
		// Should fail fast on first error
		tassert.Errorf(t, errorCount > 0, "Expected streaming GetBatch calls to fail but they succeeded")
	default:
		// Should succeed - validate first successful result with ORDER ENFORCEMENT
		var firstSuccess *streamResult
		for i := range results {
			if results[i].err == nil {
				firstSuccess = &results[i]
				break
			}
		}
		tassert.Fatalf(t, firstSuccess != nil, "Expected at least one successful streaming call")

		// NEW: Use positional validation instead of set-based validation
		validateTarStreamingWithArchive(t, m, req, bytes.NewReader(firstSuccess.tarData))

		// Validate consistency across concurrent calls
		for i := 1; i < len(results); i++ {
			if results[i].err == nil {
				tassert.Errorf(t, equalSize(test.outputFormat, firstSuccess.tarSize, results[i].tarSize),
					"Inconsistent TAR size between concurrent streaming calls: %d vs %d",
					results[i].tarSize, firstSuccess.tarSize)
			}
		}
	}
}

//
// --------------------------------------------
//

// Enhanced validation using cmn/archive package for proper format support

// Archive validation callback for collecting TAR entries
type tarValidationCallback struct {
	entries []tarEntry
	req     *apc.MossReq
	resp    *apc.MossResp // nil for streaming validation
	t       *testing.T
	mode    string // "multipart" or "streaming"
}

type tarEntry struct {
	name string
	size int64
	data []byte // only populated for multipart validation
}

// Implement archive.ArchRCB interface
func (cb *tarValidationCallback) Call(filename string, reader cos.ReadCloseSizer, _ any) (bool, error) {
	entry := tarEntry{
		name: filename,
		size: reader.Size(),
	}

	// For multipart validation, read the data to validate content
	if cb.mode == "multipart" {
		data, err := io.ReadAll(reader)
		if err != nil {
			return true, fmt.Errorf("failed to read TAR entry %s: %w", filename, err)
		}
		entry.data = data

		// Validate data length matches size
		if len(data) != int(entry.size) {
			return true, fmt.Errorf("data length mismatch for %s: expected %d, got %d",
				filename, entry.size, len(data))
		}
	} else {
		// For streaming validation, just discard data (we only care about names and order)
		_, _ = io.Copy(io.Discard, reader)
	}

	cb.entries = append(cb.entries, entry)
	return false, nil // continue processing
}

// Enhanced TAR validation for multipart
func validateTarMultipartWithArchive(t *testing.T, req *apc.MossReq, resp apc.MossResp, tarReader io.Reader, tarSize int) {
	tlog.Logln("Validating TAR contents with archive package: " + cos.ToSizeIEC(int64(tarSize), 2))

	// Determine archive format - default to plain TAR if not specified
	format := req.OutputFormat
	if format == "" {
		format = archive.ExtTar
	}

	// Create archive reader with proper format detection
	ar, err := archive.NewReader(format, tarReader, int64(tarSize))
	tassert.CheckFatal(t, err)

	// Create validation callback
	callback := &tarValidationCallback{
		entries: make([]tarEntry, 0, len(req.In)),
		req:     req,
		resp:    &resp,
		t:       t,
		mode:    "multipart",
	}

	// Process all TAR entries
	err = ar.ReadUntil(callback, cos.EmptyMatchAll, "")
	tassert.CheckFatal(t, err)

	// Contract validation: same number of entries
	tassert.Fatalf(t, len(callback.entries) == len(resp.Out),
		"TAR entries (%d) != MossResp entries (%d)", len(callback.entries), len(resp.Out))

	// Contract validation: precise order - validate each position
	for i := range resp.Out {
		var (
			expectedName string
			mossOut      = &resp.Out[i]
			tarEntry     = &callback.entries[i]
		)
		// tlog.Logfln("DEBUG: mossOut.Bucket = %q, ObjName = %q", mossOut.Bucket, mossOut.ObjName)

		// Calculate expected name based on onlyObjName setting
		if req.OnlyObjName {
			expectedName = mossOut.ObjName
			if mossOut.ArchPath != "" {
				expectedName += "/" + mossOut.ArchPath
			}
		} else {
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
			expectedName = apc.MossMissingDir + "/" + expectedName
		}

		// Enforce ordering contract: tarEntries[i] must match req.In[i]
		tassert.Errorf(t, tarEntry.name == expectedName,
			"Order violation at position %d: expected '%s', got '%s'", i, expectedName, tarEntry.name)

		// Additional validation based on file type
		if isMissing {
			// Missing files should have zero size in TAR
			tassert.Errorf(t, tarEntry.size == 0,
				"Missing file should have zero size, got %d for %s", tarEntry.size, tarEntry.name)
			// Missing files should have zero content length
			tassert.Errorf(t, len(tarEntry.data) == 0,
				"Missing file should have zero content, got %d bytes for %s", len(tarEntry.data), tarEntry.name)
		} else {
			// Valid files: validate size contract
			tassert.Errorf(t, mossOut.Size == tarEntry.size,
				"Size mismatch for %s: MossOut.Size=%d, TAR size=%d",
				mossOut.ObjName, mossOut.Size, tarEntry.size)

			// Size should be positive for valid files
			tassert.Errorf(t, mossOut.Size > 0, "Expected positive size but got %d", mossOut.Size)
		}

		// Opaque must be identical
		tassert.Errorf(t, bytes.Equal(mossOut.Opaque, mossIn.Opaque),
			"Opaque mismatch for %s: MossOut.Opaque=%s, MossIn.Opaque=%s",
			mossOut.ObjName, cos.BHead(mossOut.Opaque), cos.BHead(mossIn.Opaque))
	}

	tlog.Logfln("Archive validation passed: %d entries, correct order and naming (format: %s)",
		len(callback.entries), format)
}

// Enhanced streaming validation
func validateTarStreamingWithArchive(t *testing.T, m *ioContext, req *apc.MossReq, tarReader io.Reader) {
	// Determine archive format
	format := req.OutputFormat
	if format == "" {
		format = archive.ExtTar
	}

	// Create archive reader - for streaming we need to read all data first to get size
	tarData, err := io.ReadAll(tarReader)
	tassert.CheckFatal(t, err)

	ar, err := archive.NewReader(format, bytes.NewReader(tarData), int64(len(tarData)))
	tassert.CheckFatal(t, err)

	// Create validation callback for streaming (no data reading)
	callback := &tarValidationCallback{
		entries: make([]tarEntry, 0, len(req.In)),
		req:     req,
		resp:    nil, // streaming has no response metadata
		t:       t,
		mode:    "streaming",
	}

	// Process all TAR entries
	err = ar.ReadUntil(callback, cos.EmptyMatchAll, "")
	tassert.CheckFatal(t, err)

	// Build expected entries in order from request (same logic as before)
	expectedEntries := make([]string, 0, len(req.In))
	for i := range req.In {
		var (
			expectedName string
			mossIn       = &req.In[i]
		)

		// Calculate expected name based on onlyObjName setting
		if req.OnlyObjName {
			expectedName = mossIn.ObjName
			if mossIn.ArchPath != "" {
				expectedName += "/" + mossIn.ArchPath
			}
		} else {
			expectedName = m.bck.Name + "/" + mossIn.ObjName
			if mossIn.ArchPath != "" {
				expectedName += "/" + mossIn.ArchPath
			}
		}

		// Handle missing files
		if isMissingFile(mossIn) {
			expectedName = apc.MossMissingDir + "/" + expectedName
		}

		expectedEntries = append(expectedEntries, expectedName)
	}

	// Validate count matches expected
	tassert.Fatalf(t, len(callback.entries) == len(expectedEntries),
		"TAR entry count mismatch: expected %d, got %d", len(expectedEntries), len(callback.entries))

	// Validate precise positional order
	for i, expectedName := range expectedEntries {
		actualName := callback.entries[i].name
		tassert.Errorf(t, actualName == expectedName,
			"Streaming order violation at position %d: expected '%s', got '%s'", i, expectedName, actualName)
	}

	tlog.Logfln("Streaming archive validation passed: %d entries, correct order (format: %s)",
		len(callback.entries), format)
}

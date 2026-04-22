// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"archive/tar"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

func TestIndexShard(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-shard-idx-" + trand.String(6), Provider: apc.AIS}
		numShards  = 5
		numFiles   = 10
		fileSize   = 4 * cos.KiB
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tmpDir := t.TempDir()
	tarNames := idxUploadTarShards(t, baseParams, bck, tmpDir, "" /*prefix*/, numShards, numFiles, fileSize)

	// Also upload a non-TAR object — must not be indexed.
	nonTarName := trand.String(8) + ".txt"
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: cos.KiB, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: nonTarName, Reader: r, Size: cos.KiB})
	tassert.CheckFatal(t, err)

	tlog.Logf("Uploaded %d TAR shards and 1 non-TAR object to %s\n", numShards, bck)

	for _, tc := range []struct {
		name string
		nw   int
	}{
		{"nw=none", xact.NwpNone},
		{"nw=default", xact.NwpDflt},
		{"nw=2", 2},
		{"nw=8", 8},
		{"nw=32", 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tlog.Logf("Starting %s (workers=%d)\n", apc.ActIndexShard, tc.nw)
			idxRunAndWait(t, baseParams, bck, &apc.IndexShardMsg{NumWorkers: tc.nw})
			idxValidate(t, bck, tarNames, nonTarName, numFiles)
		})
	}
}

func TestIndexShardPrefix(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-shard-idx-pfx-" + trand.String(6), Provider: apc.AIS}
		numShards  = 5
		numFiles   = 10
		fileSize   = 4 * cos.KiB
		pfxA       = "shard-a/"
		pfxB       = "shard-b/"
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tmpDir := t.TempDir()
	namesA := idxUploadTarShards(t, baseParams, bck, tmpDir, pfxA, numShards, numFiles, fileSize)
	namesB := idxUploadTarShards(t, baseParams, bck, tmpDir, pfxB, numShards, numFiles, fileSize)
	tlog.Logf("Uploaded %d shards under %q and %d shards under %q\n", numShards, pfxA, numShards, pfxB)

	// Index only prefix A — prefix B shards must remain unindexed.
	tlog.Logf("Indexing prefix %q only\n", pfxA)
	idxRunAndWait(t, baseParams, bck, &apc.IndexShardMsg{Prefix: pfxA})

	for _, name := range namesA {
		tassert.Fatalf(t, idxFindFile(bck, name) != "",
			"shard %q (prefix A): expected index, found none", name)
	}
	for _, name := range namesB {
		tassert.Fatalf(t, idxFindFile(bck, name) == "",
			"shard %q (prefix B): unexpected index found before indexing", name)
	}
	tlog.Logf("Prefix filter OK: %d/%d indexed, %d/%d untouched\n", numShards, numShards, numShards, numShards)

	// Index everything (no prefix) — prefix B shards must now be indexed too.
	tlog.Logln("Indexing all shards (no prefix)")
	idxRunAndWait(t, baseParams, bck, &apc.IndexShardMsg{})

	for _, name := range namesA {
		tassert.Fatalf(t, idxFindFile(bck, name) != "",
			"shard %q (prefix A): index missing after full run", name)
	}
	for _, name := range namesB {
		tassert.Fatalf(t, idxFindFile(bck, name) != "",
			"shard %q (prefix B): index missing after full run", name)
	}
	tlog.Logf("Full-bucket index OK: all %d shards indexed\n", numShards*2)
}

func TestIndexShardAbort(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-shard-idx-abort-" + trand.String(6), Provider: apc.AIS}
		numShards  = 20 // enough to keep indexing busy; fresh set uploaded per sub-test
		numFiles   = 20
		fileSize   = 8 * cos.KiB
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tmpDir := t.TempDir()

	for i, tc := range []struct {
		name string
		nw   int
	}{
		{"nw=none", xact.NwpNone},
		{"nw=default", xact.NwpDflt},
		{"nw=2", 2},
		{"nw=8", 8},
		{"nw=32", 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Upload a fresh set of shards per sub-test under a unique prefix.
			// This ensures the xaction always has unindexed objects to process,
			// without relying on a Force flag.
			pfx := fmt.Sprintf("run%d/", i)
			idxUploadTarShards(t, baseParams, bck, tmpDir, pfx, numShards, numFiles, fileSize)
			tlog.Logf("Uploaded %d TAR shards under %q\n", numShards, pfx)

			tlog.Logf("Starting %s (workers=%d) — will abort\n", apc.ActIndexShard, tc.nw)
			xid, err := api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{
				NumWorkers: tc.nw,
				Prefix:     pfx,
			})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, xid != "", "expected non-empty xaction ID")

			// Wait until the xaction is actually running before aborting.
			startArgs := &xact.ArgsMsg{ID: xid, Kind: apc.ActIndexShard, Bck: bck, Timeout: 30 * time.Second}
			_, err = api.WaitForSnaps(baseParams, startArgs, startArgs.Started())
			tassert.CheckFatal(t, err)

			tlog.Logf("Aborting %s xid=%s\n", apc.ActIndexShard, xid)
			abortArgs := &xact.ArgsMsg{ID: xid, Kind: apc.ActIndexShard, Bck: bck, Timeout: 30 * time.Second}
			err = api.AbortXaction(baseParams, abortArgs)
			tassert.CheckFatal(t, err)
			tlog.Logf("  xid=%s abort sent OK\n", xid)

			// Wait for the xaction to reach a terminal state (aborted or completed before abort took effect).
			snaps, err := api.WaitForSnaps(baseParams, abortArgs, abortArgs.Finished())
			tassert.CheckFatal(t, err)
			aborted, _, _ := snaps.AggregateState(xid)
			if aborted {
				tlog.Logf("  xid=%s aborted OK\n", xid)
			} else {
				tlog.Logf("  xid=%s completed before abort took effect\n", xid)
			}
		})
	}
}

// TestIndexShardConflict verifies WhenPrevIsRunning prefix-overlap logic:
//   - same prefix → blocked
//   - non-overlapping prefixes → allowed in parallel
//   - parent prefix running → child blocked (and vice versa)
//
// Timing note: numShards must be large enough that the first xaction is still
// running when the second request arrives. NwpNone spreads work across all
// mountpath joggers in parallel; 2000 shards safely exceeds the ~10ms
// network round-trip for the second request.
func TestIndexShardConflict(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	const (
		expectedErrMsg = "abort it first"
		numShards      = 2000
		numFiles       = 5
		fileSize       = 4 * cos.KiB
	)
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-shard-idx-conflict-" + trand.String(6), Provider: apc.AIS}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	tmpDir := t.TempDir()

	// same prefix: second start must be rejected
	t.Run("same-prefix", func(t *testing.T) {
		pfx := "conflict-same/"
		idxUploadTarShards(t, baseParams, bck, tmpDir, pfx, numShards, numFiles, fileSize)
		tlog.Logf("Uploaded %d shards under %q\n", numShards, pfx)

		xid, err := api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{NumWorkers: xact.NwpNone, Prefix: pfx})
		tassert.CheckFatal(t, err)
		tlog.Logf("Started xid=%s\n", xid)

		_, err = api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{NumWorkers: xact.NwpNone, Prefix: pfx})
		tassert.Fatalf(t, err != nil, "same prefix %q must be blocked while running", pfx)
		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Fatalf(t, ok && strings.Contains(herr.Message, expectedErrMsg),
			"expected %q in error, got: %v", expectedErrMsg, err)
		tlog.Logf("Same-prefix correctly blocked: %v\n", err)

		_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid, Kind: apc.ActIndexShard, Bck: bck, Timeout: 2 * time.Minute})
		tassert.CheckFatal(t, err)
	})

	// non-overlapping prefixes: both must start successfully
	t.Run("non-overlapping-parallel", func(t *testing.T) {
		pfxA, pfxB := "conflict-a/", "conflict-b/"
		idxUploadTarShards(t, baseParams, bck, tmpDir, pfxA, numShards, numFiles, fileSize)
		tlog.Logf("Uploaded %d shards under %q; %q has no shards\n", numShards, pfxA, pfxB)

		xidA, err := api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{NumWorkers: xact.NwpNone, Prefix: pfxA})
		tassert.CheckFatal(t, err)

		xidB, err := api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{NumWorkers: xact.NwpNone, Prefix: pfxB})
		tassert.CheckFatal(t, err) // must NOT be blocked
		tassert.Fatalf(t, xidA != xidB, "parallel xactions must have distinct IDs")
		tlog.Logf("Non-overlapping prefixes running in parallel: xidA=%s xidB=%s\n", xidA, xidB)

		_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xidA, Kind: apc.ActIndexShard, Bck: bck, Timeout: 2 * time.Minute})
		tassert.CheckFatal(t, err)
		_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xidB, Kind: apc.ActIndexShard, Bck: bck, Timeout: 2 * time.Minute})
		tassert.CheckFatal(t, err)
	})

	// running parent prefix blocks a child prefix start
	t.Run("parent-blocks-child", func(t *testing.T) {
		pfxParent, pfxChild := "conflict-par/", "conflict-par/sub/"
		idxUploadTarShards(t, baseParams, bck, tmpDir, pfxParent, numShards, numFiles, fileSize)

		xid, err := api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{NumWorkers: xact.NwpNone, Prefix: pfxParent})
		tassert.CheckFatal(t, err)

		_, err = api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{Prefix: pfxChild})
		tassert.Fatalf(t, err != nil, "child prefix %q must be blocked by running parent %q", pfxChild, pfxParent)
		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Fatalf(t, ok && strings.Contains(herr.Message, expectedErrMsg),
			"expected %q in error, got: %v", expectedErrMsg, err)
		tlog.Logf("Child prefix correctly blocked by running parent: %v\n", err)

		_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid, Kind: apc.ActIndexShard, Bck: bck, Timeout: 2 * time.Minute})
		tassert.CheckFatal(t, err)
	})

	// running child prefix blocks a parent prefix start
	t.Run("child-blocks-parent", func(t *testing.T) {
		pfxChild, pfxParent := "conflict-chi/sub/", "conflict-chi/"
		idxUploadTarShards(t, baseParams, bck, tmpDir, pfxChild, numShards, numFiles, fileSize)

		xid, err := api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{NumWorkers: xact.NwpNone, Prefix: pfxChild})
		tassert.CheckFatal(t, err)

		_, err = api.IndexBucketShards(baseParams, bck, &apc.IndexShardMsg{Prefix: pfxParent})
		tassert.Fatalf(t, err != nil, "parent prefix %q must be blocked by running child %q", pfxParent, pfxChild)
		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Fatalf(t, ok && strings.Contains(herr.Message, expectedErrMsg),
			"expected %q in error, got: %v", expectedErrMsg, err)
		tlog.Logf("Parent prefix correctly blocked by running child: %v\n", err)

		_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid, Kind: apc.ActIndexShard, Bck: bck, Timeout: 2 * time.Minute})
		tassert.CheckFatal(t, err)
	})
}

// TestIndexShardSkipVerify validates the SkipVerify flag semantics:
// SkipVerify=true means "trust HasShardIdx — skip the shard without loading or
// verifying the existing index". This is the fast-rebuild mode: the user asserts
// that all existing indexes are good and just wants the missing ones filled in.
//
// Test sequence (repeated across all worker configs):
//  1. Fresh shards — no existing index → SkipVerify has no effect; all are indexed.
//  2. Re-upload (PUT preserves HasShardIdx; SrcCksum/SrcSize become stale).
//  3. SkipVerify=true → processed=0 (every shard skipped via HasShardIdx flag).
//  4. SkipVerify=false (default) → processed=N (stale detected, all re-indexed).
//     This confirms step 3 did NOT update the indexes.
func TestIndexShardSkipVerify(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-shard-idx-skipverify-" + trand.String(6), Provider: apc.AIS}
		numShards  = 50
		numFiles   = 10
		fileSize   = 4 * cos.KiB
	)
	if testing.Short() {
		numShards = 10
	}

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)
	tmpDir := t.TempDir()

	for _, tc := range []struct {
		name string
		nw   int
	}{
		{"nw=none", xact.NwpNone},
		{"nw=default", xact.NwpDflt},
		{"nw=2", 2},
		{"nw=8", 8},
		{"nw=32", 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pfx := tc.name + "/"
			names := idxUploadTarShards(t, baseParams, bck, tmpDir, pfx, numShards, numFiles, fileSize)
			tlog.Logf("Uploaded %d shards under %q\n", numShards, pfx)

			// Step 1: no existing indexes — SkipVerify is irrelevant; all shards indexed.
			processed := idxRunAndWaitCounts(t, baseParams, bck,
				&apc.IndexShardMsg{NumWorkers: tc.nw, Prefix: pfx, SkipVerify: true})
			tassert.Fatalf(t, processed == int64(numShards),
				"first run: want %d processed, got %d", numShards, processed)
			tlog.Logf("Step 1 OK: all %d shards indexed\n", numShards)

			// Step 2: re-upload shards — PUT preserves HasShardIdx but content changes.
			idxReplaceShards(t, baseParams, bck, tmpDir, names, numFiles, fileSize)

			// Step 3: SkipVerify=true — trust HasShardIdx, skip stale-check entirely.
			processed = idxRunAndWaitCounts(t, baseParams, bck,
				&apc.IndexShardMsg{NumWorkers: tc.nw, Prefix: pfx, SkipVerify: true})
			tassert.Fatalf(t, processed == 0,
				"SkipVerify=true: want 0 processed (all skipped), got %d", processed)
			tlog.Logf("Step 3 OK: SkipVerify=true skipped all %d stale shards\n", numShards)

			// Step 4: SkipVerify=false (default) — detect stale and re-index all.
			// Also proves step 3 left the indexes untouched (still stale).
			processed = idxRunAndWaitCounts(t, baseParams, bck,
				&apc.IndexShardMsg{NumWorkers: tc.nw, Prefix: pfx})
			tassert.Fatalf(t, processed == int64(numShards),
				"SkipVerify=false after stale: want %d re-indexed, got %d", numShards, processed)
			tlog.Logf("Step 4 OK: SkipVerify=false re-indexed all %d stale shards\n", numShards)
		})
	}
}

// TestIndexShardConcurrentRead verifies that shard indexing does not block concurrent
// readers on the TARs: GETs on the same objects must succeed while indexing is running,
// and all indices must be persisted once the xaction completes.
func TestIndexShardConcurrentRead(t *testing.T) {
	var (
		proxyURL     = tools.RandomProxyURL(t)
		baseParams   = tools.BaseAPIParams(proxyURL)
		bck          = cmn.Bck{Name: "test-shard-idx-rdconc-" + trand.String(6), Provider: apc.AIS}
		numShards    = 1000
		numFiles     = 10
		fileSize     = 16 * cos.KiB
		numWorkers   = 8
		getsPerShard = 10
	)
	if testing.Short() {
		numShards = 20
	}

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tmpDir := t.TempDir()
	names := idxUploadTarShards(t, baseParams, bck, tmpDir, "" /*prefix*/, numShards, numFiles, fileSize)
	tlog.Logf("Uploaded %d TAR shards (%d files x %s, formats: GNU/PAX/USTAR)\n",
		numShards, numFiles, cos.ToSizeIEC(int64(fileSize), 0))

	for _, tc := range []struct {
		name string
		nw   int
	}{
		{"nw=none", xact.NwpNone},
		{"nw=default", xact.NwpDflt},
		{"nw=2", 2},
		{"nw=8", 8},
		{"nw=32", 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Pre-populate objCh so workers stay busy through the full xaction duration.
			objCh := make(chan string, numShards*getsPerShard)
			for range getsPerShard {
				for _, name := range names {
					objCh <- name
				}
			}
			close(objCh)

			errCh := make(chan error, numWorkers)
			var wg sync.WaitGroup
			for i := range numWorkers {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for name := range objCh {
						if _, gerr := api.GetObjectWithValidation(baseParams, bck, name, nil); gerr != nil {
							errCh <- fmt.Errorf("worker-%d GET %s: %w", id, name, gerr)
							return
						}
					}
				}(i)
			}

			tlog.Logf("Starting indexing xaction (workers=%d); %d GET workers running concurrently\n",
				tc.nw, numWorkers)
			idxRunAndWait(t, baseParams, bck, &apc.IndexShardMsg{NumWorkers: tc.nw})
			wg.Wait()
			close(errCh)
			for gerr := range errCh {
				tassert.CheckFatal(t, gerr)
			}
			idxValidate(t, bck, names, "" /*nonTarName*/, numFiles)
		})
	}
}

// TestIndexShardStress indexes a large set of shards and validates all indices.
// Scales with -short (reduced count) to fit in the regular test-short/test-long runs.
func TestIndexShardStress(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-shard-idx-stress-" + trand.String(6), Provider: apc.AIS}
		numShards  = 500
		numFiles   = 100
		fileSize   = 8 * cos.KiB
	)
	if testing.Short() {
		numShards = 10
		numFiles = 30
		fileSize = 4 * cos.KiB
	}

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tmpDir := t.TempDir()
	names := idxUploadTarShards(t, baseParams, bck, tmpDir, "" /*prefix*/, numShards, numFiles, fileSize)
	tlog.Logf("Uploaded %d TAR shards (%d files x %s each)\n",
		numShards, numFiles, cos.ToSizeIEC(int64(fileSize), 0))

	for _, tc := range []struct {
		name string
		nw   int
	}{
		{"nw=none", xact.NwpNone},
		{"nw=default", xact.NwpDflt},
		{"nw=2", 2},
		{"nw=8", 8},
		{"nw=32", 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Each sub-test has fresh data: the first gets the initial upload; subsequent
			// ones get data left re-uploaded at the end of the previous sub-test.
			idxRunAndWait(t, baseParams, bck, &apc.IndexShardMsg{NumWorkers: tc.nw})
			tlog.Logf("Indexed %d shards; validating...\n", numShards)
			idxValidate(t, bck, names, "" /*nonTarName*/, numFiles)

			// Re-upload shards — PUT preserves HasShardIdx but updates content,
			// so the next indexing run detects staleness via SrcCksum/SrcSize and re-indexes.
			tlog.Logln("Re-uploading shards (preserves HasShardIdx; re-index detects stale SrcCksum/SrcSize)")
			idxReplaceShards(t, baseParams, bck, tmpDir, names, numFiles, fileSize)
			idxRunAndWait(t, baseParams, bck, &apc.IndexShardMsg{NumWorkers: tc.nw})
			tlog.Logf("Re-indexed %d shards; validating...\n", numShards)
			idxValidate(t, bck, names, "" /*nonTarName*/, numFiles)
		})
	}
}

// TestIndexShardPartialReupload verifies staleness detection when only a subset of shards
// is re-uploaded between indexing passes.
//
// Key invariant: PUT preserves HasShardIdx in xattr (the flag is loaded with the
// existing LOM and written back). Staleness is detected via the SrcCksum/SrcSize
// embedded in the index — not by HasShardIdx being cleared.
//
// Expected Stats.Objs progression (locally processed = indexed + reindexed; skipped excluded):
//   - first run:  N   — all shards fresh, all indexed
//   - second run: N/2 — re-uploaded half detected as stale and reindexed; untouched half skipped
func TestIndexShardPartialReupload(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-shard-idx-partial-" + trand.String(6), Provider: apc.AIS}
		numShards  = 200
		numFiles   = 10
		fileSize   = 4 * cos.KiB
	)
	if testing.Short() {
		numShards = 10
	}
	halved := numShards / 2

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tmpDir := t.TempDir()

	for _, tc := range []struct {
		name string
		nw   int
	}{
		{"nw=none", xact.NwpNone},
		{"nw=default", xact.NwpDflt},
		{"nw=2", 2},
		{"nw=8", 8},
		{"nw=32", 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Fresh names per variant — ensures HasShardIdx=false at the start of each first run.
			pfx := tc.name + "/"
			names := idxUploadTarShards(t, baseParams, bck, tmpDir, pfx, numShards, numFiles, fileSize)

			// First run: all shards are fresh (never indexed) → all processed, none skipped.
			processed := idxRunAndWaitCounts(t, baseParams, bck, &apc.IndexShardMsg{NumWorkers: tc.nw, Prefix: pfx})
			tlog.Logf("First run:  processed=%d\n", processed)
			tassert.Fatalf(t, processed == int64(numShards), "first run: want processed=%d, got %d", numShards, processed)

			// Re-upload half the shards. PUT preserves HasShardIdx in xattr, so re-uploaded
			// shards retain HasShardIdx=true but their SrcCksum/SrcSize become stale.
			idxReplaceShards(t, baseParams, bck, tmpDir, names[:halved], numFiles, fileSize)

			// Second run: stale half is re-indexed (processed); fresh half is skipped (not counted).
			processed = idxRunAndWaitCounts(t, baseParams, bck, &apc.IndexShardMsg{NumWorkers: tc.nw, Prefix: pfx})
			tlog.Logf("Second run: processed=%d\n", processed)
			tassert.Fatalf(t, processed == int64(halved), "second run: want processed=%d, got %d", halved, processed)

			idxValidate(t, bck, names, "" /*nonTarName*/, numFiles)
		})
	}
}

//
// helpers
//

// idxFindFile returns the on-disk FQN of the shard index for objName in bck,
// delegating to findObjOnDisk on the ais://.sys-shardidx system bucket.
// Requires initMountpaths to have been called in the test.
func idxFindFile(bck cmn.Bck, objName string) string {
	sysBck := cmn.Bck(*meta.SysBckShardIdx())
	idxObjName := string(bck.MakeUname(objName + core.IdxSuffix))
	m := &ioContext{}
	return m.findObjOnDisk(sysBck, idxObjName)
}

// idxValidate checks that every name in tarNames has a valid index with numFiles entries,
// and that nonTarName (if non-empty) has no index.
func idxValidate(t *testing.T, bck cmn.Bck, tarNames []string, nonTarName string, numFiles int) {
	t.Helper()
	for _, name := range tarNames {
		idxPath := idxFindFile(bck, name)
		tassert.Fatalf(t, idxPath != "", "TAR object %q: index file not found on any mountpath", name)

		data, err := os.ReadFile(idxPath)
		tassert.CheckFatal(t, err)

		idx := &archive.ShardIndex{}
		tassert.CheckFatal(t, idx.Unpack(data))
		tassert.Fatalf(t, len(idx.Entries) == numFiles,
			"TAR object %q: expected %d index entries, got %d", name, numFiles, len(idx.Entries))
	}
	tlog.Logf("Validated %d shard indices (%d entries each)\n", len(tarNames), numFiles)
	if nonTarName != "" {
		tassert.Fatalf(t, idxFindFile(bck, nonTarName) == "",
			"non-TAR object %q: unexpected index file found", nonTarName)
	}
}

// idxRunAndWait starts an ActIndexShard xaction with the given msg and waits for completion.
func idxRunAndWait(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, msg *apc.IndexShardMsg) {
	t.Helper()
	xid, err := api.IndexBucketShards(baseParams, bck, msg)
	tassert.CheckFatal(t, err)
	_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{
		ID:      xid,
		Kind:    apc.ActIndexShard,
		Bck:     bck,
		Timeout: 2 * time.Minute,
	})
	tassert.CheckFatal(t, err)
}

// idxRunAndWaitCounts starts an ActIndexShard xaction, waits for completion, and returns
// the cluster-wide aggregate of snap.Stats.Objs — the count of shards that were actually
// indexed or re-indexed (skipped shards do not contribute to Stats.Objs).
func idxRunAndWaitCounts(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, msg *apc.IndexShardMsg) (processed int64) {
	t.Helper()
	xid, err := api.IndexBucketShards(baseParams, bck, msg)
	tassert.CheckFatal(t, err)
	args := &xact.ArgsMsg{ID: xid, Kind: apc.ActIndexShard, Bck: bck, Timeout: 2 * time.Minute}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)
	snaps, err := api.QueryXactionSnaps(baseParams, args)
	tassert.CheckFatal(t, err)
	for _, tsnaps := range snaps {
		for _, snap := range tsnaps {
			if snap.ID == xid {
				processed += snap.Stats.Objs
			}
		}
	}
	return
}

var idxFormats = []tar.Format{tar.FormatGNU, tar.FormatPAX, tar.FormatUSTAR}

// idxUploadTarShards uploads numShards TAR archives (each with numFiles random files of fileSize bytes)
// to bck, using prefix for object names. Rotates through GNU/PAX/USTAR formats. Returns object names.
func idxUploadTarShards(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, tmpDir, prefix string, numShards, numFiles, fileSize int) []string {
	t.Helper()
	names := make([]string, numShards)
	for i := range numShards {
		objName := prefix + trand.String(8) + archive.ExtTar
		names[i] = objName
		idxPutOneTar(t, baseParams, bck, tmpDir, objName, numFiles, fileSize, idxFormats[i%len(idxFormats)])
	}
	return names
}

// idxReplaceShards re-uploads new TAR content under each name in names, rotating formats.
func idxReplaceShards(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, tmpDir string, names []string, numFiles, fileSize int) {
	t.Helper()
	for i, name := range names {
		idxPutOneTar(t, baseParams, bck, tmpDir, name, numFiles, fileSize, idxFormats[i%len(idxFormats)])
	}
}

// idxPutOneTar creates a random TAR archive (in the given format) and PUTs it to bck.
// Uses xxhash so that GetObjectWithValidation can verify object integrity.
func idxPutOneTar(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, tmpDir, objName string, numFiles, fileSize int, format tar.Format) {
	t.Helper()
	localPath := filepath.Join(tmpDir, trand.String(8)+archive.ExtTar)
	err := tarch.CreateArchRandomFiles(localPath, format, archive.ExtTar, numFiles, fileSize,
		nil /*recExts*/, nil /*randNames*/, false /*dup*/, true /*randDir*/, false /*exactSize*/)
	tassert.CheckFatal(t, err)

	fi, err := os.Stat(localPath)
	tassert.CheckFatal(t, err)

	r, err := readers.New(&readers.Arg{
		Type:      readers.File,
		Path:      localPath,
		Size:      readers.ExistingFileSize,
		CksumType: cos.ChecksumOneXxh,
	})
	tassert.CheckFatal(t, err)

	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     r,
		Size:       uint64(fi.Size()),
	})
	r.Close()
	tassert.CheckFatal(t, err)
}

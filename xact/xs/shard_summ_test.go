// Package xs_test tests xaction implementations without a running cluster.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

func newShardSummBucket(t *testing.T) *meta.Bck {
	t.Helper()
	xreg.Init()
	xs.Tinit(nil)
	fs.NewTestMFS(mock.NewIOS())

	tmpDir := t.TempDir()
	mpath := filepath.Join(tmpDir, "mpath")
	tassert.CheckFatal(t, cos.CreateDir(mpath))
	_, err := fs.AddTestMpath(mpath, "daeID")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { fs.Remove(mpath) })

	bck := meta.NewBck("shard-summ-"+trand.String(6), apc.AIS, cmn.NsGlobal, &cmn.Bprops{
		Cksum: cmn.CksumConf{Type: cos.ChecksumCesXxh},
		BID:   0x51,
	})
	sysBck := meta.NewBck(cmn.SysShardIdx, apc.AIS, cmn.NsGlobal, &cmn.Bprops{
		Cksum: cmn.CksumConf{Type: cos.ChecksumCesXxh},
		BID:   0x52,
	})
	mock.NewTarget(mock.NewBaseBownerMock(sysBck, bck))
	for _, mi := range fs.GetAvail() {
		tassert.CheckFatal(t, mi.CreateMissingBckDirs(sysBck.Bucket()))
		tassert.CheckFatal(t, mi.CreateMissingBckDirs(bck.Bucket()))
	}
	return bck
}

func saveObject(t *testing.T, bck *meta.Bck, objName string, size int64) *core.LOM {
	t.Helper()
	lom := &core.LOM{ObjName: objName}
	tassert.CheckFatal(t, lom.InitBck(bck))
	fh, err := cos.CreateFile(lom.FQN)
	tassert.CheckFatal(t, err)
	_, err = fh.Write(make([]byte, size))
	tassert.CheckFatal(t, err)
	cos.Close(fh)
	lom.SetSize(size)
	lom.SetCksum(cos.NoneCksum)
	lom.SetAtimeUnix(time.Now().UnixNano())
	tassert.CheckFatal(t, lom.Persist())
	return lom
}

func saveIndexedShard(t *testing.T, bck *meta.Bck, objName string, size int64, archivedObjs int) *core.LOM {
	t.Helper()
	lom := saveObject(t, bck, objName, size)
	idx := &archive.ShardIndex{
		Entries:  make(map[string]archive.ShardIndexEntry, archivedObjs),
		SrcCksum: cos.NoneCksum,
		SrcSize:  size,
	}
	for i := range archivedObjs {
		idx.Entries[fmt.Sprintf("obj-%03d", i)] = archive.ShardIndexEntry{Offset: int64(i) * archive.TarBlockSize, Size: 1}
	}
	tassert.CheckFatal(t, core.SaveShardIndex(lom, idx))
	return lom
}

func shardIdxFQN(t *testing.T, bck *meta.Bck, objName string) string {
	t.Helper()
	idxLom := &core.LOM{ObjName: bck.SysObjName(objName + core.IdxSuffix)}
	tassert.CheckFatal(t, idxLom.InitBck(meta.SysBckShardIdx()))
	return idxLom.FQN
}

func startShardSummary(t *testing.T, bck *meta.Bck, msg *apc.ShardSummMsg) *xs.XactShardSumm {
	t.Helper()
	if msg.UUID == "" {
		msg.UUID = cos.GenUUID()
	}
	rns := xreg.RenewBckShardSumm(bck, msg)
	tassert.CheckFatal(t, rns.Err)
	xsumm, ok := rns.Entry.Get().(*xs.XactShardSumm)
	if !ok {
		t.Fatalf("expected *xs.XactShardSumm, got %T", rns.Entry.Get())
	}
	return xsumm
}

func waitShardSummary(t *testing.T, xsumm *xs.XactShardSumm) *apc.ShardSummResult {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !xsumm.IsDone(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not finish", xsumm.Name())
		}
	}
	res, err := xsumm.Result()
	tassert.CheckFatal(t, err)
	return res
}

func wantTar(want *apc.ShardSummResult, size int64) {
	want.TarObjs++
	want.TarSize += uint64(size)
}

func putTar(t *testing.T, bck *meta.Bck, name string, size int64, want *apc.ShardSummResult) {
	saveObject(t, bck, name, size)
	wantTar(want, size)
}

func putShard(t *testing.T, bck *meta.Bck, name string, size int64, archivedObjs int, want *apc.ShardSummResult) {
	saveIndexedShard(t, bck, name, size, archivedObjs)
	wantTar(want, size)
	want.Shards++
	want.ShardSize += uint64(size)
	want.ArchivedObjs += uint64(archivedObjs)
}

func checkSummary(t *testing.T, got *apc.ShardSummResult, want apc.ShardSummResult) {
	if *got != want {
		t.Fatalf("unexpected summary: got %+v, want %+v", got, want)
	}
}

func TestShardSummaryCountsTarsAndShards(t *testing.T) {
	const (
		indexedCnt   = 12
		unindexedCnt = 8
	)

	bck := newShardSummBucket(t)
	var want apc.ShardSummResult
	for i := range indexedCnt {
		putShard(t, bck, fmt.Sprintf("keep/indexed-%02d.tar", i), int64(100+i), i+1, &want)
	}
	for i := range unindexedCnt {
		putTar(t, bck, fmt.Sprintf("keep/unindexed-%02d.tar", i), int64(50+i), &want)
	}
	saveObject(t, bck, "keep/not-tar.txt", 1000)
	saveIndexedShard(t, bck, "skip/indexed.tar", 300, 7)

	got := waitShardSummary(t, startShardSummary(t, bck, &apc.ShardSummMsg{Prefix: "keep/"}))
	checkSummary(t, got, want)
}

func TestShardSummaryCountsBadIndexesAsTarsOnly(t *testing.T) {
	const (
		indexedCnt     = 10
		unindexedCnt   = 7
		staleSize      = 201
		missingIdxSize = 300
		corruptIdxSize = 400
	)

	bck := newShardSummBucket(t)
	var want apc.ShardSummResult
	for i := range indexedCnt {
		putShard(t, bck, fmt.Sprintf("fresh-%02d.tar", i), int64(100+i), i+1, &want)
	}
	for i := range unindexedCnt {
		putTar(t, bck, fmt.Sprintf("unindexed-%02d.tar", i), int64(300+i), &want)
	}

	stale := saveIndexedShard(t, bck, "stale.tar", staleSize-1, 5)
	stale.SetSize(staleSize)
	stale.SetAtimeUnix(time.Now().UnixNano())
	tassert.CheckFatal(t, stale.Persist())
	wantTar(&want, staleSize)
	want.StaleIndexes++

	missing := saveIndexedShard(t, bck, "missing-index.tar", missingIdxSize, 7)
	tassert.CheckFatal(t, os.Remove(shardIdxFQN(t, bck, missing.ObjName)))
	wantTar(&want, missingIdxSize)

	corrupt := saveIndexedShard(t, bck, "corrupt-index.tar", corruptIdxSize, 9)
	tassert.CheckFatal(t, os.WriteFile(shardIdxFQN(t, bck, corrupt.ObjName), []byte("not a shard index payload"), 0o644))
	wantTar(&want, corruptIdxSize)
	want.InvalidIndexes++

	got := waitShardSummary(t, startShardSummary(t, bck, &apc.ShardSummMsg{}))
	checkSummary(t, got, want)
}

func TestShardSummaryResultPolling(t *testing.T) {
	const (
		indexedCnt   = 24
		unindexedCnt = 8
	)

	bck := newShardSummBucket(t)
	var want apc.ShardSummResult
	for i := range indexedCnt {
		putShard(t, bck, fmt.Sprintf("indexed-%02d.tar", i), int64(10+i), i%5+1, &want)
	}
	for i := range unindexedCnt {
		putTar(t, bck, fmt.Sprintf("unindexed-%02d.tar", i), int64(100+i), &want)
	}

	xsumm := startShardSummary(t, bck, &apc.ShardSummMsg{})
	prev := apc.ShardSummResult{}
	for {
		got, err := xsumm.Result()
		tassert.CheckFatal(t, err)
		if got.TarObjs < prev.TarObjs || got.TarSize < prev.TarSize || got.Shards < prev.Shards ||
			got.ShardSize < prev.ShardSize || got.ArchivedObjs < prev.ArchivedObjs ||
			got.StaleIndexes < prev.StaleIndexes || got.InvalidIndexes < prev.InvalidIndexes {
			t.Fatalf("summary regressed from %+v to %+v", prev, got)
		}
		prev = *got
		if xsumm.IsDone() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	checkSummary(t, waitShardSummary(t, xsumm), want)
}

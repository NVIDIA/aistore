// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestNBICachedChunkRewind(t *testing.T) {
	fs.NewTestMFS(mock.NewIOS())
	mpath := filepath.Join(t.TempDir(), "mpath")
	tassert.CheckFatal(t, cos.CreateDir(mpath))
	mi, err := fs.AddTestMpath(mpath, "daeID")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { fs.Remove(mpath) })
	bck := meta.NewBck("nbi-cache", apc.AIS, cmn.NsGlobal, &cmn.Bprops{})
	mock.NewTarget(mock.NewBaseBownerMock(bck))
	tassert.CheckFatal(t, mi.CreateMissingBckDirs(bck.Bucket()))
	lom := &core.LOM{ObjName: "inventory"}
	tassert.CheckFatal(t, lom.InitBck(bck))
	ufest, err := core.NewUfest("", lom, false /*must-exist*/)
	tassert.CheckFatal(t, err)
	w := &XactNBI{lom: lom, ufest: ufest, buf: make([]byte, cmn.MsgpLsoBufSize), cksum: cos.NewCksumHash(cos.ChecksumCRC32C)}
	chunks := []cmn.LsoEntries{
		{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}, {Name: "e"}, {Name: "f"}},
		{{Name: "g"}, {Name: "h"}, {Name: "i"}},
		{{Name: "j"}, {Name: "k"}, {Name: "l"}, {Name: "m"}, {Name: "n"}, {Name: "o"}},
	}
	for i, entries := range chunks {
		tassert.CheckFatal(t, w.writeChunk(i+1, entries))
	}
	nbi := &nbiCtx{bck: bck, ufest: ufest, buf: make([]byte, cmn.MsgpLsoBufSize), cksum: cos.NewCksumHash(cos.ChecksumCRC32C), chunkNum: 1, nat: 1}
	checkNames := func(entries, expected cmn.LsoEntries) {
		t.Helper()
		tassert.Fatalf(t, len(entries) == len(expected), "expected %d entries, got %d", len(expected), len(entries))
		for i, e := range entries {
			tassert.Fatalf(t, e.Name == expected[i].Name, "[%d]: expected %q, got %q", i, expected[i].Name, e.Name)
		}
	}

	// A shorter next chunk must not overwrite objects referenced by the cache.
	tassert.CheckFatal(t, nbi.readChunk())
	nbi.cacheIt()
	nbi.chunkNum = 2
	tassert.CheckFatal(t, nbi.readChunk())
	checkNames(nbi.cache.entries, chunks[0])

	// Restore the cached chunk, then traverse it and both subsequent chunks.
	tassert.CheckFatal(t, nbi.rewind("b"))
	checkNames(nbi.entries, chunks[0])
	msg := &apc.LsoMsg{ContinuationToken: "b", PageSize: 1}
	lst := &cmn.LsoRes{}
	tassert.CheckFatal(t, nbi.nextPage(msg, lst))
	expected := append(cmn.LsoEntries(nil), chunks[0][2:]...)
	expected = append(expected, chunks[1]...)
	expected = append(expected, chunks[2][:3]...)
	checkNames(lst.Entries, expected)
	tassert.Fatalf(t, lst.ContinuationToken == "l", "expected token l, got %q", lst.ContinuationToken)
	checkNames(nbi.cache.entries, chunks[1])

	// A proxy-selected marker can rewind into the shorter cached chunk again.
	msg.ContinuationToken = "h"
	tassert.CheckFatal(t, nbi.nextPage(msg, lst))
	expected = append(cmn.LsoEntries(nil), chunks[1][2:]...)
	expected = append(expected, chunks[2]...)
	checkNames(lst.Entries, expected)
	tassert.Fatalf(t, lst.ContinuationToken == "", "expected exhausted inventory, got token %q", lst.ContinuationToken)
}

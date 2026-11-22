// Package xs is a collection of eXtended actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

//
// archive ---------------------------------------------------------------
//

type archiveFinalizeFailTarget struct {
	core.Target
	size int64
}

func (tt *archiveFinalizeFailTarget) FinalizeObj(lom *core.LOM, _ string, _ core.Xact, _ cmn.OWT, _ bool) (int, error) {
	tt.size = lom.Lsize()
	return http.StatusInternalServerError, syscall.EIO
}

func TestArchiveAppendFinalizeFailure(t *testing.T) {
	fs.NewTestMFS(mock.NewIOS())
	mpath := t.TempDir()
	mi, err := fs.AddTestMpath(mpath, "daeID")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { fs.Remove(mpath) })
	bck := meta.NewBck("archive-rollback", apc.AIS, cmn.NsGlobal, &cmn.Bprops{})
	tt := &archiveFinalizeFailTarget{Target: &mock.TargetMock{BO: mock.NewBaseBownerMock(bck)}}
	prev := core.T
	core.Tinit(tt, nil, false /*run HK*/)
	t.Cleanup(func() { core.T = prev })
	tassert.CheckFatal(t, mi.CreateMissingBckDirs(bck.Bucket()))

	lom := core.AllocLOM("shard.tar")
	tassert.CheckFatal(t, lom.InitBck(bck))
	mainFQN := lom.FQN
	var orig bytes.Buffer
	tw := tar.NewWriter(&orig)
	data := []byte("original entry")
	tassert.CheckFatal(t, tw.WriteHeader(&tar.Header{Name: "original", Mode: 0o644, Size: int64(len(data))}))
	_, err = tw.Write(data)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, tw.Close())
	tassert.CheckFatal(t, os.WriteFile(mainFQN, orig.Bytes(), cos.PermRWR))
	lom.SetSize(int64(orig.Len()))
	lom.SetAtimeUnix(time.Now().UnixNano())
	tassert.CheckFatal(t, lom.Persist())

	r := &XactArch{}
	r.Base.InitBase(t.Context(), cos.GenUUID(), apc.ActArchive, bck)
	wi := &archwi{r: r, archlom: lom, fqn: lom.GenFQN(fs.WorkCT, fs.WorkfileCreateArch)}
	format, err := wi.openTarForAppend()
	tassert.CheckFatal(t, err)
	wi.writer = archive.NewWriter(archive.ExtTar, wi.wfh, nil, &archive.Opts{TarFormat: format})
	appended := bytes.Repeat([]byte{1}, 4*cos.KiB)
	tassert.CheckFatal(t, wi.writer.Write("appended", &cmn.ObjAttrs{Size: int64(len(appended))}, bytes.NewReader(appended)))
	wi.cnt.Inc()

	ecode, err := r._fini(wi)
	tassert.Fatalf(t, ecode == http.StatusInternalServerError && err == syscall.EIO, "expected failed finalization, got %d, %v", ecode, err)
	tassert.Fatalf(t, tt.size > int64(orig.Len()), "expected an enlarged archive at finalization, got %d", tt.size)
	got, err := os.ReadFile(mainFQN)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bytes.Equal(got, orig.Bytes()), "expected original TAR (%d bytes), got %d bytes", orig.Len(), len(got))
}

//
// blob-download ----------------------------------------------------------------------
//

func TestBlobDlChunkReadTimeout(t *testing.T) {
	tests := []struct {
		name                   string
		requested, limit, want time.Duration
	}{
		{"default", 0, 5 * time.Minute, 5 * time.Minute},
		{"explicit", 30 * time.Second, 5 * time.Minute, 30 * time.Second},
		{"explicit-capped", 10 * time.Minute, 5 * time.Minute, 5 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := &cmn.Config{}
			config.Timeout.SendFile = cos.Duration(tc.limit)
			r := XactBlobDl{
				args:   &core.BlobParams{Msg: &apc.BlobMsg{ChunkReadTimeout: cos.Duration(tc.requested)}},
				config: config,
			}
			r.setChunkReadTimeout()
			tassert.Fatalf(t, r.timeout == tc.want, "expected %v, got %v", tc.want, r.timeout)
		})
	}
}

// NOTE: unit tests do not LoadConfig, so cmn.Rom.testingEnv is left false - which is
// what keeps the `load.High && streaming` rejection reachable here (and only here).
func TestBlobDlMemErr(t *testing.T) {
	tassert.Fatalf(t, !cmn.Rom.TestingEnv(), "expecting unset cmn.Rom: this test covers the non-testing-env branch")

	const (
		sglCost = 4 * cos.MiB
		bufCost = 32 * cos.KiB
	)
	tests := []struct {
		name      string
		memLoad   load.Load
		streaming bool
		wantErr   bool
	}{
		{"critical-streaming", load.Critical, true, true},
		{"critical-background", load.Critical, false, true},
		{"high-streaming", load.High, true, true},
		{"high-background", load.High, false, false},
		{"moderate-streaming", load.Moderate, true, false},
		{"low-streaming", load.Low, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := XactBlobDl{cname: "blob-dl[" + tc.name + "]"}
			err := r.memErr(tc.memLoad, tc.streaming, sglCost, bufCost)
			tassert.Fatalf(t, (err != nil) == tc.wantErr, "expected err=%t, got %v", tc.wantErr, err)
			if err != nil {
				tassert.Fatalf(t, IsErrBlobDlAdmission(err), "expected admission error, got %v", err)
			}
		})
	}
}

//
// NBI ----------------------------------------------------------------------
//

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
	newCtx := func() *nbiCtx {
		t.Helper()
		nbi := &nbiCtx{bck: bck, ufest: ufest, buf: make([]byte, cmn.MsgpLsoBufSize), cksum: cos.NewCksumHash(cos.ChecksumCRC32C), nat: 1}
		tassert.CheckFatal(t, nbi.readChunk(1, ""))
		return nbi
	}
	checkNames := func(entries, expected cmn.LsoEntries) {
		t.Helper()
		tassert.Fatalf(t, len(entries) == len(expected), "expected %d entries, got %d", len(expected), len(entries))
		for i, e := range entries {
			tassert.Fatalf(t, e.Name == expected[i].Name, "[%d]: expected %q, got %q", i, expected[i].Name, e.Name)
		}
	}

	// short chunk must not overwrite objects referenced by the cache
	nbi := newCtx()
	nbi.cacheIt()
	tassert.CheckFatal(t, nbi.readChunk(2, ""))
	checkNames(nbi.cache.entries, chunks[0])

	// restore the cached chunk, then traverse it and subsequent chunks
	tassert.CheckFatal(t, nbi.seekAfter("b"))
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

	// proxy-selected marker can rewind into the shorter cached chunk again
	msg.ContinuationToken = "h"
	tassert.CheckFatal(t, nbi.nextPage(msg, lst))
	expected = append(cmn.LsoEntries(nil), chunks[1][2:]...)
	expected = append(expected, chunks[2]...)
	checkNames(lst.Entries, expected)
	tassert.Fatalf(t, lst.ContinuationToken == "", "expected exhausted inventory, got token %q", lst.ContinuationToken)

	// marker equal to an earlier page token must not match a stale iterator position
	msg.ContinuationToken = "l"
	tassert.CheckFatal(t, nbi.nextPage(msg, lst))
	checkNames(lst.Entries, chunks[2][3:])

	// rewind after a header skip past the end must not use stale entries
	nbi = newCtx()
	tassert.CheckFatal(t, nbi.nextPage(&apc.LsoMsg{StartAfter: "o"}, lst))
	checkNames(lst.Entries, nil)
	tassert.CheckFatal(t, nbi.nextPage(&apc.LsoMsg{ContinuationToken: "h"}, lst))
	expected = append(cmn.LsoEntries(nil), chunks[1][2:]...)
	expected = append(expected, chunks[2]...)
	checkNames(lst.Entries, expected)

	// retry the same seek after: a) open error and b) partial decode
	nbi = newCtx()
	lastChunk, err := ufest.GetChunk(3)
	tassert.CheckFatal(t, err)
	path := lastChunk.Path()
	data, err := os.ReadFile(path)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, os.Remove(path))

	msg.ContinuationToken = "l"
	err = nbi.nextPage(msg, lst)
	tassert.Fatalf(t, cos.IsNotExist(err), "expected missing chunk, got %v", err)
	tassert.Fatalf(t, nbi.chunkNum == 2 && nbi.hdr.last == "i" && len(nbi.entries) == 0, "expected only chunk 2 header")

	tassert.CheckFatal(t, os.WriteFile(path, data[:len(data)-1], cos.PermRWR))
	err = nbi.nextPage(msg, lst)
	tassert.Fatalf(t, err != nil, "expected truncated chunk payload")
	tassert.Fatalf(t, nbi.chunkNum == 3 && nbi.hdr.last == "o" && len(nbi.entries) == 0, "expected only chunk 3 header")

	tassert.CheckFatal(t, os.WriteFile(path, data, cos.PermRWR))
	tassert.CheckFatal(t, nbi.nextPage(msg, lst))
	checkNames(lst.Entries, chunks[2][3:])

	// final page exactly filled: names after "e" == minPageSize (effective page size for PageSize=1)
	remaining := len(chunks[0][5:]) + len(chunks[1]) + len(chunks[2])
	tassert.Fatalf(t, remaining == minPageSize, "test setup: %d names remain vs page size %d", remaining, minPageSize)
	msg = &apc.LsoMsg{ContinuationToken: "e", PageSize: 1}
	tassert.CheckFatal(t, nbi.nextPage(msg, lst))
	tassert.Fatalf(t, len(lst.Entries) == minPageSize && lst.ContinuationToken == "", "expected full final page, got %d entries, token %q",
		len(lst.Entries), lst.ContinuationToken)

	// rewind past an unreadable payload using its intact header
	mid, err := ufest.GetChunk(2)
	tassert.CheckFatal(t, err)
	midData, err := os.ReadFile(mid.Path())
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, os.Truncate(mid.Path(), int64(len(midData)-1)))
	nbi = newCtx()
	tassert.CheckFatal(t, nbi.readChunk(3, ""))
	first := nbi.entries[0]
	for _, token := range []string{"i-", "i"} {
		tassert.CheckFatal(t, nbi.seekAfter(token))
		tassert.Fatalf(t, nbi.chunkNum == 3 && nbi.nidx == 0 && nbi.entries[0] == first,
			"token %q: expected to retain the loaded chunk", token)
	}
	tassert.CheckFatal(t, nbi.seekAfter("0"))
	checkNames(nbi.entries[nbi.nidx:], chunks[0])

	// the gap after chunk 1 selects chunk 2 and must read its payload
	tassert.CheckFatal(t, nbi.readChunk(3, ""))
	err = nbi.seekAfter("f-")
	tassert.Fatalf(t, err != nil, "expected truncated chunk payload")
	tassert.CheckFatal(t, os.WriteFile(mid.Path(), midData, cos.PermRWR))
	tassert.CheckFatal(t, nbi.seekAfter("f-"))
	checkNames(nbi.entries[nbi.nidx:], chunks[1])
}

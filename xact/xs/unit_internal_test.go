// Package xs is a collection of eXtended actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
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

// TCB peer completion: push (T2T 'done') and pull (status reply)

func TestTCBPeerDone(t *testing.T) {
	const a, b = "peerA", "peerB"
	prev := core.T
	mock.NewTarget(mock.NewBaseBownerMock())
	t.Cleanup(func() { core.T = prev })

	r := &XactTCB{dm: &bundle.DM{}}
	r.Base.InitBase(context.Background(), cos.GenUUID(), apc.ActCopyBck, nil)
	s := &r.sntl
	smap := &meta.Smap{Tmap: meta.NodeMap{a: {DaeID: a}, b: {DaeID: b}}}
	s.init(r, &r.Base, r.dm, cmn.GCO.Get(), smap, 3)
	s.initLast(1)

	reply := func(xctn core.Xact) (core.RunState, xact.StatusFlags) {
		rs, flags, _, err := xact.ParseStatusReply(xact.StatusReply(xctn))
		tassert.CheckFatal(t, err)
		return rs, flags
	}
	done := func(sid string) error { return r.RecvCtrl(xact.NoneWID, sid, xact.OpcodeDone, nil) }

	// reject: unknown or self sid, wrong opcode, wid, or body
	tassert.Fatalf(t, done("unknown") != nil, "expected error (unknown sid)")
	tassert.Fatalf(t, done(core.T.SID()) != nil, "expected error (self)")
	tassert.Fatalf(t, r.RecvCtrl(xact.NoneWID, a, xact.OpcodeStatus, nil) != nil, "expected error (opcode)")
	tassert.Fatalf(t, r.RecvCtrl("wid", a, xact.OpcodeDone, nil) != nil, "expected error (wid)")
	tassert.Fatalf(t, r.RecvCtrl(xact.NoneWID, a, xact.OpcodeDone, []byte("x")) != nil, "expected error (body)")
	tassert.Fatalf(t, len(s.pend.last) == 2, "rejected 'done' changed pending: %v", s.pend.last)

	// duplicate push, followed by a 'running' reply that was in flight: 'a' must stay done
	tassert.CheckFatal(t, done(a))
	tassert.CheckFatal(t, done(a))
	running := &XactTCB{}
	running.Base.InitBase(context.Background(), cos.GenUUID(), apc.ActCopyBck, nil)
	rs, flags := reply(running)
	tassert.Fatalf(t, rs == core.RunStateRunning && flags == 0, "expected running, got %s, %d", rs, flags)
	s.onStatus(a, rs, flags, "")
	s.pending()
	tassert.Fatalf(t, len(s.pend.p) == 1 && s.pend.p[0] == b, "pending: %v", s.pend.p)

	// recover b's missed push while b is still quiescing (done sending)
	quiescing := &XactTCB{}
	quiescing.Base.InitBase(context.Background(), cos.GenUUID(), apc.ActCopyBck, nil)
	quiescing.sntl.txDone.Store(true)
	rs, flags = reply(quiescing)
	tassert.Fatalf(t, rs == core.RunStateRunning && flags.IsSet(xact.StatusDoneSending),
		"expected running and done sending, got %s, %d", rs, flags)
	s.onStatus(b, rs, flags, "")
	tassert.Fatalf(t, s.qcb(0) == core.QuiDone, "still waiting for done")

	s.cleanup()
	tassert.CheckFatal(t, done(b)) // late push after cleanup
}

func TestTCBPeerAbort(t *testing.T) {
	const (
		peer  = "peerA"
		cause = "archive read failed: disk I/O error"
	)
	prev := core.T
	mock.NewTarget(mock.NewBaseBownerMock())
	t.Cleanup(func() { core.T = prev })

	for _, doneSending := range []bool{false, true} {
		t.Run(cos.Ternary(doneSending, "after-done", "before-done"), func(t *testing.T) {
			source := &XactTCB{}
			source.Base.InitBase(context.Background(), cos.GenUUID(), apc.ActCopyBck, nil)
			source.sntl.txDone.Store(doneSending)
			source.Abort(errors.New(cause))

			rs, flags, got, err := xact.ParseStatusReply(xact.StatusReply(source))
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, rs == core.RunStateAborted && flags == 0 && got == cause,
				"expected aborted with original cause, got %s, %d, %q", rs, flags, got)

			r := &XactTCB{dm: &bundle.DM{}}
			r.Base.InitBase(context.Background(), cos.GenUUID(), apc.ActCopyBck, nil)
			s := &r.sntl
			smap := &meta.Smap{Tmap: meta.NodeMap{peer: {DaeID: peer}}}
			s.init(r, &r.Base, r.dm, cmn.GCO.Get(), smap, 2)
			s.onStatus(peer, rs, flags, got)
			err = r.AbortErr()
			tassert.Fatalf(t, err != nil && err.Error() == cause, "lost original cause: %v", err)
			tassert.Fatalf(t, xact.IsErrRecvAbortXact(err) && r.SkipAbortBcast(err), "received abort would be re-broadcast")
			tassert.Fatalf(t, len(s.pend.last) == 1, "aborted peer was marked done")
		})
	}
}

// accept peer completion after the demand job starts stopping
func TestTCOPeerControl(t *testing.T) {
	const a, b = "peerA", "peerB"
	fs.NewTestMFS(mock.NewIOS())
	prev := core.T
	mock.NewTarget(mock.NewBaseBownerMock())
	t.Cleanup(func() { core.T = prev })
	xreg.Init()
	newXact := func() *XactTCO {
		r := &XactTCO{
			streamingX: streamingX{p: &streamingF{dm: &bundle.DM{}}},
			args:       &xreg.TCOArgs{BckTo: meta.NewBck("dst", apc.AIS, cmn.NsGlobal)},
		}
		r.Base.InitBase(t.Context(), cos.GenUUID(), apc.ActCopyObjects, nil)
		smap := &meta.Smap{Tmap: meta.NodeMap{a: {DaeID: a}, b: {DaeID: b}}}
		r.sntl.init(r, &r.Base, r.p.dm, cmn.GCO.Get(), smap, 3)
		r.sntl.initLast(1)
		return r
	}

	t.Run("done", func(t *testing.T) {
		r := newXact()
		done := func(sid string) error { return r.RecvCtrl(xact.NoneWID, sid, xact.OpcodeDone, nil) }
		tassert.Fatalf(t, done("unknown") != nil, "accepted unknown peer")
		tassert.Fatalf(t, done(core.T.SID()) != nil, "accepted self")
		tassert.Fatalf(t, r.RecvCtrl(xact.NoneWID, a, xact.OpcodeStatus, nil) != nil, "accepted wrong opcode")
		tassert.Fatalf(t, r.RecvCtrl("txn", a, xact.OpcodeDone, nil) != nil, "accepted transaction done")
		tassert.Fatalf(t, r.RecvCtrl(xact.NoneWID, a, xact.OpcodeDone, []byte("x")) != nil, "accepted body")
		tassert.Fatalf(t, len(r.sntl.pend.last) == 2, "rejected control changed pending peers")
		tassert.Fatalf(t, r.StatusFlags() == 0, "premature done-sending flag")

		r.SetStopping()
		r.sntl.txDone.Store(true)
		rs, flags, cause, err := xact.ParseStatusReply(xact.StatusReply(r))
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, rs == core.RunStateRunning && flags.IsSet(xact.StatusDoneSending) && cause == "",
			"invalid stopping status: %s, %d, %q", rs, flags, cause)
		tassert.CheckFatal(t, done(a))
		tassert.CheckFatal(t, done(a))
		r.sntl.onStatus(a, core.RunStateRunning, 0, "") // stale reply must not resurrect a
		tassert.Fatalf(t, len(r.sntl.pend.last) == 1, "duplicate done or stale status changed pending peers")

		r.sntl.onStatus(b, rs, flags, cause) // recover a missed push
		tassert.Fatalf(t, r.sntl.qcb(0) == core.QuiDone, "still waiting for peers")
		r.sntl.cleanup()
		tassert.CheckFatal(t, done(b)) // late push after cleanup
		r.Finish()
		tassert.Fatalf(t, !r.Abort(errors.New("late abort")) && !r.IsAborted(), "finished job accepted abort")
		tassert.Fatalf(t, !r.AbortByOwner(errors.New("late owner abort")) && !r.IsAborted(), "finished job accepted owner abort")
	})

	t.Run("abort", func(t *testing.T) {
		const cause = "peer failed to drain outbound data"
		source, r := newXact(), newXact()
		source.sntl.txDone.Store(true)
		source.Abort(errors.New(cause))
		rs, flags, got, err := xact.ParseStatusReply(xact.StatusReply(source))
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, rs == core.RunStateAborted && flags == 0 && got == cause,
			"invalid aborted status: %s, %d, %q", rs, flags, got)
		r.sntl.onStatus(a, rs, flags, got)
		err = r.AbortErr()
		tassert.Fatalf(t, err != nil && err.Error() == cause, "lost original cause: %v", err)
		tassert.Fatalf(t, xact.IsErrRecvAbortXact(err) && r.SkipAbortBcast(err), "received abort would be re-broadcast")
		tassert.Fatalf(t, len(r.sntl.pend.last) == 2, "aborted peer was marked done")
	})

	t.Run("abort-owner", func(t *testing.T) {
		const cause = "peer failed to drain outbound data"

		// pull: a peer reports aborted while this job is stopping
		r := newXact()
		r.SetStopping()
		tassert.Fatalf(t, !r.Abort(errors.New("outside abort")) && !r.IsAborted(), "accepted outside abort while stopping")
		r.sntl.onStatus(a, core.RunStateAborted, 0, cause)
		err := r.AbortErr()
		tassert.Fatalf(t, err != nil && err.Error() == cause && xact.IsErrRecvAbortXact(err), "lost peer abort while stopping: %v", err)
		rs, _, got, err := xact.ParseStatusReply(xact.StatusReply(r))
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, rs == core.RunStateAborted && got == cause, "invalid status after abort: %s, %q", rs, got)
		tassert.Fatalf(t, r.EndTime().IsZero(), "owner abort finished before teardown")
		r.Finish()

		// owner: sentinel timeout and failed close while stopping
		r = newXact()
		r.SetStopping()
		tassert.Fatalf(t, r.sntl._qabort(errors.New(cause)) == core.QuiAborted && r.IsAborted(), "lost sentinel abort while stopping")
		tassert.Fatalf(t, !r.AbortByOwner(errors.New("second abort")), "accepted second abort")
		err = r.AbortErr()
		tassert.Fatalf(t, err != nil && err.Error() == cause && !xact.IsErrRecvAbortXact(err), "invalid abort cause: %v", err)
	})

	t.Run("in-band", func(t *testing.T) {
		for _, wid := range []string{"", cos.GenUUID()} {
			r := newXact()
			err := r._recv(&transport.ObjHdr{Opcode: transport.OpcDone, SID: a, Opaque: []byte(wid)}, nil)
			tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "mixed-version") && r.IsAborted(),
				"accepted in-band done (wid %q): %v", wid, err)
		}
	})
}

func TestMossAbortAfterStopping(t *testing.T) {
	r := &XactMoss{}
	r.Base.InitBase(t.Context(), cos.GenUUID(), apc.ActGetBatch, nil)
	tassert.Fatalf(t, r.SetStopping(), "failed to claim cleanup")
	err := errors.New("late peer abort")
	tassert.Fatalf(t, !r.Base.Abort(err), "base allowed abort after cleanup was claimed")
	tassert.Fatalf(t, !r.Abort(err), "abort entered cleanup owned by fini")
	tassert.Fatalf(t, !r.IsAborted(), "abort changed state after cleanup was claimed")
}

// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact/xs"
)

const (
	testMountpath = "/tmp/ais-test-mpath" // mpath is created and deleted during the test
	testBucket    = "bck"
)

func TestCopyRegularSameUnameAboveHardLimit(t *testing.T) {
	const objName = "copy-regular-same-uname"
	data := []byte("oversized")

	mpath := t.TempDir()
	extraMi, err := fs.AddTestMpath(mpath, mockTarget.SID())
	tassert.CheckFatal(t, err)
	defer fs.Remove(mpath)

	src := core.AllocLOM(objName)
	defer core.FreeLOM(src)
	err = src.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, extraMi.CreateMissingBckDirs(src.Bucket()))

	chunks := src.Bprops().Chunks
	defer restoreTestChunks(src.Bprops(), chunks)
	src.Bprops().Chunks.MaxMonolithicSize = cos.SizeIEC(len(data) + 1)

	poi := &putOI{
		t:       mockTarget,
		atime:   time.Now().UnixNano(),
		lom:     src,
		r:       readers.NewBytes(data),
		config:  cmn.GCO.Get(),
		workFQN: src.GenFQN(fs.WorkCT, fs.WorkfilePut),
		size:    int64(len(data)),
	}
	_, err = poi.putObject()
	tassert.CheckFatal(t, err)

	// Model an n-way copy: same logical object, different physical location.
	dstMi := extraMi
	if dstMi == src.Mountpath() {
		dstMi = fs.GetAvail()[testMountpath]
	}
	dst := core.AllocLOM(objName)
	defer core.FreeLOM(dst)
	err = dst.InitFQN(dstMi.MakePathFQN(src.Bucket(), fs.ObjCT, objName), src.Bucket())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, src.Uname() == dst.Uname() && src.FQN != dst.FQN,
		"expected same Uname at different FQNs: %q, %q", src.FQN, dst.FQN)

	src.Bprops().Chunks.MaxMonolithicSize = 1
	src.Bprops().Chunks.ChunkSize = 4
	coi := &coi{Buf: make([]byte, cos.KiB), Config: cmn.GCO.Get()}
	done := make(chan xs.CoiRes, 1)
	go func() { done <- coi._regular(mockTarget, src, dst, true /*lcopy*/) }()
	var res xs.CoiRes
	select {
	case res = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("same-Uname copy deadlocked (source rlock vs destination wlock)")
	}
	tassert.CheckFatal(t, res.Err)
	tassert.Fatalf(t, res.Lsize == int64(len(data)), "expected size %d, got %d", len(data), res.Lsize)

	err = dst.Load(false /*cache it*/, false /*locked*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !dst.IsChunked(), "same-Uname copy must remain monolithic")
}

func TestCopyRegularPreservesChunkedLayout(t *testing.T) {
	const (
		srcName   = "copy-regular-chunked-src"
		dstName   = "copy-regular-chunked-dst"
		chunkSize = 4
	)
	data := []byte("0123456789")

	src := core.AllocLOM(srcName)
	defer core.FreeLOM(src)
	err := src.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)

	chunks := src.Bprops().Chunks
	defer restoreTestChunks(src.Bprops(), chunks)
	src.Bprops().Chunks.MaxMonolithicSize = 1
	src.Bprops().Chunks.ChunkSize = 6 // would produce two chunks if _regular rechunked

	poi := &putOI{
		t:       mockTarget,
		lom:     src,
		r:       readers.NewBytes(data),
		oreq:    &http.Request{Header: make(http.Header)},
		config:  cmn.GCO.Get(),
		workFQN: src.GenFQN(fs.WorkCT, fs.WorkfilePut),
		size:    int64(len(data)),
	}
	_, err = poi.chunk(chunkSize)
	tassert.CheckFatal(t, err)

	dst := core.AllocLOM(dstName)
	defer core.FreeLOM(dst)
	err = dst.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)

	coi := &coi{Buf: make([]byte, cos.KiB), Config: cmn.GCO.Get()}
	res := coi._regular(mockTarget, src, dst, false /*lcopy*/)
	tassert.CheckFatal(t, res.Err)

	err = dst.Load(false /*cache it*/, false /*locked*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, dst.IsChunked(), "expected copied object to remain chunked")
	manifest, err := core.NewUfest("", dst, true /*must-exist*/)
	tassert.CheckFatal(t, err)

	dst.Lock(false)
	err = manifest.LoadCompleted(dst)
	dst.Unlock(false)
	tassert.CheckFatal(t, err)

	want := (len(data) + chunkSize - 1) / chunkSize
	tassert.Fatalf(t, manifest.Count() == want, "expected %d preserved chunks, got %d", want, manifest.Count())
}

func TestCopyRegularChunkPreservesMetadata(t *testing.T) {
	const (
		srcName = "copy-regular-chunk-md-src"
		dstName = "copy-regular-chunk-md-dst"
		mdKey   = "x-amz-meta-color"
		mdVal   = "teal"
	)
	data := []byte("0123456789")

	src := core.AllocLOM(srcName)
	defer core.FreeLOM(src)
	err := src.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)

	chunks := src.Bprops().Chunks
	defer restoreTestChunks(src.Bprops(), chunks)
	src.Bprops().Chunks.MaxMonolithicSize = cos.SizeIEC(len(data) + 1) // monolithic source

	src.SetCustomKey(mdKey, mdVal)
	poi := &putOI{
		t:       mockTarget,
		atime:   time.Now().UnixNano(),
		lom:     src,
		r:       readers.NewBytes(data),
		config:  cmn.GCO.Get(),
		workFQN: src.GenFQN(fs.WorkCT, fs.WorkfilePut),
		size:    int64(len(data)),
	}
	_, err = poi.putObject()
	tassert.CheckFatal(t, err)

	src.Lock(false)
	err = src.Load(false /*cache it*/, true /*locked*/)
	src.Unlock(false)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !src.IsChunked(), "expected monolithic source")
	v, ok := src.GetCustomKey(mdKey)
	tassert.Fatalf(t, ok && v == mdVal, "source: expected %s=%s, got %q", mdKey, mdVal, v)
	srcVer := src.Version()

	// lower the limit: _regular => _chunk
	src.Bprops().Chunks.MaxMonolithicSize = 1
	src.Bprops().Chunks.ChunkSize = 4

	dst := core.AllocLOM(dstName)
	defer core.FreeLOM(dst)
	err = dst.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)

	coi := &coi{Buf: make([]byte, cos.KiB), Config: cmn.GCO.Get()}
	res := coi._regular(mockTarget, src, dst, false /*lcopy*/)
	tassert.CheckFatal(t, res.Err)

	check := core.AllocLOM(dstName)
	defer core.FreeLOM(check)
	err = check.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	check.Lock(false)
	err = check.Load(false /*cache it*/, true /*locked*/)
	check.Unlock(false)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, check.IsChunked(), "expected chunked destination")
	v, ok = check.GetCustomKey(mdKey)
	tassert.Fatalf(t, ok && v == mdVal, "destination: expected %s=%s, got %q", mdKey, mdVal, v)
	tassert.Fatalf(t, check.Version() == srcVer, "destination version %q vs source %q", check.Version(), srcVer)
}

func TestCopyReaderChunkReleasesSourceLock(t *testing.T) {
	const (
		srcName   = "copy-reader-lock-src"
		dstName   = "copy-reader-lock-dst"
		chunkSize = 4
	)
	data := []byte("oversized")

	src := core.AllocLOM(srcName)
	defer core.FreeLOM(src)
	err := src.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	defer src.RemoveMain()

	chunks := src.Bprops().Chunks
	defer restoreTestChunks(src.Bprops(), chunks)
	src.Bprops().Chunks.MaxMonolithicSize = cos.SizeIEC(len(data) + 1)

	poi := &putOI{
		t:       mockTarget,
		atime:   time.Now().UnixNano(),
		lom:     src,
		r:       readers.NewBytes(data),
		config:  cmn.GCO.Get(),
		workFQN: src.GenFQN(fs.WorkCT, fs.WorkfilePut),
		size:    int64(len(data)),
	}
	_, err = poi.putObject()
	tassert.CheckFatal(t, err)

	src.Bprops().Chunks.MaxMonolithicSize = 1
	src.Bprops().Chunks.ChunkSize = chunkSize
	dst := core.AllocLOM(dstName)
	defer core.FreeLOM(dst)
	err = dst.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	defer dst.RemoveMain()

	coi := &coi{GetROC: core.GetDefaultROC, Config: cmn.GCO.Get(), OWT: cmn.OwtCopy}
	res := coi._reader(mockTarget, nil /*dm*/, src, dst, &core.ETLArgs{})
	tassert.CheckFatal(t, res.Err)
	tassert.Fatalf(t, res.Lsize == int64(len(data)), "expected size %d, got %d", len(data), res.Lsize)

	err = dst.Load(false /*cache it*/, false /*locked*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, dst.IsChunked(), "expected reader copy above hard limit to be chunked")

	if !src.TryLock(true) {
		t.Fatal("reader copy leaked the source read lock")
	}
	src.Unlock(true)
}

// models remote bucket copied onto itself w/ --latest or --sync (allowed; see proxy):
// unchanged object => GetROC returns a local reader that holds the (same-uname) rlock
func TestCopyReaderChunkSameUname(t *testing.T) {
	const (
		objName   = "copy-reader-same-uname"
		chunkSize = 4
	)
	data := []byte("oversized")

	src := core.AllocLOM(objName)
	defer core.FreeLOM(src)
	err := src.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	defer src.RemoveMain()

	chunks := src.Bprops().Chunks
	defer restoreTestChunks(src.Bprops(), chunks)
	src.Bprops().Chunks.MaxMonolithicSize = cos.SizeIEC(len(data) + 1)

	poi := &putOI{
		t:       mockTarget,
		atime:   time.Now().UnixNano(),
		lom:     src,
		r:       readers.NewBytes(data),
		config:  cmn.GCO.Get(),
		workFQN: src.GenFQN(fs.WorkCT, fs.WorkfilePut),
		size:    int64(len(data)),
	}
	_, err = poi.putObject()
	tassert.CheckFatal(t, err)

	src.Bprops().Chunks.MaxMonolithicSize = 1
	src.Bprops().Chunks.ChunkSize = chunkSize
	dst := core.AllocLOM(objName)
	defer core.FreeLOM(dst)
	err = dst.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, src.Uname() == dst.Uname(), "expected same uname")

	coi := &coi{GetROC: core.GetDefaultROC, Config: cmn.GCO.Get(), OWT: cmn.OwtCopy}
	done := make(chan xs.CoiRes, 1)
	go func() { done <- coi._reader(mockTarget, nil /*dm*/, src, dst, &core.ETLArgs{}) }()
	var res xs.CoiRes
	select {
	case res = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("same-uname reader copy deadlocked (source rlock held while completing w/ destination wlock)")
	}
	tassert.CheckFatal(t, res.Err)

	check := core.AllocLOM(objName)
	defer core.FreeLOM(check)
	err = check.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	check.Lock(false)
	err = check.Load(false /*cache it*/, true /*locked*/)
	check.Unlock(false)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, check.IsChunked(), "expected chunked")
	tassert.Fatalf(t, check.Lsize() == int64(len(data)), "expected size %d, got %d", len(data), check.Lsize())
}

func TestCopyReaderNoopReleasesSourceLock(t *testing.T) {
	const (
		srcName = "copy-reader-noop-lock-src"
		dstName = "copy-reader-noop-lock-dst"
	)
	data := []byte("identical")
	bck := &meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal}

	src := core.AllocLOM(srcName)
	defer core.FreeLOM(src)
	err := src.InitBck(bck)
	tassert.CheckFatal(t, err)
	defer src.RemoveMain()

	props := src.Bprops()
	chunks := props.Chunks
	defer restoreTestChunks(props, chunks)
	props.Chunks.MaxMonolithicSize = cos.SizeIEC(len(data) + 1)
	cksumType := props.Cksum.Type
	t.Cleanup(func() { props.Cksum.Type = cksumType })
	props.Cksum.Type = cos.ChecksumOneXxh

	dst := core.AllocLOM(dstName)
	defer core.FreeLOM(dst)
	err = dst.InitBck(bck)
	tassert.CheckFatal(t, err)
	defer dst.RemoveMain()

	for _, lom := range []*core.LOM{src, dst} {
		poi := &putOI{
			t:       mockTarget,
			atime:   time.Now().UnixNano(),
			lom:     lom,
			r:       readers.NewBytes(data),
			config:  cmn.GCO.Get(),
			workFQN: lom.GenFQN(fs.WorkCT, fs.WorkfilePut),
			size:    int64(len(data)),
		}
		_, err = poi.putObject()
		tassert.CheckFatal(t, err)
	}
	tassert.Fatalf(t, dst.EqCksum(src.Checksum()), "expected identical checksums")

	coi := &coi{GetROC: core.GetDefaultROC, Config: cmn.GCO.Get(), OWT: cmn.OwtCopy}
	res := coi._reader(mockTarget, nil /*dm*/, src, dst, &core.ETLArgs{})
	tassert.CheckFatal(t, res.Err)
	if !src.TryLock(true) {
		t.Fatal("no-op reader copy leaked the source read lock")
	}
	src.Unlock(true)
}

func restoreTestChunks(props *cmn.Bprops, chunks cmn.ChunksConf) { props.Chunks = chunks }

func TestApndParseHandle(t *testing.T) {
	initLom := func(objName string) *core.LOM {
		lom := core.AllocLOM(objName)
		t.Cleanup(func() { core.FreeLOM(lom) })
		err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
		tassert.CheckFatal(t, err)
		return lom
	}
	// the one handle this target produces for `lom`
	mint := func(lom *core.LOM) (packedHdl, workFQN string) {
		a := &apndOI{t: mockTarget, lom: lom, op: apc.AppendOp}
		a.hdl.partialCksum = cos.NewCksumHash(cos.ChecksumOneXxh)
		a.hdl.tie = cos.GenTie()
		return a.pack(), lom.GenFQN(fs.WorkCT, fs.WorkfileAppend, a.hdl.tie)
	}

	var (
		lom     = initLom("apnd/handle/objname")
		sibLom  = initLom("apnd/handle/objname.sibling")
		fntlLom = initLom(strings.Repeat("a", 252))
	)

	packedHdl, workFQN := mint(lom)
	_, sibWorkFQN := mint(sibLom)
	fntlHdl, fntlWorkFQN := mint(fntlLom)

	tassert.Fatalf(t, !strings.Contains(packedHdl, cos.PathSeparator), "handle carries a path: %q", packedHdl)
	tassert.Fatalf(t, filepath.Dir(workFQN) == filepath.Dir(sibWorkFQN),
		"test assumes a shared work directory, got %q vs %q", workFQN, sibWorkFQN)
	tassert.Fatalf(t, fs.HasPrefixFntl(filepath.Base(fntlWorkFQN)),
		"test assumes an fntl-shortened work basename, got %q", filepath.Base(fntlWorkFQN))

	items, err := preParse(packedHdl)
	tassert.CheckFatal(t, err)

	repack := func(idx int, val string) string {
		clone := make([]string, len(items))
		copy(clone, items)
		clone[idx] = val
		return strings.Join(clone, appendHandleSepa)
	}

	var (
		tie      = items[1]
		otherTie = "zzz"
	)
	tests := []struct {
		lom     *core.LOM
		name    string
		hdl     string
		wantTie string
	}{
		{lom, "as-is", packedHdl, tie},
		{lom, "another tie-breaker", repack(1, otherTie), otherTie},
		{fntlLom, "fntl object", fntlHdl, ""}, // (tie filled in below)

		// the handle names no file: the same bytes follow the request's own object
		{sibLom, "replayed against a sibling object", packedHdl, tie},
		{fntlLom, "replayed against an fntl object", packedHdl, tie},

		// a name or a path in the tie-breaker field
		{lom, "sibling object work basename", repack(1, filepath.Base(sibWorkFQN)), ""},
		{fntlLom, "forged fntl work basename", repack(1, ".x"+strings.Repeat("0", 64)), ""},
		{lom, "own work basename (pre-fix format)", repack(1, filepath.Base(workFQN)), ""},
		{lom, "full work fqn (pre-fix format)", repack(1, workFQN), ""},
		{lom, "absolute path", repack(1, "/etc/hostname"), ""},
		{lom, "parent traversal", repack(1, "../../../../etc/hostname"), ""},

		// malformed tie-breaker
		{lom, "empty tie-breaker", repack(1, ""), ""},
		{lom, "too short", repack(1, "zz"), ""},
		{lom, "too long", repack(1, strings.Repeat("z", 64)), ""},
		{lom, "pid does not travel", repack(1, "zzz."+strconv.FormatInt(int64(os.Getpid()), 16)), ""},

		// the remaining fields
		{lom, "another node", repack(0, "not-"+mockTarget.SID()), ""},
		{lom, "invalid checksum type", repack(2, "no-such-cksum"), ""},
		{lom, "invalid checksum state", repack(3, "!not-base64!"), ""},
		{lom, "oversized handle", repack(3, strings.Repeat("A", maxLenAppendHandle)), ""},
	}
	fntlItems, err := preParse(fntlHdl)
	tassert.CheckFatal(t, err)
	tests[2].wantTie = fntlItems[1]

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := &apndOI{t: mockTarget, lom: test.lom, op: apc.AppendOp}
			err := a.parse(test.hdl)
			if test.wantTie == "" {
				tassert.Fatalf(t, err != nil, "expected an error, got workFQN %q", a.hdl.workFQN)
				return
			}
			tassert.CheckFatal(t, err)
			expected := test.lom.GenFQN(fs.WorkCT, fs.WorkfileAppend, test.wantTie)
			tassert.Fatalf(t, a.hdl.workFQN == expected, "expected %q, got %q", expected, a.hdl.workFQN)
		})
	}
}

// validate maxLenAppendHandle (largest serializable hash state) so a change
// in the checksum implementation would not silently invalidate the bound
func TestApndPackHandleLen(t *testing.T) {
	lom := core.AllocLOM("apnd/pack/objname")
	defer core.FreeLOM(lom)
	err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)

	const maxLenSID = 32 // a node ID is "less than 32 characters" (see cos.OnlyNice)

	var maxLen int
	for _, cksumType := range cos.SupportedChecksums() {
		t.Run(cksumType, func(t *testing.T) {
			a := &apndOI{t: mockTarget, lom: lom, op: apc.AppendOp}
			a.hdl.partialCksum = cos.NewCksumHash(cksumType)
			a.hdl.tie = cos.GenTie()

			// the longest handle this target could mint for `cksumType`
			l := len(a.pack()) - len(mockTarget.SID()) + maxLenSID
			maxLen = max(maxLen, l)
			tassert.Errorf(t, l <= maxLenAppendHandle, "packed handle %dB > %dB", l, maxLenAppendHandle)
		})
	}
	t.Logf("worst case %dB (bound %d)", maxLen, maxLenAppendHandle)
}

// a well-formed APPEND handle can name a work file that does not exist
func TestPromoteSrcMustExist(t *testing.T) {
	lom := core.AllocLOM("apnd/flush/objname")
	defer core.FreeLOM(lom)
	err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)

	workFQN := lom.GenFQN(fs.WorkCT, fs.WorkfileAppend, cos.GenTie())
	tassert.Fatalf(t, cos.Stat(workFQN) != nil, "test assumes %q does not exist", workFQN)

	params := &core.PromoteParams{
		Bck:    lom.Bck(),
		Config: cmn.GCO.Get(),
		PromoteArgs: apc.PromoteArgs{
			SrcFQN:       workFQN,
			ObjName:      lom.ObjName,
			OverwriteDst: true,
			DeleteSrc:    true,
		},
	}

	// promote(directory): the source may be gone by now - not an error
	size, ecode, err := mockTarget._promLocal(params, lom)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, size == -1 && ecode == 0, "expected (-1, 0), got (%d, %d)", size, ecode)

	// append(flush): the caller owns the source
	params.SrcMustExist = true
	_, ecode, err = mockTarget._promLocal(params, lom)
	tassert.Fatalf(t, err != nil, "expected an error for a missing work file %q", workFQN)
	tassert.Errorf(t, cos.IsNotExist(err), "expected a not-exist error, got %v", err)
	tassert.Errorf(t, ecode == http.StatusNotFound, "expected %d, got %d", http.StatusNotFound, ecode)
}

func TestTargetBlobDlValidatesObjName(tst *testing.T) {
	target := &target{}
	for _, objName := range []string{
		"",
		"../escape",
		"dir/../../escape",
		"~/escape",
		"../../../../../../../../tmp/pwned_32114.txt",
	} {
		params := &core.BlobParams{Lom: &core.LOM{ObjName: objName}}
		_, _, err := target.blobdlBackground(params, nil /*oa*/)
		expErr := fmt.Sprintf("%s: invalid object name %q", badBlobRequest, objName)
		if err == nil || err.Error() != expErr {
			tst.Fatalf("expected blob-download of %q to fail with %q, got: %v", objName, expErr, err)
		}
	}
}

func TestTargetCopyValidatesDestination(tst *testing.T) {
	target := &target{}
	for _, objnameTo := range []string{
		"",
		"../escape",
		"dir/../../escape",
		"~/escape",
		"../../../../../../../../tmp/pwned_32114.txt",
	} {
		params := &xs.CoiParams{ObjnameTo: objnameTo}
		res := target.CopyObject(&core.LOM{ObjName: "source"}, nil /*dm*/, params)
		expErr := fmt.Sprintf("%s: invalid object name %q", badTcRequest, objnameTo)
		if res.Err == nil || res.Err.Error() != expErr {
			tst.Fatalf("expected copy to %q to fail with %q, got: %v", objnameTo, expErr, res.Err)
		}
	}
}

func TestObjectRejectsOversizedMergedCustomMD(t *testing.T) {
	for _, method := range []string{http.MethodPatch, http.MethodPut} {
		t.Run(method, func(t *testing.T) { testOversizedMergedCustomMD(t, method) })
	}
}

func testOversizedMergedCustomMD(t *testing.T, method string) {
	objName := "merged-custom-md-limit-" + strings.ToLower(method)
	existing := strings.Repeat("a", 2030)
	data := []byte("0123456789")

	lom := core.AllocLOM(objName)
	defer core.FreeLOM(lom)
	err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(t, err)
	lom.SetCustomKey("key", existing)
	poi := &putOI{
		t:       mockTarget,
		atime:   time.Now().UnixNano(),
		lom:     lom,
		r:       readers.NewBytes(data),
		config:  cmn.GCO.Get(),
		workFQN: lom.GenFQN(fs.WorkCT, fs.WorkfilePut),
		size:    int64(len(data)),
	}
	_, err = poi.putObject()
	tassert.CheckFatal(t, err)
	defer lom.RemoveMain()

	send := func(custom cos.StrKVs) int {
		q := url.Values{}
		q.Set(apc.QparamPID, "p1")
		q.Set(apc.QparamUnixTime, unixNano2S(time.Now().UnixNano()))
		body := data
		if method == http.MethodPatch {
			body = cos.MustMarshal(apc.ActMsg{Value: custom})
		}
		r := httptest.NewRequest(method, apc.URLPathObjects.Join(testBucket, objName)+"?"+q.Encode(), bytes.NewReader(body))
		r = r.WithContext(context.WithValue(r.Context(), keyReqNet, reqNetPub))
		if method == http.MethodPut {
			r.Header.Set(cos.HdrContentLength, strconv.Itoa(len(body)))
			for k, v := range custom {
				r.Header.Add(apc.HdrObjCustomMD, k+"="+v)
			}
		}
		w := httptest.NewRecorder()
		mockTarget.objectHandler(w, r)
		return w.Code
	}
	load := func() cos.StrKVs {
		check := core.AllocLOM(objName)
		defer core.FreeLOM(check)
		tassert.CheckFatal(t, check.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal}))
		tassert.CheckFatal(t, check.LoadMetaFromFS())
		return check.GetCustomMD()
	}

	// each map fits, the merged one does not
	code := send(cos.StrKVs{"new": strings.Repeat("b", 20)})
	tassert.Fatalf(t, code == http.StatusBadRequest, "expected oversized merge to fail with %d, got %d", http.StatusBadRequest, code)
	md := load()
	tassert.Fatalf(t, md["key"] == existing, "existing custom metadata lost: %d keys", len(md))
	_, ok := md["new"]
	tassert.Fatalf(t, !ok, "rejected key persisted")

	// a merge within the limit still succeeds
	code = send(cos.StrKVs{"new": "b"})
	tassert.Fatalf(t, code == http.StatusOK, "expected merge to succeed, got %d", code)
	md = load()
	tassert.Fatalf(t, md["key"] == existing && md["new"] == "b", "unexpected custom metadata after merge: %d keys", len(md))

	// replacing the large key frees its budget
	code = send(cos.StrKVs{"key": "a", "new": strings.Repeat("b", 100)})
	tassert.Fatalf(t, code == http.StatusOK, "expected replacing merge to succeed, got %d", code)
	md = load()
	tassert.Fatalf(t, md["key"] == "a" && len(md["new"]) == 100, "unexpected custom metadata after replacing merge: %v", md)
}

func TestS3UserMetadataRejectsOversizedMergedCustomMD(t *testing.T) {
	lom := core.AllocLOM("s3-custom-md-limit")
	defer core.FreeLOM(lom)
	tassert.CheckFatal(t, lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal}))
	lom.SetCustomKey(cos.HdrContentType, "text/plain")

	hdr := http.Header{}
	hdr.Set(s3.HeaderMetaPrefix+"Key", strings.Repeat("a", 2040-len(s3.HeaderMetaPrefix+"Key")))
	err := setS3UserMetadata(lom, hdr)
	tassert.Fatalf(t, err != nil, "expected oversized merge with %s to fail", cos.HdrContentType)
	md := lom.GetCustomMD()
	tassert.Fatalf(t, len(md) == 1 && md[cos.HdrContentType] == "text/plain", "unexpected custom metadata after rejection: %v", md)

	// no user metadata, oversized Content-Type alone
	lom.SetCustomKey(cos.HdrContentType, strings.Repeat("t", 2048))
	err = setS3UserMetadata(lom, http.Header{})
	tassert.Fatalf(t, err != nil, "expected oversized %s to fail", cos.HdrContentType)
}

func TestPromoteRemoteSourceSwapBeforeDelete(t *testing.T) {
	dir, outside := t.TempDir(), t.TempDir()
	stage := filepath.Join(dir, "stage")
	tassert.CheckFatal(t, os.Mkdir(stage, cos.PermRWXRX))
	data := []byte("staged data")
	tassert.CheckFatal(t, os.WriteFile(filepath.Join(stage, "file"), data, cos.PermRWR))
	canary := filepath.Join(outside, "file")
	tassert.CheckFatal(t, os.WriteFile(canary, []byte("outside"), cos.PermRWR))
	root, err := os.OpenRoot(dir)
	tassert.CheckFatal(t, err)
	defer root.Close()
	// Swap the parent after the remote PUT has consumed the file, before DeleteSrc.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err == nil && !bytes.Equal(got, data) {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			err = os.Rename(stage, filepath.Join(dir, "saved"))
		}
		if err == nil {
			err = os.Symlink(outside, stage)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	peer := setCopyPutTestPeer(t, srv)
	lom := core.AllocLOM("promote-delete-" + cos.GenUUID())
	defer core.FreeLOM(lom)
	tassert.CheckFatal(t, lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)))
	tsi, local, err := lom.HrwTarget(&mockTarget.owner.smap.get().Smap)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !local && tsi.ID() == peer.ID(), "test requires promotion to the remote peer")
	params := &core.PromoteParams{
		Bck: lom.Bck(), Config: cmn.GCO.Get(), SrcRoot: root,
		PromoteArgs: apc.PromoteArgs{
			SrcFQN: filepath.Join(stage, "file"), ObjName: lom.ObjName, DeleteSrc: true, OverwriteDst: true,
		},
	}
	_, err = mockTarget._promote(params, lom)
	tassert.CheckFatal(t, err)
	got, err := os.ReadFile(canary)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, string(got) == "outside", "outside file changed: %q", got)
}

//
// remote copy put
//

func TestRemoteCopyPutResponses(t *testing.T) {
	for _, operation := range []string{"rename", "promote"} {
		t.Run(operation, func(t *testing.T) {
			for _, status := range []int{http.StatusOK, http.StatusForbidden} {
				t.Run(http.StatusText(status), func(t *testing.T) {
					data := []byte("keep the source")
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						uploaded, err := io.ReadAll(r.Body)
						if err != nil || !bytes.Equal(uploaded, data) {
							http.Error(w, "unexpected upload", http.StatusInternalServerError)
							return
						}
						w.WriteHeader(status)
					}))
					defer srv.Close()
					setCopyPutTestPeer(t, srv)

					lom := core.AllocLOM("remote-copy-put-" + operation)
					defer core.FreeLOM(lom)
					tassert.CheckFatal(t, lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)))
					var (
						source string
						err    error
					)
					if operation == "rename" {
						params := &core.PutParams{
							Reader:  cos.NewByteReader(data),
							WorkTag: fs.WorkfilePut,
							Atime:   time.Now(),
							Size:    int64(len(data)),
							OWT:     cmn.OwtPut,
						}
						tassert.CheckFatal(t, mockTarget.PutObject(lom, params))
						defer func() {
							lom.Lock(true)
							defer lom.Unlock(true)
							loadErr := lom.Load(false /*cache it*/, true /*locked*/)
							if cos.IsNotExist(loadErr) {
								return
							}
							tassert.CheckFatal(t, loadErr)
							tassert.CheckFatal(t, lom.RemoveObj())
						}()
						source = lom.FQN
						err = mockTarget.objMv(lom, &apc.ActMsg{Name: lom.ObjName + "-renamed"})
					} else {
						source = filepath.Join(t.TempDir(), "source")
						tassert.CheckFatal(t, os.WriteFile(source, data, cos.PermRWR))
						params := &core.PromoteParams{
							Bck:    lom.Bck(),
							Config: cmn.GCO.Get(),
							PromoteArgs: apc.PromoteArgs{
								SrcFQN:       source,
								ObjName:      lom.ObjName,
								OverwriteDst: true,
								DeleteSrc:    true,
							},
						}
						_, err = mockTarget._promote(params, lom)
					}

					remaining, readErr := os.ReadFile(source)
					if status == http.StatusOK {
						tassert.CheckFatal(t, err)
						tassert.Errorf(t, os.IsNotExist(readErr), "successful %s must remove the source, got %v", operation, readErr)
						return
					}
					tassert.Fatalf(t, err != nil, "expected destination status %d to fail %s", status, operation)
					herr := cmn.AsErrHTTP(err)
					tassert.Fatalf(t, herr != nil, "expected an HTTP error, got %v", err)
					tassert.Errorf(t, herr.Status == status, "expected HTTP status %d, got %d", status, herr.Status)
					tassert.Errorf(t, readErr == nil && bytes.Equal(remaining, data), "failed %s must preserve source bytes, got %q: %v", operation, remaining, readErr)
				})
			}
		})
	}
}

func setCopyPutTestPeer(t *testing.T, srv *httptest.Server) *meta.Snode {
	t.Helper()
	previousClient := g.client.data
	previousSmap := mockTarget.owner.smap.get()
	previousConfig := cmn.GCO.Get()
	t.Cleanup(func() {
		g.client.data = previousClient
		mockTarget.owner.smap.put(previousSmap)
		cmn.GCO.Put(previousConfig)
	})
	g.client.data = srv.Client()
	config := cmn.GCO.BeginUpdate()
	config.Timeout.SendFile = cos.Duration(5 * time.Second)
	cmn.GCO.CommitUpdate(config)

	peer := &meta.Snode{}
	peer.Init("t-copy-put", apc.Target, nil)
	peer.DataNet.URL = srv.URL
	primary := &meta.Snode{}
	primary.Init("p-copy-put", apc.Proxy, nil)
	smap := newSmap()
	smap.Tmap[peer.ID()] = peer
	smap.Pmap[primary.ID()] = primary
	smap.Primary = primary
	smap.Version = 1
	mockTarget.owner.smap.put(smap)
	return peer
}

//
// cold get reset
//

const coldGetResetContent = "replacement fetched after eviction\n"

type coldGetResetBackend struct {
	core.Backend
	oldBMD      *bucketMD
	oldBackends backends
	objName     string
	calls       int
}

func (*coldGetResetBackend) MetricName(name string) string { return name }

func (bp *coldGetResetBackend) restore() {
	mockTarget.bps = bp.oldBackends
	mockTarget.owner.bmd.put(bp.oldBMD)
}

func (bp *coldGetResetBackend) GetObjReader(_ context.Context, lom *core.LOM, _, _ int64, _ ...*core.ObjPin) core.GetReaderResult {
	bp.calls++
	if lom.ObjName != bp.objName {
		return core.GetReaderResult{Err: fmt.Errorf("backend GET used %q, expected %q", lom.ObjName, bp.objName), ErrCode: http.StatusNotFound}
	}
	// Observe the production Reset call before the backend supplies new metadata.
	if lom.IsChunked(true) || lom.IsFntl() || lom.HasShardIdx() || lom.Checksum() != nil ||
		lom.Lsize(true) != 0 || lom.VersionPtr() != nil || lom.GetCustomMD() != nil {
		return core.GetReaderResult{Err: fmt.Errorf("cold GET retained stale metadata for %q", bp.objName), ErrCode: http.StatusInternalServerError}
	}
	return core.GetReaderResult{R: io.NopCloser(strings.NewReader(coldGetResetContent)), Size: int64(len(coldGetResetContent))}
}

func TestGetObjectColdMissReset(t *testing.T) {
	bp := &coldGetResetBackend{oldBMD: mockTarget.owner.bmd.get(), oldBackends: mockTarget.bps}
	t.Cleanup(bp.restore)
	mockTarget.bps = backends{apc.AIS: bp}
	bck := meta.NewBck("cold-get-reset", apc.AIS, cmn.Ns{UUID: "remote"})
	bmd := bp.oldBMD.clone()
	bmd.add(bck, &cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumNone}})
	mockTarget.owner.bmd.put(bmd)
	for _, err := range fs.CreateBucket(bck.Bucket(), false /*nilbmd*/) {
		tassert.CheckFatal(t, err)
	}

	for _, objName := range []string{"chunked", strings.Repeat("x", 280)} {
		t.Logf("cold GET after eviction: %q", objName)
		bp.objName, bp.calls = objName, 0
		lom := &core.LOM{ObjName: objName}
		tassert.CheckFatal(t, lom.InitBck(bck))
		originalFQN := lom.FQN

		// Cache two chunks and retain the loaded handle across eviction.
		const chunkSize = 32 * cos.KiB
		source, err := readers.New(&readers.Arg{Type: readers.Rand, Size: 2 * chunkSize, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		poi := putOI{
			t: mockTarget, lom: lom, r: source, size: 2 * chunkSize,
			oreq: httptest.NewRequest(http.MethodGet, "/", http.NoBody), skipBackend: true, locked: true,
		}
		lom.Lock(true)
		_, err = poi.chunk(chunkSize)
		lom.Unlock(true)

		// TODO:
		// rand-reader.Close() always returns nil
		// but here the source.Close() would be expected to return an error on second close

		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, lom.Load(false, false))
		tassert.Fatalf(t, lom.IsChunked(), "expected initially chunked object")
		if fs.IsFntl(objName) {
			tassert.Fatalf(t, lom.IsFntl() && lom.ObjName != objName, "expected shortened local name")
		}

		remover := &core.LOM{ObjName: objName}
		tassert.CheckFatal(t, remover.InitBck(bck))
		_, err = mockTarget.EvictObject(remover)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, cos.IsNotExist(cos.Stat(lom.FQN)), "eviction did not remove the local file")
		tassert.Fatalf(t, lom.IsChunked(), "test requires the original handle to retain stale metadata")

		// Do not call Reset here: get() must reset on the miss and run cold PUT itself.
		w := httptest.NewRecorder()
		goi := getOI{t: mockTarget, lom: lom, w: w, dpq: &dpq{}, ctx: t.Context(), atime: time.Now().UnixNano()}
		_, err = goi.getObject()
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, goi.cold && bp.calls == 1, "expected exactly one cold GET, got %d backend reads", bp.calls)
		tassert.Fatalf(t, w.Code == http.StatusOK && w.Body.String() == coldGetResetContent, "incorrect cold GET response")
		tassert.Fatalf(t, lom.IsLocked() == apc.LockNone, "cold GET leaked the object lock")

		// A fresh handle must discover the monolithic replacement, including FNTL metadata.
		fresh := &core.LOM{ObjName: objName}
		tassert.CheckFatal(t, fresh.InitBck(bck))
		fresh.Lock(false)
		fresh.Uncache() // verify persistence, not just the metadata cache
		err = fresh.Load(false, true)
		fresh.Unlock(false)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, !fresh.IsChunked() && fresh.Lsize() == int64(len(coldGetResetContent)), "invalid replacement metadata")
		if fs.IsFntl(objName) {
			tassert.Fatalf(t, fresh.IsFntl() && fresh.ObjName != objName, "cold PUT did not shorten the name again")
			ofqn, _ := fresh.GetCustomKey(cmn.OrigFntl)
			tassert.Fatalf(t, ofqn == originalFQN && fresh.OrigObjName() == objName, "lost original-name mapping")
		}
		w = httptest.NewRecorder()
		goi = getOI{t: mockTarget, lom: fresh, w: w, dpq: &dpq{}, ctx: t.Context(), atime: time.Now().UnixNano()}
		_, err = goi.getObject()
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, !goi.cold && bp.calls == 1, "warm GET unexpectedly contacted the backend")
		tassert.Fatalf(t, w.Body.String() == coldGetResetContent, "incorrect warm GET response")
		tassert.Fatalf(t, fresh.IsLocked() == apc.LockNone, "warm GET leaked the object lock")
		_, err = mockTarget.EvictObject(fresh)
		tassert.CheckFatal(t, err)
	}
}

//
// get write deadline
//

// GET write deadline (wdl): no-progress timeout for clients that stop reading

const (
	wdlTestTout = 250 * time.Millisecond // => cmn.XferMinRenew (1MiB); minimum transfer rate 4MiB/s
	wdlTestSize = 32 * cos.MiB           // well above loopback socket buffering
	wdlTestWait = 30 * time.Second       // test-level safety net
)

type wdlResult struct {
	err       error
	written   int64
	elapsed   time.Duration
	deadlines int
}

type wdlResponseWriter struct {
	http.ResponseWriter
	deadlines int
}

func (w *wdlResponseWriter) SetWriteDeadline(t time.Time) error {
	w.deadlines++
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(t)
}

func (w *wdlResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	return cos.CopySendfile(w.ResponseWriter, r)
}

type wdlCountingWriter struct {
	http.ResponseWriter
	writes, deadlines int
}

func (w *wdlCountingWriter) Write(p []byte) (int, error) {
	w.writes++
	return len(p), nil
}

func (w *wdlCountingWriter) SetWriteDeadline(time.Time) error {
	w.deadlines++
	return nil
}

func TestWdlChunkSize(t *testing.T) {
	tests := []struct {
		tout time.Duration
		want int64
	}{
		{0, cmn.XferMinRenew},
		{10 * time.Second, cmn.XferMinRenew},          // 640KiB => min (below config-validated minimum)
		{time.Minute, 60 * cmn.XferMinRate},           // 3.75MiB (config-validated minimum)
		{5 * time.Minute, 300 * cmn.XferMinRate},      // 18.75MiB (default)
		{17 * time.Minute, 17 * 60 * cmn.XferMinRate}, // 63.75MiB (last below max)
		{18 * time.Minute, cmn.XferMaxRenew},
		{2 * time.Hour, cmn.XferMaxRenew},
		{100 * 24 * time.Hour, cmn.XferMaxRenew},
	}
	for _, tc := range tests {
		if got := cmn.XferRenewSize(tc.tout); got != tc.want {
			t.Errorf("cmn.XferRenewSize(%v) = %d, want %d", tc.tout, got, tc.want)
		}
	}
}

func TestWdlInit(t *testing.T) {
	defer cmn.Rom.Set(&cmn.GCO.Get().ClusterConfig)

	tests := []struct {
		name     string
		sendfile time.Duration
		features feat.Flags
		wrap     bool // writer that does not support deadlines
		enabled  bool
	}{
		{"enabled", 5 * time.Minute, 0, false, true},
		{"opt-out", 5 * time.Minute, feat.DisableGetDeadline, false, false},
		{"zero-timeout", 0, 0, false, false},
		{"not-supported", 5 * time.Minute, 0, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cmn.GCO.Get().ClusterConfig
			cfg.Timeout.SendFile = cos.Duration(tc.sendfile)
			cfg.Features = tc.features
			cmn.Rom.Set(&cfg)

			ch := make(chan *getOI, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.wrap {
					w = struct{ http.ResponseWriter }{w} // hides SetWriteDeadline and Unwrap
				}
				goi := &getOI{w: w}
				goi.initWdl()
				ch <- goi
			}))
			defer srv.Close()

			resp, err := http.Get(srv.URL)
			tassert.CheckFatal(t, err)
			resp.Body.Close()

			goi := <-ch
			if enabled := goi.wctrl != nil; enabled != tc.enabled {
				t.Fatalf("enabled=%t, want %t", enabled, tc.enabled)
			}
			if tc.enabled && goi.wtout != tc.sendfile {
				t.Fatalf("wtout=%v, want %v", goi.wtout, tc.sendfile)
			}
		})
	}
}

func TestWdlBufferedSizes(t *testing.T) {
	const (
		bufSize = 128 * cos.KiB
		tout    = 5 * time.Minute
	)
	renewSize := cmn.XferRenewSize(tout)
	payload := make([]byte, renewSize+1)
	buf := make([]byte, bufSize)
	tests := []struct {
		name     string
		size     int64
		unknown  bool
		disabled bool
		renew    bool
	}{
		{name: "empty"},
		{name: "below-buffer", size: bufSize - 1},
		{name: "one-buffer", size: bufSize},
		{name: "above-buffer", size: bufSize + 1},
		{name: "multiple-buffers", size: 512 * cos.KiB},
		{name: "below-chunk", size: renewSize - 1},
		{name: "one-chunk", size: renewSize},
		{name: "above-chunk", size: renewSize + 1, renew: true},
		{name: "unknown-small", size: 512 * cos.KiB, unknown: true},
		{name: "unknown-chunk", size: renewSize, unknown: true, renew: true},
		{name: "disabled", size: renewSize + 1, disabled: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &wdlCountingWriter{ResponseWriter: httptest.NewRecorder()}
			goi := &getOI{w: w, wtout: tout}
			if !tc.disabled {
				goi.wctrl = w
				tassert.CheckFatal(t, w.SetWriteDeadline(time.Now().Add(tout)))
			}
			size := tc.size
			if tc.unknown {
				size = -1
			}
			n, err := goi._copyWdl(bytes.NewReader(payload[:tc.size]), buf, size)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, n == tc.size, "written %d, wanted %d", n, tc.size)
			tassert.Errorf(t, w.writes == int((tc.size+bufSize-1)/bufSize), "unexpected write count: %d", w.writes)
			wantDeadlines := 1
			if tc.disabled {
				wantDeadlines = 0
			} else if tc.renew {
				wantDeadlines++
			}
			tassert.Errorf(t, w.deadlines == wantDeadlines, "set %d deadlines, wanted %d", w.deadlines, wantDeadlines)
		})
	}
}

// buffered: non-divisible writes - one renewal per renewal boundary crossed,
// overshoot carries over (renewal k at cumulative k*renewSize)
func TestWdlWriterRenewal(t *testing.T) {
	const renewSize = 64 * cos.KiB
	tests := []struct {
		name      string
		writes    []int
		renewals  int
		remainder int64
	}{
		{"below", []int{renewSize - 1}, 0, renewSize - 1},
		{"divisible", []int{renewSize, renewSize}, 2, 0},
		{"three-quarter-steps", []int{48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB}, 3, 0}, // reset-to-zero: 2
		{"overshoot", []int{48 * cos.KiB, 48 * cos.KiB}, 1, 32 * cos.KiB},
		{"multi-renewal", []int{renewSize*2 + renewSize/2}, 1, renewSize / 2}, // one write, one renewal
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &wdlCountingWriter{ResponseWriter: httptest.NewRecorder()}
			d := &wdlWriter{w: w, rc: w, tout: time.Minute, renewSize: renewSize}
			for _, n := range tc.writes {
				if _, err := d.Write(make([]byte, n)); err != nil {
					t.Fatal(err)
				}
			}
			if w.deadlines != tc.renewals || d.n != tc.remainder {
				t.Fatalf("renewals %d, remainder %d; want %d, %d", w.deadlines, d.n, tc.renewals, tc.remainder)
			}
		})
	}
}

func TestWdlSendfile(t *testing.T) { wdlRun(t, true) }
func TestWdlBuffered(t *testing.T) { wdlRun(t, false) }

func wdlRun(t *testing.T, sendfile bool) {
	t.Run("small", func(t *testing.T) {
		const size = 64 * cos.KiB
		url, ch := wdlServer(t, size, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		n, err := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, n == size, "received %d, wanted %d", n, size)
		r := wdlWait(t, ch)
		tassert.CheckFatal(t, r.err)
		tassert.Errorf(t, r.deadlines == 1, "small response set %d deadlines, wanted initial deadline only", r.deadlines)
	})

	t.Run("fast", func(t *testing.T) {
		url, ch := wdlServer(t, wdlTestSize, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		tassert.CheckFatal(t, err)

		r := wdlWait(t, ch)
		tassert.CheckFatal(t, r.err)
		tassert.Errorf(t, r.written == wdlTestSize, "written %d != %d", r.written, wdlTestSize)
	})

	// reading steadily at ~10x the minimum transfer rate, for longer than the window:
	// must complete (deadline renewed per renewal size)
	t.Run("slow-progressing", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{Long: true}) // runs past the timeout by design
		url, ch := wdlServer(t, wdlTestSize, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		buf := make([]byte, cos.MiB)
		var received int64
		for {
			var n int
			n, err = io.ReadFull(resp.Body, buf)
			received += int64(n)
			if err != nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		resp.Body.Close()
		tassert.Errorf(t, err == io.EOF, "client read ended with %v, wanted EOF", err)
		tassert.Errorf(t, received == wdlTestSize, "received %d, wanted %d", received, wdlTestSize)

		r := wdlWait(t, ch)
		tassert.CheckFatal(t, r.err)
		tassert.Errorf(t, r.deadlines > 1, "long response never renewed its initial deadline")
		tassert.Errorf(t, r.written == wdlTestSize, "written %d != %d", r.written, wdlTestSize)
		tassert.Errorf(t, r.elapsed > wdlTestTout, "elapsed %v <= timeout %v: test did not exercise re-arming",
			r.elapsed, wdlTestTout)
	})

	// connected but not reading: must time out, and be classified benign (see _txerr)
	t.Run("stalled", func(t *testing.T) {
		url, ch := wdlServer(t, wdlTestSize, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		defer resp.Body.Close()
		_, err = io.CopyN(io.Discard, resp.Body, cos.MiB)
		tassert.CheckFatal(t, err)

		r := wdlWait(t, ch) // client stays connected
		if r.err == nil {
			t.Fatalf("expected write deadline exceeded, got nil (written %d)", r.written)
		}
		tassert.Errorf(t, cos.IsErrNetTimeoutConn(r.err), "expected write timeout, got %v", r.err)
		tassert.Errorf(t, errors.Is(r.err, os.ErrDeadlineExceeded), // => cmn.ErrSlowReadingClient (see _txerr)
			"expected os.ErrDeadlineExceeded, got %v", r.err)
		tassert.Errorf(t, r.written < wdlTestSize, "written %d, expected partial", r.written)
	})
}

// Even a one-buffer response can block on its first write. Keep the initial
// deadline installed, including when wrapping and splitting are skipped.
func TestWdlFirstWrite(t *testing.T) {
	const size = 128 * cos.KiB
	for _, sendfile := range []bool{false, true} {
		name := "buffered"
		if sendfile {
			name = "sendfile"
		}
		t.Run(name, func(t *testing.T) {
			url, ch := wdlServer(t, size, sendfile, 4*cos.KiB)
			conn, err := net.DialTimeout("tcp", strings.TrimPrefix(url, "http://"), wdlTestWait)
			tassert.CheckFatal(t, err)
			defer conn.Close()
			tassert.CheckFatal(t, conn.(*net.TCPConn).SetReadBuffer(4*cos.KiB))
			tassert.CheckFatal(t, conn.SetDeadline(time.Now().Add(wdlTestWait)))
			_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
			tassert.CheckFatal(t, err)
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			tassert.CheckFatal(t, err)
			// Do not read or close the body until the handler reports its timeout.
			r := wdlWait(t, ch)
			conn.Close()
			resp.Body.Close()
			tassert.Errorf(t, cos.IsErrNetTimeoutConn(r.err), "first write did not time out: %v", r.err)
			tassert.Errorf(t, r.written < size, "first write completed: %d", r.written)
			tassert.Errorf(t, r.deadlines == 1, "set %d deadlines, wanted initial deadline only", r.deadlines)
		})
	}
}

//
// helpers
//

// serve `size` zero bytes from a (sparse) file via either sendfile or buffered transmit,
// with write deadline enabled; "/plain" serves the same w/o deadline
func wdlServer(t *testing.T, size int64, sendfile bool, socketBuffer ...int) (string, chan wdlResult) {
	fqn := filepath.Join(t.TempDir(), "obj")
	f, err := os.Create(fqn)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, f.Truncate(size))
	tassert.CheckFatal(t, f.Close())

	ch := make(chan wdlResult, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fh, err := os.Open(fqn)
		if err != nil {
			ch <- wdlResult{err: err}
			return
		}
		defer fh.Close()
		w.Header().Set(cos.HdrContentLength, strconv.FormatInt(size, 10))

		if r.URL.Path == "/plain" {
			_, _ = cos.CopyBuffer(w, fh, make([]byte, 32*cos.KiB))
			return
		}

		var (
			res     wdlResult
			cw      = &wdlResponseWriter{ResponseWriter: w}
			goi     = &getOI{w: cw, wctrl: http.NewResponseController(cw), wtout: wdlTestTout}
			started = mono.NanoTime()
		)
		goi.ltime = started
		res.err = goi.wctrl.SetWriteDeadline(time.Now().Add(goi.wtout))
		if res.err == nil {
			if sendfile {
				res.written, res.err = goi._sendfileWdl(&io.LimitedReader{R: fh, N: size})
			} else {
				res.written, res.err = goi._copyWdl(fh, make([]byte, min(size, 128*cos.KiB)), size)
			}
		}
		res.deadlines = cw.deadlines
		res.elapsed = mono.Since(started)
		ch <- res
	}))
	if len(socketBuffer) > 0 {
		srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
			if state == http.StateNew {
				_ = c.(*net.TCPConn).SetWriteBuffer(socketBuffer[0])
			}
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.URL, ch
}

func wdlWait(t *testing.T, ch chan wdlResult) wdlResult {
	select {
	case r := <-ch:
		return r
	case <-time.After(wdlTestWait):
		t.Fatalf("handler did not return within %v", wdlTestWait)
		return wdlResult{}
	}
}

//
// rename
//

func TestTargetRenameValidatesObjectNames(t *testing.T) {
	target := &target{}
	for _, test := range []struct {
		source, destination string
	}{
		{"../source", "destination"},
		{"source", "../destination"},
		{"source", "dir/../destination"},
		{"escape-src.txt", "../../../../../../../../tmp/pwned_32114.txt"},
	} {
		err := target.objMv(
			&core.LOM{ObjName: test.source},
			&apc.ActMsg{Name: test.destination},
		)
		if err == nil {
			t.Fatalf("expected rename from %q to %q to fail", test.source, test.destination)
		}
	}
}

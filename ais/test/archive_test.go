// Package integration_test.
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
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

func TestListChunkedArchives(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	for _, test := range []struct {
		ext       string
		numChunks int
	}{
		{archive.ExtTar, 0}, {archive.ExtTar, 3},
		{archive.ExtTgz, 0}, {archive.ExtTgz, 3},
		{archive.ExtTarGz, 0}, {archive.ExtTarGz, 3},
		{archive.ExtZip, 0}, {archive.ExtZip, 3},
		{archive.ExtTarLz4, 0}, {archive.ExtTarLz4, 3},
	} {
		t.Run(fmt.Sprintf("%s/chunks=%d", test.ext, test.numChunks), func(t *testing.T) {
			bck := cmn.Bck{Name: "list-archive-" + trand.String(8), Provider: apc.AIS}
			tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{
				Chunks: &cmn.ChunksConfToSet{ObjSizeLimit: apc.Ptr(cos.SizeIEC(0))},
			}, true /*cleanup*/)
			const fileSize = 8 * cos.KiB
			names := []string{"a.txt", "nested/b.txt", "z.txt"}
			objName := "archive" + test.ext
			reader, err := readers.New(&readers.Arg{
				Type: readers.File, Path: t.TempDir(), Name: objName,
				Arch: &readers.Arch{Mime: test.ext, Names: names, Num: len(names), MinSize: fileSize},
			})
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { reader.Close() })
			size, err := reader.Seek(0, io.SeekEnd)
			tassert.CheckFatal(t, err)
			reader.Reset()
			errCh := make(chan error, 1)
			tools.Put(proxyURL, bck, objName, reader, uint64(size), test.numChunks, errCh)
			tassert.SelectErr(t, errCh, "put archive", true)

			props, err := api.HeadObjectV2(bp, bck, objName, apc.GetPropsChunked, api.HeadArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, props.Chunks != nil && props.Chunks.ChunkCount == test.numChunks,
				"expected %d chunks, got %+v", test.numChunks, props.Chunks)

			lst, err := api.ListObjects(bp, bck, &apc.LsoMsg{
				Flags: apc.LsArchDir, Props: apc.JoinProps(apc.GetPropsName, apc.GetPropsSize),
			}, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(lst.Entries) == len(names)+1, "expected archive and %d files, got %d entries", len(names), len(lst.Entries))
			tassert.Errorf(t, lst.Entries[0].Name == objName && lst.Entries[0].IsAnyFlagSet(apc.EntryIsArchive),
				"expected parent archive %s, got %+v", objName, lst.Entries[0])
			for i, name := range names {
				entry := lst.Entries[i+1]
				tassert.Errorf(t, entry.Name == path.Join(objName, name) && entry.Size == fileSize && entry.IsAnyFlagSet(apc.EntryInArch),
					"expected archived file %s (%d bytes), got %+v", name, fileSize, entry)
			}
		})
	}
}

//
// GET from
//

func TestGetFromArch(t *testing.T) {
	tmpDir := t.TempDir() // (with auto-remove)

	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			m = ioContext{
				t:   t,
				bck: bck.Clone(),
			}
			baseParams  = tools.BaseAPIParams(m.proxyURL)
			errCh       = make(chan error, m.num)
			numArchived = 100
			randomNames = make([]string, numArchived)
			// idx: state of the shard index at GET time (TAR only; no-op otherwise)
			//   "none"  — no index built; exercises sequential scan
			//   "fresh" — index built and up-to-date; exercises the fast-path seek
			//   "stale" — index built, then the TAR is re-PUT with different bytes;
			//             exercises the IsStale + fallback-to-scan path
			subtests = []struct {
				ext        string // one of archive.FileExtensions
				nested     bool   // subdirs
				autodetect bool   // auto-detect by magic
				mime       bool   // specify mime type
				ctype      bool   // specify mime type as Content-Type (see archive.ContentTypeFromExt)
				upper      bool   // specify mime type in upper case
				idx        string // "none" | "fresh" | "stale"
			}{
				{
					ext: archive.ExtTar, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtTarGz, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtZip, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtTarLz4, nested: false, autodetect: false, mime: false,
				},
				{
					ext: archive.ExtTar, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtTarGz, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtZip, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtTarLz4, nested: true, autodetect: true, mime: false,
				},
				{
					ext: archive.ExtTar, nested: true, autodetect: true, mime: true,
				},
				{
					ext: archive.ExtTarGz, nested: true, autodetect: true, mime: true,
				},
				{
					ext: archive.ExtZip, nested: true, autodetect: true, mime: true,
				},
				{
					ext: archive.ExtTarLz4, nested: true, autodetect: true, mime: true,
				},
				{
					ext: archive.ExtTgz, nested: true, autodetect: true, ctype: true,
				},
				{
					ext: archive.ExtTarLz4, nested: true, autodetect: true, ctype: true,
				},
				{
					ext: archive.ExtTarGz, nested: true, autodetect: true, mime: true, upper: true,
				},
				{
					ext: archive.ExtTar, nested: false, autodetect: false, mime: false, idx: "fresh",
				},
				{
					ext: archive.ExtTar, nested: true, autodetect: true, mime: true, idx: "fresh",
				},
				{
					ext: archive.ExtTar, nested: true, autodetect: true, mime: true, idx: "stale",
				},
			}
		)
		if testing.Short() {
			numArchived = 10
		}
		var (
			sparsePrint           atomic.Int64
			corruptAutoDetectOnce atomic.Int64
		)
		for _, test := range subtests {
			idxLabel := test.idx
			if idxLabel == "" {
				idxLabel = "none"
			}
			tname := fmt.Sprintf("%s/nested=%t/detect=%t/mime=%t/idx=%s", test.ext, test.nested, test.autodetect, test.mime, idxLabel)
			if test.ctype {
				tname += "/ctype"
			}
			if test.upper {
				tname += "/upper"
			}
			for _, tf := range []tar.Format{tar.FormatUnknown, tar.FormatGNU, tar.FormatPAX} {
				tarFormat := tf
				t.Run(path.Join(tname, "format-"+tarFormat.String()), func(t *testing.T) {
					var (
						err      error
						fsize    = rand.IntN(10*cos.KiB) + 1
						archName = tmpDir + "/" + cos.GenTie() + test.ext
						dirs     = []string{"a", "b", "c", "a/b", "a/c", "b/c", "a/b/c", "a/c/b", "b/a/c"}
					)
					for i := range numArchived {
						j := rand.Int()
						randomNames[i] = fmt.Sprintf("%d.txt", j)
						if test.nested {
							k := j % len(dirs)
							dir := dirs[k]
							randomNames[i] = dir + "/" + randomNames[i]
						}
						if j%3 == 0 {
							randomNames[i] = "/" + randomNames[i]
						}
					}
					err = tarch.CreateArchRandomFiles(
						archName,
						tarFormat,
						test.ext,
						numArchived,
						fsize,
						nil,         // record extensions
						randomNames, // pregenerated filenames
						false,       // duplication
						false,       // random dir prefix
						false,       // exact size
					)
					tassert.CheckFatal(t, err)

					objname := filepath.Base(archName)
					if test.autodetect {
						objname = objname[0 : len(objname)-len(test.ext)]
					}

					var (
						reader    readers.Reader
						corrupted bool
					)
					if test.autodetect && corruptAutoDetectOnce.Inc() == 1 {
						corrupted = true
						tlog.Logfln("============== damaging %s - overwriting w/ random data", archName)
						reader, err = readers.New(&readers.Arg{
							Type:      readers.File,
							Path:      filepath.Dir(archName),
							Name:      filepath.Base(archName),
							Size:      1024,
							CksumType: cos.ChecksumNone,
						})
					} else {
						reader, err = readers.New(&readers.Arg{
							Type:      readers.File,
							Path:      archName,
							Size:      readers.ExistingFileSize,
							CksumType: cos.ChecksumNone,
						})
					}
					tassert.CheckFatal(t, err)

					tools.Put(m.proxyURL, m.bck, objname, reader, 0 /*size*/, 0 /*numChunks*/, errCh)
					tassert.Error(t, reader.Close() != nil, "expecting file reader to be closed")

					tassert.SelectErr(t, errCh, "put", true)

					// Optionally index the shard, and optionally re-PUT different bytes
					// to make the index stale. Results must be identical in all three
					// modes (none/fresh/stale) — see subtests comment above.
					if !corrupted && (test.idx == "fresh" || test.idx == "stale") {
						idxRunAndWait(t, baseParams, m.bck, &apc.IndexShardMsg{})
					}
					if !corrupted && test.idx == "stale" {
						// re-create the TAR with the same names but fresh random bytes,
						// then re-PUT. PUT preserves HasShardIdx; new cksum != indexed
						// cksum => IsStale fires; GET falls back to sequential scan.
						err = tarch.CreateArchRandomFiles(archName, tarFormat, test.ext,
							numArchived, fsize, nil, randomNames, false, false, false)
						tassert.CheckFatal(t, err)
						r2, err := readers.New(&readers.Arg{
							Type: readers.File, Path: archName,
							Size: readers.ExistingFileSize, CksumType: cos.ChecksumNone,
						})
						tassert.CheckFatal(t, err)
						tools.Put(m.proxyURL, m.bck, objname, r2, 0, 0, errCh)
						tassert.Error(t, r2.Close() != nil, "expecting file reader to be closed")
						tassert.SelectErr(t, errCh, "re-put", true)
					}

					for _, randomName := range randomNames {
						var mime string
						if test.mime {
							mime = "application/x-" + test.ext[1:]
						}
						if test.ctype {
							mime = archive.ContentTypeFromExt(test.ext)
						}
						if test.upper {
							mime = strings.ToUpper(mime)
						}
						getArgs := api.GetArgs{
							Query: url.Values{
								apc.QparamArchpath: []string{randomName},
								apc.QparamArchmime: []string{mime},
							},
						}
						oah, err := api.GetObject(baseParams, m.bck, objname, &getArgs)
						if sparsePrint.Inc()%13 == 0 {
							tlog.Logfln("%s?%s=%s(%dB)", m.bck.Cname(objname), apc.QparamArchpath,
								randomName, oah.Size())
						}
						if corrupted {
							tassert.Errorf(t, err != nil, "expecting error reading corrupted arch %q", archName)
							break
						}
						if err != nil {
							tlog.Logfln("Error reading %s?%s=%s(%dB), where randomName=%q, objname=%q, mime=%q, archName=%q",
								m.bck.Cname(objname), apc.QparamArchpath, randomName, oah.Size(),
								randomName, objname, mime, archName)
							tassert.CheckFatal(t, err)
						}
					}
				})
			}
		}
	})
}

// archive multiple obj-s with an option to append if exists
func TestArchMultiObj(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		testArch(t, bck)
	})
}

func TestArchMultiObjInvalidArchName(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		objName    = "arch-source"
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: cos.KiB, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objName, Reader: reader})
	tassert.CheckFatal(t, err)

	for _, archName := range []string{
		"",
		"./shard.tar",
		"../shard.tar",
		"a/../../shard.tar",
		"~/shard.tar",
		"../../../../../../../../tmp/pwned.tar",
	} {
		msg := cmn.ArchiveBckMsg{
			ToBck: bck,
			ArchiveMsg: apc.ArchiveMsg{
				ArchName:  archName,
				ListRange: apc.ListRange{ObjNames: []string{objName}},
			},
		}
		_, err := api.ArchiveMultiObj(baseParams, bck, &msg)
		tassert.Errorf(t, err != nil, "expected error to occur (archname: %q)", archName)
	}
}

const (
	archLargeSize = 128 * cos.MiB
	archNumSmall  = 40
)

type archLarge struct {
	small ioContext
	bckTo cmn.Bck
	smap  *meta.Smap
	peer  *meta.Snode
	large string
	names []string
}

// a peer sends a large object last - its 'done' (other stream) can arrive first
func newArchSmall(t *testing.T, numSmall int) *archLarge {
	proxyURL := tools.RandomProxyURL(t)
	a := &archLarge{
		small: ioContext{t: t, num: numSmall, prefix: "a-", fileSize: cos.KiB, fixedSize: true, ordered: true},
		bckTo: cmn.Bck{Name: trand.String(10), Provider: apc.AIS},
	}
	a.small.init(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, a.small.bck, nil, true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, a.bckTo, nil, true /*cleanup*/)
	a.small.puts()
	return a
}

func newArchLarge(t *testing.T, numShards int, ext string) (a *archLarge, archNames []string) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, MinTargets: 2})
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
	)
	if n := smap.CountActiveTs(); n < 2 {
		t.Skipf("%s requires at least 2 active targets (have %d)", t.Name(), n)
	}
	if config := tools.GetClusterConfig(t); config.Arch.SbundleMult < 2 {
		tools.SetClusterConfig(t, cos.StrKVs{"arch.bundle_multiplier": "2"})
		t.Cleanup(func() {
			tools.SetClusterConfig(t, cos.StrKVs{"arch.bundle_multiplier": strconv.Itoa(config.Arch.SbundleMult)})
		})
	}
	a = newArchSmall(t, archNumSmall)

	var err error
	a.peer, err = smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	objName := tools.GenerateObjectNameOnTarget("z-", a.small.bck, smap, a.peer)
	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: archLargeSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: a.small.bck, ObjName: objName, Reader: reader})
	tassert.CheckFatal(t, err)
	a.smap, a.large = smap, objName
	a.names = append(slices.Clone(a.small.objNames), objName)

	archNames = make([]string, 0, numShards)
	for i := 0; len(archNames) < numShards; i++ {
		archName := fmt.Sprintf("shard-%d%s", i, ext)
		dt, err := smap.HrwObjName2T(meta.CloneBck(&a.bckTo), archName)
		tassert.CheckFatal(t, err)
		if dt.ID() != a.peer.ID() {
			archNames = append(archNames, archName)
		}
	}
	tlog.Logfln("large object on %s, shards %v", a.peer.StringEx(), archNames)
	return a, archNames
}

// archive objNames and return the job filter
func (a *archLarge) arch(t *testing.T, archName string, objNames []string, appendIfExists bool) xact.ArgsMsg {
	msg := cmn.ArchiveBckMsg{
		ToBck: a.bckTo,
		ArchiveMsg: apc.ArchiveMsg{
			ArchName:       archName,
			ListRange:      apc.ListRange{ObjNames: objNames},
			AppendIfExists: appendIfExists,
		},
	}
	xid, err := api.ArchiveMultiObj(tools.BaseAPIParams(), a.small.bck, &msg)
	tassert.CheckFatal(t, err)
	return xact.ArgsMsg{ID: xid, Kind: apc.ActArchive}
}

func (a *archLarge) countInArch(t *testing.T, archName string) (n int) {
	lsmsg := &apc.LsoMsg{Prefix: archName}
	lsmsg.SetFlag(apc.LsArchDir)
	lst, err := api.ListObjects(tools.BaseAPIParams(), a.bckTo, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	for _, en := range lst.Entries {
		if en.IsAnyFlagSet(apc.EntryInArch) {
			n++
		}
	}
	return n
}

func TestArchLargeLast(t *testing.T) {
	a, archNames := newArchLarge(t, 3 /*num shards*/, archive.ExtTar)
	baseParams := tools.BaseAPIParams()

	for _, archName := range archNames {
		flt := a.arch(t, archName, a.names, false)
		err := api.WaitForSnapsIdle(baseParams, &flt)
		tassert.CheckFatal(t, err)

		n := a.countInArch(t, archName)
		tlog.Logfln("%s: %d archived objects", a.bckTo.Cname(archName), n)
		tassert.Errorf(t, n == len(a.names), "%s: expected %d archived objects, have %d",
			a.bckTo.Cname(archName), len(a.names), n)

		// the next shard (a new job with new streams)
		tassert.CheckError(t, api.AbortXaction(baseParams, &flt))
	}
}

// the original shard (for TAR append in place)
func (a *archLarge) makeOrig(t *testing.T, archName string, objNames []string) (orig []byte) {
	baseParams := tools.BaseAPIParams()
	flt := a.arch(t, archName, objNames, false)
	tassert.CheckFatal(t, api.WaitForSnapsIdle(baseParams, &flt))
	tassert.CheckFatal(t, api.AbortXaction(baseParams, &flt))
	n := a.countInArch(t, archName)
	tassert.Fatalf(t, n == len(objNames), "%s: expected %d archived objects, have %d", a.bckTo.Cname(archName), len(objNames), n)
	return a.getShard(t, archName)
}

func (a *archLarge) getShard(t *testing.T, archName string) []byte {
	var buf bytes.Buffer
	_, err := api.GetObject(tools.BaseAPIParams(), a.bckTo, archName, &api.GetArgs{Writer: &buf})
	tassert.CheckFatal(t, err)
	return buf.Bytes()
}

func (a *archLarge) checkOrig(t *testing.T, archName string, orig []byte, numOrig int) {
	var (
		cname = a.bckTo.Cname(archName)
		n     int
	)
	err := tools.WaitForCondition(func() bool { n = a.countInArch(t, archName); return n > 0 },
		tools.WaitRetryOpts{MaxRetries: 10, Interval: time.Second})
	tassert.Fatalf(t, err == nil, "%s: no rollback after 10s (no original shard)", cname)
	shard := a.getShard(t, archName)
	tlog.Logfln("%s: size %d (original %d), %d archived objects", cname, len(shard), len(orig), n)
	tassert.Fatalf(t, n != len(a.names), "%s: the job archived all objects (increase archLargeSize)", cname)
	tassert.Errorf(t, bytes.Equal(shard, orig), "%s: expected the original (size %d), have size %d", cname, len(orig), len(shard))
	tassert.Errorf(t, n == numOrig, "%s: expected %d archived objects (original), have %d", cname, numOrig, n)
}

// the job on one target (dflt.DaemonID)
func archSnap(t *testing.T, dflt *xact.ArgsMsg) *core.Snap {
	snaps, err := api.QueryXactionSnaps(tools.BaseAPIParams(), dflt)
	tassert.CheckFatal(t, err)
	tsnaps := snaps[dflt.DaemonID]
	tassert.Fatalf(t, len(tsnaps) == 1, "expected one %s on %s, have %d", dflt.ID, dflt.DaemonID, len(tsnaps))
	return tsnaps[0]
}

// TAR append in place, aborted while the designated target waits for the large object
func TestArchAppendAbort(t *testing.T) {
	const half = archNumSmall / 2
	a, archNames := newArchLarge(t, 1 /*num shards*/, archive.ExtTar)
	archName := archNames[0]

	orig := a.makeOrig(t, archName, a.small.objNames[:half])

	flt := a.arch(t, archName, a.names[half:], true /*append*/)
	tassert.CheckFatal(t, api.AbortXaction(tools.BaseAPIParams(), &flt))

	a.checkOrig(t, archName, orig, half)
}

// TAR append in place, a PUT of the same name, then the abort (the newer object stays)
func TestArchAppendAbortPut(t *testing.T) {
	const half = archNumSmall / 2
	a, archNames := newArchLarge(t, 1 /*num shards*/, archive.ExtTar)
	var (
		baseParams = tools.BaseAPIParams()
		archName   = archNames[0]
		cname      = a.bckTo.Cname(archName)
	)
	a.makeOrig(t, archName, a.small.objNames[:half])

	flt := a.arch(t, archName, a.names[half:], true /*append*/)
	newer := []byte(trand.String(int(cos.KiB)))
	_, err := api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: a.bckTo, ObjName: archName, Reader: readers.NewBytes(newer)})
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, api.AbortXaction(baseParams, &flt))

	// the rollback is asynchronous (see checkOrig)
	for range 10 {
		time.Sleep(time.Second)
		tassert.Fatalf(t, bytes.Equal(a.getShard(t, archName), newer), "%s: expected the newer object (not the original and not the archive)", cname)
	}
}

// TAR append in place of an object that the format of the shard cannot encode (a USTAR shard and a long flat name)
func TestArchAppendSkipEntry(t *testing.T) {
	const numSmall = 4
	var (
		baseParams = tools.BaseAPIParams()
		long       = strings.Repeat("x", 160)
		archName   = "shard.tar"
		a          = newArchSmall(t, numSmall)
	)
	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: cos.KiB, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: a.small.bck, ObjName: long, Reader: reader})
	tassert.CheckFatal(t, err)

	a.makeOrig(t, archName, a.small.objNames[:2])

	flt := a.arch(t, archName, append(slices.Clone(a.small.objNames[2:]), long), true /*append*/)
	tassert.CheckFatal(t, api.WaitForSnapsIdle(baseParams, &flt))
	n := a.countInArch(t, archName)
	tlog.Logfln("%s: %d archived objects", a.bckTo.Cname(archName), n)
	tassert.Errorf(t, n == numSmall, "%s: expected %d archived objects (all but the long name), have %d", a.bckTo.Cname(archName), numSmall, n)
}

func (a *archLarge) dtNames(t *testing.T, numOrig int, dt *meta.Snode) []string {
	objNames := make([]string, 0, len(a.names)-numOrig)
	for _, name := range a.small.objNames[numOrig:] {
		tsi, err := a.smap.HrwObjName2T(meta.CloneBck(&a.small.bck), name)
		tassert.CheckFatal(t, err)
		if tsi.ID() == dt.ID() {
			objNames = append(objNames, name)
		}
	}
	return append(objNames, a.large)
}

// num small objects on the peer to delay its 'done'
func (a *archLarge) putOnPeer(t *testing.T, num int) []string {
	baseParams := tools.BaseAPIParams()
	objNames := make([]string, 0, num)
	for i := range num {
		name := tools.GenerateObjectNameOnTarget(fmt.Sprintf("p-%d-", i), a.small.bck, a.smap, a.peer)
		tassert.CheckFatal(t, tools.PutObjRR(baseParams, a.small.bck, name, cos.KiB, cos.ChecksumNone))
		objNames = append(objNames, name)
	}
	return objNames
}

// archive objNames, then freeze the peer (the only sender)
func (a *archLarge) archFreeze(t *testing.T, archName string, objNames []string, dt *meta.Snode,
	allDone, appendIfExists bool) (dflt xact.ArgsMsg, resume func() error, notDone bool) {
	pause, resume, err := tools.NodePauser(a.peer)
	tassert.CheckFatal(t, err)
	flt := a.arch(t, archName, objNames, appendIfExists)
	t.Cleanup(func() { _ = api.AbortXaction(tools.BaseAPIParams(), &flt) })
	t.Cleanup(func() { _ = resume() }) // (before the abort)

	dflt = flt
	dflt.DaemonID = dt.ID()
	tassert.CheckFatal(t, pause())
	if !allDone {
		time.Sleep(2 * time.Second) // (while frozen, the 'done' of the other peers arrives)
	}
	snap := archSnap(t, &dflt)
	deadline := time.Now().Add(5 * time.Second)
	for allDone && strings.Contains(snap.CtlMsg, "peers-not-done:") && time.Now().Before(deadline) {
		tassert.CheckFatal(t, resume())
		time.Sleep(time.Millisecond) // (a 128 MiB object takes many steps)
		tassert.CheckFatal(t, pause())
		snap = archSnap(t, &dflt)
	}
	tlog.Logfln("froze %s: %s", a.peer.StringEx(), snap.CtlMsg)
	notDone = strings.Contains(snap.CtlMsg, "peers-not-done:1,")
	if allDone {
		tassert.Fatalf(t, strings.Contains(snap.CtlMsg, "from-peers:"),
			"%s: no object in transit at the freeze, have %q", a.bckTo.Cname(archName), snap.CtlMsg)
	}
	return dflt, resume, notDone
}

// TAR append in place, the peer freezes after its 'done' w/ the large object in transit
func TestArchAppendFrozenPeer(t *testing.T) {
	const (
		half     = archNumSmall / 2
		sendFile = 10 * time.Second
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	config := tools.GetClusterConfig(t)
	if !config.TestingEnv() {
		t.Skipf("%s requires a testing env (send_file_time %v)", t.Name(), sendFile)
	}
	a, archNames := newArchLarge(t, 1 /*num shards*/, archive.ExtTar)
	var (
		archName = archNames[0]
		cname    = a.bckTo.Cname(archName)
	)
	dt, err := tools.GetClusterMap(t, tools.RandomProxyURL(t)).HrwObjName2T(meta.CloneBck(&a.bckTo), archName)
	tassert.CheckFatal(t, err)

	// give up before the primary removes the frozen peer
	tools.SetClusterConfig(t, cos.StrKVs{"timeout.send_file_time": sendFile.String()})
	t.Cleanup(func() { tools.SetClusterConfig(t, origConfig(config)) })

	orig := a.makeOrig(t, archName, a.small.objNames[:half])

	dflt, resume, _ := a.archFreeze(t, archName, []string{a.large}, dt, true /*all done*/, true /*append*/)

	// after send_file_time with no progress, the designated target times out or gives up
	dflt.Timeout = sendFile + 30*time.Second // (timeout for the quiesce checks and the API calls)
	_, err = api.WaitForSnaps(tools.BaseAPIParams(), &dflt, noFromPeers)
	tassert.CheckFatal(t, err)
	snap := archSnap(t, &dflt)
	tlog.Logfln("%s: %s", dt.StringEx(), snap.Err)
	tassert.Fatalf(t, !snap.AbortedX, "%s: expected no abort (the frozen peer left the Smap?), have %q", cname, snap.AbortErr)
	gaveUp := strings.Contains(snap.Err, "still being received")
	tassert.Fatalf(t, gaveUp || strings.Contains(snap.Err, cmn.ErrQuiesceTimeout.Error()),
		"%s: expected %s to time out, have %q", cname, dt.StringEx(), snap.Err)
	tlog.Logfln("%s: gave up (a receive in progress) %t", dt.StringEx(), gaveUp)

	tassert.CheckFatal(t, resume())
	a.checkOrig(t, archName, orig, half)
}

// the designated target does not wait for objects from peers
func noFromPeers(snaps xact.MultiSnap) (done, reset bool, err error) {
	for _, tsnaps := range snaps {
		for _, snap := range tsnaps {
			if strings.Contains(snap.CtlMsg, "from-peers:") {
				return false, false, nil
			}
		}
	}
	return true, false, nil
}

func waitAbort(t *testing.T, dflt *xact.ArgsMsg) {
	args := *dflt
	args.Timeout = 10 * time.Second
	snaps, err := api.WaitForSnaps(tools.BaseAPIParams(), &args, args.Finished())
	aborted := err == nil && len(snaps[dflt.DaemonID]) == 1 && snaps[dflt.DaemonID][0].AbortedX
	tassert.Errorf(t, aborted, "%s: expected an abort before the restart (%v)", dflt.DaemonID, err)
}

func origConfig(config *cmn.Config) cos.StrKVs {
	return cos.StrKVs{
		"timeout.send_file_time":          config.Timeout.SendFile.String(),
		"keepalivetracker.proxy.interval": config.Keepalive.Proxy.Interval.String(),
	}
}

// the peer crashes and restarts
func TestArchKilledPeer(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	t.Run("leave", func(t *testing.T) { testArchKilledPeer(t, archive.ExtTar, false /*all done*/, true /*wait rm*/) })
	t.Run("restart", func(t *testing.T) { testArchKilledPeer(t, archive.ExtTar, false /*all done*/, false /*wait rm*/) })
	t.Run("zip", func(t *testing.T) { testArchKilledPeer(t, archive.ExtZip, true /*all done*/, true /*wait rm*/) })
}

func testArchKilledPeer(t *testing.T, ext string, allDone, waitRm bool) {
	const (
		half    = archNumSmall / 2
		numPeer = 200
	)
	numTries := cos.Ternary(allDone, 1, 5)
	a, archNames := newArchLarge(t, numTries, ext)
	if !waitRm {
		// keep the peer in the Smap until it restarts
		config := tools.GetClusterConfig(t)
		tools.SetClusterConfig(t, cos.StrKVs{"keepalivetracker.proxy.interval": "40s"})
		t.Cleanup(func() { tools.SetClusterConfig(t, origConfig(config)) })
	}
	var (
		proxyURL       = tools.RandomProxyURL(t)
		baseParams     = tools.BaseAPIParams(proxyURL)
		smap           = tools.GetClusterMap(t, proxyURL)
		appendIfExists = ext == archive.ExtTar
		archName       string
		dt             *meta.Snode
		orig           []byte
		dflt           xact.ArgsMsg
		resume         func() error
		notDone        bool
		peerNames      []string
		err            error
	)
	if !allDone {
		peerNames = a.putOnPeer(t, numPeer)
	}
	for i := range numTries {
		archName = archNames[i]
		dt, err = smap.HrwObjName2T(meta.CloneBck(&a.bckTo), archName)
		tassert.CheckFatal(t, err)

		// 1. the original shard (TAR only)
		if appendIfExists {
			orig = a.makeOrig(t, archName, a.small.objNames[:half])
		}
		// 2. archive, then freeze the peer
		objNames := peerNames
		if allDone {
			objNames = a.dtNames(t, half, dt)
		}
		dflt, resume, notDone = a.archFreeze(t, archName, objNames, dt, allDone, appendIfExists)
		if allDone || notDone {
			break
		}
		tlog.Logfln("the peer was done at the freeze, try again w/ the next shard")
		tassert.CheckFatal(t, resume())
		flt := dflt
		flt.DaemonID = ""
		tassert.CheckFatal(t, api.WaitForSnapsIdle(baseParams, &flt))
	}
	cname := a.bckTo.Cname(archName)
	tassert.Fatalf(t, allDone || notDone, "%s: the peer was done at each freeze (%d tries)", cname, numTries)

	// 3. crash the peer and restart it
	cmd, err := tools.CrashNode(a.peer)
	tassert.CheckError(t, err)
	if waitRm {
		_, err = tools.WaitForClusterState(proxyURL, "target crashed", smap.Version, smap.CountActivePs(), smap.CountActiveTs()-1)
		tassert.CheckError(t, err)
		if !allDone {
			waitAbort(t, &dflt) // before the restart
		}
	}
	tassert.CheckFatal(t, tools.RestoreNode(cmd, false, "target"))
	_, err = tools.WaitForClusterState(proxyURL, "target restarted", smap.Version, smap.CountActivePs(), smap.CountActiveTs())
	tassert.CheckFatal(t, err)
	tools.WaitForRebalAndResil(t, baseParams)

	// 4. the designated target does not wait for the peer and does not publish
	snap := archSnap(t, &dflt)
	tlog.Logfln("%s: aborted %t [%s] [%s] %s", dt.StringEx(), snap.AbortedX, snap.AbortErr, snap.Err, snap.CtlMsg)
	tassert.Errorf(t, !strings.Contains(snap.CtlMsg, "peers-not-done:") && !strings.Contains(snap.CtlMsg, "from-peers:"),
		"%s: %s still waits for the crashed peer: %s", cname, dt.StringEx(), snap.CtlMsg)
	if !allDone {
		want := cos.Ternary(waitRm, "membership change", "targets changed")
		tassert.Errorf(t, snap.AbortedX && strings.Contains(snap.AbortErr, want),
			"%s: expected an abort (%s), have %t %q", cname, want, snap.AbortedX, snap.AbortErr)
	}
	if appendIfExists {
		a.checkOrig(t, archName, orig, half)
		return
	}
	_, err = api.HeadObject(baseParams, a.bckTo, archName, api.HeadArgs{FltPresence: apc.FltPresent})
	tassert.Errorf(t, cmn.IsStatusNotFound(err), "%s: expected no shard (a truncated object), have %v", cname, err)
}

func testArch(t *testing.T, bck *meta.Bck) {
	var (
		numPuts = 50
		m       = ioContext{
			t:       t,
			bck:     bck.Clone(),
			num:     numPuts,
			prefix:  "archive/",
			ordered: true,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		numArchs   = 5
		numInArch  = min(m.num/2, 7)
		fmtRange   = "%s{%d..%d}"
		subtests   = []struct {
			ext            string // one of archive.FileExtensions (same as: supported arch formats)
			list           bool
			inclSrcBckName bool
			abrt           bool
			apnd           bool
		}{
			{
				ext: archive.ExtTar, list: true,
			},
			{
				ext: archive.ExtTar, list: false, inclSrcBckName: true,
			},
			{
				ext: archive.ExtTar, list: false,
			},
			{
				ext: archive.ExtTar, list: true, apnd: true,
			},
			{
				ext: archive.ExtTarLz4, list: true,
			},
		}
		subtestsLong = []struct {
			ext            string // one of archive.FileExtensions (same as: supported arch formats)
			list           bool
			inclSrcBckName bool
			abrt           bool
			apnd           bool
		}{
			{
				ext: archive.ExtTgz, list: true,
			},
			{
				ext: archive.ExtTgz, list: false, inclSrcBckName: true,
			},
			{
				ext: archive.ExtTgz, list: true, inclSrcBckName: true, apnd: true,
			},
			{
				ext: archive.ExtTgz, list: false, apnd: true,
			},
			{
				ext: archive.ExtZip, list: true,
			},
			{
				ext: archive.ExtZip, list: false, inclSrcBckName: true,
			},
			{
				ext: archive.ExtZip, list: true,
			},
			{
				ext: archive.ExtTarLz4, list: false,
			},
		}
	)
	if testing.Short() {
		numArchs = 2
	} else { // test-long
		subtests = append(subtests, subtestsLong...)
	}
	for _, test := range subtests {
		var (
			abrt        string
			listOrRange = "list"
		)
		if !test.list {
			listOrRange = "range"
		}
		tm := mono.NanoTime()
		if tm&0x3 == 0 && !test.apnd {
			test.abrt = true
			abrt = "/abort"
		}
		tname := fmt.Sprintf("%s/%s%s", test.ext, listOrRange, abrt)
		if test.inclSrcBckName {
			tname += "/incl-src=" + m.bck.Name
		}
		if test.apnd {
			tname += "/append"
		}
		t.Run(tname, func(t *testing.T) {
			if m.bck.IsRemote() {
				m.num = numPuts >> 1
			}
			m.init(true /*cleanup*/)
			m.fileSize = min(m.fileSize+m.fileSize/3, 32*cos.KiB)
			m.puts()
			if m.bck.IsRemote() {
				defer m.del(-1)
			}
			bckTo := cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
			tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)

			if test.list {
				for i := range numArchs {
					archName := fmt.Sprintf("test_lst_%02d%s", i, test.ext)
					list := make([]string, 0, numInArch)
					for range numInArch {
						list = append(list, m.objNames[rand.IntN(m.num)])
					}
					go func(archName string, list []string, i int) {
						msg := cmn.ArchiveBckMsg{
							ToBck:      bckTo,
							ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
						}
						msg.ListRange.ObjNames = list
						msg.InclSrcBname = test.inclSrcBckName

						xids, err := api.ArchiveMultiObj(baseParams, m.bck, &msg)
						tassert.CheckFatal(t, err)
						tlog.Logfln("[%s] %2d: arch list %d objects %s => %s", xids, i, len(list), m.bck.String(), bckTo.String())
					}(archName, list, i)
				}
			} else {
				for i := range numArchs {
					archName := fmt.Sprintf("test_rng_%02d%s", i, test.ext)
					start := rand.IntN(m.num - numInArch)
					go func(archName string, start, i int) {
						msg := cmn.ArchiveBckMsg{
							ToBck:      bckTo,
							ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
						}
						msg.ListRange.Template = fmt.Sprintf(fmtRange, m.prefix, start, start+numInArch-1)
						msg.InclSrcBname = test.inclSrcBckName

						xids, err := api.ArchiveMultiObj(baseParams, m.bck, &msg)
						tassert.CheckFatal(t, err)
						tlog.Logfln("[%s] %2d: arch range %s %s => %s",
							xids, i, msg.ListRange.Template, m.bck.String(), bckTo.String())
					}(archName, start, i)
				}
			}

			flt := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
			if test.abrt {
				time.Sleep(time.Duration(rand.IntN(5)+1) * time.Second)
				tlog.Logln("Aborting...")
				api.AbortXaction(baseParams, &flt)
			}

			var lstToAppend *cmn.LsoRes
			for ii := range 2 {
				api.WaitForXaction(baseParams, &flt)

				tlog.Logfln("List %s", bckTo.String())
				msg := &apc.LsoMsg{Prefix: "test_"}
				msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
				lst, err := api.ListObjects(baseParams, bckTo, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
				for _, en := range lst.Entries {
					tlog.Logfln("%s: %dB", en.Name, en.Size)
				}
				num := len(lst.Entries)
				if num < numArchs && ii == 0 {
					tlog.Logfln("Warning: expected %d, have %d - retrying...", numArchs, num)
					time.Sleep(7 * time.Second) // TODO: ditto
					continue
				}
				tassert.Errorf(t, num == numArchs || test.abrt, "expected %d, have %d", numArchs, num)
				lstToAppend = lst
				break
			}

			msg := &apc.LsoMsg{Prefix: "test_"}
			msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			msg.SetFlag(apc.LsArchDir)
			lst, err := api.ListObjects(baseParams, bckTo, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			num := len(lst.Entries)
			expectedNum := numArchs + numArchs*numInArch

			tassert.Errorf(t, num == expectedNum || test.abrt, "expected %d, have %d", expectedNum, num)

			// multi-object APPEND
			if test.apnd {
				for _, e := range lstToAppend.Entries {
					start := rand.IntN(m.num - numInArch)
					go func(archName string, start int) {
						msg := cmn.ArchiveBckMsg{
							ToBck:      bckTo,
							ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
						}
						msg.ListRange.Template = fmt.Sprintf(fmtRange, m.prefix, start, start+numInArch-1)
						msg.InclSrcBname = test.inclSrcBckName

						msg.AppendIfExists = true // here

						xids, err := api.ArchiveMultiObj(baseParams, m.bck, &msg)
						tassert.CheckFatal(t, err)
						tlog.Logfln("[%s] APPEND %s/%s => %s/%s",
							xids, m.bck.String(), msg.ListRange.Template, bckTo.String(), archName)
					}(e.Name, start)
				}

				time.Sleep(10 * time.Second)
				flt := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
				api.WaitForXaction(baseParams, &flt)
			}

			var (
				objName string
				mime    = "application/x-" + test.ext[1:]
			)
			for _, en := range lst.Entries {
				if !en.IsAnyFlagSet(apc.EntryInArch) {
					objName = en.Name
					continue
				}
				if rand.IntN(3) > 0 {
					continue
				}

				getArgs := api.GetArgs{
					Query: url.Values{
						apc.QparamArchpath: []string{en.Name},
						apc.QparamArchmime: []string{mime},
					},
				}
				oah, err := api.GetObject(baseParams, bckTo, objName, &getArgs)
				if err != nil {
					t.Errorf("%s?%s=%s(%dB): %v", bckTo.Cname(objName), apc.QparamArchpath, en.Name, oah.Size(), err)
				}
			}
		})
	}
}

// exercises `api.ArchiveMultiObj` followed by api.PutApndArch(local rand-reader)
func TestAppendToArch(t *testing.T) {
	var (
		bckFrom = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		bckTo   = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		m       = ioContext{
			t:       t,
			bck:     bckFrom,
			num:     100,
			prefix:  "archive/",
			ordered: true,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		numArchs   = m.num
		numAdd     = m.num
		numInArch  = min(m.num/2, 30)
		objPattern = "test_lst_%04d%s"
		archPath   = "extra/newfile%04d"
		subtests   = []struct {
			ext   string // one of archive.FileExtensions (same as: supported arch formats)
			multi bool   // false - append a single file, true - append a list of objects
		}{
			{
				ext: archive.ExtTar, multi: false,
			},
			{
				ext: archive.ExtTar, multi: true,
			},
			{
				ext: archive.ExtTgz, multi: false,
			},
			{
				ext: archive.ExtTgz, multi: true,
			},
			{
				ext: archive.ExtTarLz4, multi: false,
			},
		}
		subtestsLong = []struct {
			ext   string // one of archive.FileExtensions (same as: supported arch formats)
			multi bool   // false - append a single file, true - append a list of objects
		}{
			{
				ext: archive.ExtZip, multi: false,
			},
			{
				ext: archive.ExtZip, multi: true,
			},
			{
				ext: archive.ExtTarLz4, multi: true,
			},
		}
	)
	if !testing.Short() { // test-long, and see one other Skip below
		subtests = append(subtests, subtestsLong...)
	}
	for _, test := range subtests {
		tname := fmt.Sprintf("%s/multi=%t", test.ext, test.multi)
		t.Run(tname, func(t *testing.T) {
			tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
			tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)
			m.init(true /*cleanup*/)
			m.fileSize = min(m.fileSize+m.fileSize/3, 32*cos.KiB)
			m.puts()

			if testing.Short() && test.ext != archive.ExtTar {
				// skip all multi-object appends
				if test.multi {
					tools.ShortSkipf(t)
				}
				// reduce
				numArchs = 2
				numAdd = 3
			}

			// speed up serialized tests
			if !test.multi {
				numArchs /= 2
				numAdd /= 2
			}

			for i := range numArchs {
				archName := fmt.Sprintf(objPattern, i, test.ext)
				list := make([]string, 0, numInArch)
				for range numInArch {
					list = append(list, m.objNames[rand.IntN(m.num)])
				}
				go func(archName string, list []string) {
					msg := cmn.ArchiveBckMsg{
						ToBck:      bckTo,
						ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
					}
					msg.ListRange.ObjNames = list

					_, err := api.ArchiveMultiObj(baseParams, m.bck, &msg)
					tassert.CheckFatal(t, err)
				}(archName, list)
			}

			wargs := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
			api.WaitForXaction(baseParams, &wargs)

			lsmsg := &apc.LsoMsg{Prefix: "test_lst"}
			lsmsg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			lst, err := api.ListObjects(baseParams, bckTo, lsmsg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			num := len(lst.Entries)
			tassert.Errorf(t, num == numArchs, "expected %d, have %d", numArchs, num)

			sparcePrint := max(numArchs/10, 1)
			for i := range numArchs {
				archName := fmt.Sprintf(objPattern, i, test.ext)
				if test.multi {
					if i%sparcePrint == 0 {
						tlog.Logfln("APPEND multi-obj %s => %s/%s", bckFrom.String(), bckTo.String(), archName)
					}
					list := make([]string, 0, numAdd)
					for range numAdd {
						list = append(list, m.objNames[rand.IntN(m.num)])
					}
					msg := cmn.ArchiveBckMsg{
						ToBck:      bckTo,
						ArchiveMsg: apc.ArchiveMsg{ArchName: archName},
					}
					msg.AppendIfExists = true
					msg.ListRange.ObjNames = list
					go func() {
						_, err = api.ArchiveMultiObj(baseParams, bckFrom, &msg)
						tassert.CheckError(t, err)
					}()
				} else {
					for j := range numAdd {
						reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: fileSize, CksumType: cos.ChecksumNone})
						tassert.CheckError(t, err)
						putArgs := api.PutArgs{
							BaseParams: baseParams,
							Bck:        bckTo,
							ObjName:    archName,
							Reader:     reader,
							Size:       fileSize,
						}
						archpath := fmt.Sprintf(archPath, j) + cos.GenTie()
						appendArchArgs := api.PutApndArchArgs{
							PutArgs:  putArgs,
							ArchPath: archpath,
							Flags:    apc.ArchAppend, // existence required
						}
						if i%sparcePrint == 0 && j == 0 {
							tlog.Logfln("APPEND local rand => %s/%s/%s", bckTo.String(), archName, archpath)
						}
						err = api.PutApndArch(&appendArchArgs)
						tassert.CheckError(t, err)
					}
				}
			}
			if test.multi {
				time.Sleep(4 * time.Second)
				wargs := xact.ArgsMsg{Kind: apc.ActArchive, Bck: m.bck}
				api.WaitForXaction(baseParams, &wargs)
			}

			lsmsg.SetFlag(apc.LsArchDir)
			lst, err = api.ListObjects(baseParams, bckTo, lsmsg, api.ListArgs{})
			tassert.CheckError(t, err)
			num = len(lst.Entries)
			expectedNum := numArchs + numArchs*(numInArch+numAdd)

			if num < expectedNum && test.multi && expectedNum-num < 10 {
				tlog.Logfln("Warning: expected %d, have %d", expectedNum, num) // TODO -- FIXME: remove
			} else {
				tassert.Errorf(t, num == expectedNum, "expected %d, have %d", expectedNum, num)
			}
		})
	}
}

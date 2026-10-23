// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

// to run all tests in this source:
// go test ... -run=TestFntl

// Cleanup mode with FNTL names that are too long for a file name.
func TestFntlRebalanceCleanupReclaims(t *testing.T) { testRebalanceCleanupReclaims(t, 280) }

// unresolved logical identity must neither migrate under its physical name nor
// authorize deletion against a peer holding identical bytes under that name
func TestFntlRebalanceUnresolved(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2, RequiredDeployment: tools.ClusterTypeLocal})
	var (
		bp   = tools.BaseAPIParams(proxyURL)
		bck  = cmn.Bck{Name: "reb-unresolved-fntl-" + cos.GenTie(), Provider: apc.AIS}
		data = bytes.Repeat([]byte("fntl-regression"), 64)
	)
	enableRebalance(t, bp)
	waitRunningRebResil(t, bp)
	tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{
		Cksum:  &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumOneXxh)},
		Chunks: &cmn.ChunksConfToSet{ObjSizeLimit: apc.Ptr(cos.SizeIEC(0))},
	}, true /*cleanup*/)
	initMountpaths(t, proxyURL)
	props, err := api.HeadBucket(bp, bck, true /*dontAddRemote*/)
	tassert.CheckFatal(t, err)
	bck.Props = props
	smap := tools.GetClusterMap(t, proxyURL)
	smap.InitDigests()

	// Select a long name whose physical short name belongs to another target.
	// Otherwise a walk using the wrong identity could silently skip it as local.
	var (
		objName, ofqn, shortName, fqn string
		source                        *meta.Snode
	)
	for i := range 1000 {
		objName = fmt.Sprintf("%04d-", i) + strings.Repeat("n", 280)
		source, ofqn = fntlObjectFQN(t, bp, smap, bck, objName)
		shortName = fs.ShortenFntl(ofqn)
		peer, err := smap.HrwObjName2T(meta.CloneBck(&bck), shortName)
		tassert.CheckFatal(t, err)
		if peer.ID() != source.ID() {
			fqn = filepath.Join(filepath.Dir(ofqn), shortName)
			break
		}
	}
	tassert.Fatalf(t, fqn != "", "could not select a misplaced short-name identity")
	fntlPut(t, bp, bck, objName, data)
	tools.CheckPathExists(t, fqn, false /*dir*/)
	tassert.CheckFatal(t, api.ClearLcache(bp, "" /*all targets*/))

	// Use the existing local LOM writer so the injected metadata retains a valid
	// checksum and BID. This mock is confined to the test process; it never joins.
	savedTarget, bid := core.T, props.BID
	bo := mock.NewBaseBownerMock(meta.CloneBck(&bck))
	props.BID = bid // the mock constructor assigns its own BID
	mock.NewTarget(bo)
	t.Cleanup(func() { core.T = savedTarget })
	tools.ModifyLOM(t, fqn, bck, func(_ *testing.T, lom *core.LOM) {
		lom.SetCustomKey(cmn.OrigFntl, ofqn+"-stale")
	})
	tassert.CheckFatal(t, api.ClearLcache(bp, "" /*all targets*/))

	// listing skips (and warns about) the unresolved object: neither name is listed
	for _, flags := range []uint64{0, apc.LsNameOnly} {
		lst, err := api.ListObjects(bp, bck, &apc.LsoMsg{Flags: flags}, api.ListArgs{})
		tassert.CheckFatal(t, err)
		for _, en := range lst.Entries {
			tassert.Fatalf(t, en.Name != objName && en.Name != shortName,
				"listing with flags %#x: unexpected entry %q", flags, en.Name)
		}
	}

	// A literal .x<sha256> is an ordinary object. Place it on the same source
	// target, away from its owner, to require both migration and later cleanup.
	var literal, literalFQN, misplaced string
	for i := range 1000 {
		literal = fs.ShortenFntl(fmt.Sprintf("literal-%d", i))
		owner, path := fntlObjectFQN(t, bp, smap, bck, literal)
		if owner.ID() != source.ID() && literal != shortName {
			literalFQN = path
			break
		}
	}
	tassert.Fatalf(t, literalFQN != "", "could not select a literal .x name")
	fntlPut(t, bp, bck, literal, data)
	misplaced = fntlTargetFQN(t, bp, source, bck, literal)
	tassert.CheckFatal(t, api.ClearLcache(bp, "" /*all targets*/))
	md, err := fs.GetXattr(literalFQN, fs.XattrLOM)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, cos.CreateDir(filepath.Dir(misplaced)))
	_, _, err = cos.CopyFile(literalFQN, misplaced, nil, cos.ChecksumNone)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, fs.SetXattr(misplaced, fs.XattrLOM, md))
	tassert.CheckFatal(t, os.Remove(literalFQN))

	rebID, err := lcStartRebalance(bp)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, rebID)
	snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: rebID, Kind: apc.ActRebalance})
	tassert.CheckFatal(t, err)
	var reported bool
	for _, snap := range snaps[source.ID()] {
		reported = reported || strings.Contains(snap.Err, "unresolved FNTL identity")
	}
	tassert.Errorf(t, reported, "rebalance[%s] did not report the unresolved identity", rebID)
	fntlCheckFile(t, fqn, data)
	fntlCheckFile(t, literalFQN, data)
	_, err = api.HeadObjectV2(bp, bck, shortName, apc.GetPropsSize, api.HeadArgs{FltPresence: apc.FltPresent})
	tassert.Errorf(t, api.HTTPStatus(err) == http.StatusNotFound, "phantom %q: expected 404, got %v", shortName, err)

	// Seed the peer with identical bytes under the physical name. Without the
	// cleanup guard, this peer passes CheckEq and authorizes source removal.
	fntlPut(t, bp, bck, shortName, data)
	args := &xact.ArgsMsg{Kind: apc.ActRebalance, Flags: xact.FlagRemoveMisplaced}
	xid, err := api.StartXaction(bp, args, "")
	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, bp, xid)
	snaps, err = api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActRebalance})
	tassert.CheckFatal(t, err)
	var skipped bool
	for _, snap := range snaps[source.ID()] {
		skipped = skipped || strings.Contains(snap.CtlMsg, "skip-fntl=1")
	}
	tassert.Errorf(t, skipped, "cleanup[%s] did not report skip-fntl=1", xid)
	fntlCheckFile(t, fqn, data)
	tools.CheckPathNotExists(t, misplaced)
	fntlCheckGet(t, bp, bck, objName, data)
	fntlCheckGet(t, bp, bck, literal, data)
}

// ordinary .x names must still be eligible for object and orphan-content cleanup
func TestFntlSpaceCleanupLiteralNames(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	var (
		bp    = tools.BaseAPIParams(proxyURL)
		bck   = cmn.Bck{Name: "cleanup-literal-fntl-" + cos.GenTie(), Provider: apc.AIS}
		names = []string{".xnotes", fs.ShortenFntl("literal-cleanup"), ".x" + strings.Repeat("a", 65)}
		fqns  []string
	)
	tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{
		Chunks: &cmn.ChunksConfToSet{ObjSizeLimit: apc.Ptr(cos.SizeIEC(0))},
	}, true /*cleanup*/)
	initMountpaths(t, proxyURL)
	smap := tools.GetClusterMap(t, proxyURL)
	smap.InitDigests()
	for _, name := range names {
		fntlPut(t, bp, bck, name, nil) // FlagZeroSize makes these reclaimable
		_, fqn := fntlObjectFQN(t, bp, smap, bck, name)
		fqns = append(fqns, fqn)
	}

	// Derived names can be longer than 66 bytes, but their base must have the
	// shortened-name shape. An ordinary .x prefix must not protect old orphans.
	var parsed fs.ParsedFQN
	tassert.CheckFatal(t, parsed.Init(fqns[0]))
	uploadID := cos.GenUUID()
	tassert.CheckFatal(t, cos.ValidateManifestID(uploadID))
	for _, obj := range []struct{ name, ct string }{
		{names[0], fs.ChunkCT}, {names[0], fs.ChunkMetaCT},
		// derived filenames of exactly 66 bytes whose bases are not shortened names:
		// ".x" + base + "." + uploadID [+ ".0001"]
		{".x" + strings.Repeat("c", 66-2-1-len(uploadID)-5), fs.ChunkCT},
		{".x" + strings.Repeat("m", 66-2-1-len(uploadID)), fs.ChunkMetaCT},
	} {
		extras := []string{uploadID}
		if obj.ct == fs.ChunkCT {
			extras = append(extras, "0001")
		}
		fqn := fs.CSM.Gen(obj.name, obj.ct, &bck, parsed.Mountpath, extras...)
		fh, err := cos.CreateFile(fqn)
		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, fh.Close())
		fqns = append(fqns, fqn)
	}
	tassert.CheckFatal(t, api.ClearLcache(bp, "" /*all targets*/))
	old := time.Now().Add(-tools.GetClusterConfig(t).Space.DontCleanupTime.D() - time.Hour)
	for _, fqn := range fqns {
		tassert.CheckFatal(t, os.Chtimes(fqn, old, old))
	}
	args := &xact.ArgsMsg{Kind: apc.ActStoreCleanup, Buckets: []cmn.Bck{bck}, Flags: xact.FlagZeroSize}
	xid, err := api.StartXaction(bp, args, "")
	tassert.CheckFatal(t, err)
	args.ID, args.Timeout = xid, tools.RebalanceTimeout
	tassert.CheckFatal(t, api.WaitForXaction(bp, args))
	for _, fqn := range fqns {
		_, err := os.Stat(fqn)
		tassert.Errorf(t, os.IsNotExist(err), "cleanup did not remove %q: %v", fqn, err)
	}
}

func TestFntlSpaceCleanup(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	var (
		bck = cmn.Bck{Name: "sc-fntl" + cos.GenTie(), Provider: apc.AIS}
		m   = &ioContext{
			t:             t,
			num:           100,
			fileSize:      128,
			fixedSize:     true,
			nameLen:       280,
			bck:           bck,
			getErrIsFatal: true,
			chunksConf:    &ioCtxChunksConf{}, // monolithic
		}
		bp = tools.BaseAPIParams(proxyURL)
	)
	m.initAndSaveState(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	m.puts()

	// older than `space.dont_cleanup_time`
	m.backdateLocalObjs(tools.GetClusterConfig(t).Space.DontCleanupTime.D() + time.Hour)

	args := &xact.ArgsMsg{Kind: apc.ActStoreCleanup, Buckets: []cmn.Bck{bck}}
	xid, err := api.StartXaction(bp, args, "" /*extra*/)
	tassert.CheckFatal(t, err)
	args.ID, args.Timeout = xid, tools.RebalanceTimeout
	tassert.CheckFatal(t, api.WaitForXaction(bp, args))

	snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActStoreCleanup})
	tassert.CheckFatal(t, err)
	for _, tsnaps := range snaps {
		for _, snap := range tsnaps {
			skipped := strings.Contains(snap.CtlMsg, "too-fresh") || strings.Contains(snap.CtlMsg, "recently-accessed")
			tassert.Fatalf(t, !skipped, "cleanup skipped new objects: %s", snap.CtlMsg)
		}
	}

	lst, err := api.ListObjects(bp, bck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == m.num, "listed %d objects, expecting %d", len(lst.Entries), m.num)
	m.gets(nil, false)
}

// preserve a completed FNTL object's chunks and manifest after they age past
// dont_cleanup_time (the monolithic FNTL cleanup test cannot exercise this)
func TestFntlSpaceCleanupChunked(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	const chunkSize = int64(cmn.ChunkSizeMin)
	var (
		bp      = tools.BaseAPIParams(proxyURL)
		bck     = cmn.Bck{Name: "cleanup-chunked-fntl-" + cos.GenTie(), Provider: apc.AIS}
		objName = "dir/" + strings.Repeat("c", 280)
		data    = bytes.Repeat([]byte("c"), int(3*chunkSize))
	)
	tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{Chunks: &cmn.ChunksConfToSet{
		ObjSizeLimit: apc.Ptr(cos.SizeIEC(chunkSize)), ChunkSize: apc.Ptr(cos.SizeIEC(chunkSize)),
	}}, true /*cleanup*/)
	initMountpaths(t, proxyURL)
	fntlPut(t, bp, bck, objName, data)
	checkObjectChunked(t, bp, bck, objName, chunkSize)
	fntlCheckGet(t, bp, bck, objName, data)

	var (
		fqns   []string
		counts = make(map[string]int)
		old    = time.Now().Add(-tools.GetClusterConfig(t).Space.DontCleanupTime.D() - time.Hour)
	)
	err := fs.WalkBck(&fs.WalkBckOpts{WalkOpts: fs.WalkOpts{
		Bck: bck, CTs: []string{fs.ObjCT, fs.ChunkCT, fs.ChunkMetaCT}, Sorted: true,
		Callback: func(fqn string, de fs.DirEntry) error {
			if de.IsDir() {
				return nil
			}
			var parsed fs.ParsedFQN
			if err := parsed.Init(fqn); err != nil {
				return err
			}
			counts[parsed.ContentType]++
			fqns = append(fqns, fqn)
			return os.Chtimes(fqn, old, old)
		},
	}})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, counts[fs.ObjCT] == 1 && counts[fs.ChunkCT] == 2 && counts[fs.ChunkMetaCT] == 1,
		"expected one object, two chunks and one completed manifest, got %v", counts)
	tlog.Logfln("Backdated %d FNTL object/chunk/manifest files", len(fqns))
	args := &xact.ArgsMsg{Kind: apc.ActStoreCleanup, Buckets: []cmn.Bck{bck}}
	xid, err := api.StartXaction(bp, args, "")
	tassert.CheckFatal(t, err)
	args.ID, args.Timeout = xid, tools.RebalanceTimeout
	tassert.CheckFatal(t, api.WaitForXaction(bp, args))
	snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActStoreCleanup})
	tassert.CheckFatal(t, err)
	var skipped bool
	for _, targetSnaps := range snaps {
		for _, snap := range targetSnaps {
			tassert.Errorf(t, !strings.Contains(snap.CtlMsg, "too-fresh:"), "cleanup did not examine aged files: %s", snap.CtlMsg)
			skipped = skipped || strings.Contains(snap.CtlMsg, "skip-fntl:")
		}
	}
	tassert.Errorf(t, skipped, "cleanup[%s] did not report skip-fntl", xid)
	for _, fqn := range fqns {
		tools.CheckPathExists(t, fqn, false /*dir*/)
	}
	fntlCheckGet(t, bp, bck, objName, data)
}

func fntlObjectFQN(t *testing.T, bp api.BaseParams, smap *meta.Smap, bck cmn.Bck, name string) (*meta.Snode, string) {
	t.Helper()
	tsi, err := smap.HrwObjName2T(meta.CloneBck(&bck), name)
	tassert.CheckFatal(t, err)
	return tsi, fntlTargetFQN(t, bp, tsi, bck, name)
}

func fntlTargetFQN(t *testing.T, bp api.BaseParams, tsi *meta.Snode, bck cmn.Bck, name string) string {
	t.Helper()
	mpl, err := api.GetMountpaths(bp, tsi)
	tassert.CheckFatal(t, err)
	avail, local := fs.GetAvail(), make(fs.MPI, len(mpl.Available))
	for _, path := range mpl.Available {
		local[path] = avail[path]
	}
	mi, _, err := local.Hrw(bck.MakeUname(name))
	tassert.CheckFatal(t, err)
	return mi.MakePathFQN(&bck, fs.ObjCT, name)
}

func fntlPut(t *testing.T, bp api.BaseParams, bck cmn.Bck, name string, data []byte) {
	t.Helper()
	_, err := api.PutObject(&api.PutArgs{BaseParams: bp, Bck: bck, ObjName: name, Reader: readers.NewBytes(data), Size: uint64(len(data))})
	tassert.CheckFatal(t, err)
}

func fntlCheckFile(t *testing.T, fqn string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(fqn)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, bytes.Equal(got, want), "content changed: %s", fqn)
}

func fntlCheckGet(t *testing.T, bp api.BaseParams, bck cmn.Bck, name string, want []byte) {
	t.Helper()
	var got bytes.Buffer
	_, err := api.GetObjectWithValidation(bp, bck, name, &api.GetArgs{Writer: &got})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, bytes.Equal(got.Bytes(), want), "GET %s returned different content", bck.Cname(name))
}

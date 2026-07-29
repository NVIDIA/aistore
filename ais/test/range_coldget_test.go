// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

// feat.RangeColdGET (see ais/tgtfrange.go): a ranged GET of a not-cached object is
// served directly from the remote backend, and the grid-aligned chunks covering the
// requested range get cached; once the grid is fully covered the object becomes
// a regular (chunked) in-cluster object.
//
// NOTE: using a remote AIS cluster ("remais") and out-of-band PUTs, so that the
// objects in question are, initially, not present in this cluster.

const (
	rcGridSize = 8 * cos.MiB            // must be in sync w/ rcChunkSize (ais/tgtfrange.go)
	rcObjSize  = 2*rcGridSize + cos.MiB // 3 grid chunks: two full and one partial
)

// accumulate the grid, one range read at a time
func TestRangeColdGet(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = rcBucket(t, proxyURL)
		objName    = "range-cold-get-" + trand.String(5)
		src        = rcPutRemote(t, bck, objName)
		m          = &ioContext{t: t, bck: bck}
	)
	initMountpaths(t, proxyURL)
	rcCheckPresent(t, proxyURL, bck, objName, false)

	tlog.Logln("1. read the first chunk")
	rcGet(t, baseParams, bck, objName, cmn.MakeRangeHdr(0, cos.KiB), src[:cos.KiB])
	rcCheckPresent(t, proxyURL, bck, objName, false)
	m.validateChunksOnDisk(bck, objName, -1) // at least one chunk file

	tlog.Logln("2. read the last (partial) chunk - out of order")
	rcGet(t, baseParams, bck, objName, cmn.MakeRangeHdr(2*rcGridSize, cos.MiB), src[2*rcGridSize:])
	rcCheckPresent(t, proxyURL, bck, objName, false)

	tlog.Logln("3. read the remaining gap: unaligned range across the cached chunk #1 and the missing chunk #2")
	rcGet(t, baseParams, bck, objName, cmn.MakeRangeHdr(rcGridSize-10, 20), src[rcGridSize-10:rcGridSize+10])

	tlog.Logln("4. the grid is now fully covered: expecting a chunked in-cluster object")
	rcCheckPresent(t, proxyURL, bck, objName, true)
	rcCheckChunked(t, baseParams, bck, objName)
	m.validateChunksOnDisk(bck, objName, 3)
	rcCheckWhole(t, baseParams, bck, objName, src)

	tlog.Logln("5. warm range read")
	rcGet(t, baseParams, bck, objName, cmn.MakeRangeHdr(rcGridSize/2, rcGridSize), src[rcGridSize/2:rcGridSize/2+rcGridSize])
}

// a single unaligned range covering the entire grid: one backend read that gets
// split across the grid chunks
func TestRangeColdGetFullSpan(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = rcBucket(t, proxyURL)
		objName    = "range-cold-get-span-" + trand.String(5)
		src        = rcPutRemote(t, bck, objName)
	)
	initMountpaths(t, proxyURL)

	rcGet(t, baseParams, bck, objName, cmn.MakeRangeHdr(100, rcObjSize-200), src[100:rcObjSize-100])

	rcCheckPresent(t, proxyURL, bck, objName, true)
	rcCheckChunked(t, baseParams, bck, objName)
	rcCheckWhole(t, baseParams, bck, objName, src)
}

// suffix and open-ended ranges: both require the object size, which the target
// resolves by HEAD-ing the remote object
func TestRangeColdGetOpenEnded(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = rcBucket(t, proxyURL)
	)
	tests := []struct {
		name     string
		rng      string
		from, to int64
	}{
		{name: "suffix", rng: "bytes=-100", from: rcObjSize - 100, to: rcObjSize},
		{name: "open-ended", rng: "bytes=" + strconv.FormatInt(rcObjSize-cos.MiB, 10) + "-", from: rcObjSize - cos.MiB, to: rcObjSize},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objName := "range-cold-get-" + test.name + "-" + trand.String(5)
			src := rcPutRemote(t, bck, objName)
			rcGet(t, baseParams, bck, objName, test.rng, src[test.from:test.to])

			// both ranges above are contained in the last grid chunk
			rcCheckPresent(t, proxyURL, bck, objName, false)
		})
	}

	t.Run("out-of-range", func(t *testing.T) {
		objName := "range-cold-get-oor-" + trand.String(5)
		rcPutRemote(t, bck, objName)

		hdr := http.Header{cos.HdrRange: []string{"bytes=" + strconv.FormatInt(rcObjSize+1, 10) + "-"}}
		_, err := api.GetObject(baseParams, bck, objName, &api.GetArgs{Header: hdr})
		tassert.Fatalf(t, err != nil, "expected an out-of-range error")
		herr := cmn.AsErrHTTP(err)
		tassert.Errorf(t, herr != nil && herr.Status == http.StatusRequestedRangeNotSatisfiable,
			"expected %d, got %v", http.StatusRequestedRangeNotSatisfiable, err)
	})
}

// concurrent range reads of a not-cached object: at most one of them fills the cache,
// the rest get served from the backend and/or the already accumulated chunks
func TestRangeColdGetConcurrent(t *testing.T) {
	const numReads = 8
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = rcBucket(t, proxyURL)
		objName    = "range-cold-get-conc-" + trand.String(5)
		src        = rcPutRemote(t, bck, objName)
		length     = int64(rcObjSize / numReads)
		wg         sync.WaitGroup
		errCh      = make(chan error, numReads)
	)
	initMountpaths(t, proxyURL)

	for i := range numReads {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var (
				off  = int64(i) * length
				w    = bytes.NewBuffer(nil)
				hdr  = http.Header{cos.HdrRange: []string{cmn.MakeRangeHdr(off, length)}}
				args = api.GetArgs{Writer: w, Header: hdr}
			)
			if _, err := api.GetObject(baseParams, bck, objName, &args); err != nil {
				errCh <- err
			} else if !bytes.Equal(w.Bytes(), src[off:off+length]) {
				errCh <- fmt.Errorf("range %d: payload differs from the source", i)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// the accumulated chunks (if any) must not stand in the way of caching the object
	rcGet(t, baseParams, bck, objName, cmn.MakeRangeHdr(0, rcObjSize), src)
	rcCheckPresent(t, proxyURL, bck, objName, true)
	rcCheckWhole(t, baseParams, bck, objName, src)
}

//
// helpers
//

// remais bucket with the feature enabled (bucket scope)
func rcBucket(t *testing.T, proxyURL string) cmn.Bck {
	t.Helper()
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})

	bck := cmn.Bck{Name: trand.String(10), Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.UUID}}
	tools.CreateBucket(t, proxyURL, bck, nil /*props*/, true /*cleanup*/)

	// NOTE: remote buckets ignore the props passed at creation time
	props := &cmn.BpropsToSet{Features: apc.Ptr(feat.RangeColdGET)}
	_, err := api.SetBucketProps(tools.BaseAPIParams(proxyURL), bck, props)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(tools.BaseAPIParams(proxyURL), bck, true /*dontAddRemote*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, p.Features.IsSet(feat.RangeColdGET), "%s: %s is not set", bck.String(), feat.RangeColdGET.Names())
	return bck
}

// out-of-band PUT (directly into the remote cluster); return the source bytes
func rcPutRemote(t *testing.T, bck cmn.Bck, objName string) []byte {
	t.Helper()
	const size = int64(rcObjSize)
	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)

	_, err = api.PutObject(&api.PutArgs{
		BaseParams: tools.BaseAPIParams(tools.RemoteCluster.URL),
		Bck:        cmn.Bck{Name: bck.Name, Provider: apc.AIS}, // remote cluster: no namespace
		ObjName:    objName,
		Reader:     reader,
		Size:       uint64(size),
	})
	tassert.CheckFatal(t, err)

	rd, err := reader.Open()
	tassert.CheckFatal(t, err)
	src, err := io.ReadAll(rd)
	cos.Close(rd)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, int64(len(src)) == size, "expected %d source bytes, got %d", size, len(src))
	return src
}

// ranged GET: validate the payload and the 206 response headers
func rcGet(t *testing.T, bp api.BaseParams, bck cmn.Bck, objName, rng string, expected []byte) {
	t.Helper()
	var (
		w    = bytes.NewBuffer(nil)
		hdr  = http.Header{cos.HdrRange: []string{rng}}
		args = api.GetArgs{Writer: w, Header: hdr}
	)
	oah, err := api.GetObject(bp, bck, objName, &args)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, oah.Size() == int64(len(expected)), "%s: expected %d bytes, got %d", rng, len(expected), oah.Size())
	tassert.Fatalf(t, bytes.Equal(w.Bytes(), expected), "%s: payload differs from the source", rng)

	respHdr := oah.RespHeader()
	tassert.Errorf(t, respHdr.Get(cos.HdrAcceptRanges) == "bytes", "%s: %q is not set", rng, cos.HdrAcceptRanges)
	contentRange := respHdr.Get(cos.HdrContentRange)
	tassert.Errorf(t, strings.HasSuffix(contentRange, "/"+strconv.FormatInt(rcObjSize, 10)),
		"%s: unexpected %q: %q", rng, cos.HdrContentRange, contentRange)
}

func rcCheckPresent(t *testing.T, proxyURL string, bck cmn.Bck, objName string, expected bool) {
	t.Helper()
	present := tools.CheckObjIsPresent(proxyURL, bck, objName)
	tassert.Fatalf(t, present == expected, "%s: expected present=%t", bck.Cname(objName), expected)
}

func rcCheckChunked(t *testing.T, bp api.BaseParams, bck cmn.Bck, objName string) {
	t.Helper()
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsChunked, Prefix: objName, Flags: apc.LsCached}
	lst, err := api.ListObjects(bp, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == 1, "expected 1 cached object, got %d", len(lst.Entries))
	tassert.Errorf(t, lst.Entries[0].Flags&apc.EntryIsChunked != 0, "%s must be chunked", objName)
}

func rcCheckWhole(t *testing.T, bp api.BaseParams, bck cmn.Bck, objName string, src []byte) {
	t.Helper()
	r, size, err := api.GetObjectReader(bp, bck, objName, nil /*args*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, size == int64(len(src)), "expected %d bytes, got %d", len(src), size)
	equal := tools.ReaderEqual(bytes.NewReader(src), r)
	cos.Close(r)
	tassert.Fatalf(t, equal, "%s: the cached object differs from the source", objName)
}

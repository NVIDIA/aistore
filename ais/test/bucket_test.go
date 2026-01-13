// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"

	"golang.org/x/sync/errgroup"
)

var (
	fltPresentEnum = []int{apc.FltExists, apc.FltExistsNoProps, apc.FltPresent, apc.FltExistsOutside}
	fltPresentText = map[int]string{
		apc.FltExists:        "flt-exists",
		apc.FltExistsNoProps: "flt-exists-no-props",
		apc.FltPresent:       "flt-present",
		apc.FltExistsOutside: "flt-exists-outside",
	}
)

func TestHTTPProviderBucket(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: apc.HT,
		}
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)

	err := api.CreateBucket(bp, bck, nil)
	tassert.Fatalf(t, err != nil, "expected error")

	_, err = api.GetObject(bp, bck, "nonexisting", nil)
	tassert.Fatalf(t, err != nil, "expected error")

	_, err = api.ListObjects(bp, bck, nil, api.ListArgs{})
	tassert.Fatalf(t, err != nil, "expected error")

	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: cos.KiB, CksumType: cos.ChecksumNone})
	tassert.CheckError(t, err)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: bp,
		Bck:        bck,
		ObjName:    "something",
		Reader:     reader,
	})
	tassert.Fatalf(t, err != nil, "expected error")
}

func TestListBuckets(t *testing.T) {
	var (
		bck      = cmn.Bck{Name: t.Name() + "Bucket", Provider: apc.AIS, Ns: genBucketNs()}
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		pnums    = make(map[string]cmn.Bcks)
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	bcks, err := api.ListBuckets(bp, cmn.QueryBcks{}, apc.FltExists)
	tassert.CheckFatal(t, err)

	for provider := range apc.Providers {
		qbck := cmn.QueryBcks{Provider: provider}
		bcks := bcks.Select(qbck)
		tlog.Logfln("%s:\t%2d bucket%s", apc.ToScheme(provider), len(bcks), cos.Plural(len(bcks)))
		pnums[provider] = bcks
	}

	backends, err := api.GetConfiguredBackends(bp)
	tassert.CheckFatal(t, err)
	tlog.Logfln("configured backends: %v", backends)

	// tests: vs configured backend vs count
	for provider := range apc.Providers {
		configured := slices.Contains(backends, provider)
		qbck := cmn.QueryBcks{Provider: provider}
		bcks, err := api.ListBuckets(bp, qbck, apc.FltExists)
		if err != nil {
			if !configured {
				continue
			}
			tassert.CheckError(t, err)
		} else if apc.IsCloudProvider(provider) && !configured {
			t.Fatalf("%s is not configured: expecting list-buckets to fail, got %v\n", provider, bcks)
		}
		if num := len(bcks.Select(qbck)); len(bcks) != num {
			t.Fatalf("%s: num buckets(1): %d != %d\n", provider, len(bcks), num)
		}
		if len(bcks) != len(pnums[provider]) {
			t.Fatalf("%s: num buckets(2): %d != %d\n", provider, len(bcks), len(pnums[provider]))
		}
	}

	// tests: vs present vs exist-outside, etc.

	for _, provider := range backends {
		qbck := cmn.QueryBcks{Provider: provider}
		presbcks, err := api.ListBuckets(bp, qbck, apc.FltPresent)
		tassert.CheckFatal(t, err)
		if qbck.Provider == apc.AIS {
			tassert.Fatalf(t, len(presbcks) > 0, "at least one ais bucket must be present")
			continue
		}
		// making it present if need be
		if len(presbcks) == 0 {
			if len(pnums[provider]) == 0 {
				continue
			}
			bcks, i := pnums[provider], 0
			if len(bcks) > 1 {
				i = rand.IntN(len(bcks))
			}
			pbck := bcks[i]
			tlog.Logfln("lookup and add '%s'", pbck.String())
			_, err := api.HeadBucket(bp, pbck, false /* don't add */)
			if err != nil {
				// TODO: extend api.HeadBucket to return status as well(?)
				if _, ok := err.(*cmn.ErrHTTP); ok && cos.IsErrNotFound(err) {
					tlog.Logfln("Warning: cannot HEAD(%s): not permitted(?)", pbck.String())
					continue
				}
			}
			tassert.CheckFatal(t, err)

			presbcks, err = api.ListBuckets(bp, qbck, apc.FltPresent)
			tassert.CheckFatal(t, err)

			tlog.Logfln("bucket %s is now in BMD", pbck.String())
			t.Cleanup(func() {
				err = api.EvictRemoteBucket(bp, pbck, false /*keep md*/)
				tassert.CheckFatal(t, err)
				tlog.Logfln("[cleanup] %s evicted", pbck.String())
			})
		}

		b := presbcks[0]
		err = api.EvictRemoteBucket(bp, b, false /*keep md*/)
		tassert.CheckFatal(t, err)

		evbcks, err := api.ListBuckets(bp, qbck, apc.FltPresent)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(presbcks) == len(evbcks)+1, "%s: expected one bucket less present after evicting %s (%d, %d)",
			provider, b.String(), len(presbcks), len(evbcks))

		outbcks, err := api.ListBuckets(bp, qbck, apc.FltExistsOutside)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(outbcks) > 0, "%s: expected at least one (evicted) bucket to \"exist outside\"", provider)

		allbcks, err := api.ListBuckets(bp, qbck, apc.FltExistsNoProps)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(allbcks) == len(outbcks)+len(presbcks)-1,
			"%s: expected present + outside == all (%d, %d, %d)", provider, len(presbcks)-1, len(outbcks), len(allbcks))

		_, err = api.HeadBucket(bp, b, false /* don't add */)
		tassert.CheckFatal(t, err)
		presbcks2, err := api.ListBuckets(bp, qbck, apc.FltPresentNoProps)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(presbcks2) == len(presbcks), "%s: expected num present back to original (%d, %d)",
			provider, len(presbcks2), len(presbcks))
	}

	// tests: NsGlobal
	qbck := cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsGlobal}
	aisBuckets, err := api.ListBuckets(bp, qbck, apc.FltExists)
	tassert.CheckError(t, err)
	if len(aisBuckets) != len(bcks.Select(qbck)) {
		t.Fatalf("ais buckets: %d != %d\n", len(aisBuckets), len(bcks.Select(qbck)))
	}

	// tests: NsAnyRemote
	qbck = cmn.QueryBcks{Ns: cmn.NsAnyRemote}
	bcks, err = api.ListBuckets(bp, qbck, apc.FltExists)
	tassert.CheckError(t, err)
	qbck = cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}
	aisBuckets, err = api.ListBuckets(bp, qbck, apc.FltExists)
	tassert.CheckError(t, err)
	if len(aisBuckets) != len(bcks.Select(qbck)) {
		t.Fatalf("ais buckets: %d != %d\n", len(aisBuckets), len(bcks.Select(qbck)))
	}
}

// NOTE: for remote bucket, enter the bucket name directly (as TestMain makes it "present" at init time)
func TestGetBucketInfo(t *testing.T) {
	var (
		proxyURL  = tools.RandomProxyURL(t)
		bp        = tools.BaseAPIParams(proxyURL)
		bck       = cliBck
		isPresent bool
	)
	if bck.IsRemote() {
		_, _, _, err := api.GetBucketInfo(bp, bck, &api.BinfoArgs{FltPresence: apc.FltPresent})
		isPresent = err == nil
	}
	for _, fltPresence := range fltPresentEnum {
		text := fltPresentText[fltPresence]
		tlog.Logfln("%q %s", text, strings.Repeat("-", 60-len(text)))
		args := api.BinfoArgs{
			UUID:        "",
			FltPresence: fltPresence,
		}

		if apc.IsFltNoProps(fltPresence) {
			// (fast path)
		} else {
			args.Summarize = true
			args.WithRemote = bck.IsRemote()
		}

		xid, props, info, err := api.GetBucketInfo(bp, bck, &args)
		if err != nil {
			if herr := cmn.Str2HTTPErr(err.Error()); herr != nil {
				tlog.Logln(herr.TypeCode + ": " + herr.Message)
			} else {
				tlog.Logln(err.Error())
			}
		} else {
			ps := "bucket-props = nil"
			if props != nil {
				ps = fmt.Sprintf("bucket-props(mirror) %+v", props.Mirror)
			}
			tlog.Logfln("%s: %s", bck.Cname(""), ps)

			is := "bucket-summary = nil"
			if info != nil {
				is = fmt.Sprintf("bucket-summary %+v", info.BsummResult)
			}
			tlog.Logfln("x-%s[%s] %s: %s", apc.ActSummaryBck, xid, bck.Cname(""), is)
		}
		if bck.IsRemote() && !isPresent {
			// undo the side effect of calling api.GetBucketInfo
			_ = api.EvictRemoteBucket(bp, bck, false)
		}
		tlog.Logln("")
	}
	if bck.IsRemote() {
		_, _, _, err := api.GetBucketInfo(bp, bck, &api.BinfoArgs{FltPresence: apc.FltPresent})
		isPresentEnd := err == nil
		tassert.Errorf(t, isPresent == isPresentEnd, "presence in the beginning (%t) != (%t) at the end",
			isPresent, isPresentEnd)
	}
}

func TestDefaultBucketProps(t *testing.T) {
	const dataSlices = 7
	var (
		proxyURL     = tools.RandomProxyURL(t)
		bp           = tools.BaseAPIParams(proxyURL)
		globalConfig = tools.GetClusterConfig(t)
		bck          = cmn.Bck{Name: testBucketName, Provider: apc.AIS, Ns: genBucketNs()}
	)
	tools.SetClusterConfig(t, cos.StrKVs{
		"ec.enabled":     "true",
		"ec.data_slices": strconv.FormatUint(dataSlices, 10),
	})
	defer tools.SetClusterConfig(t, cos.StrKVs{
		"ec.enabled":       "false",
		"ec.data_slices":   strconv.Itoa(globalConfig.EC.DataSlices),
		"ec.parity_slices": strconv.Itoa(globalConfig.EC.ParitySlices),
	})

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	p, err := api.HeadBucket(bp, bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if !p.EC.Enabled {
		t.Error("EC should be enabled for ais buckets")
	}
	if p.EC.DataSlices != dataSlices {
		t.Errorf("Invalid number of EC data slices: expected %d, got %d", dataSlices, p.EC.DataSlices)
	}
}

func TestCreateWithBucketProps(t *testing.T) {
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		bck      = cmn.Bck{Name: testBucketName, Provider: apc.AIS, Ns: genBucketNs()}
	)
	propsToSet := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{
			Type:            apc.Ptr(cos.ChecksumMD5),
			ValidateWarmGet: apc.Ptr(true),
			EnableReadRange: apc.Ptr(true),
			ValidateColdGet: apc.Ptr(false),
			ValidateObjMove: apc.Ptr(true),
		},
		WritePolicy: &cmn.WritePolicyConfToSet{
			Data: apc.Ptr(apc.WriteImmediate),
			MD:   apc.Ptr(apc.WriteNever),
		},
	}
	tools.CreateBucket(t, proxyURL, bck, propsToSet, true /*cleanup*/)

	p, err := api.HeadBucket(bp, bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, propsToSet, p)
}

func TestCreateRemoteBucket(t *testing.T) {
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		bck      = cliBck
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})
	exists, _ := tools.BucketExists(nil, tools.GetPrimaryURL(), bck)
	tests := []struct {
		bck    cmn.Bck
		props  *cmn.BpropsToSet
		exists bool
	}{
		{bck: bck, exists: exists},
		{bck: cmn.Bck{Provider: cliBck.Provider, Name: trand.String(10), Ns: genBucketNs()}},
	}
	for _, test := range tests {
		err := api.CreateBucket(bp, test.bck, test.props)
		if err == nil {
			continue
		}
		herr := cmn.AsErrHTTP(err)
		tassert.Fatalf(t, herr != nil, "expected ErrHTTP, got %v (bucket %q)", err, test.bck.String())
		if test.exists {
			tassert.Fatalf(t, strings.Contains(herr.Message, "already exists"),
				"expecting \"already exists\", got %+v", herr)
		} else {
			tassert.Fatalf(t, herr.Status == http.StatusNotImplemented || strings.Contains(herr.Message, "support"),
				"expecting 501 status or unsupported, got %+v", herr)
		}
	}
}

func TestCreateDestroyRemoteAISBucket(t *testing.T) {
	t.Run("withObjects", func(t *testing.T) { testCreateDestroyRemoteAISBucket(t, true) })
	t.Run("withoutObjects", func(t *testing.T) { testCreateDestroyRemoteAISBucket(t, false) })
}

func testCreateDestroyRemoteAISBucket(t *testing.T, withObjects bool) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
	bck := cmn.Bck{
		Name:     trand.String(10),
		Provider: apc.AIS,
		Ns: cmn.Ns{
			UUID: tools.RemoteCluster.UUID,
		},
	}
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	_, err := api.HeadBucket(bp, bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if withObjects {
		m := ioContext{
			t:         t,
			num:       1000,
			fileSize:  cos.KiB,
			fixedSize: true,
			bck:       bck,
		}
		m.init(true /*cleanup*/)
		m.puts()
	}

	err = api.DestroyBucket(bp, bck)
	tassert.CheckFatal(t, err)

	tlog.Logfln("%s destroyed", bck.Cname(""))
	bcks, err := api.ListBuckets(bp, cmn.QueryBcks(bck), apc.FltExists)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !tools.BucketsContain(bcks, cmn.QueryBcks(bck)), "expected bucket to not be listed")
}

func TestOverwriteLomCache(t *testing.T) {
	for _, mdwrite := range []apc.WritePolicy{apc.WriteImmediate, apc.WriteNever} {
		name := string(mdwrite)
		if name == "" {
			name = "write-immediate"
		} else {
			name = "write-" + name
		}
		t.Run(name, func(t *testing.T) {
			overwriteLomCache(mdwrite, t)
		})
	}
}

func overwriteLomCache(mdwrite apc.WritePolicy, t *testing.T) {
	var (
		m = ioContext{
			t:         t,
			num:       234,
			fileSize:  73,
			fixedSize: true,
			prefix:    trand.String(6) + "-",
		}
		bp = tools.BaseAPIParams()
	)
	if testing.Short() {
		m.num = 50
	}
	m.init(true /*cleanup*/)
	m.smap = tools.GetClusterMap(m.t, m.proxyURL)

	for _, target := range m.smap.Tmap.ActiveNodes() {
		mpList, err := api.GetMountpaths(bp, target)
		tassert.CheckFatal(t, err)
		l := len(mpList.Available)
		tassert.Fatalf(t, l >= 2, "%s has %d mountpaths, need at least 2", target, l)
	}
	tlog.Logfln("Create %s(mirrored, write-policy-md=%s)", m.bck.String(), mdwrite)
	propsToSet := &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true)},
		WritePolicy: &cmn.WritePolicyConfToSet{
			Data: apc.Ptr(apc.WriteImmediate),
			MD:   apc.Ptr(mdwrite),
		},
	}
	tools.CreateBucket(t, m.proxyURL, m.bck, propsToSet, true /*cleanup*/)

	m.puts()

	// NOTE: not waiting here for apc.ActPutCopies

	tlog.Logfln("List %s", m.bck.String())
	msg := &apc.LsoMsg{Props: apc.GetPropsName}
	lst, err := api.ListObjects(bp, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "expecting %d entries, have %d",
		m.num, len(lst.Entries))

	tlog.Logfln("Overwrite %s objects with newer versions", m.bck.String())
	nsize := int64(m.fileSize) * 10
	for _, en := range lst.Entries {
		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: nsize, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: bp,
			Bck:        m.bck,
			ObjName:    en.Name,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)
	}
	// wait for pending writes (of the copies)
	args := xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: m.bck}
	api.WaitForSnapsIdle(bp, &args)

	tlog.Logfln("List %s new versions", m.bck.String())
	msg = &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsAll...)
	lst, err = api.ListObjects(bp, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "expecting %d entries, have %d",
		m.num, len(lst.Entries))

	for _, en := range lst.Entries {
		n, s, c := en.Name, en.Size, en.Copies
		tassert.Fatalf(t, s == nsize, "%s: expecting size = %d, got %d", n, nsize, s)
		tassert.Fatalf(t, c == 2, "%s: expecting copies = %d, got %d", n, 2, c)
	}
}

func TestStressCreateDestroyBucket(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	const (
		bckCount  = 10
		iterCount = 20
	)

	var (
		bp       = tools.BaseAPIParams()
		group, _ = errgroup.WithContext(t.Context())
	)

	for range bckCount {
		group.Go(func() error {
			m := &ioContext{
				t:      t,
				num:    100,
				silent: true,
			}

			m.init(true /*cleanup*/)

			for range iterCount {
				if err := api.CreateBucket(bp, m.bck, nil); err != nil {
					return err
				}
				if rand.IntN(iterCount) == 0 { // just test couple times, no need to flood
					if err := api.CreateBucket(bp, m.bck, nil); err == nil {
						return fmt.Errorf("expected error to occur on bucket %q - create second time", m.bck.String())
					}
				}
				m.puts()
				if _, err := api.ListObjects(bp, m.bck, nil, api.ListArgs{}); err != nil {
					return err
				}
				m.gets(nil, false)
				if err := api.DestroyBucket(bp, m.bck); err != nil {
					return err
				}
				if rand.IntN(iterCount) == 0 { // just test couple times, no need to flood
					if err := api.DestroyBucket(bp, m.bck); err == nil {
						return fmt.Errorf("expected error to occur on bucket %q - destroy second time", m.bck.String())
					}
				}
			}
			return nil
		})
	}
	err := group.Wait()
	tassert.CheckFatal(t, err)
}

func TestResetBucketProps(t *testing.T) {
	var (
		proxyURL     = tools.RandomProxyURL(t)
		globalConfig = tools.GetClusterConfig(t)
		bp           = tools.BaseAPIParams(proxyURL)
		bck          = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
			Ns:       genBucketNs(),
		}
		propsToSet = &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(cos.ChecksumNone),
				ValidateWarmGet: apc.Ptr(true),
				EnableReadRange: apc.Ptr(true),
			},
			EC: &cmn.ECConfToSet{
				Enabled:      apc.Ptr(false),
				DataSlices:   apc.Ptr(1),
				ParitySlices: apc.Ptr(2),
			},
		}
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{
		MinTargets: *propsToSet.EC.DataSlices + *propsToSet.EC.ParitySlices,
	})

	tools.SetClusterConfig(t, cos.StrKVs{"ec.enabled": "true"})
	defer tools.SetClusterConfig(t, cos.StrKVs{
		"ec.enabled":       "false",
		"ec.data_slices":   strconv.Itoa(globalConfig.EC.DataSlices),
		"ec.parity_slices": strconv.Itoa(globalConfig.EC.ParitySlices),
	})

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	defaultProps, err := api.HeadBucket(bp, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	_, err = api.SetBucketProps(bp, bck, propsToSet)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(bp, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	// check that bucket props do get set
	validateBucketProps(t, propsToSet, p)
	_, err = api.ResetBucketProps(bp, bck)
	tassert.CheckFatal(t, err)

	p, err = api.HeadBucket(bp, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	if !p.Equal(defaultProps) {
		t.Errorf("props have not been reset properly: expected: %+v, got: %+v", defaultProps, p)
	}
}

func TestSetInvalidBucketProps(t *testing.T) {
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		bck      = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
			Ns:       genBucketNs(),
		}

		tests = []struct {
			name  string
			props *cmn.BpropsToSet
		}{
			{
				name: "humongous number of copies",
				props: &cmn.BpropsToSet{
					Mirror: &cmn.MirrorConfToSet{
						Enabled: apc.Ptr(true),
						Copies:  apc.Ptr[int64](120),
					},
				},
			},
			{
				name: "too many copies",
				props: &cmn.BpropsToSet{
					Mirror: &cmn.MirrorConfToSet{
						Enabled: apc.Ptr(true),
						Copies:  apc.Ptr[int64](12),
					},
				},
			},
			{
				name: "humongous number of slices",
				props: &cmn.BpropsToSet{
					EC: &cmn.ECConfToSet{
						Enabled:      apc.Ptr(true),
						ParitySlices: apc.Ptr(120),
					},
				},
			},
			{
				name: "too many slices",
				props: &cmn.BpropsToSet{
					EC: &cmn.ECConfToSet{
						Enabled:      apc.Ptr(true),
						ParitySlices: apc.Ptr(12),
					},
				},
			},
			{
				name: "enable both ec and mirroring",
				props: &cmn.BpropsToSet{
					EC:     &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
					Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true)},
				},
			},
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := api.SetBucketProps(bp, bck, test.props)
			if err == nil {
				t.Error("expected error when setting bad input")
			}
		})
	}
}

func TestBucketSingleProp(t *testing.T) {
	const (
		dataSlices   = 1
		paritySlices = 1
		objLimit     = 300 * cos.KiB
		burst        = 15
	)
	var (
		m = ioContext{
			t: t,
		}
		bp = tools.BaseAPIParams()
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(3)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	tlog.Logfln("Changing bucket %q properties...", m.bck.String())

	// Enabling EC should set default value for number of slices if it is 0
	_, err := api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckError(t, err)
	p, err := api.HeadBucket(bp, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if !p.EC.Enabled {
		t.Error("EC was not enabled")
	}
	if p.EC.DataSlices != 1 {
		t.Errorf("Number of data slices is incorrect: %d (expected 1)", p.EC.DataSlices)
	}
	if p.EC.ParitySlices != 1 {
		t.Errorf("Number of parity slices is incorrect: %d (expected 1)", p.EC.ParitySlices)
	}

	// Need to disable EC first
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)

	// Enabling mirroring should set default value for number of copies if it is 0
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(bp, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if !p.Mirror.Enabled {
		t.Error("Mirroring was not enabled")
	}
	if p.Mirror.Copies != 2 {
		t.Errorf("Number of copies is incorrect: %d (expected 2)", p.Mirror.Copies)
	}

	// Need to disable mirroring first
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)

	// Change a few more bucket properties
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{
			DataSlices:   apc.Ptr(dataSlices),
			ParitySlices: apc.Ptr(paritySlices),
			ObjSizeLimit: apc.Ptr[int64](objLimit),
		},
	})
	tassert.CheckError(t, err)

	// Enable EC again
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(bp, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if p.EC.DataSlices != dataSlices {
		t.Errorf("Number of data slices was not changed to %d. Current value %d", dataSlices, p.EC.DataSlices)
	}
	if p.EC.ParitySlices != paritySlices {
		t.Errorf("Number of parity slices was not changed to %d. Current value %d", paritySlices, p.EC.ParitySlices)
	}
	if p.EC.ObjSizeLimit != objLimit {
		t.Errorf("Minimal EC object size was not changed to %d. Current value %d", objLimit, p.EC.ObjSizeLimit)
	}

	// Need to disable EC first
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)

	// Change mirroring threshold
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Burst: apc.Ptr(burst)},
	},
	)
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(bp, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if p.Mirror.Burst != burst {
		t.Errorf("Mirror burst was not changed to %d. Current value %d", burst, p.Mirror.Burst)
	}

	// Disable mirroring
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)
}

func TestSetBucketPropsOfNonexistentBucket(t *testing.T) {
	bp := tools.BaseAPIParams()
	bucket, err := tools.GenerateNonexistentBucketName(t.Name()+"Bucket", bp)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cliBck.Provider,
	}

	_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
	})
	if err == nil {
		t.Fatal("Expected SetBucketProps error, but got none.")
	}

	status := api.HTTPStatus(err)
	if status < http.StatusBadRequest {
		t.Errorf("Expected status: %d, got %d", http.StatusNotFound, status)
	}
}

func TestSetAllBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		bp          = tools.BaseAPIParams()
		bucketProps = &cmn.BpropsToSet{}
	)

	bucket, err := tools.GenerateNonexistentBucketName(t.Name()+"Bucket", bp)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cliBck.Provider,
	}

	_, err = api.SetBucketProps(bp, bck, bucketProps)
	if err == nil {
		t.Fatal("Expected SetBucketProps error, but got none.")
	}

	status := api.HTTPStatus(err)
	if status < http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, status)
	}
}

func TestBucketInvalidName(t *testing.T) {
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)

	invalidNames := []string{"*", ".", "", " ", "bucket and name", "bucket/name", "#name", "$name", "~name"}
	for _, name := range invalidNames {
		bck := cmn.Bck{
			Name:     name,
			Provider: apc.AIS,
		}
		if err := api.CreateBucket(bp, bck, nil); err == nil {
			tools.DestroyBucket(t, proxyURL, bck)
			t.Errorf("created bucket with invalid name %q", name)
		}
	}
}

func TestLocalMirror(t *testing.T) {
	tests := []struct {
		numCopies []int // each of the numbers in the list represents the number of copies enforced on the bucket
		tag       string
		skipArgs  tools.SkipTestArgs
	}{
		// set number `copies = 1` - no copies should be created
		{numCopies: []int{1}, tag: "copies=1"},
		// set number `copies = 2` - one additional copy for each object should be created
		{numCopies: []int{2}, tag: "copies=2"},
		// first set number of copies to 2, then to 3
		{numCopies: []int{2, 3}, skipArgs: tools.SkipTestArgs{Long: true}, tag: "copies=2-then-3"},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.tag, func(t *testing.T) {
			tools.CheckSkip(t, &test.skipArgs)
			testLocalMirror(t, test.numCopies)
		})
	}
}

func testLocalMirror(t *testing.T, numCopies []int) {
	const xactTimeout = 10 * time.Second
	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 5,
		bck: cmn.Bck{
			Provider: apc.AIS,
			Name:     trand.String(10),
			Ns:       genBucketNs(),
		},
	}

	if testing.Short() {
		m.num = 250
		m.numGetsEachFile = 3
	}

	m.initAndSaveState(true /*cleanup*/)

	copies := numCopies[0]
	for i := 1; i < len(numCopies); i++ {
		copies = max(copies, numCopies[i])
	}
	skip := tools.SkipTestArgs{MinMountpaths: copies}
	tools.CheckSkip(t, &skip)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	{
		bp := tools.BaseAPIParams()
		xid, err := api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
			Mirror: &cmn.MirrorConfToSet{
				Enabled: apc.Ptr(true),
			},
		})
		tassert.CheckFatal(t, err)

		p, err := api.HeadBucket(bp, m.bck, true /* don't add */)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, p.Mirror.Copies == 2, "%d copies != 2", p.Mirror.Copies)

		// Even though the bucket is empty, it can take a short while until the
		// xaction is propagated and finished.
		reqArgs := xact.ArgsMsg{ID: xid, Kind: apc.ActMakeNCopies, Bck: m.bck, Timeout: xactTimeout}
		_, err = api.WaitForXactionIC(bp, &reqArgs)
		tassert.CheckFatal(t, err)
	}

	m.puts()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.gets(nil, false)
	}()

	bp := tools.BaseAPIParams(m.proxyURL)

	xargs := xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: m.bck, Timeout: xactTimeout}
	_, _ = api.WaitForXactionIC(bp, &xargs)

	for _, copies := range numCopies {
		makeNCopies(t, bp, m.bck, copies)
	}

	// wait for all GETs to complete
	wg.Wait()

	m.ensureNumCopies(bp, numCopies[len(numCopies)-1], false /*greaterOk*/)
}

func makeNCopies(t *testing.T, bp api.BaseParams, bck cmn.Bck, ncopies int) {
	tlog.Logfln("Set copies = %d", ncopies)

	xid, err := api.MakeNCopies(bp, bck, ncopies)
	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMakeNCopies}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	args = xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: bck}
	api.WaitForSnapsIdle(bp, &args)
}

func TestRemoteBucketMirror(t *testing.T) {
	var (
		m = &ioContext{
			t:      t,
			num:    128,
			bck:    cliBck,
			prefix: t.Name(),
		}
		bp = tools.BaseAPIParams()
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	m.init(true /*cleanup*/)
	m.remotePuts(true /*evict*/)

	// enable mirror
	_, err := api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckFatal(t, err)
	defer api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(false)},
	})

	// list
	msg := &apc.LsoMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	objectList, err := api.ListObjects(bp, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(objectList.Entries) == m.num,
		"wrong number of objects in the remote bucket %s: need %d, got %d",
		m.bck.String(), m.num, len(objectList.Entries),
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{MinMountpaths: 4})

	// cold GET - causes local mirroring
	m.remotePrefetch(m.num)
	m.ensureNumCopies(bp, 2, false /*greaterOk*/)
	time.Sleep(3 * time.Second)

	// Increase number of copies
	makeNCopies(t, bp, m.bck, 3)
	m.ensureNumCopies(bp, 3, false /*greaterOk*/)
}

func TestBucketReadOnly(t *testing.T) {
	m := ioContext{
		t:               t,
		num:             10,
		numGetsEachFile: 2,
	}
	m.init(true /*cleanup*/)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	bp := tools.BaseAPIParams()

	m.puts()
	m.gets(nil, false)

	p, err := api.HeadBucket(bp, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	// make bucket read-only
	// NOTE: must allow PATCH - otherwise api.SetBucketProps a few lines down below won't work
	aattrs := apc.AccessRO | apc.AcePATCH
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{Access: apc.Ptr(aattrs)})
	tassert.CheckFatal(t, err)

	m.init(true /*cleanup*/)
	m.puts(true /*ignoreErr*/)
	tassert.Fatalf(t, m.numPutErrs == m.num, "num failed PUTs %d, expecting %d", m.numPutErrs, m.num)

	// restore write access
	_, err = api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{Access: apc.Ptr(p.Access)})
	tassert.CheckFatal(t, err)

	// write some more and destroy
	m.init(true /*cleanup*/)
	m.puts(true /*ignoreErr*/)
	tassert.Fatalf(t, m.numPutErrs == 0, "num failed PUTs %d, expecting 0 (zero)", m.numPutErrs)
}

func TestRenameBucketEmpty(t *testing.T) {
	var (
		m = ioContext{
			t: t,
		}
		bp     = tools.BaseAPIParams()
		dstBck = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.AIS,
			Ns:       genBucketNs(),
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)

	srcBck := m.bck
	tools.CreateBucket(t, m.proxyURL, srcBck, nil, true /*cleanup*/)
	defer func() {
		tools.DestroyBucket(t, m.proxyURL, dstBck)
	}()
	tools.DestroyBucket(t, m.proxyURL, dstBck)

	m.setNonDefaultBucketProps()
	srcProps, err := api.HeadBucket(bp, srcBck, true /* don't add */)
	tassert.CheckFatal(t, err)

	// Rename it
	tlog.Logfln("rename %s => %s", srcBck.String(), dstBck.String())
	uuid, err := api.RenameBucket(bp, srcBck, dstBck)
	if err != nil && ensurePrevRebalanceIsFinished(bp, err) {
		// can retry
		uuid, err = api.RenameBucket(bp, srcBck, dstBck)
	}
	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: uuid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)

	if !tools.BucketsContain(bcks, cmn.QueryBcks(dstBck)) {
		t.Error("new bucket not found in buckets list")
	}

	tlog.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(bp, dstBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if !srcProps.Equal(dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameBucketWithRandomMirrorEnable(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 2,
		}
		bp     = tools.BaseAPIParams()
		dstBck = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.AIS,
			Ns:       genBucketNs(),
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.proxyURL = tools.RandomProxyURL(t)
	m.expectTargets(1)

	srcBck := m.bck
	tools.CreateBucket(t, m.proxyURL, srcBck, nil, true /*cleanup*/)
	defer func() {
		// This bucket should be present.
		tools.DestroyBucket(t, m.proxyURL, dstBck)
	}()
	tools.DestroyBucket(t, m.proxyURL, dstBck)

	m.setNonDefaultBucketProps()
	srcProps, err := api.HeadBucket(bp, srcBck, true /* don't add */)
	tassert.CheckFatal(t, err)

	if srcProps.Mirror.Enabled {
		tlog.Logln("NOTE: this test is currently failing (in re: meta-v1 => meta-v2 transition)")
	}

	// Put some files
	m.puts()

	// Rename it
	tlog.Logfln("rename %s => %s", srcBck.String(), dstBck.String())
	m.bck = dstBck
	xid, err := api.RenameBucket(bp, srcBck, dstBck)
	if err != nil && ensurePrevRebalanceIsFinished(bp, err) {
		// can retry
		xid, err = api.RenameBucket(bp, srcBck, dstBck)
	}

	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	// Gets on renamed ais bucket
	m.gets(nil, false)
	m.ensureNoGetErrors()

	tlog.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(bp, dstBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if !srcProps.Equal(dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameBucketAlreadyExistingDst(t *testing.T) {
	var (
		m = ioContext{
			t: t,
		}
		bp     = tools.BaseAPIParams()
		tmpBck = cmn.Bck{
			Name:     "tmp_bck_name",
			Provider: apc.AIS,
			Ns:       genBucketNs(),
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	m.setNonDefaultBucketProps()
	srcProps, err := api.HeadBucket(bp, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	tools.CreateBucket(t, m.proxyURL, tmpBck, nil, true /*cleanup*/)

	// rename
	tlog.Logfln("try rename %s => %s (that already exists)", m.bck.String(), tmpBck.String())
	if _, err := api.RenameBucket(bp, m.bck, tmpBck); err == nil {
		t.Fatal("expected an error renaming already existing bucket")
	}

	bcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)

	if !tools.BucketsContain(bcks, cmn.QueryBcks(m.bck)) || !tools.BucketsContain(bcks, cmn.QueryBcks(tmpBck)) {
		t.Errorf("one of the buckets (%s, %s) was not found in the list %+v", m.bck.String(), tmpBck.String(), bcks)
	}

	dstProps, err := api.HeadBucket(bp, tmpBck, true /* don't add */)
	tassert.CheckFatal(t, err)

	if srcProps.Equal(dstProps) {
		t.Fatalf("source and destination props (checksums, in particular) are not expected to match: %v vs %v",
			srcProps.Cksum, dstProps.Cksum)
	}
}

// Tries to rename same source bucket to two destination buckets - the second should fail.
func TestRenameBucketTwice(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: false})
	var (
		m = ioContext{
			t:        t,
			num:      500,
			fileSize: 64 * cos.KiB,
		}
		bp      = tools.BaseAPIParams()
		dstBck1 = cmn.Bck{
			Name:     cos.GenTie() + "_new1",
			Provider: apc.AIS,
		}
		dstBck2 = cmn.Bck{
			Name:     cos.GenTie() + "_new2",
			Provider: apc.AIS,
		}
	)

	m.initAndSaveState(false /*cleanup*/)
	m.expectTargets(1)

	ensureICvsSnapsConsistency(bp, apc.ActRebalance)

	srcBck := m.bck
	tools.CreateBucket(t, m.proxyURL, srcBck, nil, false /*cleanup*/)
	defer func() {
		// This bucket should not be present (thus ignoring error) but
		// try to delete in case something failed.
		api.DestroyBucket(bp, dstBck2)
		// This one should be present.
		tools.DestroyBucket(t, m.proxyURL, dstBck1)
	}()

	m.puts()

	// Get bucket summary before rename for validation
	var srcSummary *cmn.BsummResult
	msg := &apc.BsummCtrlMsg{ObjCached: true, BckPresent: true}
	_, summaries, err := api.GetBucketSummary(bp, cmn.QueryBcks(srcBck), msg, api.BsummArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(summaries) > 0, "source bucket summary not found")
	srcSummary = summaries[0]
	tlog.Logfln("Source bucket summary: %d objects, %d bytes",
		srcSummary.ObjCount.Present, srcSummary.TotalSize.PresentObjs)

	// Rename to first destination
	tlog.Logfln("rename %s => %s", srcBck.String(), dstBck1.String())
	xid, err := api.RenameBucket(bp, srcBck, dstBck1)
	if err != nil && ensurePrevRebalanceIsFinished(bp, err) {
		// can retry
		xid, err = api.RenameBucket(bp, srcBck, dstBck1)
	}
	tassert.CheckFatal(t, err)

	// Try to rename to first destination again - already in progress
	tlog.Logfln("try renaming %s => %s", srcBck.String(), dstBck1.String())
	_, err = api.RenameBucket(bp, srcBck, dstBck1)
	tlog.Logfln("error: %v", err)
	if err == nil {
		t.Error("multiple rename operations on same bucket should fail")
	}

	// Try to rename to second destination - this should fail
	tlog.Logfln("try rename %s => %s", srcBck.String(), dstBck2.String())
	_, err = api.RenameBucket(bp, srcBck, dstBck2)
	tlog.Logfln("error: %v", err)
	if err == nil {
		t.Error("multiple rename operations on same bucket should fail")
	}

	// Wait for rename to complete
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)

	if tools.BucketsContain(bcks, cmn.QueryBcks(srcBck)) {
		t.Error("source bucket found in buckets list")
	}
	if !tools.BucketsContain(bcks, cmn.QueryBcks(dstBck1)) {
		t.Error("destination bucket not found in buckets list")
	}
	if tools.BucketsContain(bcks, cmn.QueryBcks(dstBck2)) {
		t.Error("second (failed) destination bucket found in buckets list")
	}

	tlog.Logln("validating objects in renamed bucket...")
	m.bck = dstBck1   // Update context to renamed bucket
	m.gets(nil, true) // GET all objects with checksum validation
	m.ensureNoGetErrors()

	// Validate bucket summary matches original
	msg = &apc.BsummCtrlMsg{ObjCached: true, BckPresent: true}
	_, dstSummaries, err := api.GetBucketSummary(bp, cmn.QueryBcks(dstBck1), msg, api.BsummArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(dstSummaries) > 0, "destination bucket summary not found")

	dstSummary := dstSummaries[0]
	tlog.Logfln("Destination bucket summary: %d objects, %d bytes",
		dstSummary.ObjCount.Present, dstSummary.TotalSize.PresentObjs)

	// Compare key summary metrics
	tassert.Fatalf(t, srcSummary.ObjCount.Present == dstSummary.ObjCount.Present,
		"Object count mismatch: source=%d, destination=%d",
		srcSummary.ObjCount.Present, dstSummary.ObjCount.Present)
	tassert.Fatalf(t, srcSummary.TotalSize.PresentObjs == dstSummary.TotalSize.PresentObjs,
		"Total size mismatch: source=%d, destination=%d",
		srcSummary.TotalSize.PresentObjs, dstSummary.TotalSize.PresentObjs)
	tassert.Fatalf(t, srcSummary.ObjSize.Min == dstSummary.ObjSize.Min &&
		srcSummary.ObjSize.Max == dstSummary.ObjSize.Max &&
		srcSummary.ObjSize.Avg == dstSummary.ObjSize.Avg,
		"Object size stats mismatch: source={min:%d,avg:%d,max:%d}, destination={min:%d,avg:%d,max:%d}",
		srcSummary.ObjSize.Min, srcSummary.ObjSize.Avg, srcSummary.ObjSize.Max,
		dstSummary.ObjSize.Min, dstSummary.ObjSize.Avg, dstSummary.ObjSize.Max)
	tlog.Logln("bucket summary validation: PASSED")
}

func TestRenameBucketAbort(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:        t,
			num:      5000, // Large bucket to allow time for abort
			fileSize: 64 * cos.KiB,
		}
		bp     = tools.BaseAPIParams()
		dstBck = cmn.Bck{
			Name:     testBucketName + "_abort_dst",
			Provider: apc.AIS,
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.proxyURL = tools.RandomProxyURL(t)
	m.expectTargets(1)

	srcBck := m.bck
	tools.CreateBucket(t, m.proxyURL, srcBck, nil, true /*cleanup*/)

	m.puts()

	// Get bucket summary before rename for validation
	_, summaries, err := api.GetBucketSummary(bp, cmn.QueryBcks(srcBck), &apc.BsummCtrlMsg{ObjCached: true, BckPresent: true, UUID: ""}, api.BsummArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(summaries) > 0, "source bucket summary not found")
	srcSummary := summaries[0]
	tlog.Logfln("Source bucket summary before rename: %d objects, %d bytes",
		srcSummary.ObjCount.Present, srcSummary.TotalSize.PresentObjs)

	// Start rename operation
	tlog.Logfln("Starting rename %s => %s", srcBck.String(), dstBck.String())
	xid, err := api.RenameBucket(bp, srcBck, dstBck)
	tassert.CheckFatal(t, err)

	// Immediately abort the rename operation
	tlog.Logfln("Aborting rename operation %s", xid)
	err = api.AbortXaction(bp, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)

	// Wait for abort to complete
	snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)
	aborted, finished := _isAbortedOrFinished(xid, snaps)
	tassert.Fatalf(t, aborted || finished, "expecting rename operation %q to abort or finish", xid)

	if finished {
		tlog.Logfln("Rename operation %s finished before abort", xid)
	} else {
		tlog.Logfln("Rename operation %s successfully aborted", xid)
	}

	// Validate destination bucket was not created
	err = tools.WaitForCondition(
		func() bool {
			bcks, err := api.ListBuckets(bp, cmn.QueryBcks(dstBck), apc.FltExists)
			tassert.CheckError(t, err)
			return !tools.BucketsContain(bcks, cmn.QueryBcks(dstBck))
		}, tools.DefaultWaitRetry,
	)
	if aborted {
		tassert.Fatalf(t, err == nil, "when aborted, should not contain destination bucket %s", dstBck.String())
	}
	bcks, err := api.ListBuckets(bp, cmn.QueryBcks(srcBck), apc.FltExists)
	tassert.CheckFatal(t, err)

	// Validate source bucket still exists
	tassert.Fatalf(t, tools.BucketsContain(bcks, cmn.QueryBcks(srcBck)),
		"source bucket %s should still exist after aborted rename", srcBck.String())

	// Validate source bucket summary remains unchanged
	_, summariesAfter, err := api.GetBucketSummary(bp, cmn.QueryBcks(srcBck), &apc.BsummCtrlMsg{ObjCached: true, BckPresent: true, UUID: ""}, api.BsummArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(summariesAfter) > 0, "source bucket summary not found after abort")

	srcSummaryAfter := summariesAfter[0]
	tlog.Logfln("Source bucket summary after aborted rename: %d objects, %d bytes",
		srcSummaryAfter.ObjCount.Present, srcSummaryAfter.TotalSize.PresentObjs)

	// Compare summaries to ensure they are identical
	tassert.Fatalf(t, srcSummary.ObjCount.Present == srcSummaryAfter.ObjCount.Present,
		"Object count changed after abort: before=%d, after=%d",
		srcSummary.ObjCount.Present, srcSummaryAfter.ObjCount.Present)
	tassert.Fatalf(t, srcSummary.TotalSize.PresentObjs == srcSummaryAfter.TotalSize.PresentObjs,
		"Total size changed after abort: before=%d, after=%d",
		srcSummary.TotalSize.PresentObjs, srcSummaryAfter.TotalSize.PresentObjs)
	tassert.Fatalf(t, srcSummary.ObjSize.Min == srcSummaryAfter.ObjSize.Min &&
		srcSummary.ObjSize.Max == srcSummaryAfter.ObjSize.Max &&
		srcSummary.ObjSize.Avg == srcSummaryAfter.ObjSize.Avg,
		"Object size stats changed after abort: before={min:%d,avg:%d,max:%d}, after={min:%d,avg:%d,max:%d}",
		srcSummary.ObjSize.Min, srcSummary.ObjSize.Avg, srcSummary.ObjSize.Max,
		srcSummaryAfter.ObjSize.Min, srcSummaryAfter.ObjSize.Avg, srcSummaryAfter.ObjSize.Max)

	tlog.Logln("Bucket rename abort test: PASSED")
	tlog.Logln("- Destination bucket was not created")
	tlog.Logln("- Source bucket remains intact with identical summary")
}

func TestRenameBucketNonExistentSrc(t *testing.T) {
	var (
		m = ioContext{
			t: t,
		}
		bp     = tools.BaseAPIParams()
		dstBck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		srcBcks = []cmn.Bck{
			{
				Name:     trand.String(10),
				Provider: apc.AIS,
			},
			{
				Name:     trand.String(10),
				Provider: apc.AWS,
			},
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)

	for _, srcBck := range srcBcks {
		_, err := api.RenameBucket(bp, srcBck, dstBck)
		tools.CheckErrIsNotFound(t, err)
		_, err = api.HeadBucket(bp, dstBck, true /* don't add */)
		tools.CheckErrIsNotFound(t, err)
	}
}

func TestRenameBucketWithBackend(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{CloudBck: true, Bck: cliBck})

	var (
		proxyURL = tools.RandomProxyURL()
		bp       = tools.BaseAPIParams(proxyURL)
		bck      = cmn.Bck{
			Name:     "renamesrc",
			Provider: apc.AIS,
		}
		dstBck = cmn.Bck{
			Name:     "bucketname",
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck,
		&cmn.BpropsToSet{BackendBck: &cmn.BackendBckToSet{
			Name:     apc.Ptr(cliBck.Name),
			Provider: apc.Ptr(cliBck.Provider),
		}}, true /*cleanup*/)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	srcProps, err := api.HeadBucket(bp, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	xid, err := api.RenameBucket(bp, bck, dstBck)
	if err != nil && ensurePrevRebalanceIsFinished(bp, err) {
		// can retry
		xid, err = api.RenameBucket(bp, bck, dstBck)
	}

	tassert.CheckFatal(t, err)
	xargs := xact.ArgsMsg{ID: xid}
	_, err = api.WaitForXactionIC(bp, &xargs)
	tassert.CheckFatal(t, err)

	exists, err := api.QueryBuckets(bp, cmn.QueryBcks(bck), apc.FltPresent)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !exists, "source bucket shouldn't exist")

	tlog.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(bp, dstBck, true /* don't add */)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(
		t, srcProps.Versioning.Enabled == dstProps.Versioning.Enabled,
		"source and destination bucket versioning does not match: %t vs. %t, respectively",
		srcProps.Versioning.Enabled, dstProps.Versioning.Enabled,
	)

	// AWS region might be set upon rename.
	srcProps.Extra.AWS.CloudRegion = ""
	dstProps.Extra.AWS.CloudRegion = ""

	tassert.Fatalf(t, srcProps.Equal(dstProps), "source and destination bucket props do not match:\n%v\n%v", srcProps, dstProps)
}

func TestCopyBucket(t *testing.T) {
	tests := []struct {
		srcRemote        bool
		dstRemote        bool
		dstBckExist      bool // determines if destination bucket exists before copy or not
		dstBckHasObjects bool // determines if destination bucket contains any objects before copy or not
		multipleDests    bool // determines if there are multiple destinations to which objects are copied
		onlyLong         bool
		evictRemoteSrc   bool
		numWorkers       int
	}{
		// ais -> ais
		{srcRemote: false, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: false},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: false, multipleDests: false, onlyLong: true},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: false, onlyLong: true},
		{srcRemote: false, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true, onlyLong: true},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true, onlyLong: true},

		// ais -> ais (multi-workers)
		{srcRemote: false, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: false, numWorkers: 4},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: false, multipleDests: false, onlyLong: true, numWorkers: 4},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: false, onlyLong: true, numWorkers: 4},
		{srcRemote: false, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true, onlyLong: true, numWorkers: 4},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true, onlyLong: true, numWorkers: 4},

		// remote -> ais
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: false},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: true},
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true},

		// remote -> ais (multi-workers)
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, numWorkers: 4},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: false, numWorkers: 4},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, numWorkers: 4},
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true, numWorkers: 4},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true, numWorkers: 4},

		// evicted remote -> ais
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, evictRemoteSrc: true},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: false, evictRemoteSrc: true},

		// evicted remote -> ais (multi-workers)
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, evictRemoteSrc: true, numWorkers: 4},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: false, evictRemoteSrc: true, numWorkers: 4},

		// ais -> remote
		{srcRemote: false, dstRemote: true, dstBckExist: true, dstBckHasObjects: false},

		// ais -> remote (multi-workers)
		{srcRemote: false, dstRemote: true, dstBckExist: true, dstBckHasObjects: false, numWorkers: 4},
	}

	for _, test := range tests {
		// Bucket must exist when we require it to have objects.
		cos.Assert(test.dstBckExist || !test.dstBckHasObjects)

		// in integration tests, we only have 1 remote bucket (cliBck)
		// (TODO: add remote -> remote)
		cos.Assert(!test.srcRemote || !test.dstRemote)

		testName := fmt.Sprintf("src-remote=%t/dst-remote=%t/", test.srcRemote, test.dstRemote)
		if test.evictRemoteSrc {
			cos.Assert(test.srcRemote)
			testName = fmt.Sprintf("src-remote-evicted/dst-remote=%t/", test.dstRemote)
		}
		if test.dstBckExist {
			testName += "dst-present/"
			if test.dstBckHasObjects {
				testName += "with_objs"
			} else {
				testName += "without_objs"
			}
		} else {
			testName += "dst-absent"
		}
		if test.multipleDests {
			testName += "/multiple_dests"
		}
		if test.numWorkers != 0 {
			testName += fmt.Sprintf("/num_workers=%d", test.numWorkers)
		}

		t.Run(testName, func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})
			var (
				srcBckList *cmn.LsoRes

				objCnt = 100
				srcm   = &ioContext{
					t:   t,
					num: objCnt,
					bck: cmn.Bck{
						Name:     "src_copy_bck",
						Provider: apc.AIS,
						Ns:       genBucketNs(),
					},
				}
				dstms = []*ioContext{
					{
						t:   t,
						num: objCnt,
						bck: cmn.Bck{
							Name:     "dst_copy_bck_1",
							Provider: apc.AIS,
							Ns:       genBucketNs(),
						},
					},
				}
				bp = tools.BaseAPIParams()
			)
			tools.DestroyBucket(t, proxyURL, srcm.bck)
			tools.DestroyBucket(t, proxyURL, dstms[0].bck)

			if test.multipleDests {
				dstms = append(dstms, &ioContext{
					t:   t,
					num: objCnt,
					bck: cmn.Bck{
						Name:     "dst_copy_bck_2",
						Provider: apc.AIS,
						Ns:       genBucketNs(),
					},
				})
				tools.DestroyBucket(t, proxyURL, dstms[1].bck)
			}
			bckTest := cmn.Bck{Provider: apc.AIS, Ns: cmn.NsGlobal}
			if test.srcRemote {
				srcm.bck = cliBck
				srcm.deleteRemoteBckObjs = true
				bckTest.Provider = cliBck.Provider
				tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: srcm.bck})
			}
			if test.dstRemote {
				dstms = []*ioContext{
					{
						t:   t,
						num: 0, // Make sure to not put anything new to destination remote bucket
						bck: cliBck,
					},
				}
				tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: dstms[0].bck})
			}

			srcm.initAndSaveState(true /*cleanup*/)
			srcm.expectTargets(1)

			for _, dstm := range dstms {
				dstm.init(true /*cleanup*/)
			}

			if bckTest.IsAIS() {
				tools.CreateBucket(t, srcm.proxyURL, srcm.bck, nil, true)
				srcm.setNonDefaultBucketProps()
			}

			if test.dstBckExist {
				for _, dstm := range dstms {
					if !dstm.bck.IsRemote() {
						tools.CreateBucket(t, dstm.proxyURL, dstm.bck, nil, true /*cleanup*/)
					}
				}
			} else { // cleanup
				for _, dstm := range dstms {
					if !dstm.bck.IsRemote() {
						t.Cleanup(func() {
							tools.DestroyBucket(t, dstm.proxyURL, dstm.bck)
						})
					}
				}
			}

			srcProps, err := api.HeadBucket(bp, srcm.bck, true /* don't add */)
			tassert.CheckFatal(t, err)

			if test.dstBckHasObjects {
				for _, dstm := range dstms {
					// Don't make PUTs to remote bucket
					if !dstm.bck.IsRemote() {
						dstm.puts()
					}
				}
			}

			switch {
			case bckTest.IsAIS():
				srcm.puts()

				srcBckList, err = api.ListObjects(bp, srcm.bck, nil, api.ListArgs{})
				tassert.CheckFatal(t, err)
			case bckTest.IsRemote():
				srcm.remotePuts(false /*evict*/)
				srcBckList, err = api.ListObjects(bp, srcm.bck, nil, api.ListArgs{})
				tassert.CheckFatal(t, err)
				if test.evictRemoteSrc {
					tlog.Logfln("evicting %s", srcm.bck.String())
					//
					// evict all _cached_ data from the "local" cluster
					// keep the src bucket in the "local" BMD though
					//
					err := api.EvictRemoteBucket(bp, srcm.bck, true /*keep empty src bucket in the BMD*/)
					tassert.CheckFatal(t, err)
				}
				defer srcm.del()
			default:
				t.Fatal("invalid provider", bckTest.String())
			}

			xactIDs := make([]string, 0, len(dstms))
			for _, dstm := range dstms {
				var (
					uuid string
					err  error
					cmsg = &apc.TCBMsg{
						CopyBckMsg: apc.CopyBckMsg{Force: true},
						NumWorkers: test.numWorkers,
					}
				)
				if test.evictRemoteSrc {
					uuid, err = api.CopyBucket(bp, srcm.bck, dstm.bck, cmsg, apc.FltExists)
				} else {
					uuid, err = api.CopyBucket(bp, srcm.bck, dstm.bck, cmsg)
				}
				tassert.CheckFatal(t, err)
				tlog.Logfln("copying %s => %s: %s", srcm.bck.String(), dstm.bck.String(), uuid)
				if uuids := strings.Split(uuid, xact.SepaID); len(uuids) > 1 {
					for _, u := range uuids {
						tassert.Fatalf(t, xact.IsValidUUID(u), "invalid UUID %q", u)
					}
					xactIDs = append(xactIDs, uuids...)
				} else {
					tassert.Fatalf(t, xact.IsValidUUID(uuid), "invalid UUID %q", uuid)
					xactIDs = append(xactIDs, uuid)
				}
			}

			for _, uuid := range xactIDs {
				// TODO -- FIXME: remove/simplify-out this `if` here and elsewhere
				var args = xact.ArgsMsg{ID: uuid, Timeout: tools.CopyBucketTimeout}
				if test.evictRemoteSrc {
					err := api.WaitForSnapsIdle(bp, &args)
					tassert.CheckFatal(t, err)
				} else {
					args.Kind = apc.ActCopyBck // wait for TCB idle (different x-kind than TCO)
					_, err := api.WaitForXactionIC(bp, &args)
					tassert.CheckFatal(t, err)
				}
				snaps, err := api.QueryXactionSnaps(bp, &args)
				tassert.CheckFatal(t, err)
				total, err := snaps.TotalRunningTime(uuid)
				tassert.CheckFatal(t, err)
				tlog.Logfln("copy-bucket[%s] with %d workers took %v", uuid, test.numWorkers, total)
			}

			for _, dstm := range dstms {
				if dstm.bck.IsRemote() {
					continue
				}

				tlog.Logfln("checking and comparing bucket %s props", dstm.bck.String())
				dstProps, err := api.HeadBucket(bp, dstm.bck, true /* don't add */)
				tassert.CheckFatal(t, err)

				if dstProps.Provider != apc.AIS {
					t.Fatalf("destination bucket does not seem to be 'ais': %s", dstProps.Provider)
				}
				// Clear providers to compare the props across different ones
				srcProps.Provider = ""
				dstProps.Provider = ""

				// If bucket existed before, ensure that the bucket props were **not** copied over.
				if test.dstBckExist && srcProps.Equal(dstProps) {
					t.Fatalf("source and destination bucket props match, even though they should not:\n%#v\n%#v",
						srcProps, dstProps)
				}

				// When copying remote => ais we create the destination ais bucket on the fly
				// with the default props. In all other cases (including ais => ais) bucket props must match.
				if !test.dstBckExist {
					if test.srcRemote && !test.dstRemote {
						// TODO: validate default props
					} else if !srcProps.Equal(dstProps) {
						t.Fatalf("source and destination bucket props do not match:\n%#v\n%#v",
							srcProps, dstProps)
					}
				}
			}

			for _, dstm := range dstms {
				tlog.Logfln("checking and comparing objects of bucket %s", dstm.bck.String())
				expectedObjCount := srcm.num
				if test.dstBckHasObjects {
					expectedObjCount += dstm.num
				}

				_, err := api.HeadBucket(bp, srcm.bck, true /* don't add */)
				tassert.CheckFatal(t, err)
				dstmProps, err := api.HeadBucket(bp, dstm.bck, true /* don't add */)
				tassert.CheckFatal(t, err)

				msg := &apc.LsoMsg{}
				msg.AddProps(apc.GetPropsVersion)
				if test.dstRemote {
					msg.Flags = apc.LsCached
				}

				dstBckList, err := api.ListObjects(bp, dstm.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
				if len(dstBckList.Entries) != expectedObjCount {
					t.Fatalf("list_objects: dst %s, cnt %d != %d cnt, src %s",
						dstm.bck.Cname(""), len(dstBckList.Entries), expectedObjCount, srcm.bck.Cname(""))
				}

				tlog.Logfln("verifying that %d copied objects have identical props", expectedObjCount)
				for _, a := range srcBckList.Entries {
					var found bool
					for _, b := range dstBckList.Entries {
						if a.Name == b.Name {
							found = true

							if dstm.bck.IsRemote() && dstmProps.Versioning.Enabled {
								tassert.Fatalf(t, b.Version != "",
									"Expected non-empty object %q version", b.Name)
							}

							break
						}
					}
					if !found {
						t.Fatalf("%s is missing in the destination bucket %s", srcm.bck.Cname(a.Name), dstm.bck.Cname(""))
					}
				}
			}
		})
	}
}

func TestCopyBucketChecksumValidation(t *testing.T) {
	tests := []struct {
		srcCksum string
		dstCksum string
	}{
		{srcCksum: cos.ChecksumNone, dstCksum: cos.ChecksumNone},
		{srcCksum: cos.ChecksumNone, dstCksum: cos.ChecksumMD5},
		{srcCksum: cos.ChecksumMD5, dstCksum: cos.ChecksumNone},
		{srcCksum: cos.ChecksumMD5, dstCksum: cos.ChecksumSHA256},
		{srcCksum: cos.ChecksumSHA512, dstCksum: cos.ChecksumSHA512},
		{srcCksum: cos.ChecksumCesXxh, dstCksum: cos.ChecksumMD5},
	}

	for _, test := range tests {
		testName := fmt.Sprintf("%s_to_%s", test.srcCksum, test.dstCksum)
		t.Run(testName, func(t *testing.T) {
			var (
				proxyURL = tools.RandomProxyURL(t)
				bp       = tools.BaseAPIParams(proxyURL)
				objCnt   = 10
				srcBck   = cmn.Bck{
					Name:     "src_cksum_" + trand.String(6),
					Provider: apc.AIS,
					Ns:       genBucketNs(),
				}
				dstBck = cmn.Bck{
					Name:     "dst_cksum_" + trand.String(6),
					Provider: apc.AIS,
					Ns:       genBucketNs(),
				}
				srcm = &ioContext{
					t:   t,
					num: objCnt,
					bck: srcBck,
				}
			)

			// Initialize context and create source bucket
			srcm.initAndSaveState(true /*cleanup*/)
			srcm.expectTargets(1)
			tools.CreateBucket(t, proxyURL, srcBck, &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					Type: apc.Ptr(test.srcCksum),
				},
			}, true)

			// Create destination bucket and configure with specified checksum
			tools.CreateBucket(t, proxyURL, dstBck, &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					Type: apc.Ptr(test.dstCksum),
				},
			}, true)

			// Put objects to source bucket
			srcm.puts()

			// Get source bucket objects and their expected checksums
			srcObjList, err := api.ListObjects(bp, srcBck, nil, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(srcObjList.Entries) == objCnt, "expected %d objects in source bucket, got %d", objCnt, len(srcObjList.Entries))

			// Collect source object data and compute expected destination checksums
			expectedCksums := make(map[string]*cos.Cksum)
			for _, entry := range srcObjList.Entries {
				objName := entry.Name
				reader, _, err := api.GetObjectReader(bp, srcBck, objName, &api.GetArgs{})
				tassert.CheckFatal(t, err)

				objData, err := io.ReadAll(reader)
				tassert.CheckFatal(t, err)
				reader.Close()

				var expectedCksum *cos.Cksum
				if test.dstCksum != cos.ChecksumNone {
					expectedCksum, err = cos.ChecksumBytes(objData, test.dstCksum)
					tassert.CheckFatal(t, err)
				} else {
					expectedCksum = cos.NewCksum(cos.ChecksumNone, "")
				}
				expectedCksums[objName] = expectedCksum
			}

			// Copy bucket
			cmsg := &apc.TCBMsg{
				CopyBckMsg: apc.CopyBckMsg{Force: true},
			}
			uuid, err := api.CopyBucket(bp, srcBck, dstBck, cmsg)
			tassert.CheckFatal(t, err)
			tlog.Logfln("copying %s => %s: %s", srcBck.String(), dstBck.String(), uuid)

			args := xact.ArgsMsg{ID: uuid, Kind: apc.ActCopyBck, Timeout: tools.CopyBucketTimeout}
			_, err = api.WaitForXactionIC(bp, &args)
			tassert.CheckFatal(t, err)

			// Validate destination bucket properties
			dstBckProps, err := api.HeadBucket(bp, dstBck, true /* don't add */)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, dstBckProps.Cksum.Type == test.dstCksum,
				"destination bucket checksum type should be %s but got %s",
				test.dstCksum, dstBckProps.Cksum.Type)

			// Validate each object's checksum in destination bucket
			tlog.Logfln("validating checksums of %d objects in destination bucket", objCnt)
			for objName, expectedCksum := range expectedCksums {
				objProps, err := api.HeadObject(bp, dstBck, objName, api.HeadArgs{FltPresence: apc.FltPresent})
				tassert.CheckFatal(t, err)

				actualCksum := objProps.ObjAttrs.Checksum()

				// Validate checksum type and value
				if test.dstCksum == cos.ChecksumNone {
					// When destination bucket has no checksum, the object should also have no checksum
					tassert.Fatalf(t, cos.NoneC(actualCksum), "object %s should have no checksum but got %s", objName, actualCksum)
				} else {
					// When destination bucket has checksum, object should have matching checksum
					tassert.Fatalf(t, !cos.NoneC(actualCksum), "object %s should have checksum type %s but has no checksum", objName, test.dstCksum)
					tassert.Fatalf(t, actualCksum.Ty() == test.dstCksum, "object %s checksum type should be %s but got %s", objName, test.dstCksum, actualCksum.Ty())
					tassert.Fatalf(t, actualCksum.Equal(expectedCksum), "object %s checksum value should be %s but got %s", objName, expectedCksum.Val(), actualCksum.Val())
					tlog.Logfln("✓ object %s has correct %s checksum: %s", objName, test.dstCksum, actualCksum.Val())
				}
			}

			tlog.Logfln("✓ Test %s completed successfully", testName)
		})
	}
}

func TestCopyBucketSync(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long:                  true,
		RemoteBck:             true,
		Bck:                   cliBck,
		RequiresRemoteCluster: true, // NOTE: utilizing remote cluster to simulate out-of-band delete
	})
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      500,
			fileSize: 128,
			prefix:   trand.String(6) + "-",
		}
		bp = tools.BaseAPIParams()
	)

	m.init(true /*cleanup*/)

	// 1. PUT(num-objs) => cliBck
	m.puts()
	tassert.Errorf(t, len(m.objNames) == m.num, "expected %d in the source bucket, got %d", m.num, len(m.objNames))

	tlog.Logfln("list source %s objects", cliBck.Cname(""))
	msg := &apc.LsoMsg{Prefix: m.prefix, Flags: apc.LsCached}
	lst, err := api.ListObjects(bp, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == m.num, "expected %d present (cached) in the source bucket, got %d", m.num, len(lst.Entries))

	// 2. copy cliBck => dstBck
	dstBck := cmn.Bck{Name: "dst-" + cos.GenTie(), Provider: apc.AIS, Ns: genBucketNs()}
	tlog.Logfln("first copy %s => %s", m.bck.Cname(""), dstBck.Cname(""))
	xid, err := api.CopyBucket(bp, m.bck, dstBck, &apc.TCBMsg{})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	tlog.Logfln("list destination %s objects", dstBck.Cname(""))
	lst, err = api.ListObjects(bp, dstBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "expected %d in the destination bucket, got %d", m.num, len(lst.Entries))

	// 3. select random 10% to delete
	num2del := max(m.num/10, 1)
	nam2del := make([]string, 0, num2del)
	strtpos := rand.IntN(m.num)
	for i := range num2del {
		pos := (strtpos + i*3) % m.num
		name := m.objNames[pos]
		for slices.Contains(nam2del, name) {
			pos++
			name = m.objNames[pos%m.num]
		}
		nam2del = append(nam2del, name)
	}

	// 4. use remais to out-of-band delete nam2del...
	tlog.Logfln("use remote cluster '%s' to out-of-band delete %d objects from %s (source)",
		tools.RemoteCluster.Alias, len(nam2del), m.bck.Cname(""))
	remoteBP := tools.BaseAPIParams(tools.RemoteCluster.URL)
	for _, name := range nam2del {
		err := api.DeleteObject(remoteBP, cliBck, name)
		tassert.CheckFatal(t, err)
	}

	// 5. copy --sync (and note that prior to this step destination has all m.num)
	tlog.Logfln("second copy %s => %s with '--sync' option", m.bck.Cname(""), dstBck.Cname(""))
	xid, err = api.CopyBucket(bp, m.bck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{Sync: true}})
	tassert.CheckFatal(t, err)
	args.ID = xid
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	tlog.Logfln("list post-sync destination %s", dstBck.Cname(""))
	lst, err = api.ListObjects(bp, dstBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == m.num-len(nam2del), "expected %d objects in the (sync-ed) destination, got %d",
		m.num-len(nam2del), len(lst.Entries))
}

func TestCopyBucketSimple(t *testing.T) {
	var (
		srcBck = cmn.Bck{Name: "cpybck_src" + cos.GenTie(), Provider: apc.AIS, Ns: genBucketNs()}

		m = &ioContext{
			t:         t,
			num:       500, // x 2
			fileSize:  512,
			fixedSize: true,
			bck:       srcBck,
		}
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)
	if testing.Short() {
		m.num /= 10
	}

	tlog.Logfln("Preparing source bucket %s", srcBck.String())
	tools.CreateBucket(t, proxyURL, srcBck, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)

	m.puts()
	m.prefix = "subdir/"
	m.puts()
	m.num *= 2

	f := func() {
		list, err := api.ListObjects(bp, srcBck, nil, api.ListArgs{})
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, len(list.Entries) == m.num, "expected %d in the source bucket, got %d", m.num, len(list.Entries))
	}

	// pre-abort sleep
	sleep := time.Second
	if m.smap.CountActiveTs() == 1 {
		sleep = time.Millisecond
	}

	t.Run("Stats", func(t *testing.T) { f(); testCopyBucketStats(t, srcBck, m) })
	t.Run("Prepend", func(t *testing.T) { f(); testCopyBucketPrepend(t, srcBck, m) })
	t.Run("Prefix", func(t *testing.T) { f(); testCopyBucketPrefix(t, srcBck, m, m.num/2) })
	t.Run("Abort", func(t *testing.T) { f(); testCopyBucketAbort(t, srcBck, m, sleep) })
	t.Run("DryRun", func(t *testing.T) { f(); testCopyBucketDryRun(t, srcBck, m) })
	t.Run("MultiWorker", func(t *testing.T) { f(); testCopyBucketMultiWorker(t, srcBck, m) })
}

func testCopyBucketStats(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	dstBck := cmn.Bck{Name: "cpybck_dst" + cos.GenTie(), Provider: apc.AIS}

	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)

	xid, err := api.CopyBucket(bp, srcBck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{Force: true}})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)
	objs, outObjs, inObjs := snaps.ObjCounts(xid)
	tassert.Errorf(t, objs == int64(m.num), "expected %d objects copied, got (objs=%d, outObjs=%d, inObjs=%d)",
		m.num, objs, outObjs, inObjs)
	if outObjs != inObjs {
		tlog.Logfln("Warning: (sent objects) %d != %d (received objects)", outObjs, inObjs)
	} else {
		tlog.Logfln("Num sent/received objects: %d", outObjs)
	}
	expectedBytesCnt := int64(m.fileSize * uint64(m.num))
	locBytes, outBytes, inBytes := snaps.ByteCounts(xid)
	tassert.Errorf(t, locBytes == expectedBytesCnt, "expected %d bytes copied, got (bytes=%d, outBytes=%d, inBytes=%d)",
		expectedBytesCnt, locBytes, outBytes, inBytes)
}

func testCopyBucketPrepend(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		cpyPrefix = "cpy/virt" + trand.String(5) + "/"
		dstBck    = cmn.Bck{Name: "cpybck_dst" + cos.GenTie(), Provider: apc.AIS}
		proxyURL  = tools.RandomProxyURL(t)
		bp        = tools.BaseAPIParams(proxyURL)
	)

	xid, err := api.CopyBucket(bp, srcBck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{Prepend: cpyPrefix}})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	tlog.Logfln("Waiting for x-%s[%s] %s => %s", apc.ActCopyBck, xid, srcBck.String(), dstBck.String())
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(bp, dstBck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == m.num, "expected %d to be copied, got %d", m.num, len(list.Entries))
	for _, e := range list.Entries {
		tassert.Fatalf(t, strings.HasPrefix(e.Name, cpyPrefix), "expected %q to have prefix %q", e.Name, cpyPrefix)
	}
}

func testCopyBucketPrefix(t *testing.T, srcBck cmn.Bck, m *ioContext, expected int) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		dstBck   = cmn.Bck{Name: "cpybck_dst" + cos.GenTie(), Provider: apc.AIS, Ns: genBucketNs()}
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)

	xid, err := api.CopyBucket(bp, srcBck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{Prefix: m.prefix}})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	tlog.Logfln("Waiting for x-%s[%s] %s => %s", apc.ActCopyBck, xid, srcBck.String(), dstBck.String())
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(bp, dstBck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == expected, "expected %d to be copied, got %d", expected, len(list.Entries))
	for _, e := range list.Entries {
		tassert.Fatalf(t, strings.HasPrefix(e.Name, m.prefix), "expected %q to have prefix %q", e.Name, m.prefix)
	}
}

func testCopyBucketAbort(t *testing.T, srcBck cmn.Bck, m *ioContext, sleep time.Duration) {
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	dstBck := cmn.Bck{Name: testBucketName + cos.GenTie(), Provider: apc.AIS}

	xid, err := api.CopyBucket(bp, srcBck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{Force: true}})
	tassert.CheckError(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, m.proxyURL, dstBck)
	})

	time.Sleep(sleep)

	tlog.Logfln("Aborting x-%s[%s]", apc.ActCopyBck, xid)
	err = api.AbortXaction(bp, &xact.ArgsMsg{ID: xid})
	tassert.CheckError(t, err)

	snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid})
	tassert.CheckError(t, err)
	aborted, finished := _isAbortedOrFinished(xid, snaps)
	tassert.CheckError(t, err)
	tassert.Errorf(t, aborted || finished, "expecting copy-bucket %q to abort or finish", xid)

	if finished {
		tlog.Logfln("%s[%s] already finished", apc.ActCopyBck, xid)
	}

	err = tools.WaitForCondition(
		func() bool {
			bcks, err := api.ListBuckets(bp, cmn.QueryBcks(dstBck), apc.FltExists)
			tassert.CheckError(t, err)
			return !tools.BucketsContain(bcks, cmn.QueryBcks(dstBck))
		}, tools.DefaultWaitRetry,
	)

	if aborted {
		tassert.Fatalf(t, err == nil, "when aborted, should not contain destination bucket %s", dstBck.String())
	}
}

func _isAbortedOrFinished(xid string, xs xact.MultiSnap) (aborted, finished bool) {
	for _, snaps := range xs {
		for _, xsnap := range snaps {
			if xid == xsnap.ID {
				if xsnap.IsAborted() {
					return true, false
				}
				if !xsnap.IsFinished() {
					return false, false
				}
			}
		}
	}
	return false, true
}

func testCopyBucketDryRun(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	dstBck := cmn.Bck{Name: "cpybck_dst" + cos.GenTie() + trand.String(5), Provider: apc.AIS}

	xid, err := api.CopyBucket(bp, srcBck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{DryRun: true}})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	tlog.Logfln("Waiting for x-%s[%s]", apc.ActCopyBck, xid)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)

	locObjs, outObjs, inObjs := snaps.ObjCounts(xid)
	tassert.Errorf(t, locObjs+outObjs == int64(m.num), "expected %d objects, got (locObjs=%d, outObjs=%d, inObjs=%d)",
		m.num, locObjs, outObjs, inObjs)

	locBytes, outBytes, inBytes := snaps.ByteCounts(xid)
	expectedBytesCnt := int64(m.fileSize * uint64(m.num))
	tassert.Errorf(t, locBytes+outBytes == expectedBytesCnt, "expected %d bytes, got (locBytes=%d, outBytes=%d, inBytes=%d)",
		expectedBytesCnt, locBytes, outBytes, inBytes)

	exists, err := api.QueryBuckets(bp, cmn.QueryBcks(dstBck), apc.FltExists)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, exists == false, "expected destination bucket to not be created")
}

func testCopyBucketMultiWorker(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	var (
		dstBck = cmn.Bck{Name: "cpybck_dst" + cos.GenTie(), Provider: apc.AIS}
	)

	for numWorkers := range []int{0, 1, 4} {
		t.Run(fmt.Sprintf("workers=%d", numWorkers), func(t *testing.T) {
			proxyURL := tools.RandomProxyURL(t)
			bp := tools.BaseAPIParams(proxyURL)
			xid, err := api.CopyBucket(
				bp,
				srcBck,
				dstBck,
				&apc.TCBMsg{
					CopyBckMsg:      apc.CopyBckMsg{},
					NumWorkers:      numWorkers,
					ContinueOnError: false,
				},
			)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() {
				tools.DestroyBucket(t, proxyURL, dstBck)
			})

			tlog.Logfln("Waiting for x-%s[%s] %s => %s", apc.ActCopyBck, xid, srcBck.String(), dstBck.String())
			args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
			_, err = api.WaitForXactionIC(bp, &args)
			tassert.CheckFatal(t, err)

			snaps, err := api.QueryXactionSnaps(bp, &args)
			tassert.CheckFatal(t, err)
			total, err := snaps.TotalRunningTime(xid)
			tassert.CheckFatal(t, err)
			tlog.Logfln("Copying bucket %s with %d workers took %v", srcBck.Cname(""), numWorkers, total)

			list, err := api.ListObjects(bp, dstBck, nil, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, len(list.Entries) == m.num, "expected %d to be copied, got %d", m.num, len(list.Entries))
			for _, en := range list.Entries {
				r1, _, err := api.GetObjectReader(bp, srcBck, en.Name, &api.GetArgs{})
				tassert.CheckFatal(t, err)
				r2, _, err := api.GetObjectReader(bp, dstBck, en.Name, &api.GetArgs{})
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, tools.ReaderEqual(r1, r2), "object content mismatch: %s vs %s", srcBck.Cname(en.Name), dstBck.Cname(en.Name))
				tassert.CheckFatal(t, r1.Close())
				tassert.CheckFatal(t, r2.Close())
			}
		})
	}
}

// Tries to rename and then copy bucket at the same time.
func TestRenameAndCopyBucket(t *testing.T) {
	var (
		bp   = tools.BaseAPIParams()
		src  = cmn.Bck{Name: testBucketName + "_rc_src", Provider: apc.AIS}
		m    = ioContext{t: t, bck: src, num: 500}
		dst1 = cmn.Bck{Name: testBucketName + "_rc_dst1", Provider: apc.AIS}
		dst2 = cmn.Bck{Name: testBucketName + "_rc_dst2", Provider: apc.AIS}
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)
	tools.DestroyBucket(t, m.proxyURL, dst1)

	tools.CreateBucket(t, m.proxyURL, src, nil, true /*cleanup*/)
	defer func() {
		tools.DestroyBucket(t, m.proxyURL, dst1)
		tools.DestroyBucket(t, m.proxyURL, dst2)
	}()

	m.puts()

	// Rename as dst1
	tlog.Logfln("Rename %s => %s", src.String(), dst1.String())
	xid, err := api.RenameBucket(bp, src, dst1)
	if err != nil && ensurePrevRebalanceIsFinished(bp, err) {
		// retry just once
		xid, err = api.RenameBucket(bp, src, dst1)
	}
	tassert.CheckFatal(t, err)
	tlog.Logfln("x-%s[%s] in progress...", apc.ActMoveBck, xid)

	// Try to copy src to dst1 - and note that rename src => dst1 in progress
	tlog.Logfln("Copy %s => %s (note: expecting to fail)", src.String(), dst1.String())
	_, err = api.CopyBucket(bp, src, dst1, nil)
	tassert.Fatalf(t, err != nil, "expected copy %s => %s to fail", src.String(), dst1.String())

	// Try to copy bucket that is being renamed
	tlog.Logfln("Copy %s => %s (note: expecting to fail)", src.String(), dst2.String())
	_, err = api.CopyBucket(bp, src, dst2, nil)
	tassert.Fatalf(t, err != nil, "expected copy %s => %s to fail", src.String(), dst2.String())

	// Try to copy from dst1 to dst1
	tlog.Logfln("Copy %s => %s (note: expecting to fail)", dst1.String(), dst2.String())
	_, err = api.CopyBucket(bp, src, dst1, nil)
	tassert.Fatalf(t, err != nil, "expected copy %s => %s to fail (as %s is the renaming destination)", dst1.String(), dst2.String(), dst1.String())

	// Wait for rename to finish
	tlog.Logfln("Waiting for x-%s[%s] to finish", apc.ActMoveBck, xid)
	time.Sleep(2 * time.Second)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	time.Sleep(time.Second)

	//
	// more checks
	//
	tlog.Logln("Listing and counting")
	bcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS}, apc.FltExists)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, !tools.BucketsContain(bcks, cmn.QueryBcks(src)), "expected %s to not exist (be renamed from), got %v", src.String(), bcks)
	tassert.Fatalf(t, tools.BucketsContain(bcks, cmn.QueryBcks(dst1)), "expected %s to exist (be renamed to), got %v", dst1.String(), bcks)
	tassert.Fatalf(t, !tools.BucketsContain(bcks, cmn.QueryBcks(dst2)), "expected %s to not exist (got %v)", dst2.String(), bcks)

	list, err := api.ListObjects(bp, dst1, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == m.num, "expected %s to have %d, got %d", dst1.String(), m.num, len(list.Entries))

	m.bck = dst1
	m.gets(nil, false)
	m.ensureNoGetErrors()
	m.bck = src
}

// Tries to copy and then rename bucket at the same time - similar to
// `TestRenameAndCopyBucket` but in different order of operations.
// TODO: This test should be enabled (not skipped)
func TestCopyAndRenameBucket(t *testing.T) {
	t.Skip("fails - necessary checks are not yet implemented")

	var (
		m = ioContext{
			t:   t,
			num: 500,
		}
		bp      = tools.BaseAPIParams()
		dstBck1 = cmn.Bck{
			Name:     testBucketName + "_new1",
			Provider: apc.AIS,
		}
		dstBck2 = cmn.Bck{
			Name:     testBucketName + "_new2",
			Provider: apc.AIS,
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)

	srcBck := m.bck
	tools.CreateBucket(t, m.proxyURL, srcBck, nil, true /*cleanup*/)
	defer func() {
		tools.DestroyBucket(t, m.proxyURL, dstBck1)
		tools.DestroyBucket(t, m.proxyURL, dstBck2)
	}()

	m.puts()

	// Rename to first destination
	tlog.Logfln("copy %s => %s", srcBck.String(), dstBck1.String())
	xid, err := api.CopyBucket(bp, srcBck, dstBck1, nil)
	tassert.CheckFatal(t, err)

	// Try to rename to first destination - copy in progress, both for srcBck and dstBck1
	tlog.Logfln("try rename %s => %s", srcBck.String(), dstBck1.String())
	_, err = api.RenameBucket(bp, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename to second destination - copy in progress for srcBck
	tlog.Logfln("try rename %s => %s", srcBck.String(), dstBck2.String())
	_, err = api.RenameBucket(bp, srcBck, dstBck2)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename from dstBck1 to dstBck1 - rename in progress for dstBck1
	tlog.Logfln("try rename %s => %s", dstBck1.String(), dstBck2.String())
	_, err = api.RenameBucket(bp, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Wait for copy to complete
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(bp, cmn.QueryBcks(srcBck), apc.FltExists)
	tassert.CheckFatal(t, err)

	if tools.BucketsContain(bcks, cmn.QueryBcks(srcBck)) {
		t.Error("source bucket found in buckets list")
	}
	if !tools.BucketsContain(bcks, cmn.QueryBcks(dstBck1)) {
		t.Error("destination bucket not found in buckets list")
	}
	if tools.BucketsContain(bcks, cmn.QueryBcks(dstBck2)) {
		t.Error("second (failed) destination bucket found in buckets list")
	}
}

func TestBackendBucket(t *testing.T) {
	var (
		remoteBck = cliBck
		aisBck    = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		m = ioContext{
			t:      t,
			num:    10,
			bck:    remoteBck,
			prefix: t.Name(),
		}

		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{CloudBck: true, Bck: remoteBck})

	m.init(true /*cleanup*/)

	tools.CreateBucket(t, proxyURL, aisBck, nil, true /*cleanup*/)

	p, err := api.HeadBucket(bp, remoteBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	remoteBck.Provider = p.Provider

	m.remotePuts(false /*evict*/)

	msg := &apc.LsoMsg{Prefix: m.prefix}
	remoteObjList, err := api.ListObjects(bp, remoteBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(remoteObjList.Entries) > 0, "empty object list")

	// Connect backend bucket to a aisBck
	_, err = api.SetBucketProps(bp, aisBck, &cmn.BpropsToSet{
		BackendBck: &cmn.BackendBckToSet{
			Name:     apc.Ptr(remoteBck.Name),
			Provider: apc.Ptr(remoteBck.Provider),
		},
	})
	tassert.CheckFatal(t, err)
	// Try putting one of the original remote objects, it should work.
	err = tools.PutObjRR(bp, aisBck, remoteObjList.Entries[0].Name, 128, cos.ChecksumNone)
	tassert.Errorf(t, err == nil, "expected err==nil (put to a BackendBck should be allowed via aisBck)")

	p, err = api.HeadBucket(bp, aisBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, p.BackendBck.Equal(&remoteBck),
		"backend bucket wasn't set correctly (got: %s, expected: %s)",
		p.BackendBck.String(), remoteBck.String(),
	)

	// Try to cache object.
	cachedObjName := remoteObjList.Entries[0].Name
	_, err = api.GetObject(bp, aisBck, cachedObjName, nil)
	tassert.CheckFatal(t, err)

	// Check if listing objects will result in listing backend bucket objects.
	msg.AddProps(apc.GetPropsAll...)
	aisObjList, err := api.ListObjects(bp, aisBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(remoteObjList.Entries) == len(aisObjList.Entries),
		"object lists remote vs ais does not match (got: %+v, expected: %+v)",
		aisObjList.Entries, remoteObjList.Entries,
	)

	// Check if cached listing works correctly.
	cacheMsg := &apc.LsoMsg{Flags: apc.LsCached, Prefix: m.prefix}
	aisObjList, err = api.ListObjects(bp, aisBck, cacheMsg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(aisObjList.Entries) == 1,
		"bucket contains incorrect number of cached objects (got: %+v, expected: [%s])",
		aisObjList.Entries, cachedObjName,
	)

	// Disallow PUT (TODO: use apc.AceObjUpdate instead, when/if supported)

	aattrs := apc.AccessAll &^ apc.AcePUT
	_, err = api.SetBucketProps(bp, aisBck, &cmn.BpropsToSet{
		BackendBck: &cmn.BackendBckToSet{
			Name:     apc.Ptr(""),
			Provider: apc.Ptr(""),
		},
		Access: apc.Ptr(aattrs),
	})
	tassert.CheckFatal(t, err)
	p, err = api.HeadBucket(bp, aisBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, p.BackendBck.IsEmpty(), "backend bucket is still configured: %s", p.BackendBck.String())

	// Check that we can still GET and list-objects
	_, err = api.GetObject(bp, aisBck, cachedObjName, nil)
	tassert.CheckFatal(t, err)

	aisObjList, err = api.ListObjects(bp, aisBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(aisObjList.Entries) == 1,
		"bucket contains incorrect number of objects (got: %+v, expected: [%s])",
		aisObjList.Entries, cachedObjName,
	)

	// Check that we cannot cold GET anymore - no backend
	tlog.Logln("Trying to cold-GET when there's no backend anymore (expecting to fail)")
	_, err = api.GetObject(bp, aisBck, remoteObjList.Entries[1].Name, nil)
	tassert.Fatalf(t, err != nil, "expected error (object should not exist)")

	// Check that we cannot do PUT anymore.
	tlog.Logln("Trying to PUT 2nd version (expecting to fail)")
	err = tools.PutObjRR(bp, aisBck, cachedObjName, 256, cos.ChecksumNone)
	tassert.Errorf(t, err != nil, "expected err != nil")
}

//
// even more checksum tests
//

func TestAllChecksums(t *testing.T) {
	checksums := cos.SupportedChecksums()
	for _, mirrored := range []bool{false, true} {
		for _, cksumType := range checksums {
			if testing.Short() && cksumType != cos.ChecksumNone && cksumType != cos.ChecksumOneXxh && cksumType != cos.ChecksumCesXxh {
				continue
			}
			tag := cksumType
			if mirrored {
				tag = cksumType + "/mirrored"
			}
			t.Run(tag, func(t *testing.T) {
				started := time.Now()
				testWarmValidation(t, cksumType, mirrored, false)
				tlog.Logfln("Time: %v", time.Since(started))
			})
		}
	}

	for _, cksumType := range checksums {
		if testing.Short() && cksumType != cos.ChecksumNone && cksumType != cos.ChecksumOneXxh && cksumType != cos.ChecksumCesXxh {
			continue
		}
		tag := cksumType + "/EC"
		t.Run(tag, func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 4})

			started := time.Now()
			testWarmValidation(t, cksumType, false, true)
			tlog.Logfln("Time: %v", time.Since(started))
		})
	}
}

func testWarmValidation(t *testing.T, cksumType string, mirrored, eced bool) {
	const (
		copyCnt     = 2
		parityCnt   = 2
		xactTimeout = 10 * time.Second
	)
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 1,
			fileSize:        cos.MiB/2 + rand.Uint64N(cos.KiB*10),
		}
		numCorrupted = rand.IntN(m.num/100) + 2
	)
	if testing.Short() {
		m.num = 40
		m.fileSize = cos.KiB + rand.Uint64N(cos.KiB)
		numCorrupted = 13
	}

	m.initAndSaveState(true /*cleanup*/)
	bp := tools.BaseAPIParams(m.proxyURL)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	switch {
	case mirrored:
		if m.chunksConf != nil && m.chunksConf.multipart {
			t.Skip("mirroring is not supported for chunked objects")
		}
		_, err := api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(cksumType),
				ValidateWarmGet: apc.Ptr(true),
			},
			Mirror: &cmn.MirrorConfToSet{
				Enabled: apc.Ptr(true),
				Copies:  apc.Ptr[int64](copyCnt),
			},
		})
		tassert.CheckFatal(t, err)
	case eced:
		if m.smap.CountActiveTs() < parityCnt+1 {
			t.Fatalf("Not enough targets to run %s test, must be at least %d", t.Name(), parityCnt+1)
		}
		_, err := api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(cksumType),
				ValidateWarmGet: apc.Ptr(true),
			},
			EC: &cmn.ECConfToSet{
				Enabled:      apc.Ptr(true),
				ObjSizeLimit: apc.Ptr[int64](cos.GiB), // only slices
				DataSlices:   apc.Ptr(1),
				ParitySlices: apc.Ptr(parityCnt),
			},
		})
		tassert.CheckFatal(t, err)
	default:
		_, err := api.SetBucketProps(bp, m.bck, &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(cksumType),
				ValidateWarmGet: apc.Ptr(true),
			},
		})
		tassert.CheckFatal(t, err)
	}

	p, err := api.HeadBucket(bp, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if p.Cksum.Type != cksumType {
		t.Fatalf("failed to set checksum: %q != %q", p.Cksum.Type, cksumType)
	}
	if !p.Cksum.ValidateWarmGet {
		t.Fatal("failed to set checksum: validate_warm_get not enabled")
	}
	if mirrored && !p.Mirror.Enabled {
		t.Fatal("failed to mirroring")
	}
	if eced && !p.EC.Enabled {
		t.Fatal("failed to enable erasure coding")
	}

	m.puts()

	// wait for mirroring
	if mirrored {
		args := xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: m.bck, Timeout: xactTimeout}
		api.WaitForSnapsIdle(bp, &args)
		// NOTE: ref 1377
		m.ensureNumCopies(bp, copyCnt, false /*greaterOk*/)
	}
	// wait for erasure-coding
	if eced {
		args := xact.ArgsMsg{Kind: apc.ActECPut, Bck: m.bck, Timeout: xactTimeout}
		api.WaitForSnapsIdle(bp, &args)
	}

	// read all
	if cksumType != cos.ChecksumNone {
		tlog.Logfln("Reading %q objects with checksum validation", m.bck.String())
	} else {
		tlog.Logfln("Reading %q objects", m.bck.String())
	}
	m.gets(nil, false)

	msg := &apc.LsoMsg{}
	bckObjs, err := api.ListObjects(bp, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	if len(bckObjs.Entries) == 0 {
		t.Errorf("%s is empty\n", m.bck.String())
		return
	}

	if cksumType != cos.ChecksumNone {
		tlog.Logfln("Reading %d objects from %s with end-to-end %s validation", len(bckObjs.Entries), m.bck.String(), cksumType)
		wg := cos.NewLimitedWaitGroup(20, 0)

		for _, en := range bckObjs.Entries {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				_, err = api.GetObjectWithValidation(bp, m.bck, name, nil)
				tassert.CheckError(t, err)
			}(en.Name)
		}

		wg.Wait()
	}

	if docker.IsRunning() {
		tlog.Logfln("skipping %s object corruption (docker is not supported)", t.Name())
		return
	}

	initMountpaths(t, proxyURL)
	// corrupt random and read again
	{
		i := rand.IntN(len(bckObjs.Entries))
		if i+numCorrupted > len(bckObjs.Entries) {
			i -= numCorrupted
		}
		objCh := make(chan string, numCorrupted)
		tlog.Logfln("Corrupting %d objects", numCorrupted)
		go func() {
			for j := i; j < i+numCorrupted; j++ {
				objName := bckObjs.Entries[j].Name
				corruptSingleBitInFile(&m, objName, eced)
				objCh <- objName
			}
		}()
		for range numCorrupted {
			objName := <-objCh
			_, err = api.GetObject(bp, m.bck, objName, nil)
			if mirrored || eced {
				if err != nil && cksumType != cos.ChecksumNone {
					if eced {
						// retry EC
						time.Sleep(2 * time.Second)
						_, err = api.GetObject(bp, m.bck, objName, nil)
					}
					if err != nil {
						t.Errorf("%s corruption detected but not resolved, mirror=%t, ec=%t\n",
							m.bck.Cname(objName), mirrored, eced)
					}
				}
			} else {
				if err == nil && cksumType != cos.ChecksumNone {
					t.Errorf("%s corruption undetected\n", m.bck.Cname(objName))
				}
			}
		}
	}
}

func TestBucketListAndSummary(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	type test struct {
		provider string
		summary  bool
		cached   bool
	}

	providers := []string{apc.AIS}
	if cliBck.IsRemote() {
		providers = append(providers, cliBck.Provider)
	}

	var tests []test
	for _, provider := range providers {
		for _, summary := range []bool{false, true} {
			for _, cached := range []bool{false, true} {
				tests = append(tests, test{
					provider: provider,
					summary:  summary,
					cached:   cached,
				})
			}
		}
	}

	for _, test := range tests {
		p := make([]string, 3)
		p[0] = test.provider
		p[1] = "list"
		if test.summary {
			p[1] = "summary"
		}
		p[2] = "all"
		if test.cached {
			p[2] = "cached"
		}
		t.Run(strings.Join(p, "/"), func(t *testing.T) {
			var (
				m = &ioContext{
					t: t,
					bck: cmn.Bck{
						Name:     trand.String(10),
						Provider: test.provider,
					},

					num: 12345,
				}
				bp = tools.BaseAPIParams()

				expectedFiles = m.num
				cacheSize     int
			)

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(1)

			switch {
			case m.bck.IsAIS():
				tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
				m.puts()
			case m.bck.IsRemote():
				m.bck = cliBck
				tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
				tlog.Logfln("remote %s", m.bck.Cname(""))
				m.del(-1 /* delete all */)

				m.num /= 10
				cacheSize = m.num / 2
				expectedFiles = m.num

				m.remotePuts(true /*evict*/)
				if test.cached {
					m.remotePrefetch(cacheSize)
					expectedFiles = cacheSize
				}
			default:
				t.Fatal("invalid provider", test.provider)
			}

			tlog.Logln("checking objects...")

			if test.summary {
				msg := &apc.BsummCtrlMsg{ObjCached: test.cached}
				xid, summaries, err := api.GetBucketSummary(bp, cmn.QueryBcks(m.bck), msg, api.BsummArgs{})
				tassert.CheckFatal(t, err)

				if len(summaries) == 0 {
					t.Fatalf("x-%s[%s] summary for bucket %q should exist", apc.ActSummaryBck, xid, m.bck.String())
				}
				if len(summaries) != 1 {
					t.Fatalf("x-%s[%s] number of summaries (%d) is larger than 1", apc.ActSummaryBck, xid, len(summaries))
				}

				summary := summaries[0]
				if summary.ObjCount.Remote+summary.ObjCount.Present != uint64(expectedFiles) {
					t.Errorf("x-%s[%s] %s: number of objects in summary (%+v) differs from expected (%d)",
						apc.ActSummaryBck, xid, m.bck.String(), summary.ObjCount, expectedFiles)
				}
			} else {
				msg := &apc.LsoMsg{PageSize: int64(min(m.num/3, 256))} // mult. pages
				if test.cached {
					msg.Flags = apc.LsCached
				}
				lst, err := api.ListObjects(bp, m.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				if len(lst.Entries) != expectedFiles {
					t.Errorf("number of listed objects (%d) is different from expected (%d)",
						len(lst.Entries), expectedFiles)
				}
			}
		})
	}
}

// Package integration_test.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
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
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	err := api.CreateBucket(baseParams, bck, nil)
	tassert.Fatalf(t, err != nil, "expected error")

	_, err = api.GetObject(baseParams, bck, "nonexisting", nil)
	tassert.Fatalf(t, err != nil, "expected error")

	_, err = api.ListObjects(baseParams, bck, nil, api.ListArgs{})
	tassert.Fatalf(t, err != nil, "expected error")

	reader, _ := readers.NewRand(cos.KiB, cos.ChecksumNone)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    "something",
		Reader:     reader,
	})
	tassert.Fatalf(t, err != nil, "expected error")
}

func TestListBuckets(t *testing.T) {
	var (
		bck        = cmn.Bck{Name: t.Name() + "Bucket", Provider: apc.AIS}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		pnums      = make(map[string]cmn.Bcks)
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks{}, apc.FltExists)
	tassert.CheckFatal(t, err)

	for provider := range apc.Providers {
		qbck := cmn.QueryBcks{Provider: provider}
		bcks := bcks.Select(qbck)
		tlog.Logf("%s:\t%2d bucket%s\n", apc.ToScheme(provider), len(bcks), cos.Plural(len(bcks)))
		pnums[provider] = bcks
	}

	backends, err := api.GetConfiguredBackends(baseParams)
	tassert.CheckFatal(t, err)
	tlog.Logf("configured backends: %v\n", backends)

	// tests: vs configured backend vs count
	for provider := range apc.Providers {
		configured := cos.StringInSlice(provider, backends)
		qbck := cmn.QueryBcks{Provider: provider}
		bcks, err := api.ListBuckets(baseParams, qbck, apc.FltExists)
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
		presbcks, err := api.ListBuckets(baseParams, qbck, apc.FltPresent)
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
			_, err := api.HeadBucket(baseParams, pbck, false /* don't add */)
			tassert.CheckFatal(t, err)

			presbcks, err = api.ListBuckets(baseParams, qbck, apc.FltPresent)
			tassert.CheckFatal(t, err)

			tlog.Logf("%s: now present %s\n", provider, pbck)
			t.Cleanup(func() {
				err = api.EvictRemoteBucket(baseParams, pbck, false /*keep md*/)
				tassert.CheckFatal(t, err)
				tlog.Logf("[cleanup] %s evicted\n", pbck)
			})
		}

		b := presbcks[0]
		err = api.EvictRemoteBucket(baseParams, b, false /*keep md*/)
		tassert.CheckFatal(t, err)

		evbcks, err := api.ListBuckets(baseParams, qbck, apc.FltPresent)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(presbcks) == len(evbcks)+1, "%s: expected one bucket less present after evicting %s (%d, %d)",
			provider, b, len(presbcks), len(evbcks))

		outbcks, err := api.ListBuckets(baseParams, qbck, apc.FltExistsOutside)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(outbcks) > 0, "%s: expected at least one (evicted) bucket to \"exist outside\"", provider)

		allbcks, err := api.ListBuckets(baseParams, qbck, apc.FltExistsNoProps)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(allbcks) == len(outbcks)+len(presbcks)-1,
			"%s: expected present + outside == all (%d, %d, %d)", provider, len(presbcks)-1, len(outbcks), len(allbcks))

		_, err = api.HeadBucket(baseParams, b, false /* don't add */)
		tassert.CheckFatal(t, err)
		presbcks2, err := api.ListBuckets(baseParams, qbck, apc.FltPresentNoProps)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(presbcks2) == len(presbcks), "%s: expected num present back to original (%d, %d)",
			provider, len(presbcks2), len(presbcks))
	}

	// tests: NsGlobal
	qbck := cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsGlobal}
	aisBuckets, err := api.ListBuckets(baseParams, qbck, apc.FltExists)
	tassert.CheckError(t, err)
	if len(aisBuckets) != len(bcks.Select(qbck)) {
		t.Fatalf("ais buckets: %d != %d\n", len(aisBuckets), len(bcks.Select(qbck)))
	}

	// tests: NsAnyRemote
	qbck = cmn.QueryBcks{Ns: cmn.NsAnyRemote}
	bcks, err = api.ListBuckets(baseParams, qbck, apc.FltExists)
	tassert.CheckError(t, err)
	qbck = cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}
	aisBuckets, err = api.ListBuckets(baseParams, qbck, apc.FltExists)
	tassert.CheckError(t, err)
	if len(aisBuckets) != len(bcks.Select(qbck)) {
		t.Fatalf("ais buckets: %d != %d\n", len(aisBuckets), len(bcks.Select(qbck)))
	}
}

// NOTE: for remote bucket, enter the bucket name directly (as TestMain makes it "present" at init time)
func TestGetBucketInfo(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
		isPresent  bool
	)
	if bck.IsRemote() {
		_, _, _, err := api.GetBucketInfo(baseParams, bck, &api.BinfoArgs{FltPresence: apc.FltPresent})
		isPresent = err == nil
	}
	for _, fltPresence := range fltPresentEnum {
		text := fltPresentText[fltPresence]
		tlog.Logf("%q %s\n", text, strings.Repeat("-", 60-len(text)))
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

		xid, props, info, err := api.GetBucketInfo(baseParams, bck, &args)
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
			tlog.Logf("%s: %s\n", bck.Cname(""), ps)

			is := "bucket-summary = nil"
			if info != nil {
				is = fmt.Sprintf("bucket-summary %+v", info.BsummResult)
			}
			tlog.Logf("x-%s[%s] %s: %s\n", apc.ActSummaryBck, xid, bck.Cname(""), is)
		}
		if bck.IsRemote() && !isPresent {
			// undo the side effect of calling api.GetBucketInfo
			_ = api.EvictRemoteBucket(baseParams, bck, false)
		}
		tlog.Logln("")
	}
	if bck.IsRemote() {
		_, _, _, err := api.GetBucketInfo(baseParams, bck, &api.BinfoArgs{FltPresence: apc.FltPresent})
		isPresentEnd := err == nil
		tassert.Errorf(t, isPresent == isPresentEnd, "presence in the beginning (%t) != (%t) at the end",
			isPresent, isPresentEnd)
	}
}

func TestDefaultBucketProps(t *testing.T) {
	const dataSlices = 7
	var (
		proxyURL     = tools.RandomProxyURL(t)
		baseParams   = tools.BaseAPIParams(proxyURL)
		globalConfig = tools.GetClusterConfig(t)
		bck          = cmn.Bck{Name: testBucketName, Provider: apc.AIS}
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

	p, err := api.HeadBucket(baseParams, bck, true /* don't add */)
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
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: testBucketName, Provider: apc.AIS}
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

	p, err := api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, propsToSet, p)
}

func TestCreateRemoteBucket(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})
	exists, _ := tools.BucketExists(nil, tools.GetPrimaryURL(), bck)
	tests := []struct {
		bck    cmn.Bck
		props  *cmn.BpropsToSet
		exists bool
	}{
		{bck: bck, exists: exists},
		{bck: cmn.Bck{Provider: cliBck.Provider, Name: trand.String(10)}},
	}
	for _, test := range tests {
		err := api.CreateBucket(baseParams, test.bck, test.props)
		if err == nil {
			continue
		}
		herr := cmn.Err2HTTPErr(err)
		tassert.Fatalf(t, herr != nil, "expected ErrHTTP, got %v (bucket %q)", err, test.bck)
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
	_, err := api.HeadBucket(baseParams, bck, true /* don't add */)
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

	err = api.DestroyBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	tlog.Logf("%s destroyed\n", bck.Cname(""))
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(bck), apc.FltExists)
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
		baseParams = tools.BaseAPIParams()
	)
	if testing.Short() {
		m.num = 50
	}
	m.init(true /*cleanup*/)
	m.smap = tools.GetClusterMap(m.t, m.proxyURL)

	for _, target := range m.smap.Tmap.ActiveNodes() {
		mpList, err := api.GetMountpaths(baseParams, target)
		tassert.CheckFatal(t, err)
		l := len(mpList.Available)
		tassert.Fatalf(t, l >= 2, "%s has %d mountpaths, need at least 2", target, l)
	}
	tlog.Logf("Create %s(mirrored, write-policy-md=%s)\n", m.bck, mdwrite)
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

	tlog.Logf("List %s\n", m.bck)
	msg := &apc.LsoMsg{Props: apc.GetPropsName}
	objList, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objList.Entries) == m.num, "expecting %d entries, have %d",
		m.num, len(objList.Entries))

	tlog.Logf("Overwrite %s objects with newer versions\n", m.bck)
	nsize := int64(m.fileSize) * 10
	for _, en := range objList.Entries {
		reader, err := readers.NewRand(nsize, cos.ChecksumNone)
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        m.bck,
			ObjName:    en.Name,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)
	}
	// wait for pending writes (of the copies)
	args := xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: m.bck}
	api.WaitForXactionIdle(baseParams, &args)

	tlog.Logf("List %s new versions\n", m.bck)
	msg = &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsAll...)
	objList, err = api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objList.Entries) == m.num, "expecting %d entries, have %d",
		m.num, len(objList.Entries))

	for _, en := range objList.Entries {
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
		baseParams = tools.BaseAPIParams()
		group, _   = errgroup.WithContext(context.Background())
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
				if err := api.CreateBucket(baseParams, m.bck, nil); err != nil {
					return err
				}
				if rand.IntN(iterCount) == 0 { // just test couple times, no need to flood
					if err := api.CreateBucket(baseParams, m.bck, nil); err == nil {
						return fmt.Errorf("expected error to occur on bucket %q - create second time", m.bck)
					}
				}
				m.puts()
				if _, err := api.ListObjects(baseParams, m.bck, nil, api.ListArgs{}); err != nil {
					return err
				}
				m.gets(nil, false)
				if err := api.DestroyBucket(baseParams, m.bck); err != nil {
					return err
				}
				if rand.IntN(iterCount) == 0 { // just test couple times, no need to flood
					if err := api.DestroyBucket(baseParams, m.bck); err == nil {
						return fmt.Errorf("expected error to occur on bucket %q - destroy second time", m.bck)
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
		baseParams   = tools.BaseAPIParams(proxyURL)
		bck          = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
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

	defaultProps, err := api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	_, err = api.SetBucketProps(baseParams, bck, propsToSet)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	// check that bucket props do get set
	validateBucketProps(t, propsToSet, p)
	_, err = api.ResetBucketProps(baseParams, bck)
	tassert.CheckFatal(t, err)

	p, err = api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	if !p.Equal(defaultProps) {
		t.Errorf("props have not been reset properly: expected: %+v, got: %+v", defaultProps, p)
	}
}

func TestSetInvalidBucketProps(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
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
			_, err := api.SetBucketProps(baseParams, bck, test.props)
			if err == nil {
				t.Error("expected error when setting bad input")
			}
		})
	}
}

func TestListObjectsRemoteBucketVersions(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      50,
			fileSize: 128,
			prefix:   trand.String(6) + "-",
		}
		baseParams = tools.BaseAPIParams()
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: m.bck})

	m.init(true /*cleanup*/)

	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(t, err)

	if !p.Versioning.Enabled {
		t.Skipf("%s requires a remote bucket with enabled versioning", t.Name())
	}

	m.puts()

	tlog.Logf("Listing %q objects\n", m.bck)
	msg := &apc.LsoMsg{Prefix: m.prefix}
	msg.AddProps(apc.GetPropsVersion, apc.GetPropsSize)
	bckObjs, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	tlog.Logf("Checking %q object versions [total: %d]\n", m.bck, len(bckObjs.Entries))
	for _, en := range bckObjs.Entries {
		tassert.Errorf(t, en.Size != 0, "object %s does not have size", en.Name)
		tassert.Errorf(t, en.Version != "", "object %s does not have version", en.Name)
	}
}

// Minimalistic list objects test to check that everything works correctly.
func TestListObjectsSmoke(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:                   t,
				num:                 100,
				bck:                 bck.Clone(),
				deleteRemoteBckObjs: true,
				fileSize:            5 * cos.KiB,
			}

			iters = 5
			msg   = &apc.LsoMsg{PageSize: 10}
		)

		m.init(true /*cleanup*/)
		m.puts()

		// Run couple iterations to see that we get deterministic results.
		tlog.Logf("run %d list objects iterations\n", iters)
		for iter := range iters {
			objList, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objList.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d) on iter: %d",
				len(objList.Entries), m.num, iter,
			)
		}
	})
}

func TestListObjectsGoBack(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      2000,
				bck:      bck.Clone(),
				fileSize: 128,
			}

			msg = &apc.LsoMsg{PageSize: 50}
		)

		if !bck.IsAIS() {
			m.num = 300
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		var (
			tokens          []string
			entries         cmn.LsoEntries
			expectedEntries cmn.LsoEntries
		)
		tlog.Logln("listing couple pages to move iterator on targets")
		for range m.num / int(msg.PageSize) {
			tokens = append(tokens, msg.ContinuationToken)
			objPage, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			expectedEntries = append(expectedEntries, objPage.Entries...)
		}

		tlog.Logln("list bucket's content in reverse order")

		for i := len(tokens) - 1; i >= 0; i-- {
			msg.ContinuationToken = tokens[i]
			objPage, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			entries = append(entries, objPage.Entries...)
		}

		cmn.SortLso(entries)
		cmn.SortLso(expectedEntries)

		tassert.Fatalf(
			t, len(expectedEntries) == m.num,
			"unexpected number of expected entries (got: %d, expected: %d)",
			len(expectedEntries), m.num,
		)

		tassert.Fatalf(
			t, len(entries) == len(expectedEntries),
			"unexpected number of entries (got: %d, expected: %d)",
			len(entries), len(expectedEntries),
		)

		for idx := range expectedEntries {
			tassert.Errorf(
				t, entries[idx].Name == expectedEntries[idx].Name,
				"unexpected en (got: %q, expected: %q)",
				entries[idx], expectedEntries[idx],
			)
		}
	})
}

func TestListObjectsRerequestPage(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:                   t,
				bck:                 bck.Clone(),
				deleteRemoteBckObjs: true,
				num:                 500,
				fileSize:            128,
			}
			rerequests = 5
		)

		if !bck.IsAIS() {
			m.num = 50
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		var (
			err     error
			objList *cmn.LsoRes

			totalCnt = 0
			msg      = &apc.LsoMsg{PageSize: 10}
		)
		tlog.Logln("starting rerequesting routine...")
		for {
			prevToken := msg.ContinuationToken
			for range rerequests {
				msg.ContinuationToken = prevToken
				objList, err = api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
			}
			totalCnt += len(objList.Entries)
			if objList.ContinuationToken == "" {
				break
			}
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
}

func TestListObjectsStartAfter(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      200,
				bck:      bck.Clone(),
				fileSize: 128,
			}
		)

		if !bck.IsAIS() {
			m.num = 20
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		objList, err := api.ListObjects(baseParams, m.bck, nil, api.ListArgs{})
		tassert.CheckFatal(t, err)

		middleObjName := objList.Entries[m.num/2-1].Name
		tlog.Logf("start listing bucket after: %q...\n", middleObjName)

		msg := &apc.LsoMsg{PageSize: 10, StartAfter: middleObjName}
		objList, err = api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
		if bck.IsAIS() {
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objList.Entries) == m.num/2,
				"unexpected number of entries (got: %d, expected: %d)",
				len(objList.Entries), m.num/2,
			)
		} else if err != nil {
			herr := cmn.Err2HTTPErr(err)
			tlog.Logf("Error is expected here, got %q\n", herr)
		} else {
			tassert.Errorf(t, false, "expected an error, got nil")
		}
	})
}

func TestListObjectsProps(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:                   t,
				num:                 rand.IntN(5000) + 1000,
				bck:                 bck.Clone(),
				fileSize:            128,
				deleteRemoteBckObjs: true,
			}
			remoteVersioning bool
		)

		if !bck.IsAIS() {
			m.num = rand.IntN(250) + 100
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()

			s := "disabled"
			p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
			tassert.CheckFatal(t, err)
			if remoteVersioning = p.Versioning.Enabled; remoteVersioning {
				s = "enabled"
			}
			tlog.Logf("%s: versioning is %s\n", m.bck.Cname(""), s)
		}
		checkProps := func(useCache bool, props []string, f func(en *cmn.LsoEnt)) {
			msg := &apc.LsoMsg{PageSize: 100}
			if useCache {
				msg.SetFlag(apc.UseListObjsCache)
			}
			msg.AddProps(props...)
			objList, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objList.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
			)
			for _, en := range objList.Entries {
				tassert.Errorf(t, en.Name != "", "name is not set")
				f(en)
			}
		}

		for _, useCache := range []bool{false, true} {
			tlog.Logf("[cache=%t] trying empty (minimal) subset of props...\n", useCache)
			checkProps(useCache, []string{}, func(en *cmn.LsoEnt) {
				tassert.Errorf(t, en.Name != "", "name is not set")
				tassert.Errorf(t, en.Size != 0, "size is not set")

				tassert.Errorf(t, en.Atime == "", "atime is set")
				tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
				tassert.Errorf(t, en.Copies == 0, "copies is set")
			})

			tlog.Logf("[cache=%t] trying ais-default subset of props...\n", useCache)
			checkProps(useCache, apc.GetPropsDefaultAIS, func(en *cmn.LsoEnt) {
				tassert.Errorf(t, en.Size != 0, "size is not set")
				tassert.Errorf(t, en.Checksum != "", "checksum is not set")
				tassert.Errorf(t, en.Atime != "", "atime is not set")

				tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
				tassert.Errorf(t, en.Copies == 0, "copies is set")
			})

			tlog.Logf("[cache=%t] trying cloud-default subset of props...\n", useCache)
			checkProps(useCache, apc.GetPropsDefaultCloud, func(en *cmn.LsoEnt) {
				tassert.Errorf(t, en.Size != 0, "size is not set")
				tassert.Errorf(t, en.Checksum != "", "checksum is not set")
				if bck.IsAIS() || remoteVersioning {
					tassert.Errorf(t, en.Version != "", "version is not set")
				}
				tassert.Errorf(t, !m.bck.IsCloud() || en.Custom != "", "custom is not set")

				tassert.Errorf(t, en.Atime == "", "atime is set")
				tassert.Errorf(t, en.Copies == 0, "copies is set")
			})

			tlog.Logf("[cache=%t] trying specific subset of props...\n", useCache)
			checkProps(useCache,
				[]string{apc.GetPropsChecksum, apc.GetPropsVersion, apc.GetPropsCopies}, func(en *cmn.LsoEnt) {
					tassert.Errorf(t, en.Checksum != "", "checksum is not set")
					if bck.IsAIS() || remoteVersioning {
						tassert.Error(t, en.Version != "", "version is not set: "+m.bck.Cname(en.Name))
					}
					tassert.Error(t, en.Copies > 0, "copies is not set")

					tassert.Error(t, en.Atime == "", "atime is set")
					tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
				})

			tlog.Logf("[cache=%t] trying small subset of props...\n", useCache)
			checkProps(useCache, []string{apc.GetPropsSize}, func(en *cmn.LsoEnt) {
				tassert.Errorf(t, en.Size != 0, "size is not set")

				tassert.Errorf(t, en.Atime == "", "atime is set")
				tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
				tassert.Errorf(t, en.Copies == 0, "copies is set")
			})

			tlog.Logf("[cache=%t] trying all props...\n", useCache)
			checkProps(useCache, apc.GetPropsAll, func(en *cmn.LsoEnt) {
				tassert.Errorf(t, en.Size != 0, "size is not set")
				if bck.IsAIS() || remoteVersioning {
					tassert.Error(t, en.Version != "", "version is not set: "+m.bck.Cname(en.Name))
				}
				tassert.Errorf(t, en.Checksum != "", "checksum is not set")
				tassert.Errorf(t, en.Atime != "", "atime is not set")
				tassert.Errorf(t, en.Location != "", "target location is not set [%#v]", en)
				tassert.Errorf(t, en.Copies != 0, "copies is not set")
			})
		}
	})
}

// Runs remote list objects with `cached == true` (for both evicted and not evicted objects).
func TestListObjectsRemoteCached(t *testing.T) {
	var (
		baseParams = tools.BaseAPIParams()
		m          = ioContext{
			t:        t,
			bck:      cliBck,
			num:      rand.IntN(100) + 10,
			fileSize: 128,
		}

		remoteVersioning bool
		s                = "disabled"
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(t, err)
	if remoteVersioning = p.Versioning.Enabled; remoteVersioning {
		s = "enabled"
	}
	tlog.Logf("%s: versioning is %s\n", m.bck.Cname(""), s)

	m.init(true /*cleanup*/)

	for _, evict := range []bool{false, true} {
		tlog.Logf("list remote objects with evict=%t\n", evict)
		m.remotePuts(evict)

		msg := &apc.LsoMsg{PageSize: 10, Flags: apc.LsObjCached}
		msg.AddProps(apc.GetPropsDefaultAIS...)
		msg.AddProps(apc.GetPropsVersion)

		objList, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)
		if evict {
			tassert.Errorf(
				t, len(objList.Entries) == 0,
				"unexpected number of entries (got: %d, expected: 0)", len(objList.Entries),
			)
		} else {
			tassert.Errorf(
				t, len(objList.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
			)
			for _, en := range objList.Entries {
				tassert.Errorf(t, en.Name != "", "name is not set")
				tassert.Errorf(t, en.Size != 0, "size is not set")
				tassert.Errorf(t, en.Checksum != "", "checksum is not set")
				tassert.Errorf(t, en.Atime != "", "atime is not set")
				if remoteVersioning {
					tassert.Errorf(t, en.Version != "", "version is not set")
				}
				tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
				tassert.Errorf(t, en.Copies == 0, "copies is set")
			}
		}
	}
}

// Runs standard list objects but selects new random proxy every page.
func TestListObjectsRandProxy(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			m = ioContext{
				t:                   t,
				bck:                 bck.Clone(),
				num:                 rand.IntN(5000) + 1000,
				fileSize:            5 * cos.KiB,
				deleteRemoteBckObjs: true,
			}

			totalCnt = 0
			msg      = &apc.LsoMsg{PageSize: 100}
		)

		if !bck.IsAIS() {
			m.num = rand.IntN(300) + 100
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		for {
			baseParams := tools.BaseAPIParams()
			objList, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			totalCnt += len(objList.Entries)
			if objList.ContinuationToken == "" {
				break
			}
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
}

// Runs standard list objects but changes the page size every request.
func TestListObjectsRandPageSize(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			totalCnt   int
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:        t,
				bck:      bck.Clone(),
				num:      rand.IntN(5000) + 1000,
				fileSize: 128,
			}
			msg = &apc.LsoMsg{Flags: apc.LsObjCached}
		)

		if !bck.IsAIS() {
			m.num = rand.IntN(200) + 100
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		for {
			msg.PageSize = rand.Int64N(50) + 50

			objList, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			totalCnt += len(objList.Entries)
			if objList.ContinuationToken == "" {
				break
			}
			tassert.Errorf(t, len(objList.Entries) == int(msg.PageSize), "wrong page size %d (expected %d)",
				len(objList.Entries), msg.PageSize,
			)
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
}

func TestListObjects(t *testing.T) {
	type objEntry struct {
		name string
		size int64
	}

	var (
		iterations  = 10
		workerCount = 10
		dirLen      = 10

		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: apc.AIS,
		}
		wg = &sync.WaitGroup{}

		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	if testing.Short() {
		iterations = 3
	}

	tests := []struct {
		pageSize int64
	}{
		{pageSize: 0},
		{pageSize: 2000},
		{pageSize: rand.Int64N(15000)},
	}

	for _, test := range tests {
		var name string
		if test.pageSize == 0 {
			name = "pagesize:default"
		} else {
			name += "pagesize:" + strconv.FormatUint(uint64(test.pageSize), 10)
		}
		t.Run(name, func(t *testing.T) {
			var (
				objs     sync.Map
				prefixes sync.Map
			)

			tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

			p := bck.DefaultProps(initialClusterConfig)

			totalObjects := 0
			for iter := 1; iter <= iterations; iter++ {
				tlog.Logf("listing iteration: %d/%d (total_objs: %d)\n", iter, iterations, totalObjects)
				objectCount := rand.IntN(800) + 1010
				totalObjects += objectCount
				for wid := range workerCount {
					wg.Add(1)
					go func(wid int) {
						defer wg.Done()
						objectSize := int64(rand.IntN(256) + 20)
						objDir := tools.RandomObjDir(dirLen, 5)
						objectsToPut := objectCount / workerCount
						if wid == workerCount-1 { // last worker puts leftovers
							objectsToPut += objectCount % workerCount
						}
						objNames := tools.PutRR(t, baseParams, objectSize, p.Cksum.Type, bck, objDir, objectsToPut)
						for _, objName := range objNames {
							objs.Store(objName, objEntry{
								name: objName,
								size: objectSize,
							})
						}

						if objDir != "" {
							prefixes.Store(objDir, objectsToPut)
						}
					}(wid)
				}
				wg.Wait()

				// Confirm PUTs by listing objects.
				msg := &apc.LsoMsg{PageSize: test.pageSize}
				msg.AddProps(apc.GetPropsChecksum, apc.GetPropsAtime, apc.GetPropsVersion, apc.GetPropsCopies, apc.GetPropsSize)
				tassert.CheckError(t, api.ListObjectsInvalidateCache(baseParams, bck))
				lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				if lst.ContinuationToken != "" {
					t.Errorf("continuation token was unexpectedly set to: %s", lst.ContinuationToken)
				}

				empty := &cmn.LsoEnt{}
				for _, en := range lst.Entries {
					e, exists := objs.Load(en.Name)
					if !exists {
						t.Errorf("failed to locate %s", bck.Cname(en.Name))
						continue
					}

					obj := e.(objEntry)
					if obj.size != en.Size {
						t.Errorf(
							"sizes do not match for object %s, expected: %d, got: %d",
							obj.name, obj.size, en.Size,
						)
					}

					if en.Version == empty.Version {
						t.Errorf("%s version is empty (not set)", bck.Cname(en.Name))
					} else if en.Checksum == empty.Checksum ||
						en.Atime == empty.Atime ||
						en.Flags == empty.Flags ||
						en.Copies == empty.Copies {
						t.Errorf("some fields of %s are empty (not set): %#v", bck.Cname(en.Name), en)
					}
				}

				// Check if names in the entries are unique.
				objs.Range(func(key, _ any) bool {
					objName := key.(string)
					i := sort.Search(len(lst.Entries), func(i int) bool {
						return lst.Entries[i].Name >= objName
					})
					if i == len(lst.Entries) || lst.Entries[i].Name != objName {
						t.Errorf("object %s was not found in the result of bucket listing", objName)
					}
					return true
				})

				if len(lst.Entries) != totalObjects {
					t.Fatalf("actual objects %d, expected: %d", len(lst.Entries), totalObjects)
				}

				// Check listing bucket with predefined prefix.
				prefixes.Range(func(key, value any) bool {
					prefix := key.(string)
					expectedObjCount := value.(int)

					msg := &apc.LsoMsg{
						Prefix: prefix,
					}
					lst, err = api.ListObjects(baseParams, bck, msg, api.ListArgs{})
					tassert.CheckFatal(t, err)

					if expectedObjCount != len(lst.Entries) {
						t.Errorf(
							"(prefix: %s), actual objects %d, expected: %d",
							prefix, len(lst.Entries), expectedObjCount,
						)
					}

					for _, en := range lst.Entries {
						if !strings.HasPrefix(en.Name, prefix) {
							t.Errorf("object %q does not have expected prefix: %q", en.Name, prefix)
						}
					}
					return true
				})
			}

			tassert.CheckError(t, api.ListObjectsInvalidateCache(baseParams, bck))
		})
	}
}

func TestListObjectsPrefix(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	providers := []string{apc.AIS}
	if cliBck.IsRemote() {
		providers = append(providers, cliBck.Provider)
	}

	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			const objCnt = 30
			var (
				customPage = true
				bck        cmn.Bck
			)
			bckTest := cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}
			if bckTest.IsRemote() {
				bck = cliBck

				tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})

				bckProp, err := api.HeadBucket(baseParams, bck, false /* don't add */)
				tassert.CheckFatal(t, err)
				customPage = bckProp.Provider != apc.Azure

				tlog.Logf("Cleaning up the remote bucket %s\n", bck)
				lst, err := api.ListObjects(baseParams, bck, nil, api.ListArgs{})
				tassert.CheckFatal(t, err)
				for _, en := range lst.Entries {
					err := tools.Del(proxyURL, bck, en.Name, nil, nil, false /*silent*/)
					tassert.CheckFatal(t, err)
				}
			} else {
				bck = cmn.Bck{Name: testBucketName, Provider: provider}
				tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
			}

			objNames := make([]string, 0, objCnt)

			t.Cleanup(func() {
				for _, objName := range objNames {
					err := tools.Del(proxyURL, bck, objName, nil, nil, true /*silent*/)
					tassert.CheckError(t, err)
				}
			})

			for i := range objCnt {
				objName := fmt.Sprintf("prefix/obj%d", i+1)
				objNames = append(objNames, objName)

				r, _ := readers.NewRand(fileSize, cos.ChecksumNone)
				_, err := api.PutObject(&api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    objName,
					Reader:     r,
					Size:       fileSize,
				})
				tassert.CheckFatal(t, err)
			}

			tests := []struct {
				name     string
				prefix   string
				pageSize int64
				limit    int64
				expected int
			}{
				{
					"full_list_default_pageSize_no_limit",
					"prefix", 0, 0,
					objCnt,
				},
				{
					"full_list_small_pageSize_no_limit",
					"prefix", objCnt / 7, 0,
					objCnt,
				},
				{
					"full_list_limited",
					"prefix", 0, 8,
					8,
				},
				{
					"full_list_prefixed",
					"prefix/obj1", 0, 0,
					11, // obj1 and obj10..obj19
				},
				{
					"full_list_overlimited_prefixed",
					"prefix/obj1", 0, 20,
					11, // obj1 and obj10..obj19
				},
				{
					"full_list_limited_prefixed",
					"prefix/obj1", 0, 2,
					2, // obj1 and obj10
				},
				{
					"empty_list_prefixed",
					"prefix/nothing", 0, 0,
					0,
				},
			}

			for _, test := range tests {
				if test.pageSize != 0 && !customPage {
					tlog.Logf("Bucket %s does not support custom paging, skipping...\n", bck)
					continue
				}
				t.Run(test.name, func(t *testing.T) {
					tlog.Logf("Prefix: %q, Expected objects: %d\n", test.prefix, test.expected)
					msg := &apc.LsoMsg{PageSize: test.pageSize, Prefix: test.prefix}
					tlog.Logf(
						"list_objects %s [prefix: %q, page_size: %d]\n",
						bck, msg.Prefix, msg.PageSize,
					)

					lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{Limit: test.limit})
					tassert.CheckFatal(t, err)

					tlog.Logf("list_objects output: %d objects\n", len(lst.Entries))

					if len(lst.Entries) != test.expected {
						t.Errorf("returned %d objects instead of %d", len(lst.Entries), test.expected)
					}
				})
			}
		})
	}
}

func TestListObjectsCache(t *testing.T) {
	var (
		baseParams = tools.BaseAPIParams()
		m          = ioContext{
			t:        t,
			num:      rand.IntN(3000) + 1481,
			fileSize: cos.KiB,
		}
		totalIters = 10
	)

	if testing.Short() {
		m.num = 250 + rand.IntN(500)
		totalIters = 5
	}

	m.init(true /*cleanup*/)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	for _, useCache := range []bool{true, false} {
		t.Run(fmt.Sprintf("cache=%t", useCache), func(t *testing.T) {
			// Do it N times - first: fill the cache; next calls: use it.
			for iter := range totalIters {
				var (
					started = time.Now()
					msg     = &apc.LsoMsg{PageSize: rand.Int64N(20) + 4}
				)
				if useCache {
					msg.SetFlag(apc.UseListObjsCache)
				}
				objList, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				tlog.Logf(
					"[iter: %d] cache: %5t, page_size: %d, time: %s\n",
					iter, useCache, msg.PageSize, time.Since(started),
				)

				tassert.Errorf(
					t, len(objList.Entries) == m.num,
					"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
				)
			}

			if useCache {
				err := api.ListObjectsInvalidateCache(baseParams, m.bck)
				tassert.CheckError(t, err)
			}
		})
	}
}

func TestListObjectsWithRebalance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		baseParams = tools.BaseAPIParams()
		wg         = &sync.WaitGroup{}
		m          = ioContext{
			t:        t,
			num:      10000,
			fileSize: 128,
		}
		rebID string
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(2)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	target := m.startMaintenanceNoRebalance()

	m.puts()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rebID = m.stopMaintenance(target)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 15 {
			tlog.Logf("listing all objects, iter: %d\n", i)
			lst, err := api.ListObjects(baseParams, m.bck, nil, api.ListArgs{})
			tassert.CheckFatal(t, err)
			if lst.Flags == 0 {
				tassert.Errorf(t, len(lst.Entries) == m.num, "entries mismatch (%d vs %d)", len(lst.Entries), m.num)
			} else if len(lst.Entries) != m.num {
				tlog.Logf("List objects while rebalancing: %d vs %d\n", len(lst.Entries), m.num)
			}

			time.Sleep(time.Second)
		}
	}()

	wg.Wait()
	m.waitAndCheckCluState()
	tools.WaitForRebalanceByID(t, baseParams, rebID)
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
		baseParams = tools.BaseAPIParams()
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(3)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	tlog.Logf("Changing bucket %q properties...\n", m.bck)

	// Enabling EC should set default value for number of slices if it is 0
	_, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckError(t, err)
	p, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
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
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)

	// Enabling mirroring should set default value for number of copies if it is 0
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(baseParams, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if !p.Mirror.Enabled {
		t.Error("Mirroring was not enabled")
	}
	if p.Mirror.Copies != 2 {
		t.Errorf("Number of copies is incorrect: %d (expected 2)", p.Mirror.Copies)
	}

	// Need to disable mirroring first
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)

	// Change a few more bucket properties
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{
			DataSlices:   apc.Ptr(dataSlices),
			ParitySlices: apc.Ptr(paritySlices),
			ObjSizeLimit: apc.Ptr[int64](objLimit),
		},
	})
	tassert.CheckError(t, err)

	// Enable EC again
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(baseParams, m.bck, true /* don't add */)
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
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)

	// Change mirroring threshold
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Burst: apc.Ptr(burst)},
	},
	)
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(baseParams, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if p.Mirror.Burst != burst {
		t.Errorf("Mirror burst was not changed to %d. Current value %d", burst, p.Mirror.Burst)
	}

	// Disable mirroring
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)
}

func TestSetBucketPropsOfNonexistentBucket(t *testing.T) {
	baseParams := tools.BaseAPIParams()
	bucket, err := tools.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cliBck.Provider,
	}

	_, err = api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
	})
	if err == nil {
		t.Fatalf("Expected SetBucketProps error, but got none.")
	}

	status := api.HTTPStatus(err)
	if status < http.StatusBadRequest {
		t.Errorf("Expected status: %d, got %d", http.StatusNotFound, status)
	}
}

func TestSetAllBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		baseParams  = tools.BaseAPIParams()
		bucketProps = &cmn.BpropsToSet{}
	)

	bucket, err := tools.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cliBck.Provider,
	}

	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	if err == nil {
		t.Fatalf("Expected SetBucketProps error, but got none.")
	}

	status := api.HTTPStatus(err)
	if status < http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, status)
	}
}

func TestBucketInvalidName(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	invalidNames := []string{"*", ".", "", " ", "bucket and name", "bucket/name", "#name", "$name", "~name"}
	for _, name := range invalidNames {
		bck := cmn.Bck{
			Name:     name,
			Provider: apc.AIS,
		}
		if err := api.CreateBucket(baseParams, bck, nil); err == nil {
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
		baseParams := tools.BaseAPIParams()
		xid, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
			Mirror: &cmn.MirrorConfToSet{
				Enabled: apc.Ptr(true),
			},
		})
		tassert.CheckFatal(t, err)

		p, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, p.Mirror.Copies == 2, "%d copies != 2", p.Mirror.Copies)

		// Even though the bucket is empty, it can take a short while until the
		// xaction is propagated and finished.
		reqArgs := xact.ArgsMsg{ID: xid, Kind: apc.ActMakeNCopies, Bck: m.bck, Timeout: xactTimeout}
		_, err = api.WaitForXactionIC(baseParams, &reqArgs)
		tassert.CheckFatal(t, err)
	}

	m.puts()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.gets(nil, false)
	}()

	baseParams := tools.BaseAPIParams(m.proxyURL)

	xargs := xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: m.bck, Timeout: xactTimeout}
	_, _ = api.WaitForXactionIC(baseParams, &xargs)

	for _, copies := range numCopies {
		makeNCopies(t, baseParams, m.bck, copies)
	}

	// wait for all GETs to complete
	wg.Wait()

	m.ensureNumCopies(baseParams, numCopies[len(numCopies)-1], false /*greaterOk*/)
}

func makeNCopies(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, ncopies int) {
	tlog.Logf("Set copies = %d\n", ncopies)

	xid, err := api.MakeNCopies(baseParams, bck, ncopies)
	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMakeNCopies}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	args = xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: bck}
	api.WaitForXactionIdle(baseParams, &args)
}

func TestRemoteBucketMirror(t *testing.T) {
	var (
		m = &ioContext{
			t:      t,
			num:    128,
			bck:    cliBck,
			prefix: t.Name(),
		}
		baseParams = tools.BaseAPIParams()
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	m.init(true /*cleanup*/)
	m.remotePuts(true /*evict*/)

	// enable mirror
	_, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckFatal(t, err)
	defer api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(false)},
	})

	// list
	msg := &apc.LsoMsg{Prefix: m.prefix, Props: apc.GetPropsName}
	objectList, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(objectList.Entries) == m.num,
		"wrong number of objects in the remote bucket %s: need %d, got %d",
		m.bck, m.num, len(objectList.Entries),
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{MinMountpaths: 4})

	// cold GET - causes local mirroring
	m.remotePrefetch(m.num)
	m.ensureNumCopies(baseParams, 2, false /*greaterOk*/)
	time.Sleep(3 * time.Second)

	// Increase number of copies
	makeNCopies(t, baseParams, m.bck, 3)
	m.ensureNumCopies(baseParams, 3, false /*greaterOk*/)
}

func TestBucketReadOnly(t *testing.T) {
	m := ioContext{
		t:               t,
		num:             10,
		numGetsEachFile: 2,
	}
	m.init(true /*cleanup*/)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	baseParams := tools.BaseAPIParams()

	m.puts()
	m.gets(nil, false)

	p, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	// make bucket read-only
	// NOTE: must allow PATCH - otherwise api.SetBucketProps a few lines down below won't work
	aattrs := apc.AccessRO | apc.AcePATCH
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{Access: apc.Ptr(aattrs)})
	tassert.CheckFatal(t, err)

	m.init(true /*cleanup*/)
	m.puts(true /*ignoreErr*/)
	tassert.Fatalf(t, m.numPutErrs == m.num, "num failed PUTs %d, expecting %d", m.numPutErrs, m.num)

	// restore write access
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{Access: apc.Ptr(p.Access)})
	tassert.CheckFatal(t, err)

	// write some more and destroy
	m.init(true /*cleanup*/)
	m.puts(true /*ignoreErr*/)
	tassert.Fatalf(t, m.numPutErrs == 0, "num failed PUTs %d, expecting 0 (zero)", m.numPutErrs)
}

func TestRenameBucketEmpty(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t: t,
		}
		baseParams = tools.BaseAPIParams()
		dstBck     = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.AIS,
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
	srcProps, err := api.HeadBucket(baseParams, srcBck, true /* don't add */)
	tassert.CheckFatal(t, err)

	// Rename it
	tlog.Logf("rename %s => %s\n", srcBck, dstBck)
	uuid, err := api.RenameBucket(baseParams, srcBck, dstBck)
	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: uuid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)

	if !tools.BucketsContain(bcks, cmn.QueryBcks(dstBck)) {
		t.Error("new bucket not found in buckets list")
	}

	tlog.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if !srcProps.Equal(dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameBucketNonEmpty(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 2,
		}
		baseParams = tools.BaseAPIParams()
		dstBck     = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: apc.AIS,
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
	srcProps, err := api.HeadBucket(baseParams, srcBck, true /* don't add */)
	tassert.CheckFatal(t, err)

	// Put some files
	m.puts()

	// Rename it
	tlog.Logf("rename %s => %s\n", srcBck, dstBck)
	m.bck = dstBck
	xid, err := api.RenameBucket(baseParams, srcBck, dstBck)
	if err != nil && ensurePrevRebalanceIsFinished(baseParams, err) {
		// can retry
		xid, err = api.RenameBucket(baseParams, srcBck, dstBck)
	}

	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Gets on renamed ais bucket
	m.gets(nil, false)
	m.ensureNoGetErrors()

	tlog.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBck, true /* don't add */)
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
		baseParams = tools.BaseAPIParams()
		tmpBck     = cmn.Bck{
			Name:     "tmp_bck_name",
			Provider: apc.AIS,
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	m.setNonDefaultBucketProps()
	srcProps, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	tools.CreateBucket(t, m.proxyURL, tmpBck, nil, true /*cleanup*/)

	// rename
	tlog.Logf("try rename %s => %s (that already exists)\n", m.bck, tmpBck)
	if _, err := api.RenameBucket(baseParams, m.bck, tmpBck); err == nil {
		t.Fatal("expected an error renaming already existing bucket")
	}

	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)

	if !tools.BucketsContain(bcks, cmn.QueryBcks(m.bck)) || !tools.BucketsContain(bcks, cmn.QueryBcks(tmpBck)) {
		t.Errorf("one of the buckets (%s, %s) was not found in the list %+v", m.bck, tmpBck, bcks)
	}

	dstProps, err := api.HeadBucket(baseParams, tmpBck, true /* don't add */)
	tassert.CheckFatal(t, err)

	if srcProps.Equal(dstProps) {
		t.Fatalf("source and destination props (checksums, in particular) are not expected to match: %v vs %v",
			srcProps.Cksum, dstProps.Cksum)
	}
}

// Tries to rename same source bucket to two destination buckets - the second should fail.
func TestRenameBucketTwice(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:   t,
			num: 500,
		}
		baseParams = tools.BaseAPIParams()
		dstBck1    = cmn.Bck{
			Name:     testBucketName + "_new1",
			Provider: apc.AIS,
		}
		dstBck2 = cmn.Bck{
			Name:     testBucketName + "_new2",
			Provider: apc.AIS,
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.proxyURL = tools.RandomProxyURL(t)
	m.expectTargets(1)

	srcBck := m.bck
	tools.CreateBucket(t, m.proxyURL, srcBck, nil, true /*cleanup*/)
	defer func() {
		// This bucket should not be present (thus ignoring error) but
		// try to delete in case something failed.
		api.DestroyBucket(baseParams, dstBck2)
		// This one should be present.
		tools.DestroyBucket(t, m.proxyURL, dstBck1)
	}()

	m.puts()

	// Rename to first destination
	tlog.Logf("rename %s => %s\n", srcBck, dstBck1)
	xid, err := api.RenameBucket(baseParams, srcBck, dstBck1)
	if err != nil && ensurePrevRebalanceIsFinished(baseParams, err) {
		// can retry
		xid, err = api.RenameBucket(baseParams, srcBck, dstBck1)
	}
	tassert.CheckFatal(t, err)

	// Try to rename to first destination again - already in progress
	tlog.Logf("try renaming %s => %s\n", srcBck, dstBck1)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("multiple rename operations on same bucket should fail")
	}

	// Try to rename to second destination - this should fail
	tlog.Logf("try rename %s => %s\n", srcBck, dstBck2)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("multiple rename operations on same bucket should fail")
	}

	// Wait for rename to complete
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
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

func TestRenameBucketNonExistentSrc(t *testing.T) {
	var (
		m = ioContext{
			t: t,
		}
		baseParams = tools.BaseAPIParams()
		dstBck     = cmn.Bck{
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
		_, err := api.RenameBucket(baseParams, srcBck, dstBck)
		tools.CheckErrIsNotFound(t, err)
		_, err = api.HeadBucket(baseParams, dstBck, true /* don't add */)
		tools.CheckErrIsNotFound(t, err)
	}
}

func TestRenameBucketWithBackend(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{CloudBck: true, Bck: cliBck})

	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
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

	srcProps, err := api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	xid, err := api.RenameBucket(baseParams, bck, dstBck)
	if err != nil && ensurePrevRebalanceIsFinished(baseParams, err) {
		// can retry
		xid, err = api.RenameBucket(baseParams, bck, dstBck)
	}

	tassert.CheckFatal(t, err)
	xargs := xact.ArgsMsg{ID: xid}
	_, err = api.WaitForXactionIC(baseParams, &xargs)
	tassert.CheckFatal(t, err)

	exists, err := api.QueryBuckets(baseParams, cmn.QueryBcks(bck), apc.FltPresent)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !exists, "source bucket shouldn't exist")

	tlog.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBck, true /* don't add */)
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
	}{
		// ais -> ais
		{srcRemote: false, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: false},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: false, multipleDests: false, onlyLong: true},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: false, onlyLong: true},
		{srcRemote: false, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true, onlyLong: true},
		{srcRemote: false, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true, onlyLong: true},

		// remote -> ais
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: false},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: true},
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true},

		// evicted remote -> ais
		{srcRemote: true, dstRemote: false, dstBckExist: false, dstBckHasObjects: false, evictRemoteSrc: true},
		{srcRemote: true, dstRemote: false, dstBckExist: true, dstBckHasObjects: false, evictRemoteSrc: true},

		// ais -> remote
		{srcRemote: false, dstRemote: true, dstBckExist: true, dstBckHasObjects: false},
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
					},
				}
				dstms = []*ioContext{
					{
						t:   t,
						num: objCnt,
						bck: cmn.Bck{
							Name:     "dst_copy_bck_1",
							Provider: apc.AIS,
						},
					},
				}
				baseParams = tools.BaseAPIParams()
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

			srcProps, err := api.HeadBucket(baseParams, srcm.bck, true /* don't add */)
			tassert.CheckFatal(t, err)

			if test.dstBckHasObjects {
				for _, dstm := range dstms {
					// Don't make PUTs to remote bucket
					if !dstm.bck.IsRemote() {
						dstm.puts()
					}
				}
			}

			if bckTest.IsAIS() {
				srcm.puts()

				srcBckList, err = api.ListObjects(baseParams, srcm.bck, nil, api.ListArgs{})
				tassert.CheckFatal(t, err)
			} else if bckTest.IsRemote() {
				srcm.remotePuts(false /*evict*/)
				srcBckList, err = api.ListObjects(baseParams, srcm.bck, nil, api.ListArgs{})
				tassert.CheckFatal(t, err)
				if test.evictRemoteSrc {
					tlog.Logf("evicting %s\n", srcm.bck)
					//
					// evict all _cached_ data from the "local" cluster
					// keep the src bucket in the "local" BMD though
					//
					err := api.EvictRemoteBucket(baseParams, srcm.bck, true /*keep empty src bucket in the BMD*/)
					tassert.CheckFatal(t, err)
				}
				defer srcm.del()
			} else {
				panic(bckTest)
			}

			xactIDs := make([]string, 0, len(dstms))
			for _, dstm := range dstms {
				var (
					uuid string
					err  error
					cmsg = &apc.CopyBckMsg{Force: true}
				)
				if test.evictRemoteSrc {
					uuid, err = api.CopyBucket(baseParams, srcm.bck, dstm.bck, cmsg, apc.FltExists)
				} else {
					uuid, err = api.CopyBucket(baseParams, srcm.bck, dstm.bck, cmsg)
				}
				tassert.CheckFatal(t, err)
				tlog.Logf("copying %s => %s: %s\n", srcm.bck, dstm.bck, uuid)
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
				if test.evictRemoteSrc {
					// wait for TCO idle (different x-kind)
					args := xact.ArgsMsg{ID: uuid, Timeout: tools.CopyBucketTimeout}
					err := api.WaitForXactionIdle(baseParams, &args)
					tassert.CheckFatal(t, err)
				} else {
					args := xact.ArgsMsg{ID: uuid, Kind: apc.ActCopyBck, Timeout: tools.CopyBucketTimeout}
					_, err := api.WaitForXactionIC(baseParams, &args)
					tassert.CheckFatal(t, err)
				}
			}

			for _, dstm := range dstms {
				if dstm.bck.IsRemote() {
					continue
				}

				tlog.Logf("checking and comparing bucket %s props\n", dstm.bck)
				dstProps, err := api.HeadBucket(baseParams, dstm.bck, true /* don't add */)
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
				tlog.Logf("checking and comparing objects of bucket %s\n", dstm.bck)
				expectedObjCount := srcm.num
				if test.dstBckHasObjects {
					expectedObjCount += dstm.num
				}

				_, err := api.HeadBucket(baseParams, srcm.bck, true /* don't add */)
				tassert.CheckFatal(t, err)
				dstmProps, err := api.HeadBucket(baseParams, dstm.bck, true /* don't add */)
				tassert.CheckFatal(t, err)

				msg := &apc.LsoMsg{}
				msg.AddProps(apc.GetPropsVersion)
				if test.dstRemote {
					msg.Flags = apc.LsObjCached
				}

				dstBckList, err := api.ListObjects(baseParams, dstm.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
				if len(dstBckList.Entries) != expectedObjCount {
					t.Fatalf("list_objects: dst %s, cnt %d != %d cnt, src %s",
						dstm.bck.Cname(""), len(dstBckList.Entries), expectedObjCount, srcm.bck.Cname(""))
				}

				tlog.Logf("verifying that %d copied objects have identical props\n", expectedObjCount)
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
		baseParams = tools.BaseAPIParams()
	)

	m.init(true /*cleanup*/)

	// 1. PUT(num-objs) => cliBck
	m.puts()
	tassert.Errorf(t, len(m.objNames) == m.num, "expected %d in the source bucket, got %d", m.num, len(m.objNames))

	tlog.Logf("list source %s objects\n", cliBck.Cname(""))
	msg := &apc.LsoMsg{Prefix: m.prefix, Flags: apc.LsObjCached}
	lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == m.num, "expected %d present (cached) in the source bucket, got %d", m.num, len(lst.Entries))

	// 2. copy cliBck => dstBck
	dstBck := cmn.Bck{Name: "dst-" + cos.GenTie(), Provider: apc.AIS}
	tlog.Logf("first copy %s => %s\n", m.bck.Cname(""), dstBck.Cname(""))
	xid, err := api.CopyBucket(baseParams, m.bck, dstBck, &apc.CopyBckMsg{})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	tlog.Logf("list destination %s objects\n", dstBck.Cname(""))
	lst, err = api.ListObjects(baseParams, dstBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "expected %d in the destination bucket, got %d", m.num, len(lst.Entries))

	// 3. select random 10% to delete
	num2del := max(m.num/10, 1)
	nam2del := make([]string, 0, num2del)
	strtpos := rand.IntN(m.num)
	for i := range num2del {
		pos := (strtpos + i*3) % m.num
		name := m.objNames[pos]
		for cos.StringInSlice(name, nam2del) {
			pos++
			name = m.objNames[pos%m.num]
		}
		nam2del = append(nam2del, name)
	}

	// 4. use remais to out-of-band delete nam2del...
	tlog.Logf("use remote cluster '%s' to out-of-band delete %d objects from %s (source)\n",
		tools.RemoteCluster.Alias, len(nam2del), m.bck.Cname(""))
	remoteBP := tools.BaseAPIParams(tools.RemoteCluster.URL)
	for _, name := range nam2del {
		err := api.DeleteObject(remoteBP, cliBck, name)
		tassert.CheckFatal(t, err)
	}

	// 5. copy --sync (and note that prior to this step destination has all m.num)
	tlog.Logf("second copy %s => %s with '--sync' option\n", m.bck.Cname(""), dstBck.Cname(""))
	xid, err = api.CopyBucket(baseParams, m.bck, dstBck, &apc.CopyBckMsg{Sync: true})
	tassert.CheckFatal(t, err)
	args.ID = xid
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	tlog.Logf("list post-sync destination %s\n", dstBck.Cname(""))
	lst, err = api.ListObjects(baseParams, dstBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == m.num-len(nam2del), "expected %d objects in the (sync-ed) destination, got %d",
		m.num-len(nam2del), len(lst.Entries))
}

func TestCopyBucketSimple(t *testing.T) {
	var (
		srcBck = cmn.Bck{Name: "cpybck_src" + cos.GenTie(), Provider: apc.AIS}

		m = &ioContext{
			t:         t,
			num:       500, // x 2
			fileSize:  512,
			fixedSize: true,
			bck:       srcBck,
		}
	)
	if testing.Short() {
		m.num /= 10
	}

	tlog.Logf("Preparing source bucket %s\n", srcBck)
	tools.CreateBucket(t, proxyURL, srcBck, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)

	m.puts()
	m.prefix = "subdir/"
	m.puts()
	m.num *= 2

	f := func() {
		list, err := api.ListObjects(baseParams, srcBck, nil, api.ListArgs{})
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
}

func testCopyBucketStats(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	dstBck := cmn.Bck{Name: "cpybck_dst" + cos.GenTie(), Provider: apc.AIS}

	xid, err := api.CopyBucket(baseParams, srcBck, dstBck, &apc.CopyBckMsg{Force: true})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)
	objs, outObjs, inObjs := snaps.ObjCounts(xid)
	tassert.Errorf(t, objs == int64(m.num), "expected %d objects copied, got (objs=%d, outObjs=%d, inObjs=%d)",
		m.num, objs, outObjs, inObjs)
	if outObjs != inObjs {
		tlog.Logf("Warning: (sent objects) %d != %d (received objects)\n", outObjs, inObjs)
	} else {
		tlog.Logf("Num sent/received objects: %d\n", outObjs)
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
	)

	xid, err := api.CopyBucket(baseParams, srcBck, dstBck, &apc.CopyBckMsg{Prepend: cpyPrefix})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	tlog.Logf("Wating for x-%s[%s] %s => %s\n", apc.ActCopyBck, xid, srcBck, dstBck)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, dstBck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == m.num, "expected %d to be copied, got %d", m.num, len(list.Entries))
	for _, e := range list.Entries {
		tassert.Fatalf(t, strings.HasPrefix(e.Name, cpyPrefix), "expected %q to have prefix %q", e.Name, cpyPrefix)
	}
}

func testCopyBucketPrefix(t *testing.T, srcBck cmn.Bck, m *ioContext, expected int) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		dstBck = cmn.Bck{Name: "cpybck_dst" + cos.GenTie(), Provider: apc.AIS}
	)

	xid, err := api.CopyBucket(baseParams, srcBck, dstBck, &apc.CopyBckMsg{Prefix: m.prefix})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	tlog.Logf("Wating for x-%s[%s] %s => %s\n", apc.ActCopyBck, xid, srcBck, dstBck)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, dstBck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == expected, "expected %d to be copied, got %d", m.num, len(list.Entries))
	for _, e := range list.Entries {
		tassert.Fatalf(t, strings.HasPrefix(e.Name, m.prefix), "expected %q to have prefix %q", e.Name, m.prefix)
	}
}

func testCopyBucketAbort(t *testing.T, srcBck cmn.Bck, m *ioContext, sleep time.Duration) {
	dstBck := cmn.Bck{Name: testBucketName + cos.GenTie(), Provider: apc.AIS}

	xid, err := api.CopyBucket(baseParams, srcBck, dstBck, &apc.CopyBckMsg{Force: true})
	tassert.CheckError(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, m.proxyURL, dstBck)
	})

	time.Sleep(sleep)

	tlog.Logf("Aborting x-%s[%s]\n", apc.ActCopyBck, xid)
	err = api.AbortXaction(baseParams, &xact.ArgsMsg{ID: xid})
	tassert.CheckError(t, err)

	time.Sleep(time.Second)
	snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid})
	tassert.CheckError(t, err)
	aborted, err := snaps.IsAborted(xid)
	tassert.CheckError(t, err)
	tassert.Errorf(t, aborted, "failed to abort copy-bucket: %q, %v", xid, err)

	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(dstBck), apc.FltExists)
	tassert.CheckError(t, err)
	tassert.Errorf(t, !tools.BucketsContain(bcks, cmn.QueryBcks(dstBck)), "should not contain destination bucket %s", dstBck)
}

func testCopyBucketDryRun(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	dstBck := cmn.Bck{Name: "cpybck_dst" + cos.GenTie() + trand.String(5), Provider: apc.AIS}

	xid, err := api.CopyBucket(baseParams, srcBck, dstBck, &apc.CopyBckMsg{DryRun: true})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	tlog.Logf("Wating for x-%s[%s]\n", apc.ActCopyBck, xid)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)

	locObjs, outObjs, inObjs := snaps.ObjCounts(xid)
	tassert.Errorf(t, locObjs+outObjs == int64(m.num), "expected %d objects, got (locObjs=%d, outObjs=%d, inObjs=%d)",
		m.num, locObjs, outObjs, inObjs)

	locBytes, outBytes, inBytes := snaps.ByteCounts(xid)
	expectedBytesCnt := int64(m.fileSize * uint64(m.num))
	tassert.Errorf(t, locBytes+outBytes == expectedBytesCnt, "expected %d bytes, got (locBytes=%d, outBytes=%d, inBytes=%d)",
		expectedBytesCnt, locBytes, outBytes, inBytes)

	exists, err := api.QueryBuckets(baseParams, cmn.QueryBcks(dstBck), apc.FltExists)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, exists == false, "expected destination bucket to not be created")
}

// Tries to rename and then copy bucket at the same time.
func TestRenameAndCopyBucket(t *testing.T) {
	var (
		baseParams = tools.BaseAPIParams()
		src        = cmn.Bck{Name: testBucketName + "_rc_src", Provider: apc.AIS}
		m          = ioContext{t: t, bck: src, num: 500}
		dst1       = cmn.Bck{Name: testBucketName + "_rc_dst1", Provider: apc.AIS}
		dst2       = cmn.Bck{Name: testBucketName + "_rc_dst2", Provider: apc.AIS}
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
	tlog.Logf("Rename %s => %s\n", src, dst1)
	xid, err := api.RenameBucket(baseParams, src, dst1)
	if err != nil && ensurePrevRebalanceIsFinished(baseParams, err) {
		// retry just once
		xid, err = api.RenameBucket(baseParams, src, dst1)
	}
	tassert.CheckFatal(t, err)
	tlog.Logf("x-%s[%s] in progress...\n", apc.ActMoveBck, xid)

	// Try to copy src to dst1 - and note that rename src => dst1 in progress
	tlog.Logf("Copy %s => %s (note: expecting to fail)\n", src, dst1)
	_, err = api.CopyBucket(baseParams, src, dst1, nil)
	tassert.Fatalf(t, err != nil, "expected copy %s => %s to fail", src, dst1)

	// Try to copy bucket that is being renamed
	tlog.Logf("Copy %s => %s (note: expecting to fail)\n", src, dst2)
	_, err = api.CopyBucket(baseParams, src, dst2, nil)
	tassert.Fatalf(t, err != nil, "expected copy %s => %s to fail", src, dst2)

	// Try to copy from dst1 to dst1
	tlog.Logf("Copy %s => %s (note: expecting to fail)\n", dst1, dst2)
	_, err = api.CopyBucket(baseParams, src, dst1, nil)
	tassert.Fatalf(t, err != nil, "expected copy %s => %s to fail (as %s is the renaming destination)", dst1, dst2, dst1)

	// Wait for rename to finish
	tlog.Logf("Waiting for x-%s[%s] to finish\n", apc.ActMoveBck, xid)
	time.Sleep(2 * time.Second)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	time.Sleep(time.Second)

	//
	// more checks
	//
	tlog.Logln("Listing and counting")
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltExists)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, !tools.BucketsContain(bcks, cmn.QueryBcks(src)), "expected %s to not exist (be renamed from), got %v", src, bcks)
	tassert.Fatalf(t, tools.BucketsContain(bcks, cmn.QueryBcks(dst1)), "expected %s to exist (be renamed to), got %v", dst1, bcks)
	tassert.Fatalf(t, !tools.BucketsContain(bcks, cmn.QueryBcks(dst2)), "expected %s to not exist (got %v)", dst2, bcks)

	list, err := api.ListObjects(baseParams, dst1, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == m.num, "expected %s to have %d, got %d", dst1, m.num, len(list.Entries))

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
		baseParams = tools.BaseAPIParams()
		dstBck1    = cmn.Bck{
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
	tlog.Logf("copy %s => %s\n", srcBck, dstBck1)
	xid, err := api.CopyBucket(baseParams, srcBck, dstBck1, nil)
	tassert.CheckFatal(t, err)

	// Try to rename to first destination - copy in progress, both for srcBck and dstBck1
	tlog.Logf("try rename %s => %s\n", srcBck, dstBck1)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename to second destination - copy in progress for srcBck
	tlog.Logf("try rename %s => %s\n", srcBck, dstBck2)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename from dstBck1 to dstBck1 - rename in progress for dstBck1
	tlog.Logf("try rename %s => %s\n", dstBck1, dstBck2)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Wait for copy to complete
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(srcBck), apc.FltExists)
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

		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{CloudBck: true, Bck: remoteBck})

	m.init(true /*cleanup*/)

	tools.CreateBucket(t, proxyURL, aisBck, nil, true /*cleanup*/)

	p, err := api.HeadBucket(baseParams, remoteBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	remoteBck.Provider = p.Provider

	m.remotePuts(false /*evict*/)

	msg := &apc.LsoMsg{Prefix: m.prefix}
	remoteObjList, err := api.ListObjects(baseParams, remoteBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(remoteObjList.Entries) > 0, "empty object list")

	// Connect backend bucket to a aisBck
	_, err = api.SetBucketProps(baseParams, aisBck, &cmn.BpropsToSet{
		BackendBck: &cmn.BackendBckToSet{
			Name:     apc.Ptr(remoteBck.Name),
			Provider: apc.Ptr(remoteBck.Provider),
		},
	})
	tassert.CheckFatal(t, err)
	// Try putting one of the original remote objects, it should work.
	err = tools.PutObjRR(baseParams, aisBck, remoteObjList.Entries[0].Name, 128, cos.ChecksumNone)
	tassert.Errorf(t, err == nil, "expected err==nil (put to a BackendBck should be allowed via aisBck)")

	p, err = api.HeadBucket(baseParams, aisBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, p.BackendBck.Equal(&remoteBck),
		"backend bucket wasn't set correctly (got: %s, expected: %s)",
		p.BackendBck, remoteBck,
	)

	// Try to cache object.
	cachedObjName := remoteObjList.Entries[0].Name
	_, err = api.GetObject(baseParams, aisBck, cachedObjName, nil)
	tassert.CheckFatal(t, err)

	// Check if listing objects will result in listing backend bucket objects.
	msg.AddProps(apc.GetPropsAll...)
	aisObjList, err := api.ListObjects(baseParams, aisBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(remoteObjList.Entries) == len(aisObjList.Entries),
		"object lists remote vs ais does not match (got: %+v, expected: %+v)",
		aisObjList.Entries, remoteObjList.Entries,
	)

	// Check if cached listing works correctly.
	cacheMsg := &apc.LsoMsg{Flags: apc.LsObjCached, Prefix: m.prefix}
	aisObjList, err = api.ListObjects(baseParams, aisBck, cacheMsg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(aisObjList.Entries) == 1,
		"bucket contains incorrect number of cached objects (got: %+v, expected: [%s])",
		aisObjList.Entries, cachedObjName,
	)

	// Disallow PUT (TODO: use apc.AceObjUpdate instead, when/if supported)

	aattrs := apc.AccessAll &^ apc.AcePUT
	_, err = api.SetBucketProps(baseParams, aisBck, &cmn.BpropsToSet{
		BackendBck: &cmn.BackendBckToSet{
			Name:     apc.Ptr(""),
			Provider: apc.Ptr(""),
		},
		Access: apc.Ptr(aattrs),
	})
	tassert.CheckFatal(t, err)
	p, err = api.HeadBucket(baseParams, aisBck, true /* don't add */)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, p.BackendBck.IsEmpty(), "backend bucket is still configured: %s", p.BackendBck.String())

	// Check that we can still GET and list-objects
	_, err = api.GetObject(baseParams, aisBck, cachedObjName, nil)
	tassert.CheckFatal(t, err)

	aisObjList, err = api.ListObjects(baseParams, aisBck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(aisObjList.Entries) == 1,
		"bucket contains incorrect number of objects (got: %+v, expected: [%s])",
		aisObjList.Entries, cachedObjName,
	)

	// Check that we cannot cold GET anymore - no backend
	tlog.Logln("Trying to cold-GET when there's no backend anymore (expecting to fail)")
	_, err = api.GetObject(baseParams, aisBck, remoteObjList.Entries[1].Name, nil)
	tassert.Fatalf(t, err != nil, "expected error (object should not exist)")

	// Check that we cannot do PUT anymore.
	tlog.Logln("Trying to PUT 2nd version (expecting to fail)")
	err = tools.PutObjRR(baseParams, aisBck, cachedObjName, 256, cos.ChecksumNone)
	tassert.Errorf(t, err != nil, "expected err != nil")
}

//
// even more checksum tests
//

func TestAllChecksums(t *testing.T) {
	checksums := cos.SupportedChecksums()
	for _, mirrored := range []bool{false, true} {
		for _, cksumType := range checksums {
			if testing.Short() && cksumType != cos.ChecksumNone && cksumType != cos.ChecksumXXHash {
				continue
			}
			tag := cksumType
			if mirrored {
				tag = cksumType + "/mirrored"
			}
			t.Run(tag, func(t *testing.T) {
				started := time.Now()
				testWarmValidation(t, cksumType, mirrored, false)
				tlog.Logf("Time: %v\n", time.Since(started))
			})
		}
	}

	for _, cksumType := range checksums {
		if testing.Short() && cksumType != cos.ChecksumNone && cksumType != cos.ChecksumXXHash {
			continue
		}
		tag := cksumType + "/EC"
		t.Run(tag, func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 4})

			started := time.Now()
			testWarmValidation(t, cksumType, false, true)
			tlog.Logf("Time: %v\n", time.Since(started))
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
			fileSize:        uint64(cos.KiB + rand.Int64N(cos.KiB*10)),
		}
		numCorrupted = rand.IntN(m.num/100) + 2
	)
	if testing.Short() {
		m.num = 40
		m.fileSize = cos.KiB
		numCorrupted = 13
	}

	m.initAndSaveState(true /*cleanup*/)
	baseParams := tools.BaseAPIParams(m.proxyURL)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	{
		if mirrored {
			_, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
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
		} else if eced {
			if m.smap.CountActiveTs() < parityCnt+1 {
				t.Fatalf("Not enough targets to run %s test, must be at least %d", t.Name(), parityCnt+1)
			}
			_, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
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
		} else {
			_, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					Type:            apc.Ptr(cksumType),
					ValidateWarmGet: apc.Ptr(true),
				},
			})
			tassert.CheckFatal(t, err)
		}

		p, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
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
	}

	m.puts()

	// wait for mirroring
	if mirrored {
		args := xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: m.bck, Timeout: xactTimeout}
		api.WaitForXactionIdle(baseParams, &args)
		// NOTE: ref 1377
		m.ensureNumCopies(baseParams, copyCnt, false /*greaterOk*/)
	}
	// wait for erasure-coding
	if eced {
		args := xact.ArgsMsg{Kind: apc.ActECPut, Bck: m.bck, Timeout: xactTimeout}
		api.WaitForXactionIdle(baseParams, &args)
	}

	// read all
	if cksumType != cos.ChecksumNone {
		tlog.Logf("Reading %q objects with checksum validation\n", m.bck)
	} else {
		tlog.Logf("Reading %q objects\n", m.bck)
	}
	m.gets(nil, false)

	msg := &apc.LsoMsg{}
	bckObjs, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	if len(bckObjs.Entries) == 0 {
		t.Errorf("%s is empty\n", m.bck)
		return
	}

	if cksumType != cos.ChecksumNone {
		tlog.Logf("Reading %d objects from %s with end-to-end %s validation\n", len(bckObjs.Entries), m.bck, cksumType)
		wg := cos.NewLimitedWaitGroup(20, 0)

		for _, en := range bckObjs.Entries {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				_, err = api.GetObjectWithValidation(baseParams, m.bck, name, nil)
				tassert.CheckError(t, err)
			}(en.Name)
		}

		wg.Wait()
	}

	if docker.IsRunning() {
		tlog.Logf("skipping %s object corruption (docker is not supported)\n", t.Name())
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
		tlog.Logf("Corrupting %d objects\n", numCorrupted)
		go func() {
			for j := i; j < i+numCorrupted; j++ {
				objName := bckObjs.Entries[j].Name
				corruptSingleBitInFile(t, m.bck, objName)
				objCh <- objName
			}
		}()
		for range numCorrupted {
			objName := <-objCh
			_, err = api.GetObject(baseParams, m.bck, objName, nil)
			if mirrored || eced {
				if err != nil && cksumType != cos.ChecksumNone {
					if eced {
						// retry EC
						time.Sleep(2 * time.Second)
						_, err = api.GetObject(baseParams, m.bck, objName, nil)
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
				baseParams = tools.BaseAPIParams()

				expectedFiles = m.num
				cacheSize     int
			)

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(1)

			if m.bck.IsAIS() {
				tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
				m.puts()
			} else if m.bck.IsRemote() {
				m.bck = cliBck
				tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
				tlog.Logf("remote %s\n", m.bck.Cname(""))
				m.del(-1 /* delete all */)

				m.num /= 10
				cacheSize = m.num / 2
				expectedFiles = m.num

				m.remotePuts(true /*evict*/)
				if test.cached {
					m.remotePrefetch(cacheSize)
					expectedFiles = cacheSize
				}
			} else {
				t.Fatal(test.provider)
			}

			tlog.Logln("checking objects...")

			if test.summary {
				msg := &apc.BsummCtrlMsg{ObjCached: test.cached}
				xid, summaries, err := api.GetBucketSummary(baseParams, cmn.QueryBcks(m.bck), msg, api.BsummArgs{})
				tassert.CheckFatal(t, err)

				if len(summaries) == 0 {
					t.Fatalf("x-%s[%s] summary for bucket %q should exist", apc.ActSummaryBck, xid, m.bck)
				}
				if len(summaries) != 1 {
					t.Fatalf("x-%s[%s] number of summaries (%d) is larger than 1", apc.ActSummaryBck, xid, len(summaries))
				}

				summary := summaries[0]
				if summary.ObjCount.Remote+summary.ObjCount.Present != uint64(expectedFiles) {
					t.Errorf("x-%s[%s] %s: number of objects in summary (%+v) differs from expected (%d)",
						apc.ActSummaryBck, xid, m.bck, summary.ObjCount, expectedFiles)
				}
			} else {
				msg := &apc.LsoMsg{PageSize: int64(min(m.num/3, 256))} // mult. pages
				if test.cached {
					msg.Flags = apc.LsObjCached
				}
				objList, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				if len(objList.Entries) != expectedFiles {
					t.Errorf("number of listed objects (%d) is different from expected (%d)",
						len(objList.Entries), expectedFiles)
				}
			}
		})
	}
}

func TestListObjectsNoRecursion(t *testing.T) {
	type test struct {
		prefix string
		count  int
	}
	var (
		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		objs       = []string{
			"img001", "vid001",
			"img-test/obj1", "img-test/vid1", "img-test/pics/obj01",
			"img003", "img-test/pics/vid01"}
		tests = []test{
			{prefix: "", count: 4},
			{prefix: "img-test", count: 3},
			{prefix: "img-test/", count: 3},
			{prefix: "img-test/pics", count: 3},
			{prefix: "img-test/pics/", count: 3},
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	for _, nm := range objs {
		objectSize := int64(rand.IntN(256) + 20)
		reader, _ := readers.NewRand(objectSize, cos.ChecksumNone)
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    nm,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)
	}

	msg := &apc.LsoMsg{Props: apc.GetPropsName}
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == len(objs), "Invalid number of objects %d vs %d", len(lst.Entries), len(objs))

	for idx, tst := range tests {
		msg := &apc.LsoMsg{Flags: apc.LsNoRecursion | apc.LsNameSize, Prefix: tst.prefix}
		lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)

		if tst.count == len(lst.Entries) {
			continue
		}
		tlog.Logf("Failed test #%d (prefix %s). Expected %d, got %d\n",
			idx, tst.prefix, tst.count, len(lst.Entries))
		for idx, en := range lst.Entries {
			tlog.Logf("%d. %s (%v)\n", idx, en.Name, en.Flags)
		}
		tassert.Errorf(t, false, "[%s] Invalid number of objects %d (expected %d)", tst.prefix, len(lst.Entries), tst.count)
	}
}

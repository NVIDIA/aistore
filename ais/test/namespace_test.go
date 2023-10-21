// Package integration_test.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

func listAllBuckets(t *testing.T, baseParams api.BaseParams, includeRemote bool, fltPresence int) cmn.Bcks {
	buckets, err := api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)
	if includeRemote {
		allRemaisBuckets, err := api.ListBuckets(baseParams,
			cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}, fltPresence)
		tassert.CheckFatal(t, err)

		filtered := cmn.Bcks{}
		for _, bck := range allRemaisBuckets {
			if bck.Ns.UUID != tools.RemoteCluster.Alias || bck.Ns.UUID == tools.RemoteCluster.UUID {
				filtered = append(filtered, bck)
			}
		}
		buckets = append(buckets, filtered...)

		// Make sure that listing with specific UUID also works and have similar outcome.
		remoteClusterBuckets, err := api.ListBuckets(baseParams,
			cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.UUID}},
			fltPresence)
		tassert.CheckFatal(t, err)

		// TODO -- FIXME: do intead smth like: `remoteClusterBuckets.Equal(allRemaisBuckets)`
		tassert.Errorf(
			t, len(remoteClusterBuckets) == len(allRemaisBuckets),
			"specific namespace %q => %v, while all-remote %q => %v, where presence=%d\n",
			cmn.Ns{UUID: tools.RemoteCluster.UUID}, remoteClusterBuckets,
			cmn.NsAnyRemote, allRemaisBuckets, fltPresence,
		)
	}
	return buckets
}

func TestNamespace(t *testing.T) {
	tests := []struct {
		name   string
		remote bool
		bck1   cmn.Bck
		bck2   cmn.Bck
	}{
		{
			name:   "global_and_local_namespace",
			remote: false,
			bck1: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: "",
					Name: "namespace",
				},
			},
		},
		{
			name:   "two_local_namespaces",
			remote: false,
			bck1: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: "",
					Name: "ns1",
				},
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: "",
					Name: "ns2",
				},
			},
		},
		{
			name:   "global_namespaces_with_remote_cluster",
			remote: true,
			bck1: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: tools.RemoteCluster.Alias,
					Name: cmn.NsGlobal.Name,
				},
			},
		},
		{
			name:   "namespaces_with_remote_cluster",
			remote: true,
			bck1: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: "",
					Name: "ns1",
				},
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: tools.RemoteCluster.Alias,
					Name: "ns1",
				},
			},
		},
		{
			name:   "namespaces_with_only_remote_cluster",
			remote: true,
			bck1: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: tools.RemoteCluster.Alias,
					Name: "ns1",
				},
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: apc.AIS,
				Ns: cmn.Ns{
					UUID: tools.RemoteCluster.Alias,
					Name: "ns2",
				},
			},
		},
	}

	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				m1 = ioContext{
					t:   t,
					num: 100,
					bck: test.bck1,
				}
				m2 = ioContext{
					t:   t,
					num: 200,
					bck: test.bck2,
				}
			)

			tools.CheckSkip(t, tools.SkipTestArgs{
				RequiresRemoteCluster: test.remote,
			})

			m1.init(true)
			m2.init(true)

			origBuckets := listAllBuckets(t, baseParams, test.remote, apc.FltExists)
			if len(origBuckets) > 0 {
				tlog.Logf("orig buckets %+v\n", origBuckets)
			}
			err := api.CreateBucket(baseParams, m1.bck, nil)
			tassert.CheckFatal(t, err)
			defer func() {
				err = api.DestroyBucket(baseParams, m1.bck)
				tassert.CheckFatal(t, err)
			}()

			err = api.CreateBucket(baseParams, m2.bck, nil)
			tassert.CheckFatal(t, err)
			defer func() {
				err := api.DestroyBucket(baseParams, m2.bck)
				tassert.CheckFatal(t, err)
			}()

			// Test listing buckets
			newBuckets := listAllBuckets(t, baseParams, test.remote, apc.FltExists)
			tlog.Logf("created %+v\n", newBuckets)
			tassert.Errorf(
				t, len(newBuckets)-len(origBuckets) == 2,
				"number of buckets (%d) should be %d", len(newBuckets), len(origBuckets)+2,
			)

			m1.puts()
			m2.puts()

			// Now remote bucket(s) must be present
			locBuckets := listAllBuckets(t, baseParams, test.remote, apc.FltPresent)
			tassert.CheckFatal(t, err)
			tlog.Logf("present in BMD %+v\n", locBuckets)
			tassert.Errorf(
				t, len(locBuckets) == len(newBuckets), "number of buckets (%d: %v) should be (%d: %v)\n",
				len(locBuckets), locBuckets, len(newBuckets), newBuckets,
			)

			// Test listing objects
			objects, err := api.ListObjects(baseParams, m1.bck, nil, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objects.Entries) == m1.num,
				"number of entries (%d) should be (%d)", len(objects.Entries), m1.num,
			)

			objects, err = api.ListObjects(baseParams, m2.bck, nil, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objects.Entries) == m2.num,
				"number of entries (%d) should be (%d)", len(objects.Entries), m2.num,
			)

			// Test bucket summary
			var (
				summaries cmn.AllBsummResults
				xids      []string
			)
			for _, bck := range locBuckets {
				xid, summ, err := api.GetBucketSummary(baseParams, cmn.QueryBcks(bck),
					nil /*bck present true*/, api.BsummArgs{})
				tassert.CheckFatal(t, err)
				summaries = append(summaries, summ[0])
				xids = append(xids, xid)
			}
			tassert.Errorf(t, len(summaries) == len(locBuckets), "%s-%v: number of summaries (%d) should be %d",
				apc.ActSummaryBck, xids, len(summaries), len(locBuckets))

			bck1Found, bck2Found := false, false
			for _, summary := range summaries {
				if m1.bck.Ns.UUID == tools.RemoteCluster.Alias {
					m1.bck.Ns.UUID = tools.RemoteCluster.UUID
				}
				if m2.bck.Ns.UUID == tools.RemoteCluster.Alias {
					m2.bck.Ns.UUID = tools.RemoteCluster.UUID
				}

				if summary.Bck.Equal(&m1.bck) {
					bck1Found = true
					tassert.Errorf(
						t, summary.ObjCount.Present == uint64(m1.num),
						"number of objects (%d) should be (%d)", summary.ObjCount, m1.num,
					)
				} else if summary.Bck.Equal(&m2.bck) {
					bck2Found = true
					tassert.Errorf(
						t, summary.ObjCount.Present == uint64(m2.num),
						"number of objects (%d) should be (%d)", summary.ObjCount, m2.num,
					)
				}
			}
			tassert.Errorf(t, bck1Found, "%s not found in %v summ", m1.bck, summaries)
			tassert.Errorf(t, bck2Found, "%s not found in %v summ", m2.bck, summaries)
			m1.gets(nil, false)
			m2.gets(nil, false)

			m1.ensureNoGetErrors()
			m2.ensureNoGetErrors()
		})
	}
}

func TestRemoteWithAliasAndUUID(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{
		RequiresRemoteCluster: true,
	})

	// TODO: make it work
	t.Skip("NYI")

	var (
		alias = tools.RemoteCluster.Alias
		uuid  = tools.RemoteCluster.UUID

		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)

		m1 = ioContext{
			t:   t,
			num: 100,
			bck: cmn.Bck{Name: "tmp", Ns: cmn.Ns{UUID: alias}},
		}
		m2 = ioContext{
			t:   t,
			num: 200,
			bck: cmn.Bck{Name: "tmp", Ns: cmn.Ns{UUID: uuid}},
		}
	)

	m1.init(true)
	m2.init(true)

	err := api.CreateBucket(baseParams, m1.bck, nil)
	tassert.CheckFatal(t, err)
	defer func() {
		err := api.DestroyBucket(baseParams, m1.bck)
		tassert.CheckFatal(t, err)
	}()

	m1.puts()
	m2.puts()

	// TODO: works until this point

	buckets, err := api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltExists)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(buckets) == 1,
		"number of buckets (%d) should be equal to 1", len(buckets),
	)

	for _, bck := range []cmn.Bck{m1.bck, m2.bck} {
		objects, err := api.ListObjects(baseParams, bck, nil, api.ListArgs{})
		tassert.CheckFatal(t, err)
		tassert.Errorf(
			t, len(objects.Entries) == m1.num+m2.num,
			"number of entries (%d) should be equal to (%d)", len(objects.Entries), m1.num+m2.num,
		)
	}
}

func TestRemoteWithSilentBucketDestroy(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{
		RequiresRemoteCluster: true,
	})

	// TODO: make it work
	t.Skip("NYI")

	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		remoteBP   = tools.BaseAPIParams(tools.RemoteCluster.URL)

		m = ioContext{
			t:   t,
			num: 100,
			bck: cmn.Bck{Ns: cmn.Ns{UUID: tools.RemoteCluster.Alias}},
		}
	)

	m.init(true /*cleanup*/)

	err := api.CreateBucket(baseParams, m.bck, nil)
	tassert.CheckFatal(t, err)
	defer func() {
		// Delete just in case something goes wrong (therefore ignoring error)
		api.DestroyBucket(baseParams, m.bck)
	}()

	m.puts()
	m.gets(nil, false)

	err = api.DestroyBucket(remoteBP, cmn.Bck{Name: m.bck.Name})
	tassert.CheckFatal(t, err)

	// Check that bucket is still cached
	buckets, err := api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(buckets) == 1, "number of buckets (%d) should be 1", len(buckets))

	// Test listing objects
	_, err = api.ListObjects(baseParams, m.bck, nil, api.ListArgs{})
	tassert.Fatalf(t, err != nil, "expected list-objects to fail w/ \"bucket does not exist\"")

	// TODO: it works until this point

	// Check that bucket is no longer present
	buckets, err = api.ListBuckets(baseParams, cmn.QueryBcks{Provider: apc.AIS}, apc.FltPresent)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(buckets) == 0, "number of buckets (%d) should be 0 (zero)", len(buckets))
}

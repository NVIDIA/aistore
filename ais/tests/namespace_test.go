// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// TODO: add test for checking if everything works correctly with scenario:
//  1. Create remote bucket
//  2. Silently destroy bucket with remote url (not with local url, so the bucket stays locally)
//  3. Do List/Summary

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
				Provider: cmn.ProviderAIS,
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: cmn.ProviderAIS,
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
				Provider: cmn.ProviderAIS,
				Ns: cmn.Ns{
					UUID: "",
					Name: "ns1",
				},
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: cmn.ProviderAIS,
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
				Provider: cmn.ProviderAIS,
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: cmn.ProviderAIS,
				Ns: cmn.Ns{
					UUID: tutils.RemoteCluster.UUID,
					Name: cmn.NsGlobal.Name,
				},
			},
		},
		{
			name:   "namespaces_with_remote_cluster",
			remote: true,
			bck1: cmn.Bck{
				Name:     "tmp",
				Provider: cmn.ProviderAIS,
				Ns: cmn.Ns{
					UUID: "",
					Name: "ns1",
				},
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: cmn.ProviderAIS,
				Ns: cmn.Ns{
					UUID: tutils.RemoteCluster.UUID,
					Name: "ns1",
				},
			},
		},
		{
			name:   "namespaces_with_only_remote_cluster",
			remote: true,
			bck1: cmn.Bck{
				Name:     "tmp",
				Provider: cmn.ProviderAIS,
				Ns: cmn.Ns{
					UUID: tutils.RemoteCluster.UUID,
					Name: "ns1",
				},
			},
			bck2: cmn.Bck{
				Name:     "tmp",
				Provider: cmn.ProviderAIS,
				Ns: cmn.Ns{
					UUID: tutils.RemoteCluster.UUID,
					Name: "ns2",
				},
			},
		},
	}

	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
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

			tutils.CheckSkip(t, tutils.SkipTestArgs{
				RequiresRemote: test.remote,
			})

			m1.init()
			m2.init()

			err := api.CreateBucket(baseParams, m1.bck)
			tassert.CheckFatal(t, err)
			err = api.CreateBucket(baseParams, m2.bck)
			tassert.CheckFatal(t, err)

			defer func() {
				err := api.DestroyBucket(baseParams, m2.bck)
				tassert.CheckFatal(t, err)
				err = api.DestroyBucket(baseParams, m1.bck)
				tassert.CheckFatal(t, err)
			}()

			// Test listing buckets
			if !test.remote {
				buckets, err := api.ListBuckets(baseParams, cmn.Bck{Provider: cmn.ProviderAIS})
				tassert.CheckFatal(t, err)
				tassert.Fatalf(
					t, len(buckets) == 2,
					"number of buckets (%d) should be equal to 2", len(buckets),
				)
			}

			m1.puts()
			m2.puts()

			// Test listing objects
			objects, err := api.ListObjects(baseParams, m1.bck, nil, 0)
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objects.Entries) == m1.num,
				"number of entries (%d) should be equal to (%d)", len(objects.Entries), m1.num,
			)

			objects, err = api.ListObjects(baseParams, m2.bck, nil, 0)
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objects.Entries) == m2.num,
				"number of entries (%d) should be equal to (%d)", len(objects.Entries), m2.num,
			)

			// Test summary
			if !test.remote {
				summaries, err := api.GetBucketsSummaries(baseParams, cmn.Bck{Provider: cmn.ProviderAIS}, nil)
				tassert.CheckFatal(t, err)
				tassert.Errorf(
					t, len(summaries) == 2,
					"number of summaries (%d) should be equal to 2", len(summaries),
				)

				for _, summary := range summaries {
					if summary.Bck.Equal(m1.bck) {
						tassert.Errorf(
							t, summary.ObjCount == uint64(m1.num),
							"number of objects (%d) should be equal to (%d)", summary.ObjCount, m1.num,
						)
					} else if summary.Bck.Equal(m2.bck) {
						tassert.Errorf(
							t, summary.ObjCount == uint64(m2.num),
							"number of objects (%d) should be equal to (%d)", summary.ObjCount, m2.num,
						)
					} else {
						t.Errorf("unknown bucket in summary: %q", summary.Bck)
					}
				}
			}

			m1.gets()
			m2.gets()

			m1.ensureNoErrors()
			m2.ensureNoErrors()
		})
	}
}

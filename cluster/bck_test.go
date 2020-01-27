// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bck", func() {
	Describe("Uname", func() {
		DescribeTable("should convert bucket and object name to uname and back",
			func(bckName, bckProvider, bckNs, objName string) {
				bck := &Bck{Name: bckName, Provider: bckProvider, Ns: bckNs}
				uname := bck.MakeUname(objName)

				gotBck, gotObjName := ParseUname(uname)
				Expect(gotBck.Name).To(Equal(bckName))
				Expect(gotBck.Provider).To(Equal(bckProvider))
				Expect(gotBck.Ns).To(Equal(bckNs))
				Expect(gotObjName).To(Equal(objName))
			},
			Entry(
				"regular ais bucket with simple object name",
				"bck", cmn.ProviderAIS, cmn.NsGlobal, "obj",
			),
			Entry(
				"regular ais bucket with long object name",
				"bck", cmn.ProviderAIS, cmn.NsGlobal, "obj/tmp1/tmp2",
			),
			Entry(
				"non-empty namespace",
				"bck", cmn.ProviderAIS, "namespace", "obj/tmp1/tmp2",
			),
			Entry(
				"aws provider",
				"bck", cmn.ProviderAmazon, cmn.NsGlobal, "obj",
			),
			Entry(
				"gcp provider",
				"bck", cmn.ProviderGoogle, cmn.NsGlobal, "obj",
			),
			Entry(
				"cloud provider",
				"bck", cmn.ProviderGoogle, cmn.NsGlobal, "obj",
			),
		)
	})

	Describe("Equal", func() {
		DescribeTable("should not be equal",
			func(a, b *Bck) {
				Expect(a.Equal(b, true /*same BID*/)).To(BeFalse())
			},
			Entry(
				"not matching names",
				&Bck{Name: "a", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
				&Bck{Name: "b", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
			),
			Entry(
				"empty providers",
				&Bck{Name: "a", Provider: "", Ns: cmn.NsGlobal},
				&Bck{Name: "a", Provider: "", Ns: cmn.NsGlobal},
			),
			Entry(
				"not matching namespace",
				&Bck{Name: "a", Provider: "", Ns: "ns1"},
				&Bck{Name: "a", Provider: "", Ns: "ns2"},
			),
			Entry(
				"not matching providers",
				&Bck{Name: "a", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
				&Bck{Name: "a", Provider: "", Ns: cmn.NsGlobal},
			),
			Entry(
				"not matching providers #2",
				&Bck{Name: "a", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
				&Bck{Name: "a", Provider: cmn.Cloud, Ns: cmn.NsGlobal},
			),
			Entry(
				"not matching providers #3",
				&Bck{Name: "a", Provider: "", Ns: cmn.NsGlobal},
				&Bck{Name: "a", Provider: cmn.Cloud, Ns: cmn.NsGlobal},
			),
		)

		DescribeTable("should be equal",
			func(a, b *Bck) {
				Expect(a.Equal(b, true /*same BID*/)).To(BeTrue())
			},
			Entry(
				"matching AIS providers",
				&Bck{Name: "a", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
				&Bck{Name: "a", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
			),
			Entry(
				"matching namespaces",
				&Bck{Name: "a", Provider: cmn.ProviderAIS, Ns: "ns"},
				&Bck{Name: "a", Provider: cmn.ProviderAIS, Ns: "ns"},
			),
			Entry(
				"matching Cloud providers",
				&Bck{Name: "a", Provider: cmn.ProviderGoogle, Ns: cmn.NsGlobal},
				&Bck{Name: "a", Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal},
			),
			Entry(
				"matching Cloud providers #2",
				&Bck{Name: "a", Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal},
				&Bck{Name: "a", Provider: cmn.ProviderGoogle, Ns: cmn.NsGlobal},
			),
		)
	})
})

// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bck", func() {
	Describe("Uname", func() {
		DescribeTable("should convert bucket and object name to uname and back",
			func(bckName, bckProvider string, bckNs cmn.Ns, objName string) {
				bck := NewBck(bckName, bckProvider, bckNs)
				uname := bck.MakeUname(objName)

				gotBck, gotObjName := cmn.ParseUname(uname)
				Expect(gotBck.Name).To(Equal(bckName))
				Expect(gotBck.Provider).To(Equal(bckProvider))
				Expect(gotBck.Ns).To(Equal(bckNs))
				Expect(gotObjName).To(Equal(objName))
			},
			Entry(
				"regular ais bucket with simple object name",
				"bck", apc.AIS, cmn.NsGlobal, "obj",
			),
			Entry(
				"regular ais bucket with long object name",
				"bck", apc.AIS, cmn.NsGlobal, "obj/tmp1/tmp2",
			),
			Entry(
				"non-empty local namespace",
				"bck", apc.AIS, cmn.Ns{Name: "namespace"}, "obj/tmp1/tmp2",
			),
			Entry(
				"non-empty cloud namespace",
				"bck", apc.AIS, cmn.Ns{UUID: "uuid", Name: "namespace"}, "obj/tmp1/tmp2",
			),
			Entry(
				"aws provider",
				"bck", apc.AWS, cmn.NsGlobal, "obj",
			),
			Entry(
				"gcp provider",
				"bck", apc.GCP, cmn.NsGlobal, "obj",
			),
			Entry(
				"backend provider",
				"bck", apc.GCP, cmn.NsGlobal, "obj",
			),
		)
	})

	Describe("Equal", func() {
		DescribeTable("should not be equal",
			func(a, b *Bck) {
				a.Props, b.Props = &cmn.BucketProps{}, &cmn.BucketProps{}
				Expect(a.Equal(b, true /*same BID*/, true /* same backend*/)).To(BeFalse())
			},
			Entry(
				"not matching names",
				NewBck("a", apc.AIS, cmn.NsGlobal),
				NewBck("b", apc.AIS, cmn.NsGlobal),
			),
			Entry(
				"not matching namespace #1",
				NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns1"}),
				NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns2"}),
			),
			Entry(
				"not matching namespace #2",
				NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid1", Name: "ns"}),
				NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid2", Name: "ns"}),
			),
			Entry(
				"not matching providers #2",
				NewBck("a", apc.AIS, cmn.NsGlobal),
				NewBck("a", apc.AWS, cmn.NsGlobal),
			),
			Entry(
				"not matching Backend providers #4",
				NewBck("a", apc.AWS, cmn.NsGlobal),
				NewBck("a", apc.GCP, cmn.NsGlobal),
			),
			Entry(
				"not matching Backend providers #5",
				NewBck("a", apc.GCP, cmn.NsGlobal),
				NewBck("a", apc.AWS, cmn.NsGlobal),
			),
		)

		DescribeTable("should be equal",
			func(a, b *Bck) {
				a.Props, b.Props = &cmn.BucketProps{}, &cmn.BucketProps{}
				Expect(a.Equal(b, true /*same BID*/, true /* same backend */)).To(BeTrue())
			},
			Entry(
				"matching AIS providers",
				NewBck("a", apc.AIS, cmn.NsGlobal),
				NewBck("a", apc.AIS, cmn.NsGlobal),
			),
			Entry(
				"matching local namespaces",
				NewBck("a", apc.AIS, cmn.Ns{Name: "ns"}),
				NewBck("a", apc.AIS, cmn.Ns{Name: "ns"}),
			),
			Entry(
				"matching cloud namespaces",
				NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns"}),
				NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns"}),
			),
		)
	})
})

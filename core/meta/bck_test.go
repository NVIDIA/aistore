// Package meta_test: unit tests for the package
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta_test

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bck", func() {
	Describe("Uname", func() {
		DescribeTable("should convert bucket and object name to uname and back",
			func(bckName, bckProvider string, bckNs cmn.Ns, objName string) {
				bck := meta.NewBck(bckName, bckProvider, bckNs)
				uname := bck.MakeUname(objName)

				gotBck, gotObjName := cmn.ParseUname(cos.UnsafeS(uname))
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
				"oci provider",
				"bck", apc.OCI, cmn.NsGlobal, "obj",
			),
			Entry(
				"backend provider",
				"bck", apc.GCP, cmn.NsGlobal, "obj",
			),
		)
	})

	Describe("ParseUname", func() {
		DescribeTable("should reject a traversing object name",
			func(uname string) {
				_, _, err := meta.ParseUname(uname, true /*withObjname*/)
				Expect(err).To(HaveOccurred())
			},
			Entry("copy destination", "ais/@#/bck/../../../../../../../../../tmp/proof"),
			Entry("leading dotdot", "ais/@#/bck/../etc/passwd"),
			Entry("embedded dotdot", "ais/@#/bck/a/../../../etc/passwd"),
			Entry("trailing dotdot", "ais/@#/bck/a/.."),
			Entry("single dot component", "ais/@#/bck/a/./b"),
			Entry("home relative", "ais/@#/bck/~/.ssh/authorized_keys"),
			Entry("trailing separator", "ais/@#/bck/a/b/"),
		)

		DescribeTable("should accept a legitimate object name",
			func(uname, expObjName string) {
				_, objName, err := meta.ParseUname(uname, true /*withObjname*/)
				Expect(err).NotTo(HaveOccurred())
				Expect(objName).To(Equal(expObjName))
			},
			Entry("plain name", "ais/@#/bck/obj", "obj"),
			Entry("nested name", "ais/@#/bck/a/b/c.txt", "a/b/c.txt"),
			Entry("dotted name", "ais/@#/bck/archive.tar.gz", "archive.tar.gz"),
			Entry("dotfile", "ais/@#/bck/.hidden", ".hidden"),
			Entry("dotdot in basename", "ais/@#/bck/weird..name", "weird..name"),
		)

		It("should accept an empty object name when not required", func() {
			bck, objName, err := meta.ParseUname("ais/@#/bck/", false /*withObjname*/)
			Expect(err).NotTo(HaveOccurred())
			Expect(objName).To(BeEmpty())
			Expect(bck.Name).To(Equal("bck"))
		})
	})

	Describe("Equal", func() {
		DescribeTable("should not be equal",
			func(a, b *meta.Bck) {
				a.Props, b.Props = &cmn.Bprops{}, &cmn.Bprops{}
				Expect(a.Equal(b, true /*same BID*/, true /* same backend*/)).To(BeFalse())
			},
			Entry(
				"not matching names",
				meta.NewBck("a", apc.AIS, cmn.NsGlobal),
				meta.NewBck("b", apc.AIS, cmn.NsGlobal),
			),
			Entry(
				"not matching namespace #1",
				meta.NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns1"}),
				meta.NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns2"}),
			),
			Entry(
				"not matching namespace #2",
				meta.NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid1", Name: "ns"}),
				meta.NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid2", Name: "ns"}),
			),
			Entry(
				"not matching providers #2",
				meta.NewBck("a", apc.AIS, cmn.NsGlobal),
				meta.NewBck("a", apc.AWS, cmn.NsGlobal),
			),
			Entry(
				"not matching Backend providers #4",
				meta.NewBck("a", apc.AWS, cmn.NsGlobal),
				meta.NewBck("a", apc.GCP, cmn.NsGlobal),
			),
			Entry(
				"not matching Backend providers #5",
				meta.NewBck("a", apc.GCP, cmn.NsGlobal),
				meta.NewBck("a", apc.AWS, cmn.NsGlobal),
			),
		)

		DescribeTable("should be equal",
			func(a, b *meta.Bck) {
				a.Props, b.Props = &cmn.Bprops{}, &cmn.Bprops{}
				Expect(a.Equal(b, true /*same BID*/, true /* same backend */)).To(BeTrue())
			},
			Entry(
				"matching AIS providers",
				meta.NewBck("a", apc.AIS, cmn.NsGlobal),
				meta.NewBck("a", apc.AIS, cmn.NsGlobal),
			),
			Entry(
				"matching local namespaces",
				meta.NewBck("a", apc.AIS, cmn.Ns{Name: "ns"}),
				meta.NewBck("a", apc.AIS, cmn.Ns{Name: "ns"}),
			),
			Entry(
				"matching cloud namespaces",
				meta.NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns"}),
				meta.NewBck("a", apc.AIS, cmn.Ns{UUID: "uuid", Name: "ns"}),
			),
		)
	})
})

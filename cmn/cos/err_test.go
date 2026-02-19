// Package cos_test: unit tests
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Errs Unwrap", func() {
	Context("when multiple ObjErrs are added", func() {
		It("should unwrap and extract all ObjErrs correctly", func() {
			testErrs := cos.NewErrs()
			testErrs.Add(&etl.ObjErr{
				ObjName: "obj0",
				Message: "err0",
				Ecode:   404,
			})
			testErrs.Add(&etl.ObjErr{
				ObjName: "obj1",
				Message: "err1",
				Ecode:   500,
			})
			testErrs.Add(&etl.ObjErr{
				ObjName: "obj2",
				Message: "err2",
				Ecode:   403,
			})

			unwrapped := testErrs.Unwrap()
			Expect(len(unwrapped)).To(Equal(3))

			for i, err := range unwrapped {
				var objErr *etl.ObjErr
				ok := errors.As(err, &objErr)
				Expect(ok).To(BeTrue(), fmt.Sprintf("entry %d should be ObjErr", i))
				Expect(objErr.ObjName).To(Equal(fmt.Sprintf("obj%d", i)))
				Expect(objErr.Message).To(Equal(fmt.Sprintf("err%d", i)))
			}
		})
	})
})

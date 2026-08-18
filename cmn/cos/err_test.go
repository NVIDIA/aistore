// Package cos_test: unit tests
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"errors"
	"fmt"
	"testing"

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

func TestValidatePath(t *testing.T) {
	valid := []string{
		"",
		"a",
		"a.b",
		".git",
		"..foo",
		"foo..",
		"a../b",
		"a/~/b",
		"a/.../b",
		"a/",
	}
	for _, path := range valid {
		if err := cos.ValidateRname(path); err != nil {
			t.Errorf("ValidateRname(%q): unexpected error: %v", path, err)
		}
	}

	invalid := []string{
		".",
		"..",
		"./a",
		"../a",
		"a/.",
		"a/..",
		"a/./b",
		"a/../b",
		"~/a",
	}
	for _, path := range invalid {
		if err := cos.ValidateRname(path); err == nil {
			t.Errorf("ValidateRname(%q): expected error", path)
		}
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"empty-object", cos.ValidateOname(""), `invalid object name ""`},
		{"trailing-object", cos.ValidateWname("a/"), `invalid object name "a/"`},
		{"trailing-archpath", cos.ValidateArchpath("a/"), `invalid archpath "a/"`},
		{
			"prefix-context",
			cos.ValidatePrefix("bad list-objects request", "a/.."),
			`bad list-objects request: invalid prefix "a/.."`,
		},
	}
	for _, tc := range tests {
		if tc.err == nil {
			t.Errorf("%s: expected error", tc.name)
		} else if got := tc.err.Error(); got != tc.want {
			t.Errorf("%s: expected %q, got %q", tc.name, tc.want, got)
		}
	}

	if err := cos.ValidateWname(""); err != nil {
		t.Errorf("ValidateWname(empty): unexpected error: %v", err)
	}
	if err := cos.ValidateArchpath(""); err != nil {
		t.Errorf("ValidateArchpath(empty): unexpected error: %v", err)
	}
}

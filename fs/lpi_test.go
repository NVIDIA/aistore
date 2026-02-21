// Package fs_test provides tests for fs package
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */

package fs_test

import (
	"fmt"
	"math/rand/v2"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/fs/lpi"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// compare w/ namesake integration in ais/test/scripted_cli_test.go

const (
	lpiTestPrefix   = "Total files created: "
	lpiTestScript   = "../ais/test/scripts/gen-nested-dirs.sh"
	lpiTestPageSize = 10
	lpiTestVerbose  = false
)

func TestLocalPageIterator(t *testing.T) {
	// 1. create temp root
	root := t.TempDir()

	// 2. generate
	cmd := exec.Command(lpiTestScript, "--root_dir", root)
	out, err := cmd.CombinedOutput()
	tassert.CheckFatal(t, err)

	var (
		lines = strings.Split(string(out), "\n")
		num   int64
	)
	for _, ln := range lines {
		i := strings.Index(ln, lpiTestPrefix)
		if i >= 0 {
			s := strings.TrimSpace(ln[i+len(lpiTestPrefix):])
			num, err = strconv.ParseInt(s, 10, 64)
			tassert.CheckFatal(t, err)
		}
	}
	tassert.Fatalf(t, num > 0, "failed to parse script output (num %d)", num)

	// 3, 4. randomize page size and paginate
	var (
		total = int(num)
		eops  = make([]string, 0, 20)
		size  = rand.IntN(lpiTestPageSize<<1) + lpiTestPageSize>>1
		sz    = strconv.Itoa(size)
	)
	t.Run("page-size/"+sz, func(t *testing.T) { eops = lpiPageSize(t, root, eops, size, total) })
	t.Run("end-of-page/"+sz, func(t *testing.T) { lpiEndOfPage(t, root, eops, total) })
}

func lpiPageSize(t *testing.T, root string, eops []string, lpiTestPageSize, total int) []string {
	var (
		page = make(lpi.Page, 100)
		msg  = lpi.Msg{Size: lpiTestPageSize}
		num  int
	)
	it, err := lpi.New(root, "" /*prefix*/, nil /*smap*/) // TODO: add test
	tassert.CheckFatal(t, err)

	for {
		if err := it.Next(msg, page); err != nil {
			t.Fatal(err)
		}
		num += len(page)
		if lpiTestVerbose {
			for name := range page {
				fmt.Println(name)
			}
			fmt.Println()
			fmt.Println(strings.Repeat("---", 10))
			fmt.Println()
		}
		if it.Pos() == "" {
			eops = append(eops, lpi.AllPages)
			break
		}
		eops = append(eops, it.Pos())
	}
	tassert.Errorf(t, num == total, "(num) %d != %d (total)", num, total)
	return eops
}

func lpiEndOfPage(t *testing.T, root string, eops []string, total int) {
	var (
		previous string
		num      int
		page     = make(lpi.Page, 100)
		it, err  = lpi.New(root, "" /*prefix*/, nil /*smap*/) // TODO: add test
	)
	tassert.CheckFatal(t, err)

	for _, eop := range eops {
		msg := lpi.Msg{EOP: eop}
		if err := it.Next(msg, page); err != nil {
			t.Fatal(err) // TODO: check vs divisibility by page size
		}
		num += len(page)
		if lpiTestVerbose {
			fmt.Printf("Range (%q - %q]:\n\n", previous, eop)
			for name := range page {
				fmt.Println(name)
			}
			fmt.Println()
			fmt.Println(strings.Repeat("---", 10))
			fmt.Println()
		}
		previous = eop
	}
	tassert.Errorf(t, num == total, "(num) %d != %d (total)", num, total)
}

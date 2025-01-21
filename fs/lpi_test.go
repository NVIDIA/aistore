// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */

package fs_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/fs/lpi"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	lpiTestPrefix   = "Total files created: "
	lpiTestScript   = "../ais/test/scripts/gen_nested_dirs.sh"
	lpiTestPageSize = 10
	lpiTestVerbose  = false
)

func TestLocalPageIt(t *testing.T) {
	// 1. create temp root
	root, err := os.MkdirTemp("", "ais-lpi-")
	tassert.CheckFatal(t, err)
	defer func() {
		if !t.Failed() {
			os.RemoveAll(root)
		}
	}()

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
			s := ln[i+len(lpiTestPrefix):]
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
	t.Run("page-size/"+sz, func(t *testing.T) { eops = lpiPageSize(t, root, eops, lpiTestPageSize, total) })
	t.Run("end-of-page/"+sz, func(t *testing.T) { lpiEndOfPage(t, root, eops, total) })
}

func lpiPageSize(t *testing.T, root string, eops []string, lpiTestPageSize, total int) []string {
	var (
		page = make(lpi.Page, 100)
		msg  = lpi.Msg{Size: lpiTestPageSize}
		num  int
	)
	it, err := lpi.New(root)
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
		it, err  = lpi.New(root)
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

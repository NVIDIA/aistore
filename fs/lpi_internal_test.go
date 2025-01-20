// Package fs provides mountpath and FQN abstractions and methods eop resolve/map stored content
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */

package fs

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestLocalPageIt(t *testing.T) {
	const (
		rootPrefix     = "Creating root directory: "
		numFilesPrefix = "Total files created: "

		pageSize = 10
	)
	var (
		num  int64
		root string
	)

	// run the script and parse results
	cmd := exec.Command("../ais/test/scripts/gen_nested_dirs.sh") // TODO -- FIXME: will change
	out, err := cmd.CombinedOutput()
	tassert.CheckFatal(t, err)
	lines := strings.Split(string(out), "\n")
	for _, ln := range lines {
		i := strings.Index(ln, rootPrefix)
		if i >= 0 {
			root = ln[i+len(rootPrefix):]
			continue
		}
		i = strings.Index(ln, numFilesPrefix)
		if i >= 0 {
			s := ln[i+len(numFilesPrefix):]
			num, err = strconv.ParseInt(s, 10, 64)
			tassert.CheckFatal(t, err)
		}
	}
	tassert.Fatalf(t, root != "" && num > 0, "failed to parse script output (root %q, num %d)", root, num)
	tassert.Fatalf(t, strings.HasPrefix(root, "/tmp/"), "expecting root under /tmp, got %q", root)

	t.Log("root:", root, len(root), "num:", num) // TODO -- FIXME: make use of `num`
	defer os.RemoveAll(root)

	// paginate
	eops := make([]string, 0, 20)
	t.Run("page-size", func(t *testing.T) { eops = lpiPageSize(t, root, eops, pageSize) })
	t.Run("end-of-page", func(t *testing.T) { lpiEndOfPage(t, root, eops) })
}

func lpiPageSize(t *testing.T, root string, eops []string, pageSize int) []string {
	var (
		page = make(lpiPage, 100)
		msg  = lpiMsg{size: pageSize}
	)
	lpi, err := newLocalPageIt(root)
	tassert.CheckFatal(t, err)

	for {
		if err := lpi.do(msg, page); err != nil {
			t.Fatal(err)
		}
		for path := range page {
			fmt.Println(path)
		}
		fmt.Println()
		fmt.Println(strings.Repeat("---", 10))
		fmt.Println()
		if lpi.next == "" {
			eops = append(eops, allPages)
			break
		}
		eops = append(eops, lpi.next)
	}
	return eops
}

func lpiEndOfPage(t *testing.T, root string, eops []string) {
	page := make(lpiPage, 100)
	lpi, err := newLocalPageIt(root)
	tassert.CheckFatal(t, err)

	var previous string
	for _, eop := range eops {
		msg := lpiMsg{eop: eop}
		if err := lpi.do(msg, page); err != nil {
			t.Fatal(err) // TODO -- FIXME: num listed entries divisible by page size
		}
		fmt.Printf("Range (%q - %q]:\n\n", previous, eop)
		for path := range page {
			fmt.Println(path)
		}
		fmt.Println()
		fmt.Println(strings.Repeat("---", 10))
		fmt.Println()
		previous = eop
	}
}

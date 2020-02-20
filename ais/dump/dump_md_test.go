/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package dump_test

import (
	"flag"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/tutils/tassert"
	jsoniter "github.com/json-iterator/go"
)

// Examples - Smap:
// 1. go test -v -run=DumpSmap -in=~/.ais0/.ais.smap  # dump Smap to STDOUT
// 2. go test -v -run=DumpSmap -in=~/.ais0/.ais.smap -out=/tmp/smap.txt
// 3. go test -v -run=CompressSmap -in=/tmp/smap.txt -out=/tmp/.ais.smap
//
// Example - BMD:
// 4. go test -v -run=DumpBMD -in=~/.ais0/.ais.bmd  # dump BMD to STDOUT
// 5. go test -v -run=DumpBMD -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt
// 6. go test -v -run=CompressBMD -in=/tmp/bmd.txt -out=/tmp/.ais.bmd

var (
	in, out string
)

func TestMain(m *testing.M) {
	var run bool
	flag.StringVar(&in, "in", "", "fully-qualified filename")
	flag.StringVar(&out, "out", "", "output filename (can be omitted when dumping BMD or Smap)")
	flag.Parse()
	for _, option := range os.Args {
		if strings.Contains(option, "run") {
			run = true
		}
	}
	if in == "" && run {
		fmt.Println("input (the -in option) is not defined")
		os.Exit(1)
	}
	if run {
		os.Exit(m.Run())
	}
}

// "*Dump" routines expect compressed (bmd, smap)

func TestDumpSmap(t *testing.T) {
	smap := &cluster.Smap{}
	dumpMeta(t, in, smap, jsp.Options{Signature: true})
}

func TestDumpBMD(t *testing.T) {
	bmd := &cluster.BMD{}
	dumpMeta(t, in, bmd, jsp.Options{Signature: true})
}

func dumpMeta(t *testing.T, fn string, v interface{}, opts jsp.Options) {
	var f = os.Stdout
	var err error
	if out != "" {
		f, err = cmn.CreateFile(_fclean(out))
		tassert.CheckFatal(t, err)
	}
	err = jsp.Load(_fclean(fn), v, opts)
	tassert.CheckFatal(t, err)

	s, _ := jsoniter.MarshalIndent(v, "", " ")
	fmt.Fprintln(f, string(s))
}

// "*Compress" routines require output filename

func TestCompressSmap(t *testing.T) {
	if out == "" {
		t.Fatal("output filename (the -out option) must be defined")
	}
	smap := &cluster.Smap{}
	compressMeta(t, in, smap, jsp.CCSign())
}

func TestCompressBMD(t *testing.T) {
	if out == "" {
		t.Fatal("output filename (the -out option) must be defined")
	}
	bmd := &cluster.BMD{}
	compressMeta(t, in, bmd, jsp.CCSign())
}

func compressMeta(t *testing.T, fn string, v interface{}, opts jsp.Options) {
	err := jsp.Load(_fclean(fn), v, jsp.Plain())
	tassert.CheckFatal(t, err)
	err = jsp.Save(_fclean(out), v, opts)
	tassert.CheckFatal(t, err)
}

// misc helpers

func _fclean(fn string) string {
	if strings.HasPrefix(fn, "~/") {
		fn = strings.Replace(fn, "~", _homeDir(), 1)
	}
	return filepath.Clean(fn)
}

func _homeDir() string {
	currentUser, err := user.Current()
	if err != nil {
		return os.Getenv("HOME")
	}
	return currentUser.HomeDir
}

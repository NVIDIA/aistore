/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

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
// 1. go test -v ./ais/. -run=DumpSmap -fsmap=~/.ais0/.ais.smap
// 2. go test -v ./ais/. -run=DumpSmap -fsmap=~/.ais0/.ais.smap -fout=/tmp/smap.txt
// 3. go test -v ./ais/. -run=CompressSmap -fsmap=/tmp/smap.txt -fout=/tmp/.ais.smap
//
// Example - BMD:
// 4. go test -v ./ais/. -run=DumpBMD -fbmd=~/.ais0/.ais.bmd -fout=/tmp/bmd.txt
// 5. go test -v ./ais/. -run=CompressBMD -fbmd=/tmp/bmd.txt -fout=/tmp/.ais.bmd

var (
	fsmap, fbmd, fout string
)

func init() {
	flag.StringVar(&fsmap, "fsmap", "", "fully-qualified Smap filename")
	flag.StringVar(&fbmd, "fbmd", "", "fully-qualified BMD filename")
	flag.StringVar(&fout, "fout", "", "output filename if defined, otherwise stdout")
}

// "*Dump" routines expect compressed (bmd, smap)

func TestDumpSmap(t *testing.T) {
	if fsmap == "" {
		t.Skip()
	}
	smap := &cluster.Smap{}
	dumpMeta(t, fsmap, smap, jsp.Options{Signature: true})
}

func TestDumpBMD(t *testing.T) {
	if fbmd == "" {
		t.Skip()
	}
	bmd := &cluster.BMD{}
	dumpMeta(t, fbmd, bmd, jsp.Options{Signature: true})
}

func dumpMeta(t *testing.T, fn string, v interface{}, opts jsp.Options) {
	var f = os.Stdout
	var err error
	if fout != "" {
		f, err = cmn.CreateFile(_fclean(fout))
		tassert.CheckFatal(t, err)
	}
	err = jsp.Load(_fclean(fn), v, opts)
	tassert.CheckFatal(t, err)

	s, _ := jsoniter.MarshalIndent(v, "", " ")
	fmt.Fprintln(f, string(s))
}

// "*Compress" routines

func TestCompressSmap(t *testing.T) {
	if fsmap == "" {
		t.Skip()
	}
	if fout == "" {
		t.Fatal("empty output filename")
	}
	smap := &cluster.Smap{}
	compressMeta(t, fsmap, smap, jsp.CCSign())
}

func TestCompressBMD(t *testing.T) {
	if fbmd == "" {
		t.Skip()
	}
	if fout == "" {
		t.Fatal("empty output filename")
	}
	bmd := &cluster.BMD{}
	compressMeta(t, fbmd, bmd, jsp.CCSign())
}

func compressMeta(t *testing.T, fn string, v interface{}, opts jsp.Options) {
	err := jsp.Load(_fclean(fn), v, jsp.Plain())
	tassert.CheckFatal(t, err)
	err = jsp.Save(_fclean(fout), v, opts)
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

// Package xmeta provides low-level tools to format or extract
// into plain text some of the AIS control structures.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	jsoniter "github.com/json-iterator/go"
)

var (
	in, out        string
	extract, usage bool
)

func main() {
	var (
		f          func() error
		what, verb string
	)

	newFlag := flag.NewFlagSet(os.Args[0], flag.ExitOnError) // discard flags of imported packages

	newFlag.BoolVar(&extract, "x", false,
		"true: extract AIS-formatted BMD or Smap, false: format plain-text BMD or Smap for AIS")
	newFlag.StringVar(&in, "in", "", "fully-qualified input filename")
	newFlag.StringVar(&out, "out", "", "output filename (optional when extracting)")
	newFlag.BoolVar(&usage, "h", false, "print usage and exit")
	newFlag.Parse(os.Args[1:])

	if usage || len(os.Args[1:]) == 0 {
		fmt.Println("Build:")
		fmt.Println("\tgo install xmeta.go")
		fmt.Println("Examples:")
		fmt.Printf("\txmeta -h\t\t\t\t\t\t- show usage\n")
		fmt.Printf("\txmeta -x -in=~/.ais0/.ais.smap\t\t\t\t- extract Smap to STDOUT\n")
		fmt.Printf("\txmeta -x -in=~/.ais0/.ais.smap -out=/tmp/smap.txt\t- extract Smap to /tmp/smap.txt\n")
		fmt.Printf("\txmeta -in=/tmp/smap.txt -out=/tmp/.ais.smap\t\t- format plain-text /tmp/smap.txt\n")
		fmt.Printf("\txmeta -x -in=~/.ais0/.ais.bmd\t\t\t\t- extract BMD to STDOUT\n")
		fmt.Printf("\txmeta -x -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt\t\t- extract BMD to /tmp/bmd.txt\n")
		fmt.Printf("\txmeta -in=/tmp/bmd.txt -out=/tmp/.ais.bmd\t\t- format plain-text /tmp/bmd.txt\n")
		fmt.Println()
		os.Exit(0)
	}

	if extract {
		verb = "extract"
		if strings.Contains(strings.ToLower(in), "smap") {
			f, what = dumpSmap, "Smap"
		} else if strings.Contains(strings.ToLower(in), "bmd") {
			f, what = dumpBMD, "BMD"
		} else if err := dumpSmap(); err == nil {
			return
		} else {
			f, what = dumpBMD, "BMD"
		}
	} else {
		verb = "format"
		if strings.Contains(strings.ToLower(in), "smap") {
			f, what = compressSmap, "Smap"
		} else if strings.Contains(strings.ToLower(in), "bmd") {
			f, what = compressBMD, "BMD"
		} else if err := compressSmap(); err == nil {
			return
		} else {
			f, what = compressBMD, "BMD"
		}
	}
	if err := f(); err != nil {
		if extract {
			fmt.Printf("Failed to %s %s from %s: %v\n", verb, what, in, err)
		} else {
			fmt.Printf("Cannot %s %s: plain-text input %s, error=\"%v\"\n", verb, what, in, err)
		}
	}
}

// "*Dump" routines expect AIS-formatted (bmd, smap)

func dumpSmap() error {
	smap := &cluster.Smap{}
	return dumpMeta(in, smap, jsp.Options{Signature: true})
}

func dumpBMD() error {
	bmd := &cluster.BMD{}
	return dumpMeta(in, bmd, jsp.Options{Signature: true})
}

func dumpMeta(fn string, v interface{}, opts jsp.Options) (err error) {
	var f = os.Stdout
	if out != "" {
		f, err = cmn.CreateFile(_fclean(out))
		if err != nil {
			return
		}
	}
	err = jsp.Load(_fclean(fn), v, opts)
	if err != nil {
		return
	}
	s, _ := jsoniter.MarshalIndent(v, "", " ")
	fmt.Fprintln(f, string(s))
	return nil
}

// "*Compress" routines require output filename

func compressSmap() error {
	smap := &cluster.Smap{}
	return compressMeta(in, smap, jsp.CCSign())
}

func compressBMD() error {
	bmd := &cluster.BMD{}
	return compressMeta(in, bmd, jsp.CCSign())
}

func compressMeta(fn string, v interface{}, opts jsp.Options) error {
	if out == "" {
		return errors.New("output filename (the -out option) must be defined")
	}
	if err := jsp.Load(_fclean(fn), v, jsp.Plain()); err != nil {
		return err
	}
	return jsp.Save(_fclean(out), v, opts)
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

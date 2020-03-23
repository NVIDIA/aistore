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
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	jsoniter "github.com/json-iterator/go"
)

var (
	flags struct {
		in, out string
		extract bool
		help    bool
	}
)

const (
	helpMsg = `Build:
	go install xmeta.go

Examples:
	xmeta -h                                          - show usage
	xmeta -x -in=~/.ais0/.ais.smap                    - extract Smap to STDOUT
	xmeta -x -in=~/.ais0/.ais.smap -out=/tmp/smap.txt - extract Smap to /tmp/smap.txt
	xmeta -in=/tmp/smap.txt -out=/tmp/.ais.smap       - format plain-text /tmp/smap.txt
	xmeta -x -in=~/.ais0/.ais.bmd                     - extract BMD to STDOUT
	xmeta -x -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt   - extract BMD to /tmp/bmd.txt
	xmeta -in=/tmp/bmd.txt -out=/tmp/.ais.bmd         - format plain-text /tmp/bmd.txt
	xmeta -x -in=~/.ais0/.ais.rmd                     - extract RMD to STDOUT
	xmeta -x -in=~/.ais0/.ais.rmd -out=/tmp/rmd.txt   - extract RMD to /tmp/rmd.txt
	xmeta -in=/tmp/rmd.txt -out=/tmp/.ais.rmd         - format plain-text /tmp/rmd.txt
`
)

func main() {
	var (
		f          func() error
		what, verb string
	)

	newFlag := flag.NewFlagSet(os.Args[0], flag.ExitOnError) // discard flags of imported packages

	newFlag.BoolVar(&flags.extract, "x", false,
		"true: extract AIS-formatted BMD or Smap, false: format plain-text BMD or Smap for AIS")
	newFlag.StringVar(&flags.in, "in", "", "fully-qualified input filename")
	newFlag.StringVar(&flags.out, "out", "", "output filename (optional when extracting)")
	newFlag.BoolVar(&flags.help, "h", false, "print usage and exit")
	newFlag.Parse(os.Args[1:])

	if flags.help || len(os.Args[1:]) == 0 {
		fmt.Print(helpMsg)
		os.Exit(0)
	}

	flags.in = cmn.ExpandPath(flags.in)
	if flags.out != "" {
		flags.out = cmn.ExpandPath(flags.out)
	}
	in := strings.ToLower(flags.in)
	if flags.extract {
		verb = "extract"
		if strings.Contains(in, "smap") {
			f, what = dumpSmap, "Smap"
		} else if strings.Contains(in, "bmd") {
			f, what = dumpBMD, "BMD"
		} else if strings.Contains(in, "rmd") {
			f, what = dumpRMD, "RMD"
		} else if err := dumpSmap(); err == nil {
			return
		} else {
			f, what = dumpBMD, "BMD"
		}
	} else {
		verb = "format"
		if strings.Contains(in, "smap") {
			f, what = compressSmap, "Smap"
		} else if strings.Contains(in, "bmd") {
			f, what = compressBMD, "BMD"
		} else if strings.Contains(in, "rmd") {
			f, what = compressRMD, "RMD"
		} else if err := compressSmap(); err == nil {
			return
		} else {
			f, what = compressBMD, "BMD"
		}
	}
	if err := f(); err != nil {
		if flags.extract {
			fmt.Printf("Failed to %s %s from %s: %v\n", verb, what, in, err)
		} else {
			fmt.Printf("Cannot %s %s: plain-text input %s, error=\"%v\"\n", verb, what, in, err)
		}
	}
}

// "*Dump" routines expect AIS-formatted (smap, bmd, rmd)

func dumpSmap() error {
	smap := &cluster.Smap{}
	return dumpMeta(smap, jsp.Options{Signature: true})
}

func dumpBMD() error {
	bmd := &cluster.BMD{}
	return dumpMeta(bmd, jsp.Options{Signature: true})
}

func dumpRMD() error {
	rmd := &cluster.RMD{}
	return dumpMeta(rmd, jsp.Options{Signature: true})
}

func dumpMeta(v interface{}, opts jsp.Options) (err error) {
	var f = os.Stdout
	if flags.out != "" {
		f, err = cmn.CreateFile(flags.out)
		if err != nil {
			return
		}
	}
	err = jsp.Load(flags.in, v, opts)
	if err != nil {
		return
	}
	s, _ := jsoniter.MarshalIndent(v, "", " ")
	_, err = fmt.Fprintln(f, string(s))
	return err
}

// "*Compress" routines require output filename

func compressSmap() error {
	smap := &cluster.Smap{}
	return compressMeta(smap, jsp.CCSign())
}

func compressBMD() error {
	bmd := &cluster.BMD{}
	return compressMeta(bmd, jsp.CCSign())
}

func compressRMD() error {
	rmd := &cluster.RMD{}
	return compressMeta(rmd, jsp.CCSign())
}

func compressMeta(v interface{}, opts jsp.Options) error {
	if flags.out == "" {
		return errors.New("output filename (the -out option) must be defined")
	}
	if err := jsp.Load(flags.in, v, jsp.Plain()); err != nil {
		return err
	}
	return jsp.Save(flags.out, v, opts)
}

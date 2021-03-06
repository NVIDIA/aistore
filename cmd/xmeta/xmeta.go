// Package xmeta provides low-level tools to format or extract
// into plain text some of the AIS control structures.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	jsoniter "github.com/json-iterator/go"
)

var flags struct {
	in, out string
	extract bool
	help    bool
}

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
	xmeta -x -in=~/.ais0/.ais.config                  - extract Config to STDOUT
	xmeta -x -in=~/.ais0/.ais.config -out=/tmp/c.txt  - extract Config to /tmp/c.txt
	xmeta -in=/tmp/c.txt -out=/tmp/.ais.config        - format plain-text /tmp/c.txt
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
		} else if strings.Contains(in, "config") {
			f, what = dumpConfig, "Global Config"
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
		} else if strings.Contains(in, "config") {
			f, what = compressConfig, "Global Config"
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

func dumpSmap() error   { return dumpMeta(&cluster.Smap{}) }
func dumpBMD() error    { return dumpMeta(&cluster.BMD{}) }
func dumpRMD() error    { return dumpMeta(&cluster.RMD{}) }
func dumpConfig() error { return dumpMeta(&cmn.ClusterConfig{}) }

func dumpMeta(v jsp.Opts) (err error) {
	f := os.Stdout
	if flags.out != "" {
		f, err = cos.CreateFile(flags.out)
		if err != nil {
			return
		}
	}
	_, err = jsp.LoadMeta(flags.in, v)
	if err != nil {
		return
	}
	s, _ := jsoniter.MarshalIndent(v, "", " ")
	_, err = fmt.Fprintln(f, string(s))
	return err
}

// "*Compress" routines require output filename

func compressSmap() error   { return compressMeta(&cluster.Smap{}) }
func compressBMD() error    { return compressMeta(&cluster.BMD{}) }
func compressRMD() error    { return compressMeta(&cluster.RMD{}) }
func compressConfig() error { return compressMeta(&cmn.ClusterConfig{}) }

func compressMeta(v jsp.Opts) error {
	if flags.out == "" {
		return errors.New("output filename (the -out option) must be defined")
	}
	if _, err := jsp.Load(flags.in, v, jsp.Plain()); err != nil {
		return err
	}
	return jsp.SaveMeta(flags.out, v)
}

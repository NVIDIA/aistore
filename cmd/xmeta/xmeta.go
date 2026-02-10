// Package xmeta provides low-level tools to format or extract
// into plain text some of the AIS control structures.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/volume"

	jsoniter "github.com/json-iterator/go"
)

// TODO: can LOM be used? LOM.Copies outside of a target has a lot of empty fields.
type lomInfo struct {
	Attrs  *cmn.ObjAttrs `json:"attrs"`
	Copies []string      `json:"copies,omitempty"`
}

var flags struct {
	// pathname and format
	in, out string
	format  string
	// VMD-specific edit operations
	enable  string
	disable string
	// behavior
	extract bool
	help    bool
	quiet   bool
}

const (
	helpMsg = `Build:
	go install ./cmd/xmeta

Examples:
	xmeta -h                                          - show usage
	# Smap:
	xmeta -x -in=~/.ais0/.ais.smap                    - extract Smap to STDOUT
	xmeta -x -in=~/.ais0/.ais.smap -out=/tmp/smap.txt - extract Smap to /tmp/smap.txt
	xmeta -x -in=./.ais.smap -f smap                  - extract Smap to STDOUT with explicit source format
	xmeta -in=/tmp/smap.txt -out=/tmp/.ais.smap       - format plain-text /tmp/smap.txt
	# BMD:
	xmeta -x -in=~/.ais0/.ais.bmd                     - extract BMD to STDOUT
	xmeta -x -in=~/.ais0/.ais.bmd -out=/tmp/bmd.txt   - extract BMD to /tmp/bmd.txt
	xmeta -x -in=./.ais.bmd -f bmd                    - extract BMD to STDOUT with explicit source format
	xmeta -in=/tmp/bmd.txt -out=/tmp/.ais.bmd         - format plain-text /tmp/bmd.txt
	# RMD:
	xmeta -x -in=~/.ais0/.ais.rmd                     - extract RMD to STDOUT
	xmeta -x -in=~/.ais0/.ais.rmd -out=/tmp/rmd.txt   - extract RMD to /tmp/rmd.txt
	xmeta -x -in=./.ais.rmd -f rmd                    - extract RMD to STDOUT with explicit source format
	xmeta -in=/tmp/rmd.txt -out=/tmp/.ais.rmd         - format plain-text /tmp/rmd.txt
	# Config:
	xmeta -x -in=~/.ais0/.ais.conf                    - extract Config to STDOUT
	xmeta -x -in=~/.ais0/.ais.conf -out=/tmp/conf.txt - extract Config to /tmp/config.txt
	xmeta -x -in=./.ais.conf -f conf                  - extract Config to STDOUT with explicit source format
	xmeta -in=/tmp/conf.txt -out=/tmp/.ais.conf       - format plain-text /tmp/config.txt
	# VMD:
	xmeta -x -in=/ais/nvme0n1/.ais.vmd                     - extract VMD to STDOUT
	xmeta -x -in=/ais/nvme0n1/.ais.vmd -out=/tmp/vmd.txt   - extract VMD to /tmp/vmd.txt
	xmeta -x -in=/ais/nvme0n1/.ais.vmd -f vmd              - extract VMD to STDOUT with explicit source format
	xmeta -in=/tmp/vmd.txt -out=/ais/nvme7n1/.ais.vmd      - store plain-text vmd.txt as VMD in a given mountpath
	# VMD edit (in-place):
	xmeta -x -in=/ais/nvme0n1/.ais.vmd -disable /ais/nvme7n1  - disable mountpath in VMD
	xmeta -x -in=/ais/nvme0n1/.ais.vmd -enable /ais/nvme7n1   - enable mountpath in VMD
	# EC Metadata:
	xmeta -x -in=/data/@ais/abc/%mt/readme            - extract Metadata to STDOUT with auto-detection (by directory name)
	xmeta -x -in=./readme -f mt                       - extract Metadata to STDOUT with explicit source format
	# LOM (readonly, no format auto-detection):
	xmeta -x -in=/data/@ais/abc/%ob/img001.tar -f lom                   - extract LOM to STDOUT
	xmeta -x -in=/data/@ais/abc/%ob/img001.tar -out=/tmp/lom.txt -f lom - extract LOM to /tmp/lom.txt
	# EMD (ETL Metadata):
	xmeta -x -in=~/.ais0/.ais.emd                     - extract EMD to STDOUT
	xmeta -x -in=~/.ais0/.ais.emd -out=/tmp/emd.txt   - extract EMD to /tmp/emd.txt
`
)

var m = map[string]struct {
	extract func() error
	format  func() error
	what    string
}{
	"smap": {extractSmap, formatSmap, "Smap"},
	"bmd":  {extractBMD, formatBMD, "BMD"},
	"rmd":  {extractRMD, formatRMD, "RMD"},
	"conf": {extractConfig, formatConfig, "Config"},
	"vmd":  {extractVMD, formatVMD, "VMD"},
	"mt":   {extractECMeta, formatECMeta, "EC Metadata"},
	"lom":  {extractLOM, formatLOM, "LOM"},
	"emd":  {extractEMD, formatEMD, "ETL Metadata"},
}

// "extract*" routines expect AIS-formatted (smap, bmd, rmd, etc.)

func extractSmap() error   { return extractMeta(&meta.Smap{}) }
func extractBMD() error    { return extractMeta(&meta.BMD{}) }
func extractRMD() error    { return extractMeta(&meta.RMD{}) }
func extractConfig() error { return extractMeta(&cmn.ClusterConfig{}) }
func extractVMD() error    { return extractMeta(&volume.VMD{}) }
func extractEMD() error    { return extractMeta(&etl.MD{}) }

// "format*" routines require output filename

func formatSmap() error   { return formatMeta(&meta.Smap{}) }
func formatBMD() error    { return formatMeta(&meta.BMD{}) }
func formatRMD() error    { return formatMeta(&meta.RMD{}) }
func formatConfig() error { return formatMeta(&cmn.ClusterConfig{}) }
func formatVMD() error    { return formatMeta(&volume.VMD{}) }
func formatEMD() error    { return formatMeta(&etl.MD{}) }
func formatLOM() error    { return errors.New("saving LOM is unsupported") }

func main() {
	newFlag := flag.NewFlagSet(os.Args[0], flag.ExitOnError) // discard flags of imported packages
	newFlag.BoolVar(&flags.extract, "x", false,
		"true: extract AIS-formatted metadata type, false: pack and AIS-format plain-text metadata")
	newFlag.StringVar(&flags.in, "in", "", "fully-qualified input filename")
	newFlag.StringVar(&flags.out, "out", "", "output filename (optional when extracting)")
	newFlag.BoolVar(&flags.help, "h", false, "print usage and exit")
	newFlag.StringVar(&flags.format, "f", "", "override automatic format detection (one of smap, bmd, rmd, conf, vmd, mt, lom)")
	newFlag.StringVar(&flags.enable, "enable", "", "VMD: enable mountpath (use with -x -in)")
	newFlag.StringVar(&flags.disable, "disable", "", "VMD: disable mountpath (use with -x -in)")
	newFlag.BoolVar(&flags.quiet, "q", false, "quiet mode: suppress reminder banner (VMD edit)")
	newFlag.Parse(os.Args[1:])
	if flags.help || len(os.Args[1:]) == 0 {
		newFlag.Usage()
		os.Stdout.WriteString(helpMsg)
		os.Exit(0)
	}

	os.Args = []string{os.Args[0]}
	flag.Parse() // don't complain

	flags.in = cos.ExpandPath(flags.in)
	if flags.out != "" {
		flags.out = cos.ExpandPath(flags.out)
	}

	// VMD edit operations
	if flags.enable != "" || flags.disable != "" {
		if err := editVMD(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to edit VMD (%s): %v\n", flags.in, err)
			os.Exit(1)
		}
		return
	}

	inLower := strings.ToLower(flags.in)
	f, what := detectFormat(inLower)
	if err := f(); err != nil {
		if flags.extract {
			fmt.Fprintf(os.Stderr, "Failed to extract %s from %s: %v\n", what, flags.in, err)
		} else {
			fmt.Fprintf(os.Stderr, "Cannot format %s: plain-text input %s, error=%q\n", what, flags.in, err)
		}
	}
}

func editVMD() error {
	if flags.in == "" {
		return errors.New("input file required (-in)")
	}
	if !flags.extract {
		return errors.New("VMD edit requires -x (in-place edit of AIS-formatted VMD)")
	}
	if flags.out != "" {
		return errors.New("cannot use -out with -enable/-disable (in-place VMD edit)")
	}
	if flags.enable != "" && flags.disable != "" {
		return errors.New("cannot use both -enable and -disable")
	}

	var (
		mpath string
		value bool
	)
	if flags.enable != "" {
		mpath = flags.enable
		value = true
	} else {
		mpath = flags.disable
		value = false
	}

	// normalize mountpath key (matches how VMD is stored)
	mpath = filepath.Clean(cos.ExpandPath(mpath))

	// load
	vmd := &volume.VMD{}
	if _, err := jsp.LoadMeta(flags.in, vmd); err != nil {
		return fmt.Errorf("failed to load VMD from %q: %w", flags.in, err)
	}

	// find and update
	mi, ok := vmd.Mountpaths[mpath]
	if !ok {
		return cos.NewErrNotFoundFmt(vmd, "mountpath %q", mpath)
	}
	if mi.Enabled == value {
		action := "enabled"
		if !value {
			action = "disabled"
		}
		fmt.Printf("mountpath %q is already %s\n", mpath, action) // nothing to do
		return nil
	}

	mi.Enabled = value
	vmd.Version++

	if err := jsp.SaveMeta(flags.in, vmd, nil); err != nil {
		return fmt.Errorf("failed to save VMD to %q: %w", flags.in, err)
	}

	action := cos.Ternary(value, "enabled", "disabled")
	fmt.Printf("mountpath %q is now %s in %q - with resulting:\n\n** %s **\n", mpath, action, flags.in, vmd.String())

	if !flags.quiet {
		fmt.Printf("\n** REMINDER: copy the updated VMD %q to every other mountpath of this target (those listed above) **\n", flags.in)
	}
	return nil
}

func detectFormat(inLower string) (f func() error, what string) {
	if flags.format == "" {
		return parseDetect(inLower, flags.extract)
	}
	e, ok := m[flags.format]
	if !ok {
		fmt.Fprintf(os.Stderr, "Invalid file format %q. Supported formats are (", flags.format)
		for k := range m {
			fmt.Fprintf(os.Stderr, "%s, ", k)
		}
		fmt.Fprintln(os.Stderr, ")")
		os.Exit(1)
	}
	f, what = e.format, e.what
	if flags.extract {
		f = e.extract
	}
	return f, what
}

func parseDetect(inLower string, extract bool) (f func() error, what string) {
	var all []string
	for k, e := range m {
		if !strings.Contains(inLower, k) {
			all = append(all, e.what)
			continue
		}
		if extract {
			f, what = e.extract, e.what
		} else {
			f, what = e.format, e.what
		}
		break
	}
	if what == "" {
		fmt.Fprintf(os.Stderr,
			"Failed to auto-detect %q for AIS metadata type - one of %q\n(use '-f' option to specify)\n",
			flags.in, all)
		os.Exit(1)
	}
	return f, what
}

func extractMeta(v jsp.Opts) (err error) {
	f := os.Stdout
	if flags.out != "" {
		f, err = cos.CreateFile(flags.out)
		if err != nil {
			return err
		}
	}
	if _, err := jsp.LoadMeta(flags.in, v); err != nil {
		return err
	}
	s, _ := jsoniter.MarshalIndent(v, "", " ")
	_, err = fmt.Fprintln(f, string(s))
	return err
}

func extractECMeta() (err error) {
	f := os.Stdout
	if flags.out != "" {
		f, err = cos.CreateFile(flags.out)
		if err != nil {
			return err
		}
	}
	var v *ec.Metadata
	v, err = ec.LoadMetadata(flags.in)
	if err != nil {
		return err
	}
	s, _ := jsoniter.MarshalIndent(v, "", " ")
	_, err = fmt.Fprintln(f, string(s))
	return err
}

func formatMeta(v jsp.Opts) error {
	if flags.out == "" {
		return errors.New("output filename (the -out option) must be defined")
	}
	if _, err := jsp.Load(flags.in, v, jsp.Plain()); err != nil {
		return err
	}
	return jsp.SaveMeta(flags.out, v, nil)
}

func formatECMeta() error {
	if flags.out == "" {
		return errors.New("output filename (the -out option) must be defined")
	}
	v := &ec.Metadata{}
	if _, err := jsp.Load(flags.in, v, jsp.Plain()); err != nil {
		return err
	}
	file, err := cos.CreateFile(flags.out)
	if err != nil {
		return err
	}
	defer cos.Close(file)
	buf := v.NewPack()
	_, err = file.Write(buf)
	if err != nil {
		cos.RemoveFile(flags.out)
	}
	return err
}

func extractLOM() (err error) {
	f := os.Stdout
	if flags.out != "" {
		f, err = cos.CreateFile(flags.out)
		if err != nil {
			return err
		}
	}
	if flags.in == "" || flags.in == "." {
		return errors.New("make sure to specify '-in=<fully qualified source filename>', run 'xmeta' for help and examples")
	}
	os.Setenv(core.DumpLomEnvVar, "1")
	fs.TestNew(nil)

	_ = mock.NewTarget(mock.NewBaseBownerMock()) // => cluster.Tinit

	lom := &core.LOM{FQN: flags.in}
	if err := lom.LoadMetaFromFS(); err != nil {
		return err
	}

	lmi := lomInfo{Attrs: lom.ObjAttrs()}
	if lom.HasCopies() {
		lmi.Copies = make([]string, 0, lom.NumCopies())
		for mp := range lom.GetCopies() {
			lmi.Copies = append(lmi.Copies, mp)
		}
	}
	s, _ := jsoniter.MarshalIndent(lmi, "", " ")
	_, err = fmt.Fprintln(f, string(s))
	return err
}

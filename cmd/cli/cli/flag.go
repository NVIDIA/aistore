// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/sys"

	"github.com/urfave/cli"
)

type (
	DurationFlag    cli.DurationFlag
	DurationFlagVar cli.DurationFlag
)

// interface guards
var (
	_ flag.Value = &DurationFlagVar{}
	_ cli.Flag   = &DurationFlag{}
)

func argIsFlag(c *cli.Context, idx int) bool {
	return c.NArg() > idx && strings.HasPrefix(c.Args().Get(idx), flagPrefix)
}

/////////////////////
// DurationFlagVar //
/////////////////////

// "s" (seconds) is the default time unit
func (f *DurationFlagVar) Set(s string) (err error) {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		s += "s"
	}
	f.Value, err = time.ParseDuration(s)
	return err
}

func (f DurationFlagVar) String() string {
	// compare with orig. DurationFlag.String()
	return f.Value.String()
}

//////////////////
// DurationFlag //
//////////////////

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func (f DurationFlag) ApplyWithError(set *flag.FlagSet) error {
	// construction via `FlagSet.Var` to override duration-parsing default
	fvar := DurationFlagVar(f)
	parts := splitCsv(f.Name)
	for _, name := range parts {
		name = strings.Trim(name, " ")
		set.Var(&fvar, name, f.Usage)
	}
	return nil
}

func (f DurationFlag) String() string {
	// compare with DurationFlagVar.String()
	s := cli.FlagStringer(f)

	// TODO: remove the " (default: ...)" suffix - it only makes sense when actually supported
	re := regexp.MustCompile(` \(default: \S+\)$`)
	if loc := re.FindStringIndex(s); loc != nil {
		s = s[:loc[0]]
	}
	return s
}

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func (f DurationFlag) GetName() string { return f.Name }

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func (f DurationFlag) Apply(set *flag.FlagSet) { _ = f.ApplyWithError(set) }

//
// flag parsers & misc. helpers
//

// flag's printable name
func flprn(f cli.Flag) string { return flagPrefix + fl1n(f.GetName()) }

// in single quotes
func qflprn(f cli.Flag) string { return "'" + flprn(f) + "'" }

// return the first name
func fl1n(flagName string) string {
	if strings.IndexByte(flagName, ',') < 0 {
		return flagName
	}
	l := splitCsv(flagName)
	return l[0]
}

func flagIsSet(c *cli.Context, flag cli.Flag) (v bool) {
	name := fl1n(flag.GetName()) // take the first of multiple names
	switch flag.(type) {
	case cli.BoolFlag:
		v = c.Bool(name)
	case cli.BoolTFlag:
		v = c.BoolT(name)
	default:
		v = c.GlobalIsSet(name) || c.IsSet(name)
	}
	return
}

// Returns the value of a string flag (either parent or local scope - here and elsewhere)
func parseStrFlag(c *cli.Context, flag cli.Flag) string {
	flagName := fl1n(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalString(flagName)
	}
	return c.String(flagName)
}

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func parseIntFlag(c *cli.Context, flag cli.IntFlag) int {
	flagName := fl1n(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalInt(flagName)
	}
	return c.Int(flagName)
}

func parseDurationFlag(c *cli.Context, flag cli.Flag) time.Duration {
	flagName := fl1n(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalDuration(flagName)
	}
	return c.Duration(flagName)
}

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func parseUnitsFlag(c *cli.Context, flag cli.StringFlag) (units string, err error) {
	units = parseStrFlag(c, flag) // enum { unitsSI, ... }
	if err = teb.ValidateUnits(units); err != nil {
		err = fmt.Errorf("%s=%s is invalid: %v", flprn(flag), units, err)
	}
	return
}

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func parseSizeFlag(c *cli.Context, flag cli.StringFlag, unitsParsed ...string) (int64, error) {
	var (
		err   error
		units string
		val   = parseStrFlag(c, flag)
	)
	if len(unitsParsed) > 0 {
		units = unitsParsed[0]
	} else if flagIsSet(c, unitsFlag) {
		units, err = parseUnitsFlag(c, unitsFlag)
		if err != nil {
			return 0, err
		}
	}
	return cos.ParseSize(val, units)
}

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func parseRetriesFlag(c *cli.Context, flag cli.IntFlag, warn bool) (retries int) {
	const (
		maxr = 5
		efmt = "invalid option '%s=%d' (expecting 1..5 range)"
	)
	if !flagIsSet(c, flag) {
		return 0
	}
	retries = parseIntFlag(c, flag)
	if retries < 0 {
		if warn {
			actionWarn(c, fmt.Sprintf(efmt, flprn(flag), retries))
		}
		return 0
	}
	if retries > maxr {
		if warn {
			actionWarn(c, fmt.Sprintf(efmt, flprn(flag), retries))
		}
		return maxr
	}
	return retries
}

//nolint:gocritic // ignoring hugeParam - following the orig. github.com/urfave style
func parseNumWorkersFlag(c *cli.Context, flag cli.IntFlag) (n int, err error) {
	n = parseIntFlag(c, flag)
	if n < 0 {
		return n, fmt.Errorf("%s cannot be negative", qflprn(flag))
	}
	mp := 2 * sys.MaxParallelism() // NOTE: imposing (hard-coded) limit
	if n > mp {
		warn := fmt.Sprintf("%s exceeds allowed maximum (2 * CPU cores) = %d - proceeding with %d workers...",
			qflprn(flag), mp, mp)
		actionWarn(c, warn)
		n = mp
	}
	return n, nil
}

func rmFlags(flags []cli.Flag, fs ...cli.Flag) (out []cli.Flag) {
	out = make([]cli.Flag, 0, len(flags))
loop:
	for _, flag := range flags {
		for _, f := range fs {
			if flag.GetName() == f.GetName() {
				continue loop
			}
		}
		out = append(out, flag)
	}
	return
}

func sortFlags(fls []cli.Flag) []cli.Flag {
	sort.Slice(fls, func(i, j int) bool { return fls[i].GetName() < fls[j].GetName() })
	return fls
}

func parseLhotseBatchFlags(c *cli.Context) (batchSize int, pt *cos.ParsedTemplate, _ error) {
	batchSize = parseIntFlag(c, batchSizeFlag)
	if batchSize <= 0 {
		return 0, nil, nil // manifest => single TAR
	}

	template := parseStrFlag(c, outputTemplateFlag)
	if template == "" {
		return 0, nil, fmt.Errorf("%s must be set when %s > 0", qflprn(outputTemplateFlag), qflprn(batchSizeFlag))
	}

	tmpl, err := cos.NewParsedTemplate(template)
	if err != nil {
		return 0, nil, err
	}
	pt = &tmpl
	if err := pt.CheckIsRange(); err != nil {
		return 0, nil, err
	}
	return batchSize, pt, nil
}

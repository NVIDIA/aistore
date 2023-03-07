// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
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

// compare with DurationFlag.String()
func (f DurationFlagVar) String() string {
	return f.Value.String()
}

//////////////////
// DurationFlag //
//////////////////

// construction via `FlagSet.Var` to override duration-parsing default
func (f DurationFlag) ApplyWithError(set *flag.FlagSet) error {
	fvar := DurationFlagVar(f)
	parts := splitCsv(f.Name)
	for _, name := range parts {
		name = strings.Trim(name, " ")
		set.Var(&fvar, name, f.Usage)
	}
	return nil
}

// compare with DurationFlagVar.String()
func (f DurationFlag) String() string {
	s := cli.FlagStringer(f)

	// TODO: remove the " (default: ...)" suffix - it only makes sense when actually supported
	re := regexp.MustCompile(` \(default: \S+\)$`)
	if loc := re.FindStringIndex(s); loc != nil {
		s = s[:loc[0]]
	}
	return s
}

func (f DurationFlag) GetName() string         { return f.Name }
func (f DurationFlag) Apply(set *flag.FlagSet) { _ = f.ApplyWithError(set) }

//
// flag parsers & misc. helpers
//

// flag's printable name
func flprn(f cli.Flag) string { return "--" + fl1n(f.GetName()) }

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

// Returns the value of a string flag (either parent or local scope)
func parseStrFlag(c *cli.Context, flag cli.Flag) string {
	flagName := fl1n(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalString(flagName)
	}
	return c.String(flagName)
}

// Returns the value of an int flag (either parent or local scope)
func parseIntFlag(c *cli.Context, flag cli.IntFlag) int {
	flagName := fl1n(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalInt(flagName)
	}
	return c.Int(flagName)
}

// Returns the value of an duration flag (either parent or local scope)
func parseDurationFlag(c *cli.Context, flag cli.Flag) time.Duration {
	flagName := fl1n(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalDuration(flagName)
	}
	return c.Duration(flagName)
}

// enum { unitsSI, ... }
func parseUnitsFlag(c *cli.Context, flag cli.StringFlag) (units string, err error) {
	units = parseStrFlag(c, flag)
	if err = teb.ValidateUnits(units); err != nil {
		err = fmt.Errorf("%s=%s is invalid: %v", flprn(flag), units, err)
	}
	return
}

func parseSizeFlag(c *cli.Context, flag cli.StringFlag) (int64, error) {
	var (
		err   error
		units string
		val   = parseStrFlag(c, flag)
	)
	if flagIsSet(c, unitsFlag) {
		units, err = parseUnitsFlag(c, unitsFlag)
		if err != nil {
			return 0, err
		}
	}
	return cos.ParseSize(val, units)
}

func parseCksumFlags(c *cli.Context) []*cos.Cksum {
	cksums := []*cos.Cksum{}
	for _, ckflag := range supportedCksumFlags {
		if flagIsSet(c, ckflag) {
			cksums = append(cksums, cos.NewCksum(ckflag.GetName(), parseStrFlag(c, ckflag)))
		}
	}
	return cksums
}

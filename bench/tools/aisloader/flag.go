// Package aisloader
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"flag"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	BoolExt struct {
		IsSet bool
		Val   bool
	}

	DurationExt struct {
		IsSet bool
		Val   time.Duration
	}

	StringExt struct {
		IsSet bool
		Val   string
	}
)

// interface guard
var (
	_ flag.Value = (*BoolExt)(nil)
	_ flag.Value = (*DurationExt)(nil)
	_ flag.Value = (*StringExt)(nil)
)

func (b *BoolExt) Set(s string) (err error) {
	b.Val, err = cos.ParseBool(s)
	b.IsSet = true
	return err
}

func (b *BoolExt) Get() any       { return b.Val }
func (b *BoolExt) String() string { return strconv.FormatBool(b.Val) }
func (*BoolExt) IsBoolFlag() bool { return true }

func BoolExtVar(f *flag.FlagSet, p *BoolExt, name, usage string) {
	f.Var(p, name, usage)
}

func (d *DurationExt) Set(s string) (err error) {
	d.Val, err = time.ParseDuration(s)
	d.IsSet = true
	return err
}

func (d *DurationExt) Get() any       { return d.Val }
func (d *DurationExt) String() string { return d.Val.String() }
func (*DurationExt) IsBoolFlag() bool { return false }

func DurationExtVar(f *flag.FlagSet, p *DurationExt, name string, defValue time.Duration, usage string) {
	p.Val = defValue
	f.Var(p, name, usage)
}

func (s *StringExt) Set(v string) error {
	s.Val = v
	s.IsSet = true
	return nil
}

func (s *StringExt) Get() any       { return s.Val }
func (s *StringExt) String() string { return s.Val }

func StringExtVar(f *flag.FlagSet, p *StringExt, name, defValue, usage string) {
	p.Val = defValue
	f.Var(p, name, usage)
}

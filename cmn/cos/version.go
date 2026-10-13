// Package cos provides low-level common utilities.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const versionSepa = "."

// Version represents AIS major.minor software version.
//
// The optional Rc suffix preserves release-candidate/build qualifiers such as
// "5.0.rc1" for display, but compatibility checks intentionally compare only
// major and minor version numbers.
type Version struct {
	Major int
	Minor int
	Rc    string // optional suffix
}

func ParseVersion(s string) (v Version, ok bool) {
	smajor, rest, ok := strings.Cut(s, versionSepa)
	if !ok {
		return v, false
	}
	sminor, rc, _ := strings.Cut(rest, versionSepa)
	major, err := strconv.Atoi(smajor)
	if err != nil || major < 0 {
		return v, false
	}
	minor, err := strconv.Atoi(sminor)
	if err != nil || minor < 0 {
		return v, false
	}
	debug.Assert(major >= 4, s)
	return Version{Major: major, Minor: minor, Rc: rc}, true
}

func (v Version) String() string {
	s := strconv.Itoa(v.Major) + versionSepa + strconv.Itoa(v.Minor)
	if v.Rc != "" {
		s += versionSepa + v.Rc
	}
	return s
}

// VersionGap returns absolute minor-version distance for identical majors.
// Different major versions are always incompatible.
func VersionGap(expected, actual Version) (gap int, incompat bool) {
	if expected.Major != actual.Major {
		return 2, true
	}
	gap = expected.Minor - actual.Minor
	if gap < 0 {
		gap = -gap
	}
	return gap, gap > 1
}

// compare software versions by major and minor, parsing the minimum on demand
// - use capability constants from cmn/ver_const.go as minimums
// - ignore RC/build suffixes; return false if either version is empty or malformed
func VersionAtLeast(raw, minimum string) bool {
	minVer, minOk := ParseVersion(minimum)
	debug.Assert(minOk, minimum)
	if !minOk {
		return false
	}
	v, rawOk := ParseVersion(raw)
	if !rawOk {
		return false
	}
	return v.Major > minVer.Major || (v.Major == minVer.Major && v.Minor >= minVer.Minor)
}

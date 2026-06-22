// Package cos provides low-level common utilities.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"strconv"
	"strings"
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
	mm := strings.Split(s, versionSepa)
	if len(mm) < 2 {
		return v, false
	}
	major, err := strconv.Atoi(mm[0])
	if err != nil || major < 0 {
		return v, false
	}
	minor, err := strconv.Atoi(mm[1])
	if err != nil || minor < 0 {
		return v, false
	}
	v.Major, v.Minor = major, minor
	if len(mm) > 2 {
		v.Rc = strings.Join(mm[2:], versionSepa)
	}
	return v, true
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

// SupportsVersionAtLeast checks whether raw version is at least min.
//
// Release-candidate suffixes are ignored: for compatibility/capability checks,
// "5.0.rc1" satisfies "5.0".
func SupportsVersionAtLeast(raw string, minVer Version) bool {
	v, ok := ParseVersion(raw)
	if !ok {
		return false
	}
	return v.Major > minVer.Major || (v.Major == minVer.Major && v.Minor >= minVer.Minor)
}

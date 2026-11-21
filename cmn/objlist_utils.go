// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

////////////////
// LsoEntries //
////////////////

func (entries LsoEntries) cmp(i, j int) bool {
	eni, enj := entries[i], entries[j]
	return eni.less(enj)
}

////////////
// LsoEnt (for apc.* constants, see api/apc/lsmsg)
////////////

// flags:
// - see above "LsoEntry Flags" enum
// - keeping IsPresent for client convenience, with Set/IsAnyFlag covering for the rest
// - terms "cached", "present" and "in-cluster" - are interchangeable
func (be *LsoEnt) IsPresent() bool { return be.Flags&apc.EntryIsCached != 0 }

func (be *LsoEnt) SetFlag(fl uint16)           { be.Flags |= fl }
func (be *LsoEnt) ClrFlag(fl uint16)           { be.Flags &^= fl }
func (be *LsoEnt) IsAnyFlagSet(fl uint16) bool { return be.Flags&fl != 0 }

// location _status_
func (be *LsoEnt) IsStatusOK() bool { return be.Status() == 0 }
func (be *LsoEnt) Status() uint16   { return be.Flags & apc.LsoStatusMask }

// sorting
func (be *LsoEnt) less(oe *LsoEnt) bool {
	if be.IsAnyFlagSet(apc.EntryIsDir) {
		if oe.IsAnyFlagSet(apc.EntryIsDir) {
			return be.Name < oe.Name
		}
		return true
	}
	if oe.IsAnyFlagSet(apc.EntryIsDir) {
		return false
	}
	if be.Name == oe.Name {
		return be.Status() < oe.Status()
	}
	return be.Name < oe.Name
}

func (be *LsoEnt) CopyWithProps(propsSet cos.StrSet) (ne *LsoEnt) {
	ne = &LsoEnt{Name: be.Name}
	if propsSet.Contains(apc.GetPropsSize) {
		ne.Size = be.Size
	}
	if propsSet.Contains(apc.GetPropsChecksum) {
		ne.Checksum = be.Checksum
	}
	if propsSet.Contains(apc.GetPropsAtime) {
		ne.Atime = be.Atime
	}
	if propsSet.Contains(apc.GetPropsVersion) {
		ne.Version = be.Version
	}
	if propsSet.Contains(apc.GetPropsLocation) {
		ne.Location = be.Location
	}
	if propsSet.Contains(apc.GetPropsCustom) {
		ne.Custom = be.Custom
	}
	if propsSet.Contains(apc.GetPropsCopies) {
		ne.Copies = be.Copies
	}
	return
}

// Sorting list-objects results.
//
// SortLsoLex() is intended for non-recursive listing where directory entries are included.
// The default SortLso() uses dirs-first ordering (via `LsoEnt.less`) which groups
// virtual directories before objects. This breaks token-based pagination because tokens
// are lexicographical cursors - AIS always returns entries where `name > token`.

func SortLso(entries LsoEntries) { sort.Slice(entries, entries.cmp) }

func SortLsoLex(entries LsoEntries) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Name != entries[j].Name {
			return entries[i].Name < entries[j].Name
		}
		return entries.cmp(i, j) // or compare status/flags deterministically
	})
}

// Returns true if the continuation token >= object's name (in other words, the object is
// already listed and must be skipped). Note that string `>=` is lexicographic.
func TokenGreaterEQ(token, objName string) bool { return token >= objName }

// directory has to either:
// - include (or match) prefix, or
// - be contained in prefix - motivation: don't SkipDir a/b when looking for a/b/c
// an alternative name for this function could be smth. like SameBranch()
// see also: cos.TrimPrefix
func DirHasOrIsPrefix(dirPath, prefix string) bool {
	debug.Assert(prefix != "")
	return strings.HasPrefix(prefix, dirPath) || strings.HasPrefix(dirPath, prefix)
}

// see also: cos.TrimPrefix
func ObjHasPrefix(objName, prefix string) bool {
	debug.Assert(prefix != "")
	return strings.HasPrefix(objName, prefix)
}

// CheckDirNoRecurs determines how to handle a directory during non-recursive listing.
//
// Parameters:
//   - prefix:  the user's query prefix (e.g., "aaa/" or "")
//   - relPath: the directory being visited during filesystem walk (e.g., "aaa", "aaa/bbb")
//
// Returns:
//   - addDirEntry: true = add this directory to listing results
//   - error:       SkipDir = don't walk into this directory; nil = continue walking into it
//
// Cases (assuming prefix="aaa/"):
//
//	| relPath       | suffix    | addDirEntry | error   | Reason                              |
//	|---------------|-----------|-------------|---------|-------------------------------------|
//	| "bbb"         | "bbb"     | false       | SkipDir | Wrong subtree, skip entirely        |
//	| "aaa"         | ""        | false       | nil     | Prefix dir itself, walk into it     |
//	| "aaa/bbb"     | "bbb"     | true        | SkipDir | Direct child, add & don't recurse   |
//	| "aaa/bbb/ccc" | "bbb/ccc" | false       | SkipDir | Too deep, skip entirely             |
//
// Special case: if prefix="" or "/", add all top-level directories (return true, SkipDir).
func CheckDirNoRecurs(prefix, relPath string) (addDirEntry bool, _ error) {
	// Case 1: Empty prefix - user wants root level, add all top-level dirs
	if prefix == "" || prefix == cos.PathSeparator {
		return true, filepath.SkipDir
	}

	// Normalize: "aaa/" -> "aaa"
	prefix = cos.TrimLastB(prefix, '/')

	// Extract what comes after the prefix
	suffix := strings.TrimPrefix(relPath, prefix)

	// Case 2: relPath doesn't start with prefix - wrong subtree
	if suffix == relPath {
		return false, filepath.SkipDir
	}

	// Remove leading separator: "/bbb" -> "bbb"
	suffix = strings.TrimPrefix(suffix, cos.PathSeparator)

	// Case 3: relPath equals prefix directory itself - walk into it but don't add
	if suffix == "" {
		return false, nil
	}

	// Case 4: suffix contains "/" - nested too deep, skip
	if strings.Contains(suffix, cos.PathSeparator) {
		return false, filepath.SkipDir
	}

	// Case 5: Direct child directory - add to results, don't recurse into it
	return true, filepath.SkipDir
}

//
// LsoEnt.Custom ------------------------------------------------------------
// e.g. "[ETag:67c24314d6587da16bfa50dd4d2f6a0a LastModified:2023-09-20T21:04:51Z]
//

const (
	cusbeg = '[' // begin (key-value, ...) string
	cusend = ']' // end   --/--
	cusepa = ':' // key:val
	cusdlm = ' ' // k1:v1 k2:v2
)

var (
	stdCustomProps = [...]string{SourceObjMD, ETag, LsoLastModified, cos.HdrLastModified, CRC32CObjMD, MD5ObjMD, VersionObjMD}
)

// a leading colon marks an escaped pair (metadata keys cannot be empty).
// marked pairs use RFC 3986-style percent-encoding (section 2.1).
// e.g. [ETag:abc :note:a%20b]. unmarked pairs always retain literal percent signs.
var (
	customKeyEsc = strings.NewReplacer("%", "%25", " ", "%20", ":", "%3A")
	customValEsc = strings.NewReplacer("%", "%25", " ", "%20")
)

func writeCustom(sb *cos.SB, k, v string) {
	if sb.Len() > 1 {
		sb.WriteUint8(cusdlm)
	}
	if strings.IndexByte(v, cusdlm) >= 0 || strings.IndexByte(k, cusdlm) >= 0 || strings.IndexByte(k, cusepa) >= 0 {
		sb.WriteUint8(cusepa)
		k = customKeyEsc.Replace(k)
		v = customValEsc.Replace(v)
	}
	sb.WriteString(k)
	sb.WriteUint8(cusepa)
	sb.WriteString(v)
}

// reserved keys are excluded; standard keys come first (see S3Props).
func CustomMD2S(md cos.StrKVs) string {
	var sb cos.SB
	sb.Init(256)
	sb.WriteUint8(cusbeg)
	left := len(md)
	for _, k := range stdCustomProps {
		if v, ok := md[k]; ok {
			writeCustom(&sb, k, v)
			left--
		}
	}
	if left > 0 {
		for k, v := range md {
			if !isReservedCust(k) && !slices.Contains(stdCustomProps[:], k) {
				writeCustom(&sb, k, v)
			}
		}
	}
	sb.WriteUint8(cusend)
	return sb.String()
}

func CustomProps2S(nvs ...string) string {
	var (
		sb   cos.SB
		size int
	)
	for _, s := range nvs {
		size += len(s) + 1
	}
	sb.Init(size + 2)
	sb.WriteUint8(cusbeg)
	for i := 0; i < len(nvs); i += 2 {
		writeCustom(&sb, nvs[i], nvs[i+1])
	}
	sb.WriteUint8(cusend)
	return sb.String()
}

func _customBody(s string) string {
	if len(s) >= 2 && s[0] == cusbeg && s[len(s)-1] == cusend {
		return s[1 : len(s)-1]
	}
	return ""
}

// note the order: ETag and LastModified first, after the optional source
// (see stdCustomProps);
// the remaining values can contain spaces, e.g., "Content-Type:text/plain; charset=utf-8"
func (be *LsoEnt) S3Props() (etag, modified string) {
	s := _customBody(be.Custom)
	_, s = cutS3Prop(s, SourceObjMD+":")
	etag, s = cutS3Prop(s, ETag+":")
	modified, _ = cutS3Prop(s, LsoLastModified+":")
	return
}

func cutS3Prop(s, prefix string) (value, rest string) {
	body, escaped := strings.CutPrefix(s, ":")
	tail, ok := strings.CutPrefix(body, prefix)
	if !ok {
		return "", s
	}
	value, rest, _ = strings.Cut(tail, " ")
	if escaped {
		value, _ = url.PathUnescape(value)
	}
	return
}

// old writers emitted unquoted spaces. keep them until the next key:value.
func cutLegacyValue(key, s string) (value, rest string) {
	if key == cos.HdrLastModified && len(s) >= len(http.TimeFormat) {
		// the time of day in an HTTP date also contains colons
		date := s[:len(http.TimeFormat)]
		if tail := s[len(date):]; strings.HasSuffix(date, " GMT") && (tail == "" || tail[0] == cusdlm) {
			return date, strings.TrimPrefix(tail, " ")
		}
	}
	i := strings.IndexByte(s, cusdlm)
	for i >= 0 {
		tail := s[i+1:]
		word, _, more := strings.Cut(tail, " ")
		if strings.IndexByte(word, cusepa) >= 0 {
			return s[:i], tail
		}
		if !more {
			break
		}
		i += len(word) + 1
	}
	return s, ""
}

// cut one pair; an empty key ends the scan
func _cutCustomPair(s string) (key, value, rest string) {
	body, escaped := strings.CutPrefix(s, ":")
	key, tail, ok := strings.Cut(body, ":")
	if !ok || key == "" {
		return "", "", ""
	}
	if !escaped {
		value, rest = cutLegacyValue(key, tail)
		return
	}
	value, rest, _ = strings.Cut(tail, " ")
	var err error
	if key, err = url.PathUnescape(key); err != nil {
		return "", "", ""
	}
	if value, err = url.PathUnescape(value); err != nil {
		return "", "", ""
	}
	return
}

func S2CustomMD(md cos.StrKVs, custom, version string) {
	debug.Assert(len(md) == 0)
	for s := _customBody(custom); s != ""; {
		k, v, rest := _cutCustomPair(s)
		if k == "" {
			break
		}
		s = rest
		// a legacy value can contain what looks like a second copy of a key
		if _, exists := md[k]; !exists {
			md[k] = v
		}
	}
	if len(md) != 0 && md[VersionObjMD] == "" && version != "" {
		md[VersionObjMD] = version
	}
}

func S2CustomVal(custom, name string) string {
	for s := _customBody(custom); s != ""; {
		k, v, rest := _cutCustomPair(s)
		if k == "" {
			break
		}
		s = rest
		if k == name {
			return v
		}
	}
	return ""
}

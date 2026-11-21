// Package tests_test exercises AIStore's common types and utilities.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
)

//
// action messages --------------------------------------------------------------------------------
//

type actmsgTestConf struct {
	action       string
	vals         []string
	isSelectObjs bool
}

func testRawUnmarshal(t *testing.T, tc actmsgTestConf) {
	t.Run(tc.action, func(t *testing.T) {
		for _, val := range tc.vals {
			msg := &apc.ActMsg{}
			raw := fmt.Sprintf(val, tc.action)
			err := jsoniter.Unmarshal([]byte(raw), &msg)
			tassert.CheckError(t, err)
			tassert.Errorf(t, tc.action == msg.Action, "actions do not match (%q vs %q)", tc.action, msg.Action)

			// parse the template
			if tc.isSelectObjs {
				lrMsg := &apc.ListRange{}
				err = cos.MorphMarshal(msg.Value, lrMsg)
				tassert.CheckError(t, err)
			}
		}
	})
}

func TestActMsgRawUnmarshal(t *testing.T) {
	tests := []actmsgTestConf{
		{
			action: apc.ActEvictObjects,
			vals: []string{
				`{"action":"%s","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"%s","value":{"objnames":["o1","o2","o3"]}}`,
			},
			isSelectObjs: true,
		},
		{
			action: apc.ActPrefetchObjects,
			vals: []string{
				`{"action":"%s","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"%s","value":{"objnames":["o1","o2","o3"]}}`,
			},
			isSelectObjs: true,
		},
		{
			action: apc.ActDeleteObjects,
			vals: []string{
				`{"action":"%s","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"%s","value":{"objnames":["o1","o2","o3"]}}`,
			},
			isSelectObjs: true,
		},
		{
			action: apc.ActSetBprops,
			vals: []string{
				`{"action":"%s","value":{"checksum": {"type": "sha256"}, "mirror": {"enable": true}}}`,
			},
		},
		{
			action: apc.ActCreateBck,
			vals: []string{
				`{"action":"%s","value":{"checksum": {"type": "sha256"}, "mirror": {"enable": true}}}`,
			},
		},
		{
			action: apc.ActXactStart,
			vals: []string{
				`{"action":"%s","value":{"kind": "rebalance"}}`,
			},
		},
		{
			action: apc.ActXactStop,
			vals: []string{
				`{"action":"%s","value":{"kind": "rebalance"}}`,
			},
		},
		{
			action: apc.ActList,
			vals: []string{
				`{"action":"%s","value":{"props": "size"}}`,
			},
		},
		{
			action: apc.ActPromote,
			vals: []string{
				`{"action":"%s","value":{"target": "234ed78", "recurs": true, "keep": false}}`,
			},
		},
	}
	for _, test := range tests {
		testRawUnmarshal(t, test)
	}
}

//
// bucket properties and summaries ---------------------------------------------------------
//

func TestBpropsPlacementUpdate(t *testing.T) {
	for _, test := range []struct {
		name        string
		initial     bool
		update      *string
		wantUniform bool
		wantErr     bool
	}{
		{name: "set-uniform", update: apc.Ptr("uniform"), wantUniform: true},
		{name: "reset-empty", initial: true, update: apc.Ptr("")},
		{name: "keep-uniform", initial: true, wantUniform: true},
		{name: "default"},
		{name: "invalid", initial: true, update: apc.Ptr("node"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := &cmn.Bprops{Provider: apc.AIS, Cksum: cmn.CksumConf{Type: cos.ChecksumNone}}
			if test.initial {
				original.Placement = &cmn.PlacementConf{Weighting: cmn.WeightingUniform}
			}
			if err := original.Validate(3); err != nil {
				t.Fatal(err)
			}
			update := &cmn.BpropsToSet{}
			if test.update != nil {
				var err error
				update, err = cmn.NewBpropsToSet(cos.StrKVs{cmn.PropPlacementWeighting: *test.update})
				if err != nil {
					t.Fatal(err)
				}
			}
			props := original.Clone()
			props.Apply(update)
			err := props.Validate(3)
			if test.initial && original.Placement.Weighting != cmn.WeightingUniform {
				t.Fatal("update mutated original bucket properties")
			}
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), cmn.PropPlacementWeighting) {
					t.Fatalf("expected placement validation error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantUniform {
				if props.Placement == nil || props.Placement.Weighting != cmn.WeightingUniform {
					t.Fatalf("expected uniform placement, got %+v", props.Placement)
				}
			} else if props.Placement != nil {
				t.Fatalf("default placement not normalized: %+v", props.Placement)
			}
			data, err := cos.JSON.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}
			if present := bytes.Contains(data, []byte(`"placement":`)); present != test.wantUniform {
				t.Fatalf("placement wire presence = %t, want %t", present, test.wantUniform)
			}
		})
	}
}

func TestBpropsHTTPExtraCompatibility(t *testing.T) {
	var bprops cmn.Bprops
	data := []byte(`{"extra":{"http":{"original_url":"https://example.com/"},"aws":{"profile":"p1"},"gcp":{"application_creds":"gcp.json"},"oci":{"region":"us-phoenix-1"},"custom":"k=v"}}`)
	if err := cos.JSON.Unmarshal(data, &bprops); err != nil {
		t.Fatal(err)
	}
	want := cmn.ExtraProps{
		AWS:    cmn.ExtraPropsAWS{Profile: "p1"},
		GCP:    cmn.ExtraPropsGCP{ApplicationCreds: "gcp.json"},
		OCI:    cmn.ExtraPropsOCI{Region: "us-phoenix-1"},
		Custom: "k=v",
	}
	if !reflect.DeepEqual(bprops.Extra, want) {
		t.Fatalf("got %+v, want %+v", bprops.Extra, want)
	}
}

func TestBsummAggregatePerTarget(t *testing.T) {
	mk := func(name string, cnt uint64, size uint64) *cmn.BsummResult {
		r := &cmn.BsummResult{Bck: cmn.Bck{Name: name, Provider: apc.AIS}}
		r.ObjCount.Present = cnt
		r.TotalSize.PresentObjs = size
		r.ObjSize.Min = math.MaxInt64 // as in: empty target
		return r
	}
	for _, perTarget := range []bool{false, true} {
		var all cmn.AllBsummResults
		for tid, cnt := range map[string]uint64{"t1": 10, "t2": 20, "t3": 30} {
			ptid := ""
			if perTarget {
				ptid = tid
			}
			all = all.Aggregate(mk("a", cnt, cnt*100), ptid)
			all = all.Aggregate(mk("b", cnt+1, 0), ptid)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 buckets, got %d", len(all))
		}
		for _, summ := range all {
			var sum uint64
			for _, tres := range summ.Targets {
				sum += tres.ObjCount.Present
				if tres.ObjSize.Min != 0 {
					t.Errorf("%s: expected zero min size, got %d", summ.Bck.Name, tres.ObjSize.Min)
				}
			}
			switch {
			case !perTarget && summ.Targets != nil:
				t.Errorf("%s: unexpected per-target results", summ.Bck.Name)
			case perTarget && len(summ.Targets) != 3:
				t.Errorf("%s: expected 3 targets, got %d", summ.Bck.Name, len(summ.Targets))
			case perTarget && sum != summ.ObjCount.Present:
				t.Errorf("%s: per-target sum %d != aggregate %d", summ.Bck.Name, sum, summ.ObjCount.Present)
			}
		}
	}
}

//
// object equality --------------------------------------------------------------------
//

// NOTE: duplicated it here for the test
var supportedRemAttrs = [...]string{cmn.VersionObjMD, cmn.CRC32CObjMD, cmn.MD5ObjMD, cmn.ETag, cos.HdrLastModified, cos.HdrContentType}

func TestCheckEqKeys(t *testing.T) {
	for _, key := range append(supportedRemAttrs[:], cmn.SourceObjMD) {
		var a, b cmn.ObjAttrs
		for _, oa := range []*cmn.ObjAttrs{&a, &b} { // the same size and checksum
			oa.Size = 1
			oa.SetCksum(cos.ChecksumOneXxh, "0123456789abcdef")
		}
		a.SetCustomKey(key, "a")
		b.SetCustomKey(key, "b")
		reads, listed := a.CheckEq(&b) != nil, slices.Contains(cmn.CheckEqKeys[:], key)
		if reads != listed {
			t.Errorf("%q: CheckEq reads it: %t, CheckEqKeys has it: %t", key, reads, listed)
		}
	}
}

func TestCheckEq(t *testing.T) {
	const (
		xxh  = "0123456789abcdef"
		crc  = "89abcdef"
		etag = "e7a3c1d5f0b2948e7a3c1d5f0b2948ee" // unquoted, as stored in custom MD
	)
	type md struct {
		size     int64
		ty, val  string // checksum
		ver, tag string // version, ETag
	}
	mk := func(m md) *cmn.ObjAttrs {
		oa := &cmn.ObjAttrs{Size: m.size}
		if m.ty != "" {
			oa.SetCksum(m.ty, m.val)
		}
		if m.ver != "" {
			oa.SetCustomKey(cmn.VersionObjMD, m.ver)
		}
		if m.tag != "" {
			oa.SetCustomKey(cmn.ETag, m.tag)
		}
		return oa
	}
	for _, tc := range []struct {
		name     string
		loc, rem md
		eq       bool
	}{
		// checksum of the same type
		{"same-cksum", md{ty: cos.ChecksumCesXxh, val: xxh}, md{ty: cos.ChecksumCesXxh, val: xxh}, true},
		{"diff-cksum", md{ty: cos.ChecksumCesXxh, val: xxh, ver: "1"}, md{ty: cos.ChecksumCesXxh, val: "fedcba9876543210", ver: "1"}, false},
		{"crc-alone", md{ty: cos.ChecksumCRC32C, val: crc}, md{ty: cos.ChecksumCRC32C, val: crc}, false}, // not trusted
		{"crc-and-version", md{ty: cos.ChecksumCRC32C, val: crc, ver: "1"}, md{ty: cos.ChecksumCRC32C, val: crc, ver: "1"}, true},

		// checksums of different types: no information
		{"cross-type-alone", md{ty: cos.ChecksumCRC32C, val: crc}, md{ty: cos.ChecksumCesXxh, val: xxh}, false},
		{"cross-type-and-version", md{ty: cos.ChecksumCRC32C, val: crc, ver: "1"}, md{ty: cos.ChecksumCesXxh, val: xxh, ver: "1"}, false},
		{"cross-type-version-etag", md{ty: cos.ChecksumCRC32C, val: crc, ver: "1", tag: etag}, md{ty: cos.ChecksumCesXxh, val: xxh, ver: "1", tag: etag}, true},
		{"cross-type-etag", md{ty: cos.ChecksumCRC32C, val: crc, tag: etag}, md{ty: cos.ChecksumCesXxh, val: xxh, tag: etag}, true},

		// other
		{"size", md{size: 1, ver: "1", tag: etag}, md{size: 2, ver: "1", tag: etag}, false},
		{"version", md{ver: "1", tag: etag}, md{ver: "2", tag: etag}, false},
		{"etag", md{tag: etag}, md{tag: "f8b4d2e6a1c3059f8b4d2e6a1c3059ff"}, false},
		{"no-local-md", md{}, md{size: 1, ty: cos.ChecksumCesXxh, val: xxh, ver: "1", tag: etag}, false}, // e.g., not present
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := mk(tc.loc).CheckEq(mk(tc.rem))
			if (err == nil) != tc.eq {
				t.Errorf("expected eq=%t, got %v", tc.eq, err)
			}
		})
	}
}

//
// listing metadata --------------------------------------------------------------------------
//

func TestLsoCustomDelimiter(t *testing.T) {
	const modified = "2026-11-20T12:34:56Z"
	md := cos.StrKVs{
		cmn.ETag:            "abc",
		cmn.LsoLastModified: modified,
		cos.HdrContentType:  "text/plain; charset=utf-8",
		cos.HdrLastModified: "Fri, 20 Nov 2026 12:34:56 GMT",
		"note":              " a  b ETag:other ",
		"x y:z":             "quotes \" and \\ and ]",
		"empty":             "",
		"quoted":            `"literal"`,
		"raw":               "last\xff byte",
		"verbatim%20":       "%20+%zz",
		"escaped %20:":      "%20 + %%",
	}
	nvs := []string{cmn.ETag, md[cmn.ETag], cmn.LsoLastModified, modified}
	for k, v := range md {
		if k != cmn.ETag && k != cmn.LsoLastModified {
			nvs = append(nvs, k, v)
		}
	}
	for _, custom := range []string{cmn.CustomMD2S(md), cmn.CustomProps2S(nvs...)} {
		got := make(cos.StrKVs)
		cmn.S2CustomMD(got, custom, "")
		if !maps.Equal(got, md) {
			t.Fatalf("round trip %q: got %#v, want %#v", custom, got, md)
		}
		for k, v := range md {
			if got := cmn.S2CustomVal(custom, k); got != v {
				t.Fatalf("lookup %q: got %q, want %q", k, got, v)
			}
		}
		en := cmn.LsoEnt{Custom: custom}
		if etag, mtime := en.S3Props(); etag != "abc" || mtime != modified {
			t.Fatalf("S3 listing: got %q, %q", etag, mtime)
		}
	}
	en := cmn.LsoEnt{Custom: cmn.CustomProps2S(cmn.SourceObjMD, "remote source", cmn.ETag, "two words", cmn.LsoLastModified, modified)}
	if etag, mtime := en.S3Props(); etag != "two words" || mtime != modified {
		t.Fatalf("escaped S3 prefix: got %q, %q", etag, mtime)
	}
	if got := cmn.CustomProps2S(cmn.ETag, "abc", "note", "a b"); got != "[ETag:abc :note:a%20b]" {
		t.Fatal(got)
	}
	if got := cmn.CustomProps2S(cmn.ETag, "abc", "version", "42"); got != "[ETag:abc version:42]" {
		t.Fatal(got)
	}
}

func TestLsoCustomLegacy(t *testing.T) {
	const date = "Fri, 20 Nov 2026 12:34:56 GMT"
	const custom = `[ETag:abc Last-Modified:` + date + ` Content-Type:text/plain; charset=utf-8 note:"two words" ETag:other]`
	md := make(cos.StrKVs)
	cmn.S2CustomMD(md, custom, "42")
	want := cos.StrKVs{cmn.ETag: "abc", cos.HdrLastModified: date, cos.HdrContentType: "text/plain; charset=utf-8", "note": `"two words"`, cmn.VersionObjMD: "42"}
	if !maps.Equal(md, want) {
		t.Fatalf("got %#v, want %#v", md, want)
	}
	if got := cmn.S2CustomVal(custom, "Tag"); got != "" {
		t.Fatalf("matched part of a key: %q", got)
	}
}

// network

func TestIsBlockedEgressIP(t *testing.T) {
	tests := []struct {
		name         string
		ip           string
		allowPrivate bool
		blocked      bool
	}{
		{"public IPv4", "8.8.8.8", false, false},
		{"public IPv6", "2001:4860:4860::8888", false, false},
		{"loopback", "127.0.0.1", false, true},
		{"IPv4-mapped loopback", "::ffff:127.0.0.1", false, true},
		{"metadata", "169.254.169.254", false, true},
		{"private", "10.0.0.1", false, true},
		{"private allowed", "10.0.0.1", true, false},
		{"ULA", "fd00::1", false, true},
		{"ULA allowed", "fd00::1", true, false},
		{"CGNAT", "100.64.0.1", false, true},
		{"CGNAT with private allowed", "100.64.0.1", true, true},
		{"NAT64 metadata", "64:ff9b::169.254.169.254", false, true},
		{"NAT64 private", "64:ff9b::10.0.0.1", false, true},
		{"NAT64 private allowed", "64:ff9b::10.0.0.1", true, false},
		{"NAT64 public", "64:ff9b::8.8.8.8", false, false},
		{"local NAT64", "64:ff9b:1::8.8.8.8", false, true},
		{"6to4", "2002:c001:0203::1", false, true},
		{"6to4 with private allowed", "2002:c001:0203::1", true, true},
		{"Teredo", "2001:0000:4136:e378:8000:63bf:5665:5665", false, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cmn.IsBlockedEgressIP(net.ParseIP(test.ip), test.allowPrivate); got != test.blocked {
				t.Fatalf("IsBlockedEgressIP(%s, %t) = %t, expected %t", test.ip, test.allowPrivate, got, test.blocked)
			}
		})
	}
	if !cmn.IsBlockedEgressIP(nil, false) {
		t.Fatal("invalid IP must be blocked")
	}
}

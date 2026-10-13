// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
)

func TestParseNVPairs(t *testing.T) {
	for _, args := range [][]string{
		{"key=value"}, {"key", "value"}, {"key=", "value"},
		{"key", "=value"}, {"key", "=", "value"}, {" key = value "},
		{"\u2003key\t=\tvalue\u2003"},
	} {
		pairs, err := parseNVPairs(args, nil)
		want := nvpairList{{Name: "key", Value: "value"}}
		if err != nil || !reflect.DeepEqual(pairs, want) {
			t.Fatalf("%q: pairs=%v err=%v, want %v", args, pairs, err, want)
		}
	}
	for _, test := range []struct {
		args []string
		want nvpairList
	}{
		{[]string{"key="}, nvpairList{{Name: "key"}}},
		{[]string{"key", ""}, nvpairList{{Name: "key"}}},
		{[]string{"key", "="}, nvpairList{{Name: "key"}}},
		{[]string{"key=", "other=value"}, nvpairList{{Name: "key"}, {Name: "other", Value: "value"}}},
		{[]string{"key", "=", "other=value"}, nvpairList{{Name: "key", Value: "other=value"}}},
		{[]string{"key=a=b,c=d"}, nvpairList{{Name: "key", Value: "a=b,c=d"}}},
		{[]string{"key", "a=b,c=d"}, nvpairList{{Name: "key", Value: "a=b,c=d"}}},
		{[]string{"name with spaces", "value with spaces"}, nvpairList{{Name: "name with spaces", Value: "value with spaces"}}},
		{[]string{`name\=with\ spaces=value\=with\ spaces`}, nvpairList{{Name: "name=with spaces", Value: "value=with spaces"}}},
		{[]string{`key=\ value\ `}, nvpairList{{Name: "key", Value: " value "}}},
		{[]string{`key=C:\\path\file`}, nvpairList{{Name: "key", Value: `C:\path\file`}}},
		{[]string{`key=literal\n`}, nvpairList{{Name: "key", Value: `literal\n`}}},
		{[]string{`key=trailing\`}, nvpairList{{Name: "key", Value: `trailing\`}}},
		{[]string{`json={"text":"a=b, c","path":"a\\b","newline":"a\nb"}`},
			nvpairList{{Name: "json", Value: `{"text":"a=b, c","path":"a\\b","newline":"a\nb"}`}}},
		{[]string{"json=", `{"text":"a=b, c","path":"a\\b"}`},
			nvpairList{{Name: "json", Value: `{"text":"a=b, c","path":"a\\b"}`}}},
		{[]string{"key=first", "key=second"}, nvpairList{{Name: "key", Value: "first"}, {Name: "key", Value: "second"}}},
	} {
		pairs, err := parseNVPairs(test.args, nil)
		if err != nil || !reflect.DeepEqual(pairs, test.want) {
			t.Fatalf("%q: pairs=%v err=%v, want %v", test.args, pairs, err, test.want)
		}
	}
}

func TestParseNVPairsErrors(t *testing.T) {
	for _, args := range [][]string{{"key"}, {"=value"}, {"", "value"}, {"key=value", "dangling"}} {
		if _, err := parseNVPairs(args, nil); err == nil {
			t.Fatalf("%q: expected an invalid-pair error", args)
		}
	}
}

func TestSplitNVArgs(t *testing.T) {
	args := splitNVArgs(` checksum.type = md5   extra.custom=two\ words `)
	nvs, err := makeBckPropPairs(args)
	want := cos.StrKVs{"checksum.type": "md5", "extra.custom": "two words"}
	if err != nil || !reflect.DeepEqual(nvs, want) {
		t.Fatalf("pairs=%v err=%v, want %v", nvs, err, want)
	}
}

func TestSplitNVArgsJSON(t *testing.T) {
	for _, value := range []string{
		`{"text":"a b", "nested":[1, {"key":"x=y, } ["}]}`,
		`["a b", {"path":"a\\b", "quote":"\" }"}]`,
	} {
		for _, input := range []string{
			"meta=" + value, "meta " + value, "meta= " + value,
			"meta =" + value, "meta = " + value,
		} {
			pairs, err := parseNVPairs(splitNVArgs(input+" checksum.type=md5"), nil)
			want := nvpairList{{Name: "meta", Value: value}, {Name: "checksum.type", Value: "md5"}}
			if err != nil || !reflect.DeepEqual(pairs, want) {
				t.Fatalf("%q: pairs=%v err=%v, want %v", input, pairs, err, want)
			}
		}
	}
	// brackets in ordinary names and values do not protect whitespace
	input := `t[abc]=3 extra.custom=prefix{ suffix`
	want := []string{"t[abc]=3", "extra.custom=prefix{", "suffix"}
	if args := splitNVArgs(input); !reflect.DeepEqual(args, want) {
		t.Fatalf("%q: args=%v, want %v", input, args, want)
	}
}

func TestBckPropPairsCase(t *testing.T) {
	for _, args := range [][]string{
		{"Access=GET,PUT", "Features=S3-Use-Path-Style", "Backend_Bck=none"},
		{"ACCESS", "GET", "PUT", "FEATURES", "S3-Use-Path-Style", "BACKEND_BCK", "none"},
	} {
		nvs, err := makeBckPropPairs(args)
		want := cos.StrKVs{"access": (apc.AceGET | apc.AcePUT).String(), "features": feat.S3UsePathStyle.String(), "backend_bck": "none"}
		if err != nil || !reflect.DeepEqual(nvs, want) {
			t.Fatalf("%q: pairs=%v err=%v, want %v", args, nvs, err, want)
		}
	}
	for _, args := range [][]string{{"Mirror.Enabled"}, {"Mirror.Enabled", "Mirror.Copies", "2"}} {
		_, err := makeBckPropPairs(args)
		if err == nil || err.Error() != `missing property "mirror.enabled" value` {
			t.Fatalf("%q: err=%v", args, err)
		}
	}
}

func TestNVPairsLists(t *testing.T) {
	for _, args := range [][]string{
		{"log.modules", "ec", "xs", "log.level", "4"},
		{"log.modules=ec", "xs", "log.level=4"},
		{"log.modules=", "ec", "xs", "log.level", "=4"},
		{"log.modules", "=", "ec", "xs", "log.level", "=", "4"},
	} {
		nvs, err := makePairs(args, nil)
		want := cos.StrKVs{"log.modules": "ec,xs", "log.level": "4"}
		if err != nil || !reflect.DeepEqual(nvs, want) {
			t.Fatalf("%q: pairs=%v err=%v, want %v", args, nvs, err, want)
		}
	}
	for _, args := range [][]string{
		{"access", "GET", "PUT", "features", "S3-Use-Path-Style", "checksum.type", "md5"},
		{"access=GET,PUT", "features=S3-Use-Path-Style", "checksum.type=md5"},
		{"access", "=", "GET", "PUT", "features", "=S3-Use-Path-Style", "checksum.type=", "md5"},
		{"access=GET PUT", "features=S3-Use-Path-Style", "checksum.type=md5"}, // members within one argument
	} {
		nvs, err := makeBckPropPairs(args)
		want := cos.StrKVs{"access": (apc.AceGET | apc.AcePUT).String(), "features": feat.S3UsePathStyle.String(), "checksum.type": "md5"}
		if err != nil || !reflect.DeepEqual(nvs, want) {
			t.Fatalf("%q: pairs=%v err=%v, want %v", args, nvs, err, want)
		}
	}
}

// known names prevent misaligned pairs: missing values and invalid list members fail at the offending token
func TestNVPairsKnownNames(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"mirror.enabled", "mirror.copies", "2"}, `missing property "mirror.enabled" value`},
		{[]string{"checksum.type", "mirror.enabled", "true"}, `missing property "checksum.type" value`},
		{[]string{"checksum.type", "mirror.enabled=true"}, `missing property "checksum.type" value`},
		{[]string{"access", "GET", "HEAD", "mirror.copies", "3"}, `invalid access value "HEAD"`},
		{[]string{"features", "S3-Use-Path-Style", "bogus", "checksum.type", "md5"}, `invalid features value "bogus"`},
		{[]string{"=true"}, `missing property name in "=true"`},
		{[]string{"mirror.copies"}, `missing property "mirror.copies" value`},
		{[]string{"unknown.field"}, `invalid property "unknown.field"`},
		{[]string{"unknown.field", "mirror.copies", "2"}, `invalid property "unknown.field"`},
		{[]string{"unknown.field", "blah", "access", "GET", "HEAD"}, `unknown property "unknown.field"`},
		{[]string{"unknown.field=blah", "features", "S3-Use-Path-Style", "bogus"}, `unknown property "unknown.field"`},
		{[]string{"provider=ais", "access", "GET", "HEAD"}, `property "provider" is readonly`},
		{[]string{"provider", "ais", "access", "GET", "HEAD"}, `property "provider" is readonly`},
	} {
		_, err := makeBckPropPairs(test.args)
		if err == nil || err.Error() != test.want {
			t.Fatalf("%q: err=%v, want %q", test.args, err, test.want)
		}
	}

	// pseudo-property and case-insensitive names (see reformatBackendProps, cmn.NewBpropsToSet)
	for _, args := range [][]string{{"backend_bck=s3://abc"}, {"backend_bck", "none"}, {"Mirror.Enabled=true"}} {
		if _, err := makeBckPropPairs(args); err != nil {
			t.Fatalf("%q: %v", args, err)
		}
	}
	if _, err := makeBckPropPairs([]string{"Provider=ais"}); err == nil || err.Error() != `property "provider" is readonly` {
		t.Fatalf("err=%v", err)
	}

	// "name=" does not consume a following known name
	nvs, err := makeBckPropPairs([]string{"extra.aws.endpoint=", "checksum.type", "md5"})
	want := cos.StrKVs{"extra.aws.endpoint": "", "checksum.type": "md5"}
	if err != nil || !reflect.DeepEqual(nvs, want) {
		t.Fatalf("pairs=%v err=%v, want %v", nvs, err, want)
	}
	// valid lists still end at the next known name
	nvs, err = makeBckPropPairs([]string{"access", "GET", "HEAD-OBJECT", "mirror.copies", "3"})
	want = cos.StrKVs{"access": (apc.AceGET | apc.AceObjHEAD).String(), "mirror.copies": "3"}
	if err != nil || !reflect.DeepEqual(nvs, want) {
		t.Fatalf("pairs=%v err=%v, want %v", nvs, err, want)
	}

	// config: same diagnostics given known names; none without
	names := []string{"log.level", "log.modules", "space.highwm"}
	if _, err := makePairs([]string{"log.level", "space.highwm", "98"}, names); err == nil {
		t.Fatal("expected missing-value error")
	}
	if _, err := makePairs([]string{"log.modules", "ec", "bogus", "log.level", "4"}, names); err == nil ||
		err.Error() != `invalid log.modules value "bogus"` {
		t.Fatalf("err=%v", err)
	}
	if nvs, err := makePairs([]string{"log.level", "space.highwm"}, nil); err != nil || nvs["log.level"] != "space.highwm" {
		t.Fatalf("no names: pairs=%v err=%v", nvs, err)
	}
}

// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestCfgToSet(t *testing.T) {
	// name is a path; values sharing a section merge
	toUpdate, err := cfgToSet(cos.StrKVs{
		"checksum": `{"type":"md5","validate_warm_get":true}`,
		"net.http": `{"idle_conns":64}`,
		"net.l4":   `{"sndrcv_buf_size":131072}`,
		"space":    `{"highwm":91}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if toUpdate.Cksum == nil || toUpdate.Cksum.Type == nil || *toUpdate.Cksum.Type != "md5" ||
		toUpdate.Cksum.ValidateWarmGet == nil || !*toUpdate.Cksum.ValidateWarmGet {
		t.Fatalf("checksum: %+v", toUpdate.Cksum)
	}
	if toUpdate.Net == nil || toUpdate.Net.HTTP == nil || toUpdate.Net.HTTP.MaxIdleConns == nil ||
		*toUpdate.Net.HTTP.MaxIdleConns != 64 || toUpdate.Net.L4 == nil || toUpdate.Net.L4.SndRcvBufSize == nil {
		t.Fatalf("net: %+v", toUpdate.Net)
	}
	if toUpdate.Space == nil || toUpdate.Space.HighWM == nil || *toUpdate.Space.HighWM != 91 {
		t.Fatalf("space: %+v", toUpdate.Space)
	}
	if toUpdate.Mirror != nil || toUpdate.EC != nil {
		t.Fatalf("unexpected sections: %+v", toUpdate)
	}

	for _, nvs := range []cos.StrKVs{
		{"checksum.type": `{"type":"md5"}`}, // leaf name with a section value
		{"space": `{"highwm":"high"}`},      // type mismatch
		{"space": `{"highwn":91}`},          // unknown field
		{"net.http": `{"idle_conn":64}`},    // unknown nested field
		{"log": `{"modules":"ec"}`},         // CLI pseudo-property is not a JSON field
	} {
		if _, err := cfgToSet(nvs); err == nil {
			t.Fatalf("%v: expected error", nvs)
		}
	}
	if _, err := cfgToSet(cos.StrKVs{"bogus": `{"a":1}`}); err == nil || !strings.Contains(err.Error(), NIY) {
		t.Fatalf("unknown section: err=%v", err)
	}
}

func TestIsCfgSection(t *testing.T) {
	propList := []string{"checksum.type", "net.http.idle_conns", "features"}
	for name, want := range map[string]bool{
		"checksum": true, "net": true, "net.http": true,
		"checksum.type": false, "features": false, "check": false, "net.ht": false,
	} {
		if got := isCfgSection(propList, name); got != want {
			t.Fatalf("%q: got %v, want %v", name, got, want)
		}
	}
}

func TestIsFmtJSON(t *testing.T) {
	for _, test := range []struct {
		nvs        cos.StrKVs
		ans, isErr bool
	}{
		{cos.StrKVs{"log.level": "4"}, false, false},
		{cos.StrKVs{"checksum": `{"type":"md5"}`}, true, false},
		{cos.StrKVs{"checksum": `{"type":"md5"}`, "log.level": "4"}, true, true},
		{cos.StrKVs{"checksum.type": `{"type":`}, false, false}, // not JSON: a plain value
	} {
		_, ans, err := isFmtJSON(test.nvs)
		if ans != test.ans || (err != nil) != test.isErr {
			t.Fatalf("%v: ans=%v err=%v", test.nvs, ans, err)
		}
	}
}

func TestParseJobFilter(t *testing.T) {
	col, regex, err := parseJobFilter(` state = Run=ning\d `)
	if err != nil || col != jobColState || regex.String() != `Run=ning\d` {
		t.Fatalf("col=%q regex=%v err=%v", col, regex, err)
	}
	for _, s := range []string{"STATE", "COLOR=red", "STATE=", "STATE=(", "=x"} {
		if _, _, err := parseJobFilter(s); err == nil {
			t.Fatalf("%q: expected error", s)
		}
	}
}

func TestSetLogModules(t *testing.T) {
	// log.level on the same command line: no API call
	for _, test := range []struct {
		modules, want string
	}{
		{"ec,xs", "4 (modules: ec,xs)"},
		{" ec , xs ", "4 (modules: ec,xs)"},
		{"ec xs", "4 (modules: ec,xs)"},
		{"none", "4"},
		{"", "4"},
	} {
		nvs := cos.StrKVs{confLogModules: test.modules, confLogLevel: "4"}
		if err := setLogModules(nvs, nil); err != nil {
			t.Fatalf("%q: %v", test.modules, err)
		}
		if _, ok := nvs[confLogModules]; ok {
			t.Fatalf("%q: %s not removed", test.modules, confLogModules)
		}
		if got := cos.LogLevel(nvs[confLogLevel]).String(); got != test.want {
			t.Fatalf("%q: got %q, want %q", test.modules, got, test.want)
		}
	}
	for _, nvs := range []cos.StrKVs{
		{confLogModules: "ec,bogus", confLogLevel: "4"},
		{confLogModules: "ec", confLogLevel: "four"},
		{confLogModules: "ec", confLogLevel: "0"},
	} {
		if err := setLogModules(nvs, nil); err == nil {
			t.Fatalf("%v: expected error", nvs)
		}
	}
	// no log.modules: no-op
	nvs := cos.StrKVs{confLogLevel: "3"}
	if err := setLogModules(nvs, nil); err != nil || nvs[confLogLevel] != "3" {
		t.Fatalf("nvs=%v err=%v", nvs, err)
	}
}

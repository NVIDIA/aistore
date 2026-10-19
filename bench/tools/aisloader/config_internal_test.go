// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestLoadConfig(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing.yaml")
	tests := []struct {
		name, input string
		args        []string // success cases must end with numworkers=20, from config or CLI
		wantErr     string
		wantArchPct int
	}{
		{name: "empty config", input: "{}", args: []string{"-numworkers=20"}},
		{name: "explicit empty", args: []string{"-config="}, wantErr: "-config requires a nonempty path"},
		{name: "missing file", args: []string{"-config=" + missingPath}, wantErr: "open " + missingPath + ":"},
		{name: "partial config", input: "numworkers: 20"},
		{name: "CLI overrides bad config", input: "numworkers: broken", args: []string{"-numworkers=20"}},
		{name: "invalid duration", input: configYAML("# workload", "duration: broken"), wantErr: `line 2: duration="broken":`},
		{name: "quoted integer", input: "numworkers: '20'"},
		{name: "aliases", input: configYAML("numworkers: &count 20", "arch.pct: *count"), wantArchPct: 20},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, p := newConfigFlags(t)
			path := writeTestConfig(t, test.input)
			args := append([]string{"-config", path}, test.args...)
			err := f.Parse(args)
			if err == nil {
				err = loadConfig(f, p.configPath)
			}
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if p.numWorkers != 20 {
				t.Fatalf("workers = %d, want 20", p.numWorkers)
			}
			if p.archParams.pct != test.wantArchPct {
				t.Fatalf("archive percentage = %d, want %d", p.archParams.pct, test.wantArchPct)
			}
		})
	}

	t.Run("Load Sample Config", func(t *testing.T) {
		f, p := newConfigFlags(t)
		if err := f.Parse([]string{"-config", "config.yaml"}); err != nil {
			t.Fatal(err)
		}
		if err := loadConfig(f, p.configPath); err != nil {
			t.Fatal(err)
		}
		f.VisitAll(func(fl *flag.Flag) {
			if fl.Name == "config" {
				return
			}
			if got := fl.Value.String(); got != fl.DefValue {
				t.Errorf("sample %s = %q, want default %q", fl.Name, got, fl.DefValue)
			}
		})
		if p.duration.IsSet || p.cleanUp.IsSet {
			t.Errorf("sample set omitted options: duration=%+v, cleanup=%+v", p.duration, p.cleanUp)
		}
	})
}

func TestReadConfigRejectsInvalid(t *testing.T) {
	tests := []struct{ name, input, wantErr string }{
		{"unknown key", "unknown: value", "line 1: unknown config key \"unknown\""},
		{"duplicate key", configYAML("ip: null", "ip:"), "line 2: duplicate config key \"ip\""},
		{"scalar document", "value", "config must be a flat YAML mapping"},
		{"nested value", "bucket: [a, b]", "line 1: bucket: config values must be scalars"},
		{"multiple documents", configYAML("{}", "---", "ip: 127.0.0.1"), "config must contain exactly one YAML document"},
		{"cli-only flags", "dry-run: true", "line 1: \"dry-run\" is CLI-only"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, _ := newConfigFlags(t)
			if _, err := readConfig(strings.NewReader(test.input), f); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestConfigPresence(t *testing.T) {
	t.Run("Null Leaves Unset", func(t *testing.T) {
		f, p := newConfigFlags(t)
		loadTestConfig(t, f, p, configYAML("duration: null", "cleanup: null"))
		if p.duration.IsSet || p.cleanUp.IsSet {
			t.Fatalf("null set options: duration=%+v, cleanup=%+v", p.duration, p.cleanUp)
		}
		f.Visit(func(fl *flag.Flag) {
			if fl.Name != "config" {
				t.Errorf("null flag %q was marked as supplied", fl.Name)
			}
		})
	})

	for _, test := range []struct {
		value string
		want  time.Duration
	}{
		{"0", 0},
		{"1m", time.Minute},
	} {
		t.Run("Explicit Duration "+test.value, func(t *testing.T) {
			f, p := newConfigFlags(t)
			loadTestConfig(t, f, p, configYAML("duration: "+test.value, "cleanup: false"))
			if !p.duration.IsSet || p.duration.Val != test.want || !p.cleanUp.IsSet || p.cleanUp.Val {
				t.Fatalf("explicit options lost presence: duration=%+v, cleanup=%+v", p.duration, p.cleanUp)
			}
		})
	}
}

func newConfigFlags(t *testing.T) (*flag.FlagSet, *params) {
	t.Helper()
	resetValidationGlobals(t)
	oldCargs, oldProfile, oldPathStyle := cargs, s3Profile, s3UsePathStyle
	oldUsage, oldVersion, oldQuiet := flagUsage, flagVersion, flagQuiet
	t.Cleanup(func() {
		cargs, s3Profile, s3UsePathStyle = oldCargs, oldProfile, oldPathStyle
		flagUsage, flagVersion, flagQuiet = oldUsage, oldVersion, oldQuiet
	})
	f := flag.NewFlagSet("aisloader", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	p := &params{}
	addCmdLine(f, p)
	return f, p
}

func configYAML(lines ...string) string { return strings.Join(lines, "\n") }

func writeTestConfig(t *testing.T, input string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(input), cos.PermRWR); err != nil {
		t.Fatal(err)
	}
	return path
}

func loadTestConfig(t *testing.T, f *flag.FlagSet, p *params, input string) {
	t.Helper()
	if err := f.Parse([]string{"-config", writeTestConfig(t, input)}); err != nil {
		t.Fatal(err)
	}
	if err := loadConfig(f, p.configPath); err != nil {
		t.Fatal(err)
	}
}

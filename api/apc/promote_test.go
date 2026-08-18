// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
)

func TestValidatePromoteSource(t *testing.T) {
	tests := []struct {
		src  string
		want bool
	}{
		{apc.PromoteRoot, true},
		{apc.PromoteRoot + "/", true},
		{apc.PromoteRoot + "/data", true},
		{apc.PromoteRoot + "/data/file", true},
		{apc.PromoteRoot + "/../promote/data", false}, // traversal rejected, not cleaned
		{"", false},
		{"relative/path", false},
		{"/tmp/data", false},
		{"/var/lib/ais", false},
		{"/var/lib/ais/promote-extra", false},
		{"/var/lib/ais/promote2", false},
		{apc.PromoteRoot + "/../passwd", false},
		{apc.PromoteRoot + "/foo/../../passwd", false},
		{apc.PromoteRoot + "/..", false},     // resolves to the parent of the promote root
		{apc.PromoteRoot + "/./..", false},   // ditto
		{apc.PromoteRoot + "/foo/..", false}, // rejected even though it resolves back inside
		{apc.PromoteRoot + "/.", false},
		{apc.PromoteRoot + "/~/secret", true}, // literal directory named "~"
		{apc.PromoteRoot + "/.hidden", true},  // dotfile
		{apc.PromoteRoot + "/..hidden", true}, // ditto
	}
	for _, tt := range tests {
		err := apc.ValidatePromoteSource(tt.src)
		if tt.want && err != nil {
			t.Errorf("ValidatePromoteSource(%q): unexpected error: %v", tt.src, err)
		}
		if !tt.want && err == nil {
			t.Errorf("ValidatePromoteSource(%q): expected error", tt.src)
		}
	}
}

func TestValidatePromote(t *testing.T) {
	src := apc.PromoteRoot + "/data"
	tests := []struct {
		name    string
		args    *apc.PromoteArgs
		wantErr bool
		wantSrc string
	}{
		{
			name:    src,
			args:    &apc.PromoteArgs{},
			wantSrc: src,
		},
		{
			name:    "",
			args:    &apc.PromoteArgs{SrcFQN: src},
			wantSrc: src,
		},
		{
			name:    src,
			args:    &apc.PromoteArgs{SrcFQN: src},
			wantSrc: src,
		},
		{
			name:    src + "/",
			args:    &apc.PromoteArgs{SrcFQN: src},
			wantErr: true, // exact match; do not Clean
		},
		{
			name:    src,
			args:    &apc.PromoteArgs{SrcFQN: apc.PromoteRoot + "/other"},
			wantErr: true,
		},
		{
			name:    "/tmp/data",
			args:    &apc.PromoteArgs{},
			wantErr: true,
		},
		{
			name:    src,
			args:    &apc.PromoteArgs{ObjName: "../x"},
			wantErr: true,
		},
		{
			name:    apc.PromoteRoot + "/../promote/data",
			args:    &apc.PromoteArgs{},
			wantErr: true,
		},
		{
			name:    src,
			args:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := apc.ValidatePromote(tt.name, tt.args)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ValidatePromote(%q, %+v): expected error", tt.name, tt.args)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValidatePromote(%q, %+v): unexpected error: %v", tt.name, tt.args, err)
			continue
		}
		if got != tt.wantSrc {
			t.Errorf("ValidatePromote(%q): got %q, want %q", tt.name, got, tt.wantSrc)
		}
	}
}

// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "testing"

func TestValidatePromoteSource(t *testing.T) {
	tests := []struct {
		src  string
		want bool
	}{
		{PromoteRoot, true},
		{PromoteRoot + "/", true},
		{PromoteRoot + "/data", true},
		{PromoteRoot + "/data/file", true},
		{PromoteRoot + "/../promote/data", false}, // traversal rejected, not cleaned
		{"", false},
		{"relative/path", false},
		{"/tmp/data", false},
		{"/var/lib/ais", false},
		{"/var/lib/ais/promote-extra", false},
		{"/var/lib/ais/promote2", false},
		{PromoteRoot + "/../passwd", false},
		{PromoteRoot + "/foo/../../passwd", false},
		{PromoteRoot + "/..", false},     // resolves to the parent of the promote root
		{PromoteRoot + "/./..", false},   // ditto
		{PromoteRoot + "/foo/..", false}, // rejected even though it resolves back inside
		{PromoteRoot + "/.", false},
		{PromoteRoot + "/~/secret", true}, // literal directory named "~"
		{PromoteRoot + "/.hidden", true},  // dotfile
		{PromoteRoot + "/..hidden", true}, // ditto
	}
	for _, tt := range tests {
		err := validatePromoteSource(tt.src)
		if tt.want && err != nil {
			t.Errorf("ValidatePromoteSource(%q): unexpected error: %v", tt.src, err)
		}
		if !tt.want && err == nil {
			t.Errorf("ValidatePromoteSource(%q): expected error", tt.src)
		}
	}
}

func TestValidatePromote(t *testing.T) {
	src := PromoteRoot + "/data"
	tests := []struct {
		name    string
		args    *PromoteArgs
		wantErr bool
		wantSrc string
	}{
		{
			name:    src,
			args:    &PromoteArgs{},
			wantSrc: src,
		},
		{
			name:    "",
			args:    &PromoteArgs{SrcFQN: src},
			wantSrc: src,
		},
		{
			name:    src,
			args:    &PromoteArgs{SrcFQN: src},
			wantSrc: src,
		},
		{
			name:    src + "/",
			args:    &PromoteArgs{SrcFQN: src},
			wantErr: true, // exact match; do not Clean
		},
		{
			name:    src,
			args:    &PromoteArgs{SrcFQN: PromoteRoot + "/other"},
			wantErr: true,
		},
		{
			name:    "/tmp/data",
			args:    &PromoteArgs{},
			wantErr: true,
		},
		{
			name:    src,
			args:    &PromoteArgs{ObjName: "../x"},
			wantErr: true,
		},
		{
			name:    PromoteRoot + "/../promote/data",
			args:    &PromoteArgs{},
			wantErr: true,
		},
		{
			name:    src,
			args:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := ValidatePromote(tt.name, tt.args)
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

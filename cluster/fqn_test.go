/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package cluster provides local access to cluster-level metadata
package cluster

import (
	"path/filepath"
	"testing"
)

const customWorkfileType = "custom"

type customWorkfile struct{}

func (wf *customWorkfile) PermToMove() bool {
	return false
}

func (wf *customWorkfile) PermToEvict() bool {
	return false
}

func (wf *customWorkfile) PermToProcess() bool {
	return false
}

func (wf *customWorkfile) GenUniqueFQN(base string) string {
	return base
}

func (wf *customWorkfile) ParseUniqueFQN(base string) (orig string, old bool, ok bool) {
	return base, false, true
}

func TestContentFQNGeneration(t *testing.T) {
	tests := []struct {
		testName string
		dir      string
		base     string
		ty       string
	}{
		{
			"simple file name with extension",
			"/root/dfc/",
			"superfilename.txt",
			DefaultWorkfileType,
		},
		{
			"file name with some numbers and dots in name",
			"/root/dfc/",
			"super.file.name1001..txt",
			DefaultWorkfileType,
		},
		{
			"file name with numbers in name",
			"/root/dfc/",
			"101ec-slice1001",
			DefaultWorkfileType,
		},

		{
			"custom workfile type",
			"/root/dfc/",
			"101ec-slice1001",
			customWorkfileType,
		},
	}

	RegisterFileType(customWorkfileType, &customWorkfile{})
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			fqn := GenContentFQN(filepath.Join(tt.dir, tt.base), tt.ty)
			spec, info := FileSpec(fqn)
			if spec == nil {
				t.Errorf("spec should not be empty for %s", fqn)
			}
			if info == nil {
				t.Fatalf("info should not be empty for %s", fqn)
			}

			if info.Dir != tt.dir {
				t.Errorf("dirs differ; expected: %s, got: %s", tt.dir, info.Dir)
			}
			if info.Base != tt.base {
				t.Errorf("bases differ; expected: %s, got: %s", tt.base, info.Base)
			}
			if info.Type != tt.ty {
				t.Errorf("types differ; expected: %s, got: %s", tt.ty, info.Type)
			}
		})
	}
}

func TestFileTypeRegistration(t *testing.T) {
	if err := RegisterFileType("something.with.dot", &customWorkfile{}); err == nil {
		t.Error("registration should return error when file type contains dot")
	}

	if err := RegisterFileType(DefaultWorkfileType, &customWorkfile{}); err == nil {
		t.Error("registration should return error when same file type is registered twice")
	}
}

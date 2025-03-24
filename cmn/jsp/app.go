// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"fmt"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func LoadAppConfig(configDir, configFname string, v any) error {
	path := filepath.Join(configDir, configFname)
	if err := cos.Stat(path); err != nil {
		return err
	}
	if _, err := Load(path, v, Options{Indent: true}); err != nil {
		return fmt.Errorf("failed to load config file %q: %w", path, err)
	}
	return nil
}

func SaveAppConfig(configDir, configFname string, v any) error {
	path := filepath.Join(configDir, configFname)
	return Save(path, v, Options{Indent: true}, nil /*sgl*/)
}

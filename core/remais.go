// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package core

import "github.com/NVIDIA/aistore/core/meta"

type (
	RemAis struct {
		URL   string     `json:"url"`
		Alias string     `json:"alias"`
		UUID  string     `json:"uuid"` // Smap.UUID
		Smap  *meta.Smap `json:"smap"`
	}
	Remotes struct {
		A   []*RemAis `json:"a"`
		Ver int64     `json:"ver"`
	}
)

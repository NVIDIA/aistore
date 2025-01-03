// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package meta

type (
	RemAis struct {
		Smap  *Smap  `json:"smap"`
		URL   string `json:"url"`
		Alias string `json:"alias"`
		UUID  string `json:"uuid"` // Smap.UUID
	}
	RemAisVec struct {
		A   []*RemAis `json:"a"`
		Ver int64     `json:"ver"`
	}
)

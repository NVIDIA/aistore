// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package meta

type (
	RemAis struct {
		URL   string `json:"url"`
		Alias string `json:"alias"`
		UUID  string `json:"uuid"` // Smap.UUID
		Smap  *Smap  `json:"smap"`
	}
	RemAisVec struct {
		A   []*RemAis `json:"a"`
		Ver int64     `json:"ver"`
	}
)

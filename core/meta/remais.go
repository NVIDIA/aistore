// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	// node ID => remote clusters as currently seen by this node
	// (see apc.WhatRemaisNodes)
	RemAisNodes struct {
		Proxy  map[string]*RemAisVec `json:"proxy"`
		Target map[string]*RemAisVec `json:"target"`
	}
)

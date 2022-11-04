// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

type (
	RemAis struct {
		URL     string `json:"url"`
		Alias   string `json:"alias"` // NOTE: strings.Join(aliases, RemAisAliasSeparator)
		UUID    string `json:"uuid"`  // Smap.UUID
		Primary string `json:"primary"`
		Smap    int64  `json:"smap"`
		Targets int32  `json:"targets"`
		Online  bool   `json:"online"`
	}
	RemAises struct {
		A   []*RemAis `json:"a"`
		Ver int64     `json:"ver"`
	}
)

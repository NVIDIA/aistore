// Package apc: API constants and message types
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// control message to generate bucket summary or summaries
type BckSummMsg struct {
	UUID   string `json:"uuid"`
	Fast   bool   `json:"fast"`
	Cached bool   `json:"cached"`
}

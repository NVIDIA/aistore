// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"time"
)

type Msg struct {
	ID          string        `json:"id"`
	WaitTimeout time.Duration `json:"wait_timeout"`
	CommType    string        `json:"communication_type"`
	Spec        []byte        `json:"spec"`
}

// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import jsoniter "github.com/json-iterator/go"

type Msg struct {
	Id   string              `json:"id"`
	Spec jsoniter.RawMessage `json:"spec"`
}

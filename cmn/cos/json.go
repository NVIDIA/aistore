// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

type (
	JSONRawMsgs map[string]jsoniter.RawMessage
)

// JSON is used to Marshal/Unmarshal API json messages and is initialized in init function.
var JSON jsoniter.API

func init() {
	rtie.Store(1013)

	jsonConf := jsoniter.Config{
		EscapeHTML:             false, // we don't send HTMLs
		ValidateJsonRawMessage: false, // RawMessages are validated by "morphing"
		DisallowUnknownFields:  true,  // make sure we have exactly the struct user requested.
		SortMapKeys:            true,
	}
	JSON = jsonConf.Froze()
}

//
// JSON & JSONLocal
//

func MustMarshalToString(v any) string {
	s, err := JSON.MarshalToString(v)
	debug.AssertNoErr(err)
	return s
}

// MustMarshal marshals v and panics if error occurs.
func MustMarshal(v any) []byte {
	b, err := JSON.Marshal(v)
	AssertNoErr(err)
	return b
}

func MorphMarshal(data, v any) error {
	// `data` can be of type `map[string]any` or just same type as `v`.
	// Therefore, the easiest way is to marshal the `data` again and unmarshal it
	// with hope that every field will be set correctly.
	b := MustMarshal(data)
	return JSON.Unmarshal(b, v)
}

func MustMorphMarshal(data, v any) {
	err := MorphMarshal(data, v)
	AssertNoErr(err)
}

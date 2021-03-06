// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"errors"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type (
	JSONRawMsgs  map[string]jsoniter.RawMessage
	DurationJSON time.Duration
	SizeJSON     int64
)

//////////////////
// DurationJSON //
//////////////////

func (d DurationJSON) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(d.String()) }
func (d DurationJSON) String() string               { return time.Duration(d).String() }
func (d DurationJSON) IsZero() bool                 { return d == 0 }

func (d *DurationJSON) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := jsoniter.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = DurationJSON(value)
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = DurationJSON(tmp)
		return nil
	default:
		return errors.New("invalid duration")
	}
}

//////////////
// SizeJSON //
//////////////

func (sj SizeJSON) MarshalJSON() ([]byte, error) {
	return []byte(B2S(int64(sj), 2)), nil
}

// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type Msg struct {
	ID          string `json:"id"`
	WaitTimeout string `json:"wait_timeout"`
	CommType    string `json:"communication_type"`
	Spec        []byte `json:"spec"`
}

func (m *Msg) Validate() error {
	cmn.Assert(m.ID != "")

	if m.WaitTimeout == "" {
		m.WaitTimeout = "0s"
	}
	if _, err := time.ParseDuration(m.WaitTimeout); err != nil {
		return fmt.Errorf("wait timeout is invalid: %v", err)
	}

	if m.CommType == "" {
		// By default assume `putComm`
		m.CommType = putCommType
	}
	if !cmn.StringInSlice(m.CommType, []string{pushPullCommType, putCommType}) {
		return fmt.Errorf("unknown communication type: %q", m.CommType)
	}
	return nil
}

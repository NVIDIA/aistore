// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	// NOTE: see closely related `api.XactReqArgs` and comments
	// TODO: apc package, here and elsewhere
	QueryMsg struct {
		OnlyRunning *bool     `json:"show_active"`
		Bck         cmn.Bck   `json:"bck"`
		ID          string    `json:"id"`
		Kind        string    `json:"kind"`
		DaemonID    string    `json:"node,omitempty"`
		Buckets     []cmn.Bck `json:"buckets,omitempty"`
	}
)

//////////////
// QueryMsg //
//////////////

func (msg *QueryMsg) String() (s string) {
	if msg.ID == "" {
		s = fmt.Sprintf("x-%s", msg.Kind)
	} else {
		s = fmt.Sprintf("x-%s[%s]", msg.Kind, msg.ID)
	}
	if msg.Bck.IsEmpty() {
		return
	}
	return fmt.Sprintf("%s, bucket %s", s, msg.Bck)
}

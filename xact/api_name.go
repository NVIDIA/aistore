// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	SepaID = ","

	LeftID  = "["
	RightID = "]"
)

type (
	// simplified JSON-tagged version of the ArgsMsg (internal use)
	QueryMsg struct {
		OnlyRunning *bool     `json:"show_active"`
		Bck         cmn.Bck   `json:"bck"`
		ID          string    `json:"id"`
		Kind        string    `json:"kind"`
		DaemonID    string    `json:"node,omitempty"`
		Buckets     []cmn.Bck `json:"buckets,omitempty"`
	}
)

func Cname(kind, uuid string) string { return kind + LeftID + uuid + RightID }

func ParseCname(cname string) (xactKind, xactID string, _ error) {
	const efmt = "invalid name %q"
	l := len(cname)
	if l == 0 || cname[l-1] != RightID[0] {
		return "", "", fmt.Errorf(efmt, cname)
	}
	i := strings.IndexByte(cname, LeftID[0])
	if i < 0 {
		return "", "", fmt.Errorf(efmt, cname)
	}
	xactKind, xactID = cname[:i], cname[i+1:l-1]
	return xactKind, xactID, nil
}

//
// validators (helpers)
//

const fmtErrInvalidKind = "invalid xaction (job) kind %q"

func IsValidKind(kind string) bool {
	_, ok := Table[kind]
	return ok
}

func CheckValidKind(kind string) (err error) {
	_, dtor := getDtor(kind)
	if dtor == nil {
		err = fmt.Errorf(fmtErrInvalidKind, kind)
	}
	return err
}

func CheckValidKindIC(kind string) (err error) {
	_, dtor := getDtor(kind)
	if dtor == nil {
		return fmt.Errorf(fmtErrInvalidKind, kind)
	}
	if dtor.ICMode == ICNone {
		return cmn.NewErrXactNonIC(kind)
	}
	return nil
}

func IsValidUUID(id string) bool { return cos.IsValidUUID(id) || IsValidRebID(id) }

func CheckValidUUID(id string) (err error) {
	if !cos.IsValidUUID(id) && !IsValidRebID(id) {
		err = fmt.Errorf("invalid xaction (job) UUID %q", id)
	}
	return err
}

//////////////
// QueryMsg (internal use)
//////////////

func (msg *QueryMsg) String() (s string) {
	if msg.ID == "" {
		s = "x-" + msg.Kind
	} else {
		s = fmt.Sprintf("x-%s[%s]", msg.Kind, msg.ID)
	}
	if !msg.Bck.IsEmpty() {
		s += "-" + msg.Bck.String()
	}
	if msg.DaemonID != "" {
		s += "-node[" + msg.DaemonID + "]"
	}
	if msg.OnlyRunning != nil && *msg.OnlyRunning {
		s += "-only-running"
	}
	return
}

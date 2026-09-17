// Package apc: API control messages and constants
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"fmt"
)

// Head-batch (hdb): the intra-cluster batch HEAD(object).
// Entries are positional: HdbResp.Status[i] answers cmn.HdbReq.In[i].

const (
	HdbSizeDflt = 64   // default number of items per request
	HdbSizeMax  = 1024 // hard cap
)

// the per-object answer
type HdbStatus uint8

const (
	HdbNone HdbStatus = iota // the peer did not answer

	// identity, as returned by cmn.ObjAttrs.CheckEq
	HdbSame
	HdbDiverged // see HdbResp.Cause
	HdbMissing

	HdbBusy   // could not lock it
	HdbFailed // see HdbResp.Cause
)

type (
	HdbMsg struct {
		Msg string `json:"msg"`
		Idx int32  `json:"idx"`
	}
	HdbResp struct {
		Status []HdbStatus `json:"status"`
		Msg    []HdbMsg    `json:"msg,omitempty"`
	}
)

func (s HdbStatus) String() string {
	switch s {
	case HdbNone:
		return "no-answer"
	case HdbSame:
		return "same"
	case HdbDiverged:
		return "diverged"
	case HdbMissing:
		return "missing"
	case HdbBusy:
		return "busy"
	case HdbFailed:
		return "failed"
	default:
		return "invalid-status"
	}
}

func (s HdbStatus) Valid() bool { return s >= HdbSame && s <= HdbFailed }

func NewHdbResp(n int) *HdbResp { return &HdbResp{Status: make([]HdbStatus, n)} }

func (resp *HdbResp) Set(i int, status HdbStatus, cause error) {
	resp.Status[i] = status
	if cause != nil {
		resp.Msg = append(resp.Msg, HdbMsg{Idx: int32(i), Msg: cause.Error()})
	}
}

// the sender should validate the decoded response before using it
func (resp *HdbResp) Validate(nreq int) error {
	if len(resp.Status) != nreq {
		return fmt.Errorf("head-batch: got %d statuses, expecting %d", len(resp.Status), nreq)
	}
	for i, status := range resp.Status {
		if !status.Valid() {
			return fmt.Errorf("head-batch: invalid status %d at position %d", status, i)
		}
	}
	for i := range resp.Msg {
		if idx := resp.Msg[i].Idx; idx < 0 || int(idx) >= nreq {
			return fmt.Errorf("head-batch: message index %d out of range [0, %d)", idx, nreq)
		}
	}
	return nil
}

func (resp *HdbResp) Cause(i int) string {
	for j := range resp.Msg {
		if resp.Msg[j].Idx == int32(i) {
			return resp.Msg[j].Msg
		}
	}
	return ""
}

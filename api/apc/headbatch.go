// Package apc: API control messages and constants
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// Head-batch (hdb): the intra-cluster batch HEAD(object).
// Entries are positional: HdbResp.Status[i] answers cmn.HdbReq.In[i].
//
// Packed layout (strings are length-prefixed):
//
//	HdbResp: | n int32 | status uint8 x n | nmsg int32 | (idx int32, msg str) x nmsg |

const HdbTag = "head-batch"

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
		Msg string
		Idx int32
	}
	HdbResp struct {
		Status []HdbStatus
		Msg    []HdbMsg
	}
)

// interface guard
var (
	_ cos.Packer   = (*HdbResp)(nil)
	_ cos.Unpacker = (*HdbResp)(nil)
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
		return fmt.Errorf("%s: got %d statuses, expecting %d", HdbTag, len(resp.Status), nreq)
	}
	for i, status := range resp.Status {
		if !status.Valid() {
			return fmt.Errorf("%s: invalid status %d at position %d", HdbTag, status, i)
		}
	}
	for i := range resp.Msg {
		idx := resp.Msg[i].Idx
		if idx < 0 || int(idx) >= nreq {
			return fmt.Errorf("%s: message index %d out of range [0, %d)", HdbTag, idx, nreq)
		}
		if i > 0 && idx <= resp.Msg[i-1].Idx {
			return fmt.Errorf("%s: message index %d out of order at %d", HdbTag, idx, i)
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

func (resp *HdbResp) NewPack() []byte {
	p := cos.NewPacker(nil, resp.PackedSize())
	p.WriteAny(resp)
	return p.Bytes()
}

func (resp *HdbResp) PackedSize() int {
	n := cos.SizeofI32 + len(resp.Status) /*one byte each*/ + cos.SizeofI32
	for i := range resp.Msg {
		n += cos.SizeofI32 + cos.PackedStrLen(resp.Msg[i].Msg)
	}
	return n
}

func (resp *HdbResp) Pack(p *cos.BytePack) {
	p.WriteInt32(int32(len(resp.Status)))
	for _, status := range resp.Status {
		p.WriteUint8(uint8(status))
	}
	p.WriteInt32(int32(len(resp.Msg)))
	for i := range resp.Msg {
		p.WriteInt32(resp.Msg[i].Idx)
		p.WriteString(resp.Msg[i].Msg)
	}
}

func (resp *HdbResp) Unpack(u *cos.ByteUnpack) error {
	n, err := u.ReadInt32()
	if err != nil {
		return fmt.Errorf("%s: %w", HdbTag, err)
	}
	if n < 0 || n > HdbSizeMax {
		return fmt.Errorf("%s: invalid number of statuses %d, expecting at most %d", HdbTag, n, HdbSizeMax)
	}
	resp.Status = make([]HdbStatus, n)
	for i := range resp.Status {
		b, err := u.ReadByte()
		if err != nil {
			return fmt.Errorf("%s: status %d: %w", HdbTag, i, err)
		}
		resp.Status[i] = HdbStatus(b)
	}

	nmsg, err := u.ReadInt32()
	if err != nil {
		return fmt.Errorf("%s: %w", HdbTag, err)
	}
	if nmsg < 0 || nmsg > n {
		return fmt.Errorf("%s: invalid number of messages %d, expecting at most %d", HdbTag, nmsg, n)
	}
	resp.Msg = make([]HdbMsg, nmsg)
	for i := range resp.Msg {
		if resp.Msg[i].Idx, err = u.ReadInt32(); err != nil {
			return fmt.Errorf("%s: message %d: %w", HdbTag, i, err)
		}
		if resp.Msg[i].Msg, err = u.ReadString(); err != nil {
			return fmt.Errorf("%s: message %d: %w", HdbTag, i, err)
		}
	}
	if u.Len() != 0 {
		return fmt.Errorf("%s: %d trailing bytes", HdbTag, u.Len())
	}
	return nil
}

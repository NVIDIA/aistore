// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"strings"
	"time"
)

// On the sending side, intra-cluster notification is a tuple containing answers
// to the following existential questions:
// 	- when to notify
// 	- who to notify
// 	- how to notify
// The "how" part is usually notification-type specific - hence, the callback.
//
// TODO: add more notification types over time
//       and, in particular, other than xaction completion notifications.

/////////////////////////
// notification sender //
/////////////////////////

// enum: when to notify
const (
	UponTerm     = Upon(1 << iota) // success or fail is separately provided via error
	UponProgress                   // periodic (BytesCount, ObjCount)
)

type (
	Upon int

	// intra-cluster notification interface
	Notif interface {
		OnFinishedCB() func(Notif, error, bool /*aborted*/)
		OnProgressCB() func(Notif)
		NotifyInterval() time.Duration // notify interval in secs
		LastNotifTime() int64          // time last notified
		SetLastNotified(now int64)
		Upon(u Upon) bool
		Subscribers() []string
		ToNotifMsg(aborted bool) NotifMsg
	}

	// intra-cluster notification message
	NotifMsg struct {
		UUID     string `json:"uuid"`    // xaction UUID
		NodeID   string `json:"node_id"` // notifier node ID
		Kind     string `json:"kind"`    // xaction `Kind`
		ErrMsg   string `json:"err"`     // error.Error()
		Data     []byte `json:"message"` // (e.g. usage: custom progress stats)
		AbortedX bool   `json:"aborted"` // true if aborted (see related: Snap.AbortedX)
	}
)

func (msg *NotifMsg) String() (s string) {
	var sb strings.Builder
	sb.WriteString("nmsg-")
	sb.WriteString(msg.Kind)
	sb.WriteByte('[')
	sb.WriteString(msg.UUID)
	sb.WriteByte(']')
	sb.WriteString("<=")
	sb.WriteString(msg.NodeID)
	if msg.ErrMsg != "" {
		sb.WriteString(", err: ")
		sb.WriteString(msg.ErrMsg)
	}
	return sb.String()
}

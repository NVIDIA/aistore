// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
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
		OnFinishedCB() func(Notif, error)
		OnProgressCB() func(Notif)
		NotifyInterval() time.Duration // notify interval in secs
		LastNotifTime() int64          // time last notified
		SetLastNotified(now int64)
		Upon(u Upon) bool
		Subscribers() []string
		ToNotifMsg() NotifMsg
	}
	// intra-cluster notification base

	NotifMsg struct {
		UUID   string `json:"uuid"`    // xaction UUID
		NodeID string `json:"node_id"` // notifier node ID
		Kind   string `json:"kind"`    // xaction `Kind`
		ErrMsg string `json:"err"`     // error.Error()
		Data   []byte `json:"message"` // typed message
	}
)

func (msg *NotifMsg) String() (s string) {
	s = fmt.Sprintf("nmsg-%s[%s]<=%s", msg.Kind, msg.UUID, msg.NodeID)
	if msg.ErrMsg != "" {
		s += ", err: " + msg.ErrMsg
	}
	return
}

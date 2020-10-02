// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
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
		Callback(n Notif, err error)
		OnProgress(n Notif, err error)
		NotifyInterval() int64 // notify interval in secs
		Upon(u Upon) bool
		Category() int
		Subscribers() []string
		ToNotifMsg() NotifMsg
	}
	// intra-cluster notification base

	NotifMsg struct {
		UUID   string `json:"string"`
		Ty     int32  `json:"type"`    // enumerated type, one of (notifXact, et al.) - see above
		Flags  int32  `json:"flags"`   // TODO: add
		NodeID string `json:"node_id"` // notifier node id
		Kind   string `json:"kind"`    // kind of xaction
		Data   []byte `json:"message"` // typed message
		ErrMsg string `json:"err"`     // error.Error()
	}
)

func (msg *NotifMsg) String() string {
	// TODO: print text msg.Ty after moving notifs to a new package
	return fmt.Sprintf("%d[%s,%v]<=%s", msg.Ty, string(msg.Data), msg.ErrMsg, msg.NodeID)
}

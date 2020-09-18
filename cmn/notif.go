// Package cmn provides common API constants and types, and low-level utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
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
	NotifBase struct {
		When     Upon                     // see the enum below
		Interval int64                    // interval at which progress needs to be updated
		Dsts     []string                 // node IDs to notify
		F        func(n Notif, err error) // notification callback
		P        func(n Notif, err error) // on progress notification callback

		lastNotified atomic.Int64
		Ty           int // notification category enum
	}

	NotifMsg struct {
		UUID   string `json:"string"`
		Ty     int32  `json:"type"`    // enumerated type, one of (notifXact, et al.) - see above
		Flags  int32  `json:"flags"`   // TODO: add
		NodeID string `json:"node_id"` // notifier node id
		Data   []byte `json:"message"` // typed message
		ErrMsg string `json:"err"`     // error.Error()
	}
)

// enum: when to notify
const (
	UponTerm     = Upon(1 << iota) // success or fail is separately provided via error
	UponProgress                   // periodic (BytesCount, ObjCount)
)

func (notif *NotifBase) Callback(n Notif, err error) {
	notif.F(n, err)
}
func (notif *NotifBase) OnProgress(n Notif, err error) {
	if !notif.Upon(UponProgress) {
		return
	}
	if notif.shouldNotify() {
		notif.lastNotified.Store(time.Now().Unix())
		notif.P(n, err)
	}
}
func (notif *NotifBase) Upon(u Upon) bool { return notif != nil && notif.When&u != 0 }
func (notif *NotifBase) Category() int    { return notif.Ty }
func (notif *NotifBase) Subscribers() []string {
	return notif.Dsts
}
func (notif *NotifBase) NotifyInterval() int64 {
	if notif.Interval == 0 {
		return int64(GCO.Get().Periodic.NotifTime.Seconds())
	}
	return notif.Interval
}

func (notif *NotifBase) shouldNotify() bool {
	lastTime := notif.lastNotified.Load()
	return lastTime == 0 || time.Now().Unix()-lastTime > notif.NotifyInterval()
}

func (msg *NotifMsg) String() string {
	// TODO: print text msg.Ty after moving notifs to a new package
	return fmt.Sprintf("%d[%s,%v]<=%s", msg.Ty, string(msg.Data), msg.ErrMsg, msg.NodeID)
}

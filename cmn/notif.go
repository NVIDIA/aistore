// Package cmn provides common API constants and types, and low-level utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

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
		Upon(u Upon) bool
	}
	// intra-cluster notification base
	NotifBase struct {
		When Upon                     // see the enum below
		Dsts []string                 // node IDs to notify
		F    func(n Notif, err error) // notification callback
	}
)

// enum: when to notify
const (
	UponTerm     = Upon(1 << iota) // success or fail is separately provided via error
	UponProgress                   // periodic (BytesCount, ObjCount)
)

// interface guard
var (
	_ Notif = &NotifBase{}
)

func (notif *NotifBase) Callback(n Notif, err error) { notif.F(n, err) }
func (notif *NotifBase) Upon(u Upon) bool            { return notif.When&u != 0 }

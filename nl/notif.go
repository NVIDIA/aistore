// Package notifications provides interfaces for AIStore notifications
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package nl

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	NotifBase struct {
		When     cluster.Upon                     // see the enum below
		Interval int64                            // interval at which progress needs to be updated
		Dsts     []string                         // node IDs to notify
		F        func(n cluster.Notif, err error) // notification callback
		P        func(n cluster.Notif, err error) // on progress notification callback

		lastNotified atomic.Int64
		Ty           int // notification category enum
	}
)

func (notif *NotifBase) Callback(n cluster.Notif, err error) {
	notif.F(n, err)
}
func (notif *NotifBase) OnProgress(n cluster.Notif, err error) {
	if !notif.Upon(cluster.UponProgress) {
		return
	}
	if notif.shouldNotify() {
		notif.lastNotified.Store(time.Now().Unix())
		notif.P(n, err)
	}
}
func (notif *NotifBase) Upon(u cluster.Upon) bool { return notif != nil && notif.When&u != 0 }
func (notif *NotifBase) Category() int            { return notif.Ty }
func (notif *NotifBase) Subscribers() []string {
	return notif.Dsts
}
func (notif *NotifBase) NotifyInterval() int64 {
	if notif.Interval == 0 {
		return int64(cmn.GCO.Get().Periodic.NotifTime.Seconds())
	}
	return notif.Interval
}

func (notif *NotifBase) shouldNotify() bool {
	lastTime := notif.lastNotified.Load()
	return lastTime == 0 || time.Now().Unix()-lastTime > notif.NotifyInterval()
}

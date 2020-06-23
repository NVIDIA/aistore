// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

type signalError struct {
	signal syscall.Signal
}

func (se *signalError) Error() string { return fmt.Sprintf("Signal %d", se.signal) }

//===========================================================================
//
// sig runner
//
//===========================================================================
type sigrunner struct {
	cmn.Named
	ch chan os.Signal
}

// signal handler
func (r *sigrunner) Run() error {
	r.ch = make(chan os.Signal, 1)
	signal.Notify(r.ch,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGTERM, // kill -SIGTERM XXXX
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
	)
	if s, ok := <-r.ch; ok {
		signal.Stop(r.ch)
		return &signalError{signal: s.(syscall.Signal)}
	}
	return nil
}

func (r *sigrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.GetRunName(), err)
	signal.Stop(r.ch)
	close(r.ch)
}

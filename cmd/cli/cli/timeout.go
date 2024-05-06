// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"time"
)

// see also: xact/api.go

const (
	// e.g. xquery --all
	longClientTimeout = 60 * time.Second

	// list-objects progress; list-objects with --summary
	listObjectsWaitTime = 8 * time.Second

	// default '--refresh' durations and counts
	refreshRateDefault = 5 * time.Second
	refreshRateMinDur  = time.Second
	countDefault       = 1
	countUnlimited     = -1

	logFlushTime = 10 * time.Second // as the name implies

	//  progress bar: when stats stop moving (increasing)
	timeoutNoChange = 10 * time.Second

	// download started
	dloadStartedTime = refreshRateDefault

	// job wait: start printing "."(s)
	wasFast = refreshRateDefault
)

// execute a function in goroutine and wait for it to finish;
// if the function runs longer than `timeLong`: return "please wait" error
func waitForFunc(f func() error, timeLong time.Duration, prompts ...string) error {
	var (
		timer  = time.NewTimer(timeLong)
		chDone = make(chan struct{}, 1)
		err    error
		prompt = "Please wait, the operation may take some time..."
	)
	if len(prompts) > 0 && prompts[0] != "" {
		prompt = prompts[0]
	}
	go func() {
		err = f()
		chDone <- struct{}{}
	}()
loop:
	for {
		select {
		case <-timer.C:
			fmt.Println(prompt)
		case <-chDone:
			timer.Stop()
			break loop
		}
	}

	return err
}

// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
)

const GitHubHome = "https://github.com/NVIDIA/aistore"

// WaitForFunc executes a function in goroutine and waits for it to finish.
// If the function runs longer than `timeLong` WaitForFunc notifies a user
// that the user should wait for the result
func WaitForFunc(f func() error, timeLong time.Duration, prompts ...string) error {
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

//////////////////////////////
// config: path, load, save //
//////////////////////////////

// PropToHeader converts a property full name to an HTTP header tag name
func PropToHeader(prop string) string {
	if strings.HasPrefix(prop, apc.HeaderPrefix) {
		return prop
	}
	if prop[0] == '.' || prop[0] == '_' {
		prop = prop[1:]
	}
	prop = strings.ReplaceAll(prop, ".", "-")
	prop = strings.ReplaceAll(prop, "_", "-")
	return apc.HeaderPrefix + prop
}

// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

///////////////
// RetryArgs //
///////////////

const (
	RetryLogVerbose = iota
	RetryLogQuiet
	RetryLogOff
)

type (
	RetryArgs struct {
		Call    func() (int, error)
		IsFatal func(error) bool

		Action string
		Caller string

		SoftErr int // How many retries on ConnectionRefused or ConnectionReset error.
		HardErr int // How many retries on any other error.
		Sleep   time.Duration

		Verbosity int  // Verbosity level for logging.
		BackOff   bool // If true, retries will wait progressively longer between attempts (backoff).
		IsClient  bool // True when client side (with no access to cluster config timeouts)
	}
)

func (args *RetryArgs) Do() (ecode int, err error) {
	var (
		hardErrCnt, softErrCnt, iter int
		lastErr                      error
		callerStr                    string
		sleep                        = args.Sleep
	)
	if args.Sleep == 0 {
		if args.IsClient {
			args.Sleep = time.Second / 2
		} else {
			args.Sleep = Rom.CplaneOperation() / 4
		}
	}
	if args.Caller != "" {
		callerStr = args.Caller + ": "
	}
	if args.Action == "" {
		args.Action = "call"
	}
	for iter = 1; ; iter++ {
		if ecode, err = args.Call(); err == nil {
			if args.Verbosity == RetryLogVerbose && (hardErrCnt > 0 || softErrCnt > 0) {
				nlog.Warningf("%s successful %s after (soft/hard errors: %d/%d, last: %v)",
					callerStr, args.Action, softErrCnt, hardErrCnt, lastErr)
			}
			return 0, nil
		}
		lastErr = err

		// handle
		if args.IsFatal != nil && args.IsFatal(err) {
			return ecode, err
		}
		if args.Verbosity == RetryLogVerbose {
			nlog.Errorf("%s failed to %s, iter %d, err: %v(%d)", callerStr, args.Action, iter, err, ecode)
		}
		if cos.IsErrRetriableConn(err) {
			softErrCnt++
		} else {
			hardErrCnt++
		}
		if args.BackOff && iter > 1 {
			if args.IsClient {
				sleep = min(sleep+(args.Sleep/2), 4*time.Second)
			} else {
				sleep = min(sleep+(args.Sleep/2), Rom.MaxKeepalive())
			}
		}
		if hardErrCnt > args.HardErr || softErrCnt > args.SoftErr {
			break
		}
		time.Sleep(sleep)
	}
	// Quiet: print once the summary (Verbose: no need)
	if args.Verbosity == RetryLogQuiet {
		nlog.Errorf("%sfailed to %s (soft/hard errors: %d/%d, last: %v)",
			callerStr, args.Action, softErrCnt, hardErrCnt, err)
	}
	return ecode, err
}

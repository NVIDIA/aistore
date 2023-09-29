// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/pkg/errors"
)

const (
	fmtErrInvalidAlg     = "invalid sorting algorithm (expecting one of: %+v)" // <--- supportedAlgorithms
	fmtErrInvalidMaxSize = "invalid max shard size (%d) for usage with external key map"
	fmtErrNegOutputSize  = "output shard size must be >= 0 (got %d)"
	fmtErrOrderURL       = "failed to parse order file ('order_file') URL %q: %v"
	fmtErrSeed           = "invalid seed %q (expecting integer value)"
)

var (
	errAlgExt            = errors.New("algorithm: invalid extension")
	errNegConcLimit      = errors.New("negative concurrency limit")
	errMissingOutputSize = errors.New("output shard size must be set (cannot be 0 and cannot be omitted)")
	errMissingSrcBucket  = errors.New("missing source bucket")
)

func (m *Manager) newErrAborted() error {
	err := m.xctn.AbortErr()
	return cmn.NewErrAborted(fmt.Sprintf("%s[%s]", apc.ActDsort, m.ManagerUUID), "", err)
}

// Returns if the error is not abort error - in other cases we need to report
// the error to the user.
func isReportableError(err error) bool {
	return !cmn.IsErrAborted(err)
}

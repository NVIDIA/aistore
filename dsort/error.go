// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"github.com/NVIDIA/aistore/cmn"
)

func newDSortAbortedError(managerUUID string) *cmn.AbortedError {
	return cmn.NewAbortedError(cmn.DSortName, managerUUID)
}

// Returns if the error is not abort error - in other cases we need to report
// the error to the user.
func isReportableError(err error) bool {
	return !cmn.IsErrAborted(err)
}

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"errors"

	"github.com/NVIDIA/aistore/cmn"
)

func newDsortAbortedError(managerUUID string) cmn.AbortedError {
	return cmn.NewAbortedErrorDetails(cmn.DSortName, managerUUID)
}

// Returns if the error is not abort error - in other cases we need to report
// the error to the user.
func isReportableError(err error) bool {
	return !errors.As(err, &cmn.AbortedError{})
}

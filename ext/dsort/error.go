// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

func newDSortAbortedError(managerUUID string) *cmn.ErrAborted {
	return cmn.NewErrAborted(fmt.Sprintf("%s[%s]", apc.ActDsort, managerUUID), "", nil)
}

// Returns if the error is not abort error - in other cases we need to report
// the error to the user.
func isReportableError(err error) bool {
	return !cmn.IsErrAborted(err)
}

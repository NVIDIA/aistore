package dsort

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	abortError struct {
		managerUUID string
	}
)

func newAbortError(managerUUID string) error {
	return &abortError{
		managerUUID: managerUUID,
	}
}

func (e *abortError) Error() string {
	return fmt.Sprintf("%s %s process was aborted", cmn.DSortName, e.managerUUID)
}

// Returns if the error is not abort error - in other cases we need to report
// the error to the user.
func isReportableError(err error) bool {
	_, ok := err.(*abortError)
	return !ok
}

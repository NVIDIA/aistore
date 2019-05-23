package dsort

import "fmt"

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
	return fmt.Sprintf("dsort %s process was aborted", e.managerUUID)
}

// Returns if the error is not abort error - in other cases we need to report
// the error to the user.
func isReportableError(err error) bool {
	_, ok := err.(*abortError)
	return !ok
}

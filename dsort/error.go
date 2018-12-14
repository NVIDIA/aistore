package dsort

import "fmt"

type abortError struct {
	managerUUID string
}

func newAbortError(managerUUID string) error {
	return &abortError{
		managerUUID: managerUUID,
	}
}

func (e *abortError) Error() string {
	return fmt.Sprintf("dsort %s process was aborted", e.managerUUID)
}

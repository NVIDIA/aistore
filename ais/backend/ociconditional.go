//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
// no-copyright
package backend

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/cmn"

	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
)

// OCI evaluates these predicates atomically at PutObject or multipart commit.
// Limit the accepted syntax to the create-only and single strong-ETag forms
// supported by this adapter; never silently drop an unsupported predicate.
func ociWriteConditions(r *http.Request) (match, none *string, err error) {
	if r == nil {
		return nil, nil, nil
	}
	values := [2]string{}
	for i, key := range []string{"If-Match", "If-None-Match"} {
		headers := r.Header.Values(key)
		if len(headers) == 0 {
			continue
		}
		if len(headers) != 1 || strings.TrimSpace(headers[0]) == "" {
			return nil, nil, fmt.Errorf("invalid %s condition", key)
		}
		values[i] = strings.TrimSpace(headers[0])
	}
	if values[0] != "" && values[1] != "" {
		return nil, nil, errors.New("combined If-Match and If-None-Match are unsupported")
	}
	if value := values[0]; value != "" {
		if len(value) <= 2 || value[0] != '"' || value[len(value)-1] != '"' ||
			strings.ContainsAny(value[1:len(value)-1], "\"\r\n") {
			return nil, nil, errors.New("If-Match requires one quoted strong ETag")
		}
		// S3 wraps ETags in double quotes; native OCI compares the bare token.
		// Strip only the validated outer quotes, never alter the opaque token.
		value = value[1 : len(value)-1]
		match = &value
	}
	if value := values[1]; value != "" {
		if value != "*" {
			return nil, nil, errors.New("If-None-Match only supports *")
		}
		none = &value
	}
	return match, none, nil
}

// preserve the native error while identifying a retryable conditional-write conflict.
func ociWriteError(op, bucket, object string, err error, response any, match, none *string) (int, error) {
	status, mapped := ociErrorToAISError(op, bucket, object, "", err, response)
	var serviceErr ocicmn.ServiceError
	if status == http.StatusConflict && (match != nil || none != nil) &&
		errors.As(err, &serviceErr) && serviceErr.GetCode() == "ConcurrentObjectUpdate" {
		wire := cmn.NewErrHTTP(nil, mapped, status)
		wire.TypeCode = "ConditionalRequestConflict"
		return status, wire
	}
	return status, mapped
}

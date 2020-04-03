// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"

	"github.com/NVIDIA/aistore/cmn"
)

// Declare a new type for Context field names
type contextID string

const (
	CtxUserID    contextID = "userID"    // a field name of a context that contains userID
	CtxCredsDir  contextID = "credDir"   // a field of a context that contains path to directory with credentials
	CtxUserCreds contextID = "userCreds" // a field of a context that contains user credentials
)

const (
	// nolint:unused,varcheck,deadcode // used by `aws` and `gcp` but needs to compiled by tags
	initialBucketListSize = 128
)

// Retrieves a string from context field or empty string if nothing found or
// the field is not of string type.
//
// nolint:unused,deadcode // used by `aws` and `gcp` but needs to compiled by tags
func getStringFromContext(ct context.Context, fieldName contextID) string {
	fieldIf := ct.Value(fieldName)
	if fieldIf == nil {
		return ""
	}

	strVal, ok := fieldIf.(string)
	if !ok {
		return ""
	}

	return strVal
}

// Retreives a userCreds from context or nil if nothing found
//
// nolint:unused,deadcode // used by `aws` and `gcp` but needs to compiled by tags
func userCredsFromContext(ct context.Context) cmn.SimpleKVs {
	userIf := ct.Value(CtxUserCreds)
	if userIf == nil {
		return nil
	}

	if userCreds, ok := userIf.(cmn.SimpleKVs); ok {
		return userCreds
	}

	return nil
}

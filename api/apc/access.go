// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"fmt"
	"strconv"
	"strings"
)

type AccessAttrs uint64

// ACL aka access permissions
const (
	// object level
	AceGET     = AccessAttrs(1) << iota
	AceObjHEAD // permission to get object props
	AcePUT
	AceAPPEND
	AceObjDELETE
	AceObjMOVE
	AcePromote
	// permission to overwrite objects that were previously read from:
	// a) any remote backend that is currently not configured as the bucket's backend
	// b) HTPP ("ht://") since it's not writable
	AceDisconnectedBackend
	// bucket metadata
	AceBckHEAD   // get bucket props and ACL
	AceObjLIST   // list objects in a bucket
	AcePATCH     // set bucket props
	AceBckSetACL // set bucket permissions
	// cluster level
	AceListBuckets
	AceShowCluster
	AceCreateBucket
	AceDestroyBucket
	AceMoveBucket
	AceAdmin
	// note: must be the last one
	AceMax
)

// access => operation
var accessOp = map[AccessAttrs]string{
	// object
	AceGET:                 "GET",
	AceObjHEAD:             "HEAD-OBJECT",
	AcePUT:                 "PUT",
	AceAPPEND:              "APPEND",
	AceObjDELETE:           "DELETE-OBJECT",
	AceObjMOVE:             "MOVE-OBJECT",
	AcePromote:             "PROMOTE",
	AceDisconnectedBackend: "DISCONNECTED-BACKEND",
	// bucket
	AceBckHEAD:   "HEAD-BUCKET",
	AceObjLIST:   "LIST-OBJECTS",
	AcePATCH:     "PATCH",
	AceBckSetACL: "SET-BUCKET-ACL",
	// cluster
	AceListBuckets:   "LIST-BUCKETS",
	AceShowCluster:   "SHOW-CLUSTER",
	AceCreateBucket:  "CREATE-BUCKET",
	AceDestroyBucket: "DESTROY-BUCKET",
	AceMoveBucket:    "MOVE-BUCKET",
	AceAdmin:         "ADMIN",

	// NOTE: update Describe() when adding/deleting
}

// derived (convenience) constants
const (
	// encompasses all ACEs, current and future
	AccessAll      = AccessAttrs(^uint64(0))
	AllowAllAccess = "su"

	// read-only and read-write access to bucket
	AccessRO             = AceGET | AceObjHEAD | AceListBuckets | AceBckHEAD | AceObjLIST
	AllowReadOnlyAccess  = "ro"
	AccessRW             = AccessRO | AcePUT | AceAPPEND | AceObjDELETE | AceObjMOVE
	AllowReadWriteAccess = "rw"

	AccessNone = AccessAttrs(0)

	// permission to perform cluster-level ops
	AccessCluster = AceListBuckets | AceCreateBucket | AceDestroyBucket | AceMoveBucket | AceAdmin
)

// verbs
func SupportedPermissions() []string {
	accList := []string{"ro", "rw", "su"}
	for _, v := range accessOp {
		accList = append(accList, v)
	}
	return accList
}

func (a AccessAttrs) Has(perms AccessAttrs) bool { return a&perms == perms }
func (a AccessAttrs) String() string             { return strconv.FormatUint(uint64(a), 10) }

func (a AccessAttrs) Describe() string {
	if a == 0 {
		return "No access"
	}
	accList := make([]string, 0, 24)
	if a.Has(AceGET) {
		accList = append(accList, accessOp[AceGET])
	}
	if a.Has(AceObjHEAD) {
		accList = append(accList, accessOp[AceObjHEAD])
	}
	if a.Has(AcePUT) {
		accList = append(accList, accessOp[AcePUT])
	}
	if a.Has(AceAPPEND) {
		accList = append(accList, accessOp[AceAPPEND])
	}
	if a.Has(AceObjDELETE) {
		accList = append(accList, accessOp[AceObjDELETE])
	}
	if a.Has(AceObjMOVE) {
		accList = append(accList, accessOp[AceObjMOVE])
	}
	if a.Has(AcePromote) {
		accList = append(accList, accessOp[AcePromote])
	}
	if a.Has(AceDisconnectedBackend) {
		accList = append(accList, accessOp[AceDisconnectedBackend])
	}
	//
	if a.Has(AceBckHEAD) {
		accList = append(accList, accessOp[AceBckHEAD])
	}
	if a.Has(AceObjLIST) {
		accList = append(accList, accessOp[AceObjLIST])
	}
	if a.Has(AcePATCH) {
		accList = append(accList, accessOp[AcePATCH])
	}
	if a.Has(AceBckSetACL) {
		accList = append(accList, accessOp[AceBckSetACL])
	}
	//
	if a.Has(AceListBuckets) {
		accList = append(accList, accessOp[AceListBuckets])
	}
	if a.Has(AceShowCluster) {
		accList = append(accList, accessOp[AceShowCluster])
	}
	if a.Has(AceCreateBucket) {
		accList = append(accList, accessOp[AceCreateBucket])
	}
	if a.Has(AceDestroyBucket) {
		accList = append(accList, accessOp[AceDestroyBucket])
	}
	if a.Has(AceMoveBucket) {
		accList = append(accList, accessOp[AceMoveBucket])
	}
	if a.Has(AceAdmin) {
		accList = append(accList, accessOp[AceAdmin])
	}
	return strings.Join(accList, ",")
}

func AccessOp(access AccessAttrs) string {
	if s, ok := accessOp[access]; ok {
		return s
	}
	return "<unknown access>"
}

func StrToAccess(accessStr string) (access AccessAttrs, err error) {
	switch accessStr {
	case AllowReadOnlyAccess:
		access |= AccessRO
	case AllowReadWriteAccess:
		access |= AccessRW
	case AllowAllAccess:
		access = AccessAll
	case "":
		access = AccessNone
	default:
		found := false
		for k, v := range accessOp {
			if v == accessStr {
				access |= k
				found = true
			}
		}
		if !found {
			err = fmt.Errorf("invalid access value: %q", accessStr)
		}
	}
	return
}

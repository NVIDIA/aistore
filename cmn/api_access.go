// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strconv"
	"strings"
)

type AccessAttrs uint64

// ACL aka access permissions
const (
	// object level
	AccessGET     = AccessAttrs(1) << iota
	AccessObjHEAD // get object props
	AccessPUT
	AccessAPPEND
	AccessObjDELETE
	AccessObjMOVE
	AccessPROMOTE
	// bucket metadata
	AccessBckHEAD   // get bucket props and ACL
	AccessObjLIST   // list objects in a bucket
	AccessPATCH     // set bucket props
	AccessBckSetACL // set bucket permissions
	// cluster level
	AccessListBuckets
	AccessCreateBucket
	AccessDestroyBucket
	AccessMoveBucket
	AccessAdmin
	// note: must be the last one
	AccessMax
)

// access => operation
var accessOp = map[AccessAttrs]string{
	// object
	AccessGET:       "GET",
	AccessObjHEAD:   "HEAD-OBJECT",
	AccessPUT:       "PUT",
	AccessAPPEND:    "APPEND",
	AccessObjDELETE: "DELETE-OBJECT",
	AccessObjMOVE:   "MOVE-OBJECT",
	AccessPROMOTE:   "PROMOTE",
	// bucket
	AccessBckHEAD:   "HEAD-BUCKET",
	AccessObjLIST:   "LIST-OBJECTS",
	AccessPATCH:     "PATCH",
	AccessBckSetACL: "SET-BUCKET-ACL",
	// cluster
	AccessListBuckets:   "LIST-BUCKETS",
	AccessCreateBucket:  "CREATE-BUCKET",
	AccessDestroyBucket: "DESTROY-BUCKET",
	AccessMoveBucket:    "MOVE-BUCKET",
	AccessAdmin:         "ADMIN",
}

// derived (convenience) constants
const (
	// encompasses all ACEs, current and future
	AccessAll      = AccessAttrs(^uint64(0))
	AllowAllAccess = "su"

	// read-only and read-write access to bucket
	AccessRO             = AccessGET | AccessObjHEAD | AccessBckHEAD | AccessObjLIST
	AllowReadOnlyAccess  = "ro"
	AccessRW             = AccessRO | AccessPUT | AccessAPPEND | AccessObjDELETE | AccessObjMOVE
	AllowReadWriteAccess = "rw"

	AccessNone = AccessAttrs(0)

	// permission to perform cluster-level ops
	AccessCluster = AccessListBuckets | AccessCreateBucket | AccessDestroyBucket | AccessMoveBucket |
		AccessAdmin
)

// verbs
const (
	AllowAccess = "allow"
	DenyAccess  = "deny"
)

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
	if a.Has(AccessGET) {
		accList = append(accList, accessOp[AccessGET])
	}
	if a.Has(AccessObjHEAD) {
		accList = append(accList, accessOp[AccessObjHEAD])
	}
	if a.Has(AccessPUT) {
		accList = append(accList, accessOp[AccessPUT])
	}
	if a.Has(AccessAPPEND) {
		accList = append(accList, accessOp[AccessAPPEND])
	}
	if a.Has(AccessObjDELETE) {
		accList = append(accList, accessOp[AccessObjDELETE])
	}
	if a.Has(AccessObjMOVE) {
		accList = append(accList, accessOp[AccessObjMOVE])
	}
	if a.Has(AccessPROMOTE) {
		accList = append(accList, accessOp[AccessPROMOTE])
	}
	//
	if a.Has(AccessBckHEAD) {
		accList = append(accList, accessOp[AccessBckHEAD])
	}
	if a.Has(AccessObjLIST) {
		accList = append(accList, accessOp[AccessObjLIST])
	}
	if a.Has(AccessMoveBucket) {
		accList = append(accList, accessOp[AccessMoveBucket])
	}
	if a.Has(AccessPATCH) {
		accList = append(accList, accessOp[AccessPATCH])
	}
	if a.Has(AccessDestroyBucket) {
		accList = append(accList, accessOp[AccessDestroyBucket])
	}
	if a.Has(AccessBckSetACL) {
		accList = append(accList, accessOp[AccessBckSetACL])
	}
	if a.Has(AccessAdmin) {
		accList = append(accList, accessOp[AccessAdmin])
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

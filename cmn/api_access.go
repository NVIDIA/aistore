// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// object
	AccessGET = 1 << iota
	AccessObjHEAD
	AccessPUT
	AccessAPPEND
	AccessDOWNLOAD
	AccessObjDELETE
	AccessObjMove
	AccessPROMOTE
	// bucket
	AccessBckHEAD
	AccessObjLIST
	AccessBckMove
	AccessPATCH
	AccessMAKENCOPIES
	AccessEC
	AccessSYNC
	AccessBckDELETE
	AccessBckPERMISSION
	// cluster
	AccessBckCREATE
	AccessBckLIST
	AccessADMIN
	// must be the last one
	AccessMax

	// Permissions
	allowAllAccess       = ^uint64(0)
	allowReadOnlyAccess  = AccessGET | AccessObjHEAD | AccessBckHEAD | AccessObjLIST
	allowReadWriteAccess = allowReadOnlyAccess |
		AccessPUT | AccessAPPEND | AccessDOWNLOAD | AccessObjDELETE | AccessObjMove
	allowClusterAccess = allowAllAccess & (AccessBckCREATE - 1)

	// Permission Operations
	AllowAccess = "allow"
	DenyAccess  = "deny"
)

type AccessAttrs uint64

// access => operation
var accessOp = map[int]string{
	// object
	AccessGET:       "GET",
	AccessObjHEAD:   "HEAD-OBJECT",
	AccessPUT:       "PUT",
	AccessAPPEND:    "APPEND",
	AccessDOWNLOAD:  "DOWNLOAD",
	AccessObjDELETE: "DELETE-OBJECT",
	AccessObjMove:   "MOVE-OBJECT",
	AccessPROMOTE:   "PROMOTE",
	// bucket
	AccessBckHEAD:     "HEAD-BUCKET",
	AccessObjLIST:     "LIST-OBJECTS",
	AccessBckMove:     "MOVE-BUCKET",
	AccessPATCH:       "PATCH",
	AccessMAKENCOPIES: "MAKE-NCOPIES",
	AccessEC:          "EC",
	AccessSYNC:        "SYNC-BUCKET",
	AccessBckDELETE:   "DELETE-BUCKET",
	// cluster
	AccessBckPERMISSION: "SET-BUCKET-PERMISSIONS",
	AccessBckCREATE:     "CREATE-BUCKET",
	AccessADMIN:         "ADMIN",
}

func NoAccess() AccessAttrs                      { return 0 }
func AllAccess() AccessAttrs                     { return AccessAttrs(allowAllAccess) }
func ReadOnlyAccess() AccessAttrs                { return allowReadOnlyAccess }
func ReadWriteAccess() AccessAttrs               { return allowReadWriteAccess }
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
	if a.Has(AccessDOWNLOAD) {
		accList = append(accList, accessOp[AccessDOWNLOAD])
	}
	if a.Has(AccessObjDELETE) {
		accList = append(accList, accessOp[AccessObjDELETE])
	}
	if a.Has(AccessObjMove) {
		accList = append(accList, accessOp[AccessObjMove])
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
	if a.Has(AccessBckMove) {
		accList = append(accList, accessOp[AccessBckMove])
	}
	if a.Has(AccessPATCH) {
		accList = append(accList, accessOp[AccessPATCH])
	}
	if a.Has(AccessMAKENCOPIES) {
		accList = append(accList, accessOp[AccessMAKENCOPIES])
	}
	if a.Has(AccessSYNC) {
		accList = append(accList, accessOp[AccessSYNC])
	}
	if a.Has(AccessBckDELETE) {
		accList = append(accList, accessOp[AccessBckDELETE])
	}
	if a.Has(AccessBckPERMISSION) {
		accList = append(accList, accessOp[AccessBckPERMISSION])
	}
	return strings.Join(accList, ",")
}

func AccessOp(access int) string {
	if s, ok := accessOp[access]; ok {
		return s
	}
	return "<unknown access>"
}

func ModifyAccess(aattr uint64, action string, bits uint64) (uint64, error) {
	if action == AllowAccess {
		return aattr | bits, nil
	}
	if action != DenyAccess {
		return 0, fmt.Errorf("unknown make-access action %q", action)
	}
	return aattr & (allowAllAccess ^ bits), nil
}

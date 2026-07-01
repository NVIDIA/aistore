// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"github.com/NVIDIA/aistore/api/authn"
)

type bckACLList []*authn.BckACL

func (bckList bckACLList) updated(bckACL *authn.BckACL, union bool) bool {
	for _, acl := range bckList {
		if acl.Bck.Equal(&bckACL.Bck) {
			if union {
				acl.Access |= bckACL.Access
			} else {
				acl.Access = bckACL.Access
			}
			return true
		}
	}
	return false
}

type cluACLList []*authn.CluACL

func (cluList cluACLList) updated(cluACL *authn.CluACL, union bool) bool {
	for _, acl := range cluList {
		if acl.ID == cluACL.ID {
			if union {
				acl.Access |= cluACL.Access
			} else {
				acl.Access = cluACL.Access
			}
			return true
		}
	}
	return false
}

// mergeBckACLs appends bucket ACLs from fromACLs which are not in toACL.
// If a bucket ACL is already in the list, its permissions are replaced
// when union is false (role update), or OR'd together when union is true
// (combining a user's roles into one token).
// If cluIDFlt is set, only ACLs for buckets of the cluster with this ID are appended.
func mergeBckACLs(toACLs, fromACLs bckACLList, cluIDFlt string, union bool) []*authn.BckACL {
	for _, n := range fromACLs {
		if cluIDFlt != "" && n.Bck.Ns.UUID != cluIDFlt {
			continue
		}
		if !toACLs.updated(n, union) {
			toACLs = append(toACLs, n)
		}
	}
	return toACLs
}

// mergeClusterACLs appends cluster ACLs from fromACLs which are not in toACL.
// If a cluster ACL is already in the list, its permissions are replaced
// when union is false (role update), or OR'd together when union is true
// (combining a user's roles into one token).
// If cluIDFlt is set, only ACLs for cluster with this ID are appended.
func mergeClusterACLs(toACLs, fromACLs cluACLList, cluIDFlt string, union bool) []*authn.CluACL {
	for _, n := range fromACLs {
		if cluIDFlt != "" && cluIDFlt != n.ID {
			continue
		}
		if !toACLs.updated(n, union) {
			toACLs = append(toACLs, n)
		}
	}
	return toACLs
}

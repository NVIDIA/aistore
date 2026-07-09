// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import "slices"

// MergeClusterACLs merges fromACLs into toACLs. Duplicate cluster IDs are OR'd (union=true)
// or replaced (union=false). If cluIDFlt is non-empty, only ACLs for that cluster are merged.
// Nil ACL entries are ignored.
// NOTE: cmd/cli whoamiAccessStale mirrors the union=true path (keep in sync).
func MergeClusterACLs(toACLs, fromACLs []*CluACL, cluIDFlt string, union bool) []*CluACL {
	toACLs = slices.DeleteFunc(toACLs, func(acl *CluACL) bool { return acl == nil })
	for _, n := range fromACLs {
		if n == nil {
			continue
		}
		if cluIDFlt != "" && cluIDFlt != n.ID {
			continue
		}
		if !updateCluACL(toACLs, n, union) {
			toACLs = append(toACLs, n)
		}
	}
	return toACLs
}

func updateCluACL(acls []*CluACL, n *CluACL, union bool) bool {
	if n == nil {
		return false
	}
	for _, acl := range acls {
		if acl == nil {
			continue
		}
		if acl.ID == n.ID {
			if union {
				acl.Access |= n.Access
			} else {
				acl.Access = n.Access
			}
			return true
		}
	}
	return false
}

// MergeBckACLs merges fromACLs into toACLs. Duplicate bucket entries are OR'd (union=true)
// or replaced (union=false). If cluIDFlt is non-empty, only ACLs for buckets in that cluster are merged.
// Nil ACL entries are ignored.
// NOTE: cmd/cli whoamiAccessStale mirrors the union=true path (keep in sync).
func MergeBckACLs(toACLs, fromACLs []*BckACL, cluIDFlt string, union bool) []*BckACL {
	toACLs = slices.DeleteFunc(toACLs, func(acl *BckACL) bool { return acl == nil })
	for _, n := range fromACLs {
		if n == nil {
			continue
		}
		if cluIDFlt != "" && n.Bck.Ns.UUID != cluIDFlt {
			continue
		}
		if !updateBckACL(toACLs, n, union) {
			toACLs = append(toACLs, n)
		}
	}
	return toACLs
}

func updateBckACL(acls []*BckACL, n *BckACL, union bool) bool {
	if n == nil {
		return false
	}
	for _, acl := range acls {
		if acl == nil {
			continue
		}
		if acl.Bck.Equal(&n.Bck) {
			if union {
				acl.Access |= n.Access
			} else {
				acl.Access = n.Access
			}
			return true
		}
	}
	return false
}

// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	AuthAdminRole        = "Admin"
	AuthClusterOwnerRole = "ClusterOwner"
	AuthBucketOwnerRole  = "BucketOwner"
	AuthGuestRole        = "Guest"
)

type (
	// list of types used in AuthN and its API

	// A registered user
	AuthUser struct {
		ID       string         `json:"id"`
		Password string         `json:"pass,omitempty"`
		Roles    []string       `json:"roles"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets"` // list of buckets with special permissions
	}
	// Default permissions for a cluster
	AuthCluster struct {
		ID     string      `json:"id"`
		Access AccessAttrs `json:"perm,string"`
	}
	// Permissions for a single bucket
	AuthBucket struct {
		Bck    Bck         `json:"bck"`
		Access AccessAttrs `json:"perm,string"`
	}
	AuthRole struct {
		Name     string         `json:"name"`
		Desc     string         `json:"desc"`
		Roles    []string       `json:"roles"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets"`
		IsAdmin  bool           `json:"admin"`
	}
	AuthToken struct {
		UserID   string         `json:"username"`
		Expires  time.Time      `json:"expires"`
		Token    string         `json:"token"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets,omitempty"`
		IsAdmin  bool           `json:"admin"`
	}
)

var (
	ErrNoPermissions = errors.New("insufficient permissions")
	ErrInvalidToken  = errors.New("invalid token")
)

func (tk *AuthToken) CheckPermissions(clusterID string, bck *Bck, perms AccessAttrs) error {
	if tk.IsAdmin {
		return nil
	}
	debug.AssertMsg(perms != 0, "Empty permissions requested")
	cluPerms := perms & AccessAttrs(allowClusterAccess)
	objPerms := perms ^ AccessAttrs(allowClusterAccess)
	// Cluster-wide permissions requested
	hasPerms := true
	if cluPerms != 0 {
		debug.AssertMsg(clusterID != "", "Requested cluster permissions without cluster ID")
		hasPerms = false
		for _, pm := range tk.Clusters {
			if pm.ID != clusterID {
				continue
			}
			hasPerms = pm.Access.Has(cluPerms)
			break
		}
	}
	if !hasPerms {
		return ErrNoPermissions
	}
	if objPerms == 0 {
		return nil
	}

	// Check only bucket specific permissions.
	// For AuthN all buckets are external, so they have UUIDs. To correctly
	// compare with local bucket, token's bucket should be fixed.
	debug.AssertMsg(bck != nil, "Requested bucket permissions without bucket name")
	for _, b := range tk.Buckets {
		tbBck := b.Bck
		if tbBck.Ns.UUID == clusterID {
			tbBck.Ns.UUID = ""
		}
		if b.Bck.Equal(*bck) {
			if b.Access.Has(perms) {
				return nil
			}
			return ErrNoPermissions
		}
	}
	return ErrNoPermissions
}

func (uInfo *AuthUser) MergeBckACLs(newACLs []*AuthBucket) {
	for _, n := range newACLs {
		found := false
		for _, o := range uInfo.Buckets {
			if o.Bck.Equal(n.Bck) {
				found = true
				o.Access = n.Access
				break
			}
			if !found {
				uInfo.Buckets = append(uInfo.Buckets, n)
			}
		}
	}
}

func (uInfo *AuthUser) MergeClusterACLs(newACLs []*AuthCluster) {
	for _, n := range newACLs {
		found := false
		for _, o := range uInfo.Clusters {
			if o.ID == n.ID {
				found = true
				o.Access = n.Access
				break
			}
		}
		if !found {
			uInfo.Clusters = append(uInfo.Clusters, n)
		}
	}
}

func (uInfo *AuthUser) IsAdmin() bool {
	for _, r := range uInfo.Roles {
		if r == AuthAdminRole {
			return true
		}
	}
	return false
}

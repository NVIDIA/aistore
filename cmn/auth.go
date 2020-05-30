// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"time"
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
		ID     string `json:"id"`
		Access uint64 `json:"perm,string"`
	}
	// Permissions for a single bucket
	AuthBucket struct {
		Bck    Bck    `json:"bck"`
		Access uint64 `json:"perm,string"`
	}
	AuthRole struct {
		Name     string         `json:"name"`
		Desc     string         `json:"desc"`
		Roles    []string       `json:"roles"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets"`
	}
	AuthToken struct {
		UserID   string         `json:"username"`
		Expires  time.Time      `json:"expires"`
		Token    string         `json:"token"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets,omitempty"`
		IsAdmin  bool
	}
)

var ErrNoPermissions = errors.New("insufficient permissions")

func (tk *AuthToken) HasPermissions(clusterID string, bck *Bck, perms uint64) bool {
	AssertMsg(perms != 0, "Empty permissions requested")
	cluPerms := perms & allowClusterAccess
	objPerms := perms ^ allowClusterAccess
	// Cluster-wide permissions requested
	hasPerms := true
	if cluPerms != 0 {
		AssertMsg(clusterID != "", "Requested cluster permissions without cluster ID")
		hasPerms = false
		for _, pm := range tk.Clusters {
			if pm.ID != clusterID {
				continue
			}
			hasPerms = pm.Access&cluPerms == cluPerms
			break
		}
	}
	if !hasPerms || objPerms == 0 {
		return hasPerms
	}

	// Check only bucket specific permissions
	AssertMsg(bck != nil, "Requested bucket permissions without bucket name")
	for _, b := range tk.Buckets {
		if b.Bck.Equal(*bck) {
			return b.Access&perms == perms
		}
	}
	return false
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

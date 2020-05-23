// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"time"
)

const (
	AuthPowerUserRole   = "PowerUser"
	AuthBucketOwnerRole = "BucketOwner"
	AuthGuestRole       = "Guest"
)

type (
	// list of types used in AuthN and its API

	// A registered user
	AuthUser struct {
		UserID   string        `json:"name"`
		Password string        `json:"pass,omitempty"`
		Role     string        `json:"role"`
		Access   uint64        `json:"perm,string"` // default permissions
		Buckets  []*AuthBucket `json:"buckets"`     // list of buckets with special permissions
		SU       bool          `json:"su"`          // true - superuser permissions
	}
	// Permissions for a single bucket
	AuthBucket struct {
		Name   Bck    `json:"bck"`
		Access uint64 `json:"perm,string"`
	}
	AuthRole struct {
		Name    string        `json:"name"`
		Desc    string        `json:"desc"`
		Access  uint64        `json:"perm,string"`
		Buckets []*AuthBucket `json:"buckets"`
	}
	AuthToken struct {
		UserID  string        `json:"username"`
		Issued  time.Time     `json:"issued"`
		Expires time.Time     `json:"expires"`
		Token   string        `json:"token"`
		Access  uint64        `json:"perm,string,omitempty"`
		Buckets []*AuthBucket `json:"buckets,omitempty"`
	}
)

func (uInfo *AuthUser) MergeBckACLs(newACLs []*AuthBucket) {
	if len(newACLs) == 0 {
		return
	}
	for _, n := range newACLs {
		found := false
		for _, o := range uInfo.Buckets {
			if o.Name == n.Name {
				found = true
				o.Access = n.Access
			}
			if !found {
				uInfo.Buckets = append(uInfo.Buckets, n)
			}
		}
	}
	// squash the bucket list by removing those that have the same access
	// as the user. The less buckets, the shorter token
	curr, last := 0, len(uInfo.Buckets)-1
	for curr <= last {
		if uInfo.Buckets[curr].Access != uInfo.Access {
			curr++
			continue
		}
		uInfo.Buckets[curr] = uInfo.Buckets[last]
		last--
	}
	uInfo.Buckets = uInfo.Buckets[:last+1]
}

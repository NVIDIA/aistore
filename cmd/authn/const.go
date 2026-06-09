// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

const (
	ClusterOwnerRole = "ClusterOwner"
	BucketOwnerRole  = "BucketOwner"
	GuestRole        = "Guest"
)

const (
	usersCollection     = "user"
	rolesCollection     = "role"
	roleUsersCollection = "role_user"
	revokedCollection   = "revoked"
	clustersCollection  = "cluster"
	metaCollection      = "meta"

	adminUserID = "admin"
)

// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

const (
	ClusterOwnerRole = "ClusterOwner"
	BucketOwnerRole  = "BucketOwner"
	GuestRole        = "Guest"
)

const (
	usersCollection    = "user"
	rolesCollection    = "role"
	revokedCollection  = "revoked"
	clustersCollection = "cluster"

	adminUserID = "admin"
)

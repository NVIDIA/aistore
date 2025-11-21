// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import "time"

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

	// ForeverTokenTime is a duration of 20 years, used to define, effectively, no expiration on tokens
	// Used when user-provided token expiration time is zero
	foreverTokenTime = 20 * 365 * 24 * time.Hour
)

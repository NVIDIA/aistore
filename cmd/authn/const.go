// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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

	adminUserID   = "admin"
	adminUserPass = "admin"

	// when user-provided token expiration time is zero it means the token never expires;
	// we then create a token and set it to expire in 20 years - effectively, never
	foreverTokenTime = 20 * 365 * 24 * time.Hour
)

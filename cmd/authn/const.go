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

	foreverTokenTime = 24 * 365 * 20 * time.Hour // kind of never-expired token // TODO -- FIXME: make it -1s
)

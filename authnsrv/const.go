// Package authnsrv provides AuthN server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authnsrv

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

	proxyTimeout     = 2 * time.Minute           // maximum time for syncing Authn data with primary proxy
	proxyRetryTime   = 5 * time.Second           // an interval between primary proxy detection attempts
	foreverTokenTime = 24 * 365 * 20 * time.Hour // kind of never-expired token
)

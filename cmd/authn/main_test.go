// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// NOTE: when a fresh user manager is created, it initailized users DB and
// adds a default user with role Guest, so all length checks must add 1

var (
	users = []string{"user1", "user2", "user3"}
	passs = []string{"pass2", "pass1", "passs"}
)

func init() {
	// Set default expiration time to 30 minutes
	if conf.Auth.ExpirePeriod == 0 {
		conf.Auth.ExpirePeriod = time.Minute * 30
	}
}

func createUsers(mgr *userManager, t *testing.T) {
	for idx := range users {
		user := &cmn.AuthUser{UserID: users[idx], Password: passs[idx], Role: cmn.AuthGuestRole}
		err := mgr.addUser(user)
		if err != nil {
			t.Errorf("Failed to create a user %s: %v", users[idx], err)
		}
	}

	srvUsers, err := mgr.userList()
	tassert.CheckFatal(t, err)
	if len(srvUsers) != len(users)+1 {
		t.Errorf("User count mismatch. Found %d users instead of %d", len(srvUsers), len(users)+1)
	}
	for _, username := range users {
		_, ok := srvUsers[username]
		if !ok {
			t.Errorf("User %q not found", username)
		}
	}
}

func deleteUsers(mgr *userManager, skipNotExist bool, t *testing.T) {
	var err error
	for _, username := range users {
		err = mgr.delUser(username)
		if err != nil {
			if !dbdriver.IsErrNotFound(err) || !skipNotExist {
				t.Errorf("Failed to delete user %s: %v", username, err)
			}
		}
	}
}

func testInvalidUser(mgr *userManager, t *testing.T) {
	user := &cmn.AuthUser{UserID: users[0], Password: passs[1], Role: cmn.AuthGuestRole}
	err := mgr.addUser(user)
	if err == nil {
		t.Errorf("User with the existing name %s was created: %v", users[0], err)
	}

	nonexisting := "someuser"
	err = mgr.delUser(nonexisting)
	if err == nil {
		t.Errorf("Non-existing user %s was deleted: %v", nonexisting, err)
	}
}

func testUserDelete(mgr *userManager, t *testing.T) {
	const (
		username = "newuser"
		userpass = "newpass"
	)
	user := &cmn.AuthUser{UserID: username, Password: userpass, Role: cmn.AuthGuestRole}
	err := mgr.addUser(user)
	if err != nil {
		t.Errorf("Failed to create a user %s: %v", username, err)
	}
	srvUsers, err := mgr.userList()
	tassert.CheckFatal(t, err)
	if len(srvUsers) != len(users)+2 {
		t.Errorf("Expected %d users but found %d", len(users)+2, len(srvUsers))
	}

	token, err := mgr.issueToken(username, userpass)
	if err != nil || token == "" {
		t.Errorf("Failed to generate token for %s: %v", username, err)
	}

	err = mgr.delUser(username)
	if err != nil {
		t.Errorf("Failed to delete user %s: %v", username, err)
	}
	srvUsers, err = mgr.userList()
	tassert.CheckFatal(t, err)
	if len(srvUsers) != len(users)+1 {
		t.Errorf("Expected %d users but found %d", len(users)+1, len(srvUsers))
	}
	token, err = mgr.issueToken(username, userpass)
	if err == nil {
		t.Errorf("Token issued for deleted user  %s: %v", username, token)
	} else if err != errInvalidCredentials {
		t.Errorf("Invalid error: %v", err)
	}
}

func TestManager(t *testing.T) {
	driver := dbdriver.NewDBMock()
	mgr, err := newUserManager(driver)
	tassert.CheckError(t, err)
	createUsers(mgr, t)
	testInvalidUser(mgr, t)
	testUserDelete(mgr, t)
	deleteUsers(mgr, false, t)
	clusterList(t)
}

func TestToken(t *testing.T) {
	var (
		err   error
		token string
	)

	driver := dbdriver.NewDBMock()
	mgr, err := newUserManager(driver)
	tassert.CheckError(t, err)
	createUsers(mgr, t)

	// correct user creds
	shortExpiration := 2 * time.Second
	token, err = mgr.issueToken(users[1], passs[1], shortExpiration)
	if err != nil || token == "" {
		t.Errorf("Failed to generate token for %s: %v", users[1], err)
	}
	info, err := mgr.userByToken(token)
	if err != nil {
		t.Errorf("Failed to get user by token %v: %v", token, err)
	}
	if info == nil || info.UserID != users[1] {
		if info == nil {
			t.Errorf("No user returned for token %v", token)
		} else {
			t.Errorf("Invalid user %s returned for token %v", info.UserID, token)
		}
	}

	// incorrect user creds
	tokenInval, err := mgr.issueToken(users[1], passs[0])
	if tokenInval != "" || err == nil {
		t.Errorf("Some token generated for incorrect user creds: %v", tokenInval)
	}

	// expired token test
	time.Sleep(shortExpiration)
	_, err = mgr.tokenByUser(users[1])
	if err != nil {
		t.Errorf("No token found for %s", users[1])
	}
	info, err = mgr.userByToken(token)
	if info != nil || err == nil {
		t.Errorf("Token %s expected to be expired[%p]: %v", token, info, err)
	} else if err != errTokenExpired {
		t.Errorf("Invalid error(must be 'token expired'): %v", err)
	}

	// revoke token test
	token, err = mgr.issueToken(users[1], passs[1])
	if err == nil {
		_, err = mgr.userByToken(token)
	}
	if err != nil {
		t.Errorf("Failed to test revoking token% v", err)
	} else {
		mgr.revokeToken(token)
		info, err = mgr.userByToken(token)
		if info != nil {
			t.Errorf("Some user returned by revoken token %s: %s", token, info.UserID)
		} else if err == nil {
			t.Error("No error for revoked token")
		} else if err != errTokenNotFound {
			t.Errorf("Invalid error: %v", err)
		}
	}

	deleteUsers(mgr, false, t)
}

func clusterList(t *testing.T) {
	cluList := &clusterConfig{
		Conf: map[string][]string{
			"clu1": {"1.1.1.1"},
			"clu2": {},
		},
	}
	err := conf.updateClusters(cluList)
	if err == nil {
		t.Error("Registering a cluster with empty URL list must fail")
	}
	cluList = &clusterConfig{
		Conf: map[string][]string{
			"clu1": {"1.1.1.1"},
			"clu2": {"2.2.2.2"},
		},
	}
	err = conf.updateClusters(cluList)
	tassert.CheckError(t, err)
	cluList = &clusterConfig{
		Conf: map[string][]string{
			"clu3": {"3.3.3.3"},
			"clu2": {"2.2.2.2", "4.4.4.4"},
		},
	}
	err = conf.updateClusters(cluList)
	tassert.CheckError(t, err)
	if len(conf.Cluster.Conf) != 3 {
		t.Errorf("Expected 3 registered clusters, %d found\n%+v",
			len(conf.Cluster.Conf), conf.Cluster.Conf)
	}
	urls, ok := conf.Cluster.Conf["clu2"]
	if !ok || len(urls) != 2 {
		t.Errorf("clu2 must be updated: %v - %+v", ok, urls)
	}
}

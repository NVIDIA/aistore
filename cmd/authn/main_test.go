// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

// NOTE: when a fresh user manager is created, it initailized users DB and
// adds a default user with role Guest, so all length checks must add 1

var (
	users = []string{"user1", "user2", "user3"}
	passs = []string{"pass2", "pass1", "passs"}
)

func init() {
	// Set default expiration time to 30 minutes
	if conf.Server.ExpirePeriod == 0 {
		conf.Server.ExpirePeriod = cmn.DurationJSON(time.Minute * 30)
	}
}

func createUsers(mgr *userManager, t *testing.T) {
	for idx := range users {
		user := &cmn.AuthUser{ID: users[idx], Password: passs[idx], Roles: []string{cmn.AuthGuestRole}}
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
	user := &cmn.AuthUser{ID: users[0], Password: passs[1], Roles: []string{cmn.AuthGuestRole}}
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
	user := &cmn.AuthUser{ID: username, Password: userpass, Roles: []string{cmn.AuthGuestRole}}
	err := mgr.addUser(user)
	if err != nil {
		t.Errorf("Failed to create a user %s: %v", username, err)
	}
	srvUsers, err := mgr.userList()
	tassert.CheckFatal(t, err)
	if len(srvUsers) != len(users)+2 {
		t.Errorf("Expected %d users but found %d", len(users)+2, len(srvUsers))
	}

	token, err := mgr.issueToken(username, userpass, nil)
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
	token, err = mgr.issueToken(username, userpass, nil)
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
}

func TestToken(t *testing.T) {
	var (
		err    error
		token  string
		secret = conf.Server.Secret
	)

	driver := dbdriver.NewDBMock()
	mgr, err := newUserManager(driver)
	tassert.CheckError(t, err)
	createUsers(mgr, t)
	defer deleteUsers(mgr, false, t)

	// correct user creds
	shortExpiration := 2 * time.Second
	token, err = mgr.issueToken(users[1], passs[1], &shortExpiration)
	if err != nil || token == "" {
		t.Errorf("Failed to generate token for %s: %v", users[1], err)
	}
	info, err := cmn.DecryptToken(token, secret)
	if err != nil {
		t.Fatalf("Failed to decript token %v: %v", token, err)
	}
	if info.UserID != users[1] {
		t.Errorf("Invalid user %s returned for token of %s", info.UserID, users[1])
	}

	// incorrect user creds
	tokenInval, err := mgr.issueToken(users[1], passs[0], nil)
	if tokenInval != "" || err == nil {
		t.Errorf("Some token generated for incorrect user creds: %v", tokenInval)
	}

	// expired token test
	time.Sleep(shortExpiration)
	_, err = cmn.DecryptToken(token, secret)
	if err == nil {
		t.Fatalf("Token must be expired: %s", token)
	}
	if err != cmn.ErrTokenExpired {
		t.Errorf("Invalid error(must be 'token expired'): %v", err)
	}
}

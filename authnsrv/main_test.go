// Package authnsrv provides AuthN server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authnsrv

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn/cos"
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
	if Conf.Server.ExpirePeriod == 0 {
		Conf.Server.ExpirePeriod = cos.Duration(time.Minute * 30)
	}
}

func createUsers(mgr *UserManager, t *testing.T) {
	for idx := range users {
		user := &authn.User{ID: users[idx], Password: passs[idx], Roles: []string{GuestRole}}
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

func deleteUsers(mgr *UserManager, skipNotExist bool, t *testing.T) {
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

func testInvalidUser(mgr *UserManager, t *testing.T) {
	user := &authn.User{ID: users[0], Password: passs[1], Roles: []string{GuestRole}}
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

func testUserDelete(mgr *UserManager, t *testing.T) {
	const (
		username = "newuser"
		userpass = "newpass"
	)
	user := &authn.User{ID: username, Password: userpass, Roles: []string{GuestRole}}
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
	driver := mock.NewDBDriver()
	mgr, err := NewUserManager(driver)
	tassert.CheckError(t, err)
	createUsers(mgr, t)
	testInvalidUser(mgr, t)
	testUserDelete(mgr, t)
	deleteUsers(mgr, false, t)
}

func TestToken(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	var (
		err    error
		token  string
		secret = Conf.Server.Secret
	)

	driver := mock.NewDBDriver()
	mgr, err := NewUserManager(driver)
	tassert.CheckError(t, err)
	createUsers(mgr, t)
	defer deleteUsers(mgr, false, t)

	// correct user creds
	shortExpiration := 2 * time.Second
	token, err = mgr.issueToken(users[1], passs[1], &shortExpiration)
	if err != nil || token == "" {
		t.Errorf("Failed to generate token for %s: %v", users[1], err)
	}
	info, err := DecryptToken(token, secret)
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
	_, err = DecryptToken(token, secret)
	if err == nil {
		t.Fatalf("Token must be expired: %s", token)
	}
	if err != ErrTokenExpired {
		t.Errorf("Invalid error(must be 'token expired'): %v", err)
	}
}

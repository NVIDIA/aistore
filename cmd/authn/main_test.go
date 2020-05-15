// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tutils/tassert"
)

const (
	dbPath = "/tmp/users.json"
)

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
	var (
		err error
	)

	if mgr.Path != dbPath {
		t.Fatalf("Invalid path used for user list: %s", mgr.Path)
	}

	for idx := range users {
		err = mgr.addUser(users[idx], passs[idx])
		if err != nil {
			t.Errorf("Failed to create a user %s: %v", users[idx], err)
		}
	}

	if len(mgr.Users) != len(users) {
		t.Errorf("User count mismatch. Found %d users instead of %d", len(mgr.Users), len(users))
	}
	for _, username := range users {
		info, ok := mgr.Users[username]
		if info == nil || !ok {
			t.Errorf("User %s not found", username)
		}
	}
}

func deleteUsers(mgr *userManager, skipNotExist bool, t *testing.T) {
	var err error
	for _, username := range users {
		err = mgr.delUser(username)
		if err != nil {
			if !strings.Contains(err.Error(), "not exist") || !skipNotExist {
				t.Errorf("Failed to delete user %s: %v", username, err)
			}
		}
	}

	err = os.Remove(dbPath)
	if err != nil {
		t.Error(err)
	}
}

func testInvalidUser(mgr *userManager, t *testing.T) {
	err := mgr.addUser(users[0], passs[1])
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Errorf("User with the existing name %s was created: %v", users[0], err)
	}

	nonexisting := "someuser"
	err = mgr.delUser(nonexisting)
	if err == nil || !strings.Contains(err.Error(), "") {
		t.Errorf("Non-existing user %s was deleted: %v", nonexisting, err)
	}
}

func reloadFromFile(mgr *userManager, t *testing.T) {
	newmgr := newUserManager(dbPath)
	if newmgr == nil {
		t.Error("New manager has not been created")
	}
	for username, creds := range mgr.Users {
		if username == conf.Auth.Username {
			continue
		}
		if info, ok := newmgr.Users[username]; !ok || info == nil || info.Password != creds.Password {
			t.Errorf("User %s not found in reloaded list", username)
		}
	}
	for username, creds := range newmgr.Users {
		if username == conf.Auth.Username {
			continue
		}
		if info, ok := mgr.Users[username]; !ok || info == nil || info.Password != creds.Password {
			t.Errorf("User %s should not be in saved list", username)
		}
	}
}

func testUserDelete(mgr *userManager, t *testing.T) {
	const (
		username = "newuser"
		userpass = "newpass"
	)
	err := mgr.addUser(username, userpass)
	if err != nil {
		t.Errorf("Failed to create a user %s: %v", username, err)
	}
	if len(mgr.Users) != len(users)+1 {
		t.Errorf("Expected %d users but found %d", len(users)+1, len(mgr.Users))
	}

	token, err := mgr.issueToken(username, userpass)
	if err != nil || token == "" {
		t.Errorf("Failed to generate token for %s: %v", username, err)
	}

	err = mgr.delUser(username)
	if err != nil {
		t.Errorf("Failed to delete user %s: %v", username, err)
	}
	if len(mgr.Users) != len(users) {
		t.Errorf("Expected %d users but found %d", len(users), len(mgr.Users))
	}
	token, err = mgr.issueToken(username, userpass)
	if token != "" || err == nil || !strings.Contains(err.Error(), "credential") {
		t.Errorf("Token issued for deleted user  %s: %v", username, token)
	}
}

func TestManager(t *testing.T) {
	mgr := newUserManager(dbPath)
	if mgr == nil {
		t.Fatal("Manager has not been created")
	}
	createUsers(mgr, t)
	testInvalidUser(mgr, t)
	testUserDelete(mgr, t)
	reloadFromFile(mgr, t)
	deleteUsers(mgr, false, t)
	clusterList(t)
}

func TestToken(t *testing.T) {
	var (
		err   error
		token string
	)

	mgr := newUserManager(dbPath)
	if mgr == nil {
		t.Fatal("Manager has not been created")
	}
	createUsers(mgr, t)

	// correct user creds
	token, err = mgr.issueToken(users[1], passs[1])
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
	tokeninfo, ok := mgr.tokens[users[1]]
	if !ok || tokeninfo == nil {
		t.Errorf("No token found for %s", users[1])
	}
	if tokeninfo != nil {
		tokeninfo.Expires = time.Now().Add(-1 * time.Hour)
	}
	info, err = mgr.userByToken(token)
	if info != nil || err == nil {
		t.Errorf("Token %s expected to be expired[%x]: %v", token, info, err)
	} else if !strings.Contains(err.Error(), "expire") {
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
		} else if !strings.Contains(err.Error(), "not found") {
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

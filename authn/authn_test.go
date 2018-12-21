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

	"github.com/NVIDIA/dfcpub/cmn"
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
	proxy := &proxy{}
	newmgr := newUserManager(dbPath, proxy)
	if newmgr == nil {
		t.Error("New manager has not been created")
	}
	if len(newmgr.Users) != len(mgr.Users) {
		t.Errorf("Number of users mismatch: old=%d, new=%d", len(mgr.Users), len(newmgr.Users))
	}
	for username, creds := range mgr.Users {
		if info, ok := newmgr.Users[username]; !ok || info == nil || info.Password != creds.Password {
			t.Errorf("User %s not found in reloaded list", username)
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

func addRemoveCreds(mgr *userManager, t *testing.T) {
	const (
		AWS01 = "aws-01"
		GCP01 = "gcp-01"
		AWS02 = "aws-02"
	)
	userID := users[0]

	// add valid credentials
	changed, err := mgr.updateCredentials(userID, cmn.ProviderAmazon, AWS01)
	if err != nil {
		t.Errorf("Failed to update credentials")
	}
	if !changed {
		t.Error("Credentials were not updated")
	}
	changed, err = mgr.updateCredentials(userID, cmn.ProviderGoogle, GCP01)
	if err != nil {
		t.Errorf("Failed to update credentials")
	}
	if !changed {
		t.Error("Credentials were not updated")
	}
	userInfo, ok := mgr.Users[userID]
	if !ok {
		t.Errorf("User %s not found", userID)
	}
	userAws, ok := userInfo.Creds[cmn.ProviderAmazon]
	if !ok || userAws != AWS01 {
		t.Errorf("User %s AWS credentials are invalid: %s (expected %s)", userID, userAws, AWS01)
	}
	userGcp, ok := userInfo.Creds[cmn.ProviderGoogle]
	if !ok || userGcp != GCP01 {
		t.Errorf("User %s GCP credentials are invalid: %s (expected %s)", userID, userGcp, GCP01)
	}
	userDfc, ok := userInfo.Creds[cmn.ProviderDFC]
	if ok || userDfc != "" {
		t.Errorf("DFC credentials must be empty (current: %s)", userDfc)
	}

	// update credentials
	changed, err = mgr.updateCredentials(userID, cmn.ProviderAmazon, AWS02)
	if err != nil {
		t.Errorf("Failed to update credentials")
	}
	if !changed {
		t.Error("Credentials were not updated")
	}
	userInfo = mgr.Users[userID]
	userAws, ok = userInfo.Creds[cmn.ProviderAmazon]
	if !ok || userAws != AWS02 {
		t.Errorf("User %s AWS credentials are invalid: %s (expected %s)", userID, userAws, AWS02)
	}

	// update invalid provider
	changed, err = mgr.updateCredentials(userID, "Provider", "0123")
	if changed {
		t.Error("Credentials were updated")
	}
	userInfo = mgr.Users[userID]
	userAws = userInfo.Creds[cmn.ProviderAmazon]
	userGcp = userInfo.Creds[cmn.ProviderGoogle]
	if userAws != AWS02 || userGcp != GCP01 {
		t.Errorf("Credentials changed: AWS %s -> %s, GCP: %s -> %s",
			AWS02, userAws, GCP01, userGcp)
	}
	if err == nil || !strings.Contains(err.Error(), "cloud provider") {
		t.Errorf("Invalid error: %v", err)
	}

	// update invalid user
	changed, err = mgr.updateCredentials(userID+userID, cmn.ProviderAmazon, "0123")
	if changed {
		t.Errorf("Credentials were updated for %s", userID+userID)
	}
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Invalid error: %v", err)
	}

	// delete invalid user credentials
	changed, err = mgr.deleteCredentials(userID+userID, cmn.ProviderAmazon)
	if changed {
		t.Errorf("Credentials were deleted for %s", userID+userID)
	}
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Invalid error: %v", err)
	}

	// delete invalid provider credentials
	changed, err = mgr.deleteCredentials(userID, "Provider")
	if changed {
		t.Errorf("Credentials were deleted for %s", userID)
	}
	if err == nil || !strings.Contains(err.Error(), "cloud provider") {
		t.Errorf("Invalid error: %v", err)
	}

	// delete valid credentials
	changed, err = mgr.deleteCredentials(userID, cmn.ProviderAmazon)
	if !changed {
		t.Errorf("Credentials were not deleted for %s", userID)
	}
	if err != nil {
		t.Errorf("Failed to delete credentials: %v", err)
	}
	userInfo = mgr.Users[userID]
	if len(userInfo.Creds) != 1 {
		t.Errorf("Invalid number of credentials: %d(expected 1)\n%v", len(userInfo.Creds), userInfo.Creds)
	}

	// delete the same once more
	changed, err = mgr.deleteCredentials(userID, cmn.ProviderAmazon)
	if changed {
		t.Errorf("Credentials were changed for %s", userID)
	}
	if err != nil {
		t.Errorf("Failed to delete credentials: %v", err)
	}
	userInfo = mgr.Users[userID]
	if len(userInfo.Creds) != 1 {
		t.Errorf("Invalid number of credentials: %d(expected 1)\n%v", len(userInfo.Creds), userInfo.Creds)
	}

	// delete the last credentials
	changed, err = mgr.deleteCredentials(userID, cmn.ProviderGoogle)
	if !changed {
		t.Errorf("Credentials were not changed for %s", userID)
	}
	if err != nil {
		t.Errorf("Failed to delete credentials: %v", err)
	}
	userInfo = mgr.Users[userID]
	if len(userInfo.Creds) != 0 {
		t.Errorf("Invalid number of credentials: %d(expected empty)\n%v", len(userInfo.Creds), userInfo.Creds)
	}
}

func TestManager(t *testing.T) {
	proxy := &proxy{}
	mgr := newUserManager(dbPath, proxy)
	if mgr == nil {
		t.Fatal("Manager has not been created")
	}
	createUsers(mgr, t)
	testInvalidUser(mgr, t)
	addRemoveCreds(mgr, t)
	testUserDelete(mgr, t)
	reloadFromFile(mgr, t)
	deleteUsers(mgr, false, t)
}

func TestToken(t *testing.T) {
	var (
		err   error
		token string
	)

	proxy := &proxy{}
	mgr := newUserManager(dbPath, proxy)
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
	} else if err != nil && !strings.Contains(err.Error(), "expire") {
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

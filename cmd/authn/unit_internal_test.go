// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/tools/tassert"
)

var (
	users     = []string{"user1", "user2", "user3"}
	passs     = []string{"pass2", "pass1", "passs"}
	guestRole = &authn.Role{
		Name:        GuestRole,
		Description: "Read-only access to buckets",
		ClusterACLs: []*authn.CluACL{
			{ID: "test-clu-id", Access: apc.AccessRO},
		},
	}
)

func createUsers(mgr *mgr, t *testing.T) {
	for idx := range users {
		user := &authn.User{ID: users[idx], Password: passs[idx], Roles: []*authn.Role{guestRole}}
		_, err := mgr.addUser(user)
		if err != nil {
			t.Errorf("Failed to create user %s: %v", users[idx], err)
		}
	}

	srvUsers, _, err := mgr.userList()
	tassert.CheckFatal(t, err)
	expectedUsersCount := len(users) + 1 // including the admin user
	if len(srvUsers) != expectedUsersCount {
		t.Errorf("User count mismatch. Found %d users instead of %d", len(srvUsers), expectedUsersCount)
	}
	for _, username := range users {
		if _, ok := srvUsers[username]; !ok {
			t.Errorf("User %q not found", username)
		}
	}
}

func deleteUsers(mgr *mgr, skipNotExist bool, t *testing.T) {
	for _, username := range users {
		_, err := mgr.delUser(username)
		if err != nil && (!cos.IsErrNotFound(err) || !skipNotExist) {
			t.Errorf("Failed to delete user %s: %v", username, err)
		}
	}
}

func testInvalidUser(mgr *mgr, t *testing.T) {
	user := &authn.User{ID: users[0], Password: passs[1], Roles: []*authn.Role{guestRole}}
	_, err := mgr.addUser(user)
	if err == nil {
		t.Errorf("User with the existing name %s was created", users[0])
	}

	nonexisting := "someuser"
	_, err = mgr.delUser(nonexisting)
	if err == nil {
		t.Errorf("Non-existing user %s was deleted", nonexisting)
	}
}

func testUserDelete(mgr *mgr, t *testing.T) {
	const (
		username = "newuser"
		userpass = "newpass"
	)
	user := &authn.User{ID: username, Password: userpass, Roles: []*authn.Role{guestRole}}
	_, err := mgr.addUser(user)
	if err != nil {
		t.Errorf("Failed to create user %s: %v", username, err)
	}
	srvUsers, _, err := mgr.userList()
	tassert.CheckFatal(t, err)
	expectedUsersCount := len(users) + 2 // including the admin user and the new user
	if len(srvUsers) != expectedUsersCount {
		t.Errorf("Expected %d users but found %d", expectedUsersCount, len(srvUsers))
	}

	clu := authn.CluACL{
		ID:    "ABCD",
		Alias: "cluster-test",
		URLs:  []string{"http://localhost:8080"},
	}
	if _, err := mgr.db.Set(clustersCollection, clu.ID, clu); err != nil {
		t.Error(err)
	}
	defer mgr.delCluster(clu.ID)

	loginMsg := &authn.LoginMsg{}
	token, _, err := mgr.issueToken(username, userpass, loginMsg)
	if err != nil || token == "" {
		t.Errorf("Failed to generate token for %s: %v", username, err)
	}

	_, err = mgr.delUser(username)
	if err != nil {
		t.Errorf("Failed to delete user %s: %v", username, err)
	}
	srvUsers, _, err = mgr.userList()
	tassert.CheckFatal(t, err)
	expectedUsersCount = len(users) + 1 // including the admin user
	if len(srvUsers) != expectedUsersCount {
		t.Errorf("Expected %d users but found %d", expectedUsersCount, len(srvUsers))
	}
	token, _, err = mgr.issueToken(username, userpass, loginMsg)
	if err == nil {
		t.Errorf("Token issued for deleted user %s: %v", username, token)
	} else if err != errInvalidCredentials {
		t.Errorf("Invalid error: %v", err)
	}
}

func createCM(t *testing.T, conf *authn.Config) *config.ConfManager {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "authn.json")
	err := jsp.SaveMeta(path, conf, nil)
	tassert.Fatalf(t, err == nil, "failed to write config: %v", err)
	cm := config.NewConfManager()
	cm.Init(path)
	return cm
}

// Create a CM with a pre-configured config struct
func createEmptyCM(t *testing.T) *config.ConfManager {
	conf := &authn.Config{Server: authn.ServerConf{Secret: "mytestsecret"}}
	return createCM(t, conf)
}

func createManagerWithAdmin(cm *config.ConfManager, driver kvdb.Driver) (*mgr, error) {
	oldPass, wasSet := os.LookupEnv(env.AisAuthAdminPassword)
	os.Setenv(env.AisAuthAdminPassword, "admin-pass-for-test")
	// Reset after test
	defer func() {
		if wasSet {
			os.Setenv(env.AisAuthAdminPassword, oldPass)
		} else {
			os.Unsetenv(env.AisAuthAdminPassword)
		}
	}()
	m, _, err := newMgr(cm, driver)
	return m, err
}

func TestManager(t *testing.T) {
	driver := mock.NewDBDriver()
	cm := createEmptyCM(t)
	// NOTE: new manager initializes users DB and adds a default user as a Guest
	mgr, err := createManagerWithAdmin(cm, driver)
	tassert.CheckError(t, err)
	createUsers(mgr, t)
	testInvalidUser(mgr, t)
	testUserDelete(mgr, t)
	deleteUsers(mgr, false, t)
}

func TestManagerNoAdminPass(t *testing.T) {
	driver := mock.NewDBDriver()
	cm := createEmptyCM(t)
	// If no admin password exists in env, initializing manager must fail
	_, _, err := newMgr(cm, driver)
	if err == nil {
		t.Fatal("expected error initializing manager without admin password Env, got nil")
	}
}

func TestToken(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	var (
		err   error
		token string
	)
	driver := mock.NewDBDriver()
	cm := createEmptyCM(t)
	mgr, err := createManagerWithAdmin(cm, driver)
	tassert.CheckFatal(t, err)
	createUsers(mgr, t)
	defer deleteUsers(mgr, false, t)

	clu := authn.CluACL{
		ID:    "ABCD",
		Alias: "cluster-test",
		URLs:  []string{"http://localhost:8080"},
	}
	if _, err := mgr.db.Set(clustersCollection, clu.ID, clu); err != nil {
		t.Error(err)
	}
	defer mgr.delCluster(clu.ID)

	// correct user creds
	shortExpiration := 2 * time.Second
	loginMsg := &authn.LoginMsg{ExpiresIn: &shortExpiration}
	token, _, err = mgr.issueToken(users[1], passs[1], loginMsg)
	if err != nil || token == "" {
		t.Errorf("Failed to generate token for %s: %v", users[1], err)
	}
	info, err := mgr.tkParser.ValidateToken(t.Context(), token)
	if err != nil {
		t.Fatalf("Failed to decrypt token %v: %v", token, err)
	}
	sub, err := info.GetSubject()
	tassert.CheckFatal(t, err)
	if sub != users[1] {
		t.Errorf("Invalid subject %s returned for token of %s", sub, users[1])
	}

	// incorrect user creds
	loginMsg = &authn.LoginMsg{}
	tokenInval, _, err := mgr.issueToken(users[1], passs[0], loginMsg)
	if tokenInval != "" || err == nil {
		t.Errorf("Some token generated for incorrect user creds: %v", tokenInval)
	}

	// expired token test
	time.Sleep(shortExpiration)
	_, err = mgr.tkParser.ValidateToken(t.Context(), token)
	tassert.Fatalf(t, errors.Is(err, tok.ErrTokenExpired), "Token must be expired: %s", token)
}

func TestMergeCluACLS(t *testing.T) {
	tests := []struct {
		title    string
		cluFlt   string
		toACLs   cluACLList
		fromACLs cluACLList
		resACLs  cluACLList
	}{
		{
			title: "The same lists",
			toACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
			},
			fromACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
			},
			resACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
			},
		},
		{
			title: "Update permissions only",
			toACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
			},
			fromACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 40,
				},
			},
			resACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 40,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
			},
		},
		{
			title: "Append new cluster",
			toACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
			},
			fromACLs: []*authn.CluACL{
				{
					ID:     "abcde",
					Alias:  "third",
					Access: 40,
				},
				{
					ID:     "hijk",
					Alias:  "fourth",
					Access: 40,
				},
			},
			resACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
				{
					ID:     "abcde",
					Alias:  "third",
					Access: 40,
				},
				{
					ID:     "hijk",
					Alias:  "fourth",
					Access: 40,
				},
			},
		},
		{
			title: "Update permissions for existing cluster and append new ones",
			toACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
			},
			fromACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 40,
				},
				{
					ID:     "abcde",
					Alias:  "third",
					Access: 60,
				},
			},
			resACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 40,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
				{
					ID:     "abcde",
					Alias:  "third",
					Access: 60,
				},
			},
		},
		{
			title:  "Append only 'abcde' cluster",
			cluFlt: "abcde",
			toACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
			},
			fromACLs: []*authn.CluACL{
				{
					ID:     "abcde",
					Alias:  "third",
					Access: 40,
				},
				{
					ID:     "hijk",
					Alias:  "fourth",
					Access: 40,
				},
			},
			resACLs: []*authn.CluACL{
				{
					ID:     "1234",
					Alias:  "one",
					Access: 20,
				},
				{
					ID:     "5678",
					Alias:  "second",
					Access: 20,
				},
				{
					ID:     "abcde",
					Alias:  "third",
					Access: 40,
				},
			},
		},
	}
	for _, test := range tests {
		res := mergeClusterACLs(test.toACLs, test.fromACLs, test.cluFlt)
		for i, r := range res {
			if r.String() != test.resACLs[i].String() || r.Access != test.resACLs[i].Access {
				t.Errorf("%s[filter: %s]: %v[%v] != %v[%v]", test.title, test.cluFlt, r, r.Access, test.resACLs[i], test.resACLs[i].Access)
			}
		}
	}
}

func newBck(name, provider, uuid string) cmn.Bck {
	return cmn.Bck{
		Name:     name,
		Provider: provider,
		Ns:       cmn.Ns{UUID: uuid},
	}
}

func TestMergeBckACLS(t *testing.T) {
	tests := []struct {
		title    string
		cluFlt   string
		toACLs   bckACLList
		fromACLs bckACLList
		resACLs  bckACLList
	}{
		{
			title: "Nothing to update",
			toACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 20,
				},
			},
			fromACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
			},
			resACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 20,
				},
			},
		},
		{
			title: "Update permissions only",
			toACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 20,
				},
			},
			fromACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 40,
				},
			},
			resACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 40,
				},
			},
		},
		{
			title: "Append new buckets",
			toACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 20,
				},
			},
			fromACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 30,
				},
				{
					Bck:    newBck("bck", "aws", "5678"),
					Access: 40,
				},
				{
					Bck:    newBck("bck1", "ais", "1234"),
					Access: 50,
				},
			},
			resACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 30,
				},
				{
					Bck:    newBck("bck", "aws", "5678"),
					Access: 40,
				},
				{
					Bck:    newBck("bck1", "ais", "1234"),
					Access: 50,
				},
			},
		},
		{
			title: "Update permissions for existing buckets and append new ones",
			toACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 20,
				},
			},
			fromACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck2", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 70,
				},
			},
			resACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 70,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 20,
				},
				{
					Bck:    newBck("bck2", "ais", "1234"),
					Access: 20,
				},
			},
		},
		{
			title:  "Append and update buckets of '5678' cluster only",
			cluFlt: "5678",
			toACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 20,
				},
			},
			fromACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck2", "ais", "5678"),
					Access: 60,
				},
				{
					Bck:    newBck("bck2", "ais", "1234"),
					Access: 70,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 90,
				},
			},
			resACLs: []*authn.BckACL{
				{
					Bck:    newBck("bck", "ais", "1234"),
					Access: 20,
				},
				{
					Bck:    newBck("bck", "ais", "5678"),
					Access: 90,
				},
				{
					Bck:    newBck("bck2", "ais", "5678"),
					Access: 60,
				},
			},
		},
	}
	for _, test := range tests {
		res := mergeBckACLs(test.toACLs, test.fromACLs, test.cluFlt)
		for i, r := range res {
			if !r.Bck.Equal(&test.resACLs[i].Bck) || r.Access != test.resACLs[i].Access {
				t.Errorf("%s[filter: %s]: %v[%v] != %v[%v]", test.title, test.cluFlt, r.Bck, r.Access, test.resACLs[i], test.resACLs[i].Access)
			}
		}
	}
}

// Test retrieving the max age header for JWKS based on the configured key expiry with bounds
func TestGetJWKSMaxAge(t *testing.T) {
	driver := mock.NewDBDriver()
	tests := []struct {
		expire time.Duration
		want   int
	}{
		{1 * time.Hour, int((50 * time.Minute).Seconds())},
		{2 * time.Minute, int((5 * time.Minute).Seconds())},
		{9000 * time.Hour, int((720 * time.Hour).Seconds())},
		{0, int((720 * time.Hour).Seconds())},
	}

	for _, tt := range tests {
		conf := &authn.Config{Server: authn.ServerConf{Secret: "secret", Expire: cos.Duration(tt.expire)}}
		cm := createCM(t, conf)
		mgr, err := createManagerWithAdmin(cm, driver)
		tassert.CheckFatal(t, err)
		h := newServer(mgr)

		got := h.getJWKSMaxAge()
		tassert.Errorf(t, got == tt.want, "getJWKSMaxAge() = %d, want %d", got, tt.want)
	}
}

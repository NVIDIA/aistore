// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/tools/tassert"
)

var (
	errTestDBGetFailure = errors.New("test db get failure")
	errTestDBSetFailure = errors.New("test db set failure")
)

type failGetCollectionDB struct {
	kvdb.Driver
	collection string
}

func (db *failGetCollectionDB) Get(collection, key string, object any) (int, error) {
	if collection == db.collection {
		return http.StatusInternalServerError, errTestDBGetFailure
	}
	return db.Driver.Get(collection, key, object)
}

type failSetKeyDB struct {
	kvdb.Driver
	collection string
	key        string
}

func (db *failSetKeyDB) Set(collection, key string, object any) (int, error) {
	if collection == db.collection && key == db.key {
		return http.StatusInternalServerError, errTestDBSetFailure
	}
	return db.Driver.Set(collection, key, object)
}

type slowGetCollectionDB struct {
	kvdb.Driver
	collection string
	delay      time.Duration
}

func (db *slowGetCollectionDB) Get(collection, key string, object any) (int, error) {
	code, err := db.Driver.Get(collection, key, object)
	if collection == db.collection {
		time.Sleep(db.delay)
	}
	return code, err
}

func TestRoleNames(t *testing.T) {
	tests := []struct {
		name  string
		roles []*authn.Role
		want  []string
	}{
		{
			name: "SkipNilAndEmpty",
			roles: []*authn.Role{
				nil,
				{Name: ""},
				{Name: "role-a"},
				{Name: "role-b"},
			},
			want: []string{"role-a", "role-b"},
		},
		{name: "Empty", roles: nil, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := roleNames(tt.roles)
			tassert.Errorf(t, slices.Equal(got, tt.want), "roleNames() = %v, want %v", got, tt.want)
		})
	}
}

func TestCloneRole(t *testing.T) {
	role := &authn.Role{
		Name:        "clone-me",
		Description: "desc",
		ClusterACLs: []*authn.CluACL{{ID: "clu1", Access: apc.AccessRO}},
		BucketACLs:  []*authn.BckACL{{Access: apc.AccessRW}},
	}
	cloned := cloneRole(role)
	tassert.Errorf(t, cloned != role, "clone should be a distinct object")
	tassert.Errorf(t, cloned.Name == role.Name, "name mismatch")
	tassert.Errorf(t, cloned.ClusterACLs[0] != role.ClusterACLs[0], "cluster ACL should be deep-copied")

	cloned.ClusterACLs[0].Access = apc.AccessAll
	tassert.Errorf(t, role.ClusterACLs[0].Access == apc.AccessRO, "mutating clone should not affect source")
}

func TestFetchUserRoles(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	existing := &authn.Role{
		Name:        "existing-role",
		Description: "from collection",
		ClusterACLs: []*authn.CluACL{{ID: "clu1", Access: apc.AccessRW}},
	}
	if _, err := testMgr.addRole(existing); err != nil {
		t.Fatal(err)
	}

	missing := &authn.Role{
		Name:        "missing-role",
		Description: "not in collection",
		ClusterACLs: []*authn.CluACL{{ID: "clu2", Access: apc.AccessRO}},
	}

	resolved, code, err := testMgr.fetchUserRoles([]*authn.Role{
		{Name: existing.Name, Description: "stale"},
		missing,
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == 200, "expected status 200, got %d", code)
	tassert.Errorf(t, len(resolved) == 1, "expected 1 resolved role, got %d", len(resolved))

	tassert.Errorf(t, resolved[0].Name == existing.Name,
		"expected role %q, got %q", existing.Name, resolved[0].Name)
	tassert.Errorf(t, resolved[0].Description == existing.Description,
		"expected description %q from collection, got %q", existing.Description, resolved[0].Description)
	tassert.Errorf(t, resolved[0].ClusterACLs[0].Access == apc.AccessRW,
		"expected ACLs from collection, got %s", resolved[0].ClusterACLs[0].Access)
}

func TestRoleUsersIndexCRUD(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "indexed-role"
		userA    = "user-a"
		userB    = "user-b"
	)
	if err := testMgr.addUserToRoleIndex(roleName, userA); err != nil {
		t.Fatal(err)
	}
	if err := testMgr.addUserToRoleIndex(roleName, userB); err != nil {
		t.Fatal(err)
	}
	if err := testMgr.addUserToRoleIndex(roleName, userA); err != nil {
		t.Fatal(err)
	}

	users, err := testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Equal(users, []string{userA, userB}),
		"expected sorted deduplicated users, got %v", users)

	if err := testMgr.removeUserFromRoleIndex(roleName, userA); err != nil {
		t.Fatal(err)
	}
	users, err = testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Equal(users, []string{userB}), "expected [%s], got %v", userB, users)

	if err := testMgr.removeUserFromRoleIndex(roleName, userB); err != nil {
		t.Fatal(err)
	}
	users, err = testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, users == nil, "expected index entry to be removed, got %v", users)
}

func TestConcurrentAddUsersPreservesRoleIndex(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "concurrent-role"
		userCnt  = 16
	)
	if _, err := testMgr.addRole(&authn.Role{Name: roleName}); err != nil {
		t.Fatal(err)
	}
	origDB := testMgr.db
	testMgr.db = &slowGetCollectionDB{Driver: origDB, collection: roleUsersCollection, delay: time.Millisecond}

	var wg sync.WaitGroup
	errCh := make(chan error, userCnt)
	for i := range userCnt {
		userID := fmt.Sprintf("concurrent-user-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := testMgr.addUser(&authn.User{
				ID:       userID,
				Password: "pass",
				Roles:    []*authn.Role{{Name: roleName}},
			})
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	testMgr.db = origDB

	for err := range errCh {
		t.Fatal(err)
	}
	users, err := testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(users) == userCnt, "expected %d indexed users, got %d: %v", userCnt, len(users), users)
	for i := range userCnt {
		userID := fmt.Sprintf("concurrent-user-%02d", i)
		tassert.Errorf(t, slices.Contains(users, userID), "expected indexed users to contain %q: %v", userID, users)
	}
}

func TestAddUserFailsWithoutPersistOnRoleIndexSyncFailure(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "persist-role"
		userID   = "persist-user"
	)
	if _, err := testMgr.addRole(&authn.Role{Name: roleName}); err != nil {
		t.Fatal(err)
	}

	origDB := testMgr.db
	testMgr.db = &failGetCollectionDB{Driver: origDB, collection: roleUsersCollection}
	_, err := testMgr.addUser(&authn.User{
		ID:       userID,
		Password: "pass",
		Roles:    []*authn.Role{{Name: roleName}},
	})
	testMgr.db = origDB

	tassert.Errorf(t, err != nil, "expected addUser to fail")
	_, _, err = testMgr.lookupUser(userID)
	tassert.Errorf(t, err != nil, "expected user %q not to be created", userID)
	users, err := testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !slices.Contains(users, userID), "expected %q absent from %q index, got %v", userID, roleName, users)
}

func TestAddUserRetryAfterUserSetFailure(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "retry-role"
		userID   = "retry-user"
	)
	if _, err := testMgr.addRole(&authn.Role{Name: roleName}); err != nil {
		t.Fatal(err)
	}

	origDB := testMgr.db
	testMgr.db = &failSetKeyDB{Driver: origDB, collection: usersCollection, key: userID}
	_, err := testMgr.addUser(&authn.User{
		ID:       userID,
		Password: "pass",
		Roles:    []*authn.Role{{Name: roleName}},
	})
	testMgr.db = origDB

	tassert.Errorf(t, err != nil, "expected addUser to fail on user persist")
	_, _, err = testMgr.lookupUser(userID)
	tassert.Errorf(t, err != nil, "expected user %q not to be created", userID)
	users, err := testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(users, userID), "expected orphan %q in %q index after failed persist, got %v", userID, roleName, users)

	if _, err := testMgr.addUser(&authn.User{
		ID:       userID,
		Password: "pass",
		Roles:    []*authn.Role{{Name: roleName}},
	}); err != nil {
		t.Fatal(err)
	}
	uInfo, _, err := testMgr.lookupUser(userID)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, uInfo.Roles[0].Name == roleName, "expected role %q on retry, got %q", roleName, uInfo.Roles[0].Name)
	users, err = testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(users, userID), "expected %q indexed for %q after retry, got %v", userID, roleName, users)
}

func TestUpdateUserRoleIndex(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleA  = "role-a"
		roleB  = "role-b"
		userID = "swap-user"
	)
	for _, role := range []string{roleA, roleB} {
		if err := testMgr.addUserToRoleIndex(role, userID); err != nil {
			t.Fatal(err)
		}
	}

	oldRoles := []*authn.Role{{Name: roleA}}
	newRoles := []*authn.Role{{Name: roleB}}
	if err := testMgr.updateUserRoleIndex(userID, oldRoles, newRoles); err != nil {
		t.Fatal(err)
	}

	usersA, err := testMgr.loadRoleUsers(roleA)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(usersA) == 0, "expected %q to be empty after swap, got %v", roleA, usersA)

	usersB, err := testMgr.loadRoleUsers(roleB)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Equal(usersB, []string{userID}), "expected [%s] in %q, got %v", userID, roleB, usersB)
}

func TestUpdateUserPersistsOnRoleIndexFailure(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleA   = "persist-user-a"
		roleB   = "persist-user-b"
		userID  = "persist-update-user"
		oldPass = "old-pass"
		newPass = "new-pass"
	)
	for _, roleName := range []string{roleA, roleB} {
		if _, err := testMgr.addRole(&authn.Role{Name: roleName}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := testMgr.addUser(&authn.User{
		ID:       userID,
		Password: oldPass,
		Roles:    []*authn.Role{{Name: roleA}},
	}); err != nil {
		t.Fatal(err)
	}

	origDB := testMgr.db
	testMgr.db = &failGetCollectionDB{Driver: origDB, collection: roleUsersCollection}
	code, err := testMgr.updateUser(userID, &authn.User{Password: newPass, Roles: []*authn.Role{{Name: roleB}}})
	testMgr.db = origDB

	tassert.Errorf(t, err != nil, "expected updateUser to fail")
	tassert.Errorf(t, code == http.StatusInternalServerError, "expected status %d, got %d", http.StatusInternalServerError, code)
	uInfo, _, err := testMgr.lookupUser(userID)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(uInfo.Roles) == 1, "expected user to keep one role, got %d", len(uInfo.Roles))
	tassert.Errorf(t, uInfo.Roles[0].Name == roleB, "expected user role %q, got %q", roleB, uInfo.Roles[0].Name)
	_, _, err = testMgr.issueToken(userID, oldPass, &authn.LoginMsg{})
	tassert.Errorf(t, err != nil, "expected persisted password update to reject old password")
	_, _, err = testMgr.issueToken(userID, newPass, &authn.LoginMsg{})
	tassert.CheckFatal(t, err)

	usersA, err := testMgr.loadRoleUsers(roleA)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(usersA, userID), "expected %q to remain indexed for %q, got %v", userID, roleA, usersA)
	usersB, err := testMgr.loadRoleUsers(roleB)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !slices.Contains(usersB, userID), "expected %q not indexed for %q, got %v", userID, roleB, usersB)
}

func TestRebuildAndEnsureRoleUsersIndex(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	// newMgr already built the index for the seeded admin user.
	adminUsers, err := testMgr.loadRoleUsers(authn.AdminRole)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(adminUsers, adminUserID),
		"expected admin user in %q index, got %v", authn.AdminRole, adminUsers)

	legacyUser := &authn.User{
		ID:    "legacy-user",
		Roles: []*authn.Role{{Name: "legacy-role", ClusterACLs: []*authn.CluACL{{ID: "clu", Access: apc.AccessRO}}}},
	}
	if _, err := testMgr.db.Set(usersCollection, legacyUser.ID, legacyUser); err != nil {
		t.Fatal(err)
	}
	if _, err := testMgr.db.DeleteCollection(roleUsersCollection); err != nil {
		t.Fatal(err)
	}
	// Simulate a legacy/incomplete build by clearing the version marker so the
	// rebuild is forced even though some index state may linger.
	if _, err := testMgr.db.Delete(metaCollection, roleUsersIndexMetaKey); err != nil {
		t.Fatal(err)
	}

	if err := testMgr.ensureRoleUsersIndex(); err != nil {
		t.Fatal(err)
	}

	for _, roleName := range []string{authn.AdminRole, "legacy-role"} {
		users, err := testMgr.loadRoleUsers(roleName)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, len(users) > 0, "expected non-empty index for %q after rebuild", roleName)
	}
	legacyUsers, err := testMgr.loadRoleUsers("legacy-role")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(legacyUsers, legacyUser.ID),
		"expected legacy user in rebuilt index, got %v", legacyUsers)

	// ensureRoleUsersIndex is a no-op when the index already exists.
	if err := testMgr.ensureRoleUsersIndex(); err != nil {
		t.Fatal(err)
	}
	usersAfter, err := testMgr.loadRoleUsers("legacy-role")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Equal(usersAfter, legacyUsers), "index should remain unchanged on second ensure")
}

func TestEnsureRoleUsersIndexRebuildsPartial(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	realUser := &authn.User{
		ID:    "real-user",
		Roles: []*authn.Role{{Name: "real-role", ClusterACLs: []*authn.CluACL{{ID: "clu", Access: apc.AccessRO}}}},
	}
	if _, err := testMgr.db.Set(usersCollection, realUser.ID, realUser); err != nil {
		t.Fatal(err)
	}

	// Simulate a crashed rebuild: a stale index entry referencing a user that no
	// longer exists, and a missing version marker.
	if _, err := testMgr.saveRoleUsers("ghost-role", []string{"ghost-user"}); err != nil {
		t.Fatal(err)
	}
	if _, err := testMgr.db.Delete(metaCollection, roleUsersIndexMetaKey); err != nil {
		t.Fatal(err)
	}

	if err := testMgr.ensureRoleUsersIndex(); err != nil {
		t.Fatal(err)
	}

	// Stale entry from the partial build must be gone.
	ghost, err := testMgr.loadRoleUsers("ghost-role")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(ghost) == 0, "expected stale ghost-role entry to be discarded, got %v", ghost)

	// The real user must be indexed and the marker restored.
	realUsers, err := testMgr.loadRoleUsers("real-role")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(realUsers, realUser.ID),
		"expected real user in rebuilt index, got %v", realUsers)

	meta := &roleUsersIndexMeta{}
	code, err := testMgr.db.Get(metaCollection, roleUsersIndexMetaKey, meta)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK && meta.Version == roleUsersIndexVersion,
		"expected version marker %d to be set, got %d (code %d)", roleUsersIndexVersion, meta.Version, code)
}

func TestPropagateRoleToUsers(t *testing.T) {
	const (
		adminPass = "admin-pass"
		roleName  = "test-role"
		userName  = "role-user"
		userPass  = "role-pass"
		clusterID = "clu-propagation"
	)
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	role := &authn.Role{
		Name:        roleName,
		Description: "initial",
		ClusterACLs: []*authn.CluACL{{ID: clusterID, Access: apc.AccessRO}},
	}
	if _, err := testMgr.addRole(role); err != nil {
		t.Fatal(err)
	}
	if _, err := testMgr.addUser(&authn.User{
		ID:       userName,
		Password: userPass,
		Roles:    []*authn.Role{{Name: roleName}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := testMgr.db.Set(clustersCollection, clusterID, &authn.CluACL{ID: clusterID}); err != nil {
		t.Fatal(err)
	}

	t.Run("GetFailure", func(t *testing.T) {
		origDB := testMgr.db
		testMgr.db = &failGetCollectionDB{Driver: origDB, collection: usersCollection}
		_, err := testMgr.updateRole(roleName, &authn.Role{
			ClusterACLs: []*authn.CluACL{{ID: clusterID, Access: apc.AccessRW}},
		})
		testMgr.db = origDB

		tassert.Errorf(t, err != nil, "expected updateRole to fail")
		roleUsers, err := testMgr.loadRoleUsers(roleName)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, slices.Contains(roleUsers, userName), "expected %q in role index for %q", userName, roleName)

		uInfo, _, err := testMgr.lookupUser(userName)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, len(uInfo.Roles) == 1, "expected one role on user, got %d", len(uInfo.Roles))
		tassert.Errorf(t, uInfo.Roles[0].Name == roleName, "expected user role %q, got %q", roleName, uInfo.Roles[0].Name)
		tassert.Errorf(t, uInfo.Roles[0].ClusterACLs[0].Access == apc.AccessRO,
			"expected original RO access, got %s", uInfo.Roles[0].ClusterACLs[0].Access)
		rInfo, _, err := testMgr.lookupRole(roleName)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, rInfo.ClusterACLs[0].Access == apc.AccessRW,
			"expected canonical role to keep updated RW access after failed propagation, got %s", rInfo.ClusterACLs[0].Access)
	})

	update := &authn.Role{
		ClusterACLs: []*authn.CluACL{{ID: clusterID, Access: apc.AccessRW}},
	}
	if _, err := testMgr.updateRole(roleName, update); err != nil {
		t.Fatal(err)
	}

	uInfo, _, err := testMgr.lookupUser(userName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(uInfo.Roles) == 1, "expected one role on user, got %d", len(uInfo.Roles))
	tassert.Errorf(t, uInfo.Roles[0].ClusterACLs[0].Access == apc.AccessRW,
		"expected propagated RW access, got %s", uInfo.Roles[0].ClusterACLs[0].Access)

	token, _, err := testMgr.issueToken(userName, userPass, &authn.LoginMsg{})
	tassert.CheckFatal(t, err)
	claims, err := testMgr.validateToken(t.Context(), token)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(claims.ClusterACLs) == 1, "expected one cluster ACL in token, got %d", len(claims.ClusterACLs))
	tassert.Errorf(t, claims.ClusterACLs[0].Access == apc.AccessRW,
		"expected RW access in token, got %s", claims.ClusterACLs[0].Access)

	roleUsers, err := testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(roleUsers, userName), "expected %q in role index for %q", userName, roleName)
}

func TestUpdateRoleBestEffortPartialPropagationFailure(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "partial-propagation-role"
		userOne  = "partial-user-one"
		userTwo  = "partial-user-two"
		cluster  = "partial-clu"
	)
	if _, err := testMgr.addRole(&authn.Role{
		Name:        roleName,
		ClusterACLs: []*authn.CluACL{{ID: cluster, Access: apc.AccessRO}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{userOne, userTwo} {
		if _, err := testMgr.addUser(&authn.User{
			ID:       uid,
			Password: "pass",
			Roles:    []*authn.Role{{Name: roleName}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	origDB := testMgr.db
	testMgr.db = &failSetKeyDB{Driver: origDB, collection: usersCollection, key: userTwo}
	_, err := testMgr.updateRole(roleName, &authn.Role{
		ClusterACLs: []*authn.CluACL{{ID: cluster, Access: apc.AccessRW}},
	})
	testMgr.db = origDB

	tassert.Errorf(t, err != nil, "expected updateRole to fail")
	rInfo, _, err := testMgr.lookupRole(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, rInfo.ClusterACLs[0].Access == apc.AccessRW,
		"expected canonical role to keep updated RW access, got %s", rInfo.ClusterACLs[0].Access)
	uInfo, _, err := testMgr.lookupUser(userOne)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, uInfo.Roles[0].ClusterACLs[0].Access == apc.AccessRW,
		"expected %q role update to succeed best-effort, got %s", userOne, uInfo.Roles[0].ClusterACLs[0].Access)
	uInfo, _, err = testMgr.lookupUser(userTwo)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, uInfo.Roles[0].ClusterACLs[0].Access == apc.AccessRO,
		"expected %q failed write to keep old RO access, got %s", userTwo, uInfo.Roles[0].ClusterACLs[0].Access)
}

func TestRemoveRoleFromAllUsers(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "doomed-role"
		userOne  = "user-one"
		userTwo  = "user-two"
	)
	role := &authn.Role{Name: roleName, ClusterACLs: []*authn.CluACL{{ID: "clu", Access: apc.AccessRO}}}
	if _, err := testMgr.addRole(role); err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{userOne, userTwo} {
		if _, err := testMgr.addUser(&authn.User{
			ID:       uid,
			Password: "pass",
			Roles:    []*authn.Role{{Name: roleName}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("GetFailure", func(t *testing.T) {
		origDB := testMgr.db
		testMgr.db = &failGetCollectionDB{Driver: origDB, collection: usersCollection}
		_, err := testMgr.removeRoleFromAllUsers(roleName)
		testMgr.db = origDB

		tassert.Errorf(t, err != nil, "expected removeRoleFromAllUsers to fail")
		users, err := testMgr.loadRoleUsers(roleName)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, slices.Contains(users, userOne), "expected %q in role index for %q", userOne, roleName)
		tassert.Errorf(t, slices.Contains(users, userTwo), "expected %q in role index for %q", userTwo, roleName)
		for _, uid := range []string{userOne, userTwo} {
			uInfo, _, err := testMgr.lookupUser(uid)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, len(uInfo.Roles) == 1, "expected %q to keep one role, got %d", uid, len(uInfo.Roles))
			tassert.Errorf(t, uInfo.Roles[0].Name == roleName, "expected %q to keep role %q, got %q",
				uid, roleName, uInfo.Roles[0].Name)
		}
	})

	if _, err := testMgr.removeRoleFromAllUsers(roleName); err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{userOne, userTwo} {
		uInfo, _, err := testMgr.lookupUser(uid)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, len(uInfo.Roles) == 0, "expected %q to have no roles, got %d", uid, len(uInfo.Roles))
	}
	users, err := testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, users == nil, "expected index entry removed, got %v", users)
}

func TestRoleIndexUserLifecycle(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleA    = "lifecycle-a"
		roleB    = "lifecycle-b"
		userID   = "lifecycle-user"
		userPass = "lifecycle-pass"
	)
	for _, spec := range []*authn.Role{
		{Name: roleA, ClusterACLs: []*authn.CluACL{{ID: "clu-a", Access: apc.AccessRO}}},
		{Name: roleB, ClusterACLs: []*authn.CluACL{{ID: "clu-b", Access: apc.AccessRW}}},
	} {
		if _, err := testMgr.addRole(spec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := testMgr.addUser(&authn.User{
		ID:       userID,
		Password: userPass,
		Roles:    []*authn.Role{{Name: roleA}},
	}); err != nil {
		t.Fatal(err)
	}
	usersA, err := testMgr.loadRoleUsers(roleA)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(usersA, userID), "expected user in %q index after add", roleA)

	if _, err := testMgr.updateUser(userID, &authn.User{Roles: []*authn.Role{{Name: roleB}}}); err != nil {
		t.Fatal(err)
	}
	usersA, err = testMgr.loadRoleUsers(roleA)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !slices.Contains(usersA, userID), "expected user removed from %q index after update", roleA)
	usersB, err := testMgr.loadRoleUsers(roleB)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, slices.Contains(usersB, userID), "expected user in %q index after update", roleB)

	if _, err := testMgr.delUser(userID); err != nil {
		t.Fatal(err)
	}
	usersB, err = testMgr.loadRoleUsers(roleB)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !slices.Contains(usersB, userID), "expected user removed from %q index after delete", roleB)
}

func TestDelRoleRemovesFromUsers(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "delete-me"
		userID   = "role-holder"
	)
	if _, err := testMgr.addRole(&authn.Role{
		Name:        roleName,
		ClusterACLs: []*authn.CluACL{{ID: "clu", Access: apc.AccessRO}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := testMgr.addUser(&authn.User{
		ID:       userID,
		Password: "pass",
		Roles:    []*authn.Role{{Name: roleName}},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := testMgr.delRole(roleName); err != nil {
		t.Fatal(err)
	}
	if _, _, err := testMgr.lookupRole(roleName); err == nil {
		t.Fatal("expected role to be deleted")
	}
	uInfo, _, err := testMgr.lookupUser(userID)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(uInfo.Roles) == 0, "expected role stripped from user, got %v", uInfo.Roles)
}

func TestAddUserDropsMissingRoles(t *testing.T) {
	const adminPass = "admin-pass"
	t.Setenv(env.AisAuthAdminPassword, adminPass)
	testMgr := newMgrWithConf(t, &authn.Config{
		Server: authn.ServerConf{Secret: "test-secret", Expire: cos.Duration(time.Hour)},
	})

	const (
		roleName = "missing-role"
		userID   = "missing-role-holder"
	)
	if _, err := testMgr.addUser(&authn.User{
		ID:       userID,
		Password: "pass",
		Roles:    []*authn.Role{{Name: roleName, ClusterACLs: []*authn.CluACL{{ID: "clu", Access: apc.AccessRO}}}},
	}); err != nil {
		t.Fatal(err)
	}

	uInfo, _, err := testMgr.lookupUser(userID)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(uInfo.Roles) == 0, "expected role missing from collection to be dropped, got %v", uInfo.Roles)
	users, err := testMgr.loadRoleUsers(roleName)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !slices.Contains(users, userID), "expected %q not indexed for missing role %q, got %v", userID, roleName, users)
}

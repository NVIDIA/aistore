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

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type roleUsersEntry struct {
	UserIDs []string `json:"users"`
}

// roleUsersIndexVersion should be manually bumped whenever the index layout/semantics change
const roleUsersIndexVersion = 1

// roleUsersIndexMetaKey identifies the role-to-users index marker within metaCollection.
const roleUsersIndexMetaKey = "role_user_index"

type roleUsersIndexMeta struct {
	Version int `json:"version"`
}

func roleNames(roles []*authn.Role) []string {
	names := make([]string, 0, len(roles))
	for _, r := range roles {
		if r != nil && r.Name != "" {
			names = append(names, r.Name)
		}
	}
	return names
}

func cloneRole(r *authn.Role) *authn.Role {
	c := &authn.Role{}
	cos.MustMarshalFromString(cos.MustMarshalToString(r), c)
	return c
}

// fetchUserRoles resolves each role reference against the roles collection (the source of truth).
func (m *mgr) fetchUserRoles(roles []*authn.Role) ([]*authn.Role, int, error) {
	resolved := make([]*authn.Role, 0, len(roles))
	for _, r := range roles {
		if r == nil || r.Name == "" {
			continue
		}
		rInfo, code, err := m.lookupRole(r.Name)
		if err == nil {
			resolved = append(resolved, rInfo)
			continue
		}
		if code == http.StatusNotFound {
			continue
		}
		return nil, code, err
	}
	return resolved, http.StatusOK, nil
}

func (m *mgr) loadRoleUsers(roleName string) ([]string, error) {
	entry := &roleUsersEntry{}
	code, err := m.db.Get(roleUsersCollection, roleName, entry)
	if err != nil {
		if code == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	return entry.UserIDs, nil
}

func (m *mgr) saveRoleUsers(roleName string, userIDs []string) (int, error) {
	if len(userIDs) == 0 {
		return m.db.Delete(roleUsersCollection, roleName)
	}
	slices.Sort(userIDs)
	userIDs = slices.Compact(userIDs)
	return m.db.Set(roleUsersCollection, roleName, &roleUsersEntry{UserIDs: userIDs})
}

func (m *mgr) addUserToRoleIndex(roleName, userID string) error {
	users, err := m.loadRoleUsers(roleName)
	if err != nil {
		return err
	}
	if slices.Contains(users, userID) {
		return nil
	}
	users = append(users, userID)
	_, err = m.saveRoleUsers(roleName, users)
	return err
}

func (m *mgr) removeUserFromRoleIndex(roleName, userID string) error {
	users, err := m.loadRoleUsers(roleName)
	if err != nil {
		return err
	}
	idx := slices.Index(users, userID)
	if idx < 0 {
		return nil
	}
	users = slices.Delete(users, idx, idx+1)
	_, err = m.saveRoleUsers(roleName, users)
	return err
}

// syncUserRoleIndex adds the user to the role index for each role.
func (m *mgr) syncUserRoleIndex(userID string, roles []*authn.Role) error {
	for _, name := range roleNames(roles) {
		if err := m.addUserToRoleIndex(name, userID); err != nil {
			return err
		}
	}
	return nil
}

func (m *mgr) updateUserRoleIndex(userID string, oldRoles, newRoles []*authn.Role) error {
	oldSet := roleNames(oldRoles)
	newSet := roleNames(newRoles)

	for _, name := range oldSet {
		if !slices.Contains(newSet, name) {
			if err := m.removeUserFromRoleIndex(name, userID); err != nil {
				return err
			}
		}
	}
	for _, name := range newSet {
		if !slices.Contains(oldSet, name) {
			if err := m.addUserToRoleIndex(name, userID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *mgr) clearUserFromRoleIndex(userID string, roles []*authn.Role) error {
	for _, name := range roleNames(roles) {
		if err := m.removeUserFromRoleIndex(name, userID); err != nil {
			return err
		}
	}
	return nil
}

// syncRoleChangeToUsers walks the role index and applies apply to each user's roles.
// apply returns the updated role list and whether the user record changed.
func (m *mgr) syncRoleChangeToUsers(roleName, action string, apply func([]*authn.Role) ([]*authn.Role, bool)) (int, error) {
	userIDs, err := m.loadRoleUsers(roleName)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	var updateErrs []error
	for _, uid := range userIDs {
		uInfo := &authn.User{}
		code, err := m.db.Get(usersCollection, uid, uInfo)
		if err != nil {
			if code == http.StatusNotFound {
				nlog.Warningf("%s role %q: stale index entry for user %q", action, roleName, uid)
				_ = m.removeUserFromRoleIndex(roleName, uid)
				continue
			}
			nlog.Errorf("%s role %q for user %q: %v", action, roleName, uid, err)
			updateErrs = append(updateErrs, fmt.Errorf("%s role %q for user %q: %w", action, roleName, uid, err))
			continue
		}
		newRoles, updated := apply(uInfo.Roles)
		if !updated {
			continue
		}
		uInfo.Roles = newRoles
		if _, err = m.db.Set(usersCollection, uid, uInfo); err != nil {
			nlog.Errorf("%s role %q for user %q: %v", action, roleName, uid, err)
			updateErrs = append(updateErrs, fmt.Errorf("%s role %q for user %q: %w", action, roleName, uid, err))
		}
	}
	if len(updateErrs) > 0 {
		return http.StatusInternalServerError, errors.Join(updateErrs...)
	}
	return http.StatusOK, nil
}

// propagateRoleToUsers copies the updated role into every user that references it.
func (m *mgr) propagateRoleToUsers(roleName string, rInfo *authn.Role) (int, error) {
	return m.syncRoleChangeToUsers(roleName, "propagate", func(roles []*authn.Role) ([]*authn.Role, bool) {
		updated := false
		for i, r := range roles {
			if r != nil && r.Name == roleName {
				roles[i] = cloneRole(rInfo)
				updated = true
			}
		}
		return roles, updated
	})
}

func (m *mgr) removeRoleFromAllUsers(roleName string) (int, error) {
	code, err := m.syncRoleChangeToUsers(roleName, "remove", func(roles []*authn.Role) ([]*authn.Role, bool) {
		newRoles := make([]*authn.Role, 0, len(roles))
		updated := false
		for _, r := range roles {
			if r == nil || r.Name != roleName {
				newRoles = append(newRoles, r)
			} else {
				updated = true
			}
		}
		return newRoles, updated
	})
	if err != nil {
		return code, err
	}
	if code, err := m.db.Delete(roleUsersCollection, roleName); err != nil && code != http.StatusNotFound {
		nlog.Errorf("delete role %q users index: %v", roleName, err)
		return code, err
	}
	return http.StatusOK, nil
}

// rebuildRoleUsersIndex recomputes the entire role-to-users index from the users
// collection. It wipes any pre-existing (possibly partial) index first and
// writes the version marker last, so that a crash or error mid-rebuild leaves no
// marker and forces a clean retry on the next startup.
func (m *mgr) rebuildRoleUsersIndex() error {
	if _, err := m.db.DeleteCollection(roleUsersCollection); err != nil {
		return err
	}
	users, _, err := m.userList()
	if err != nil {
		return err
	}
	index := make(map[string][]string)
	for uid, uInfo := range users {
		for _, name := range roleNames(uInfo.Roles) {
			index[name] = append(index[name], uid)
		}
	}
	for roleName, userIDs := range index {
		if _, err := m.saveRoleUsers(roleName, userIDs); err != nil {
			return err
		}
	}
	_, err = m.db.Set(metaCollection, roleUsersIndexMetaKey, &roleUsersIndexMeta{Version: roleUsersIndexVersion})
	return err
}

// ensureRoleUsersIndex builds the role-to-users index from existing users unless
// a current version marker proves a previous build completed successfully. A
// missing or stale marker (e.g., after an upgrade or a crash mid-rebuild) forces
// a rebuild.
func (m *mgr) ensureRoleUsersIndex() error {
	meta := &roleUsersIndexMeta{}
	code, err := m.db.Get(metaCollection, roleUsersIndexMetaKey, meta)
	if err != nil && code != http.StatusNotFound {
		return err
	}
	if err == nil && meta.Version >= roleUsersIndexVersion {
		return nil
	}
	nlog.Infoln("building role-to-users index from existing users...")
	return m.rebuildRoleUsersIndex()
}

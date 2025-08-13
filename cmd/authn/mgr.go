// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"

	jsoniter "github.com/json-iterator/go"
	"golang.org/x/crypto/bcrypt"
)

type mgr struct {
	clientH   *http.Client
	clientTLS *http.Client
	db        kvdb.Driver
}

var (
	errInvalidCredentials = errors.New("invalid credentials")

	predefinedRoles = []struct {
		prefix string
		desc   string
		perms  apc.AccessAttrs
	}{
		{ClusterOwnerRole, "Admin access to %s", apc.AccessAll},
		{BucketOwnerRole, "Full access to buckets in %s", apc.AccessRW},
		{GuestRole, "Read-only access to buckets in %s", apc.AccessRO},
	}
)

// If user DB exists, loads the data from the file and decrypts passwords
func newMgr(driver kvdb.Driver) (m *mgr, code int, err error) {
	m = &mgr{
		db: driver,
	}
	m.clientH, m.clientTLS = cmn.NewDefaultClients(time.Duration(Conf.Timeout.Default))
	code, err = initializeDB(driver)
	return
}

func (*mgr) String() string { return svcName }

//
// users ============================================================
//

// Registers a new user. It is info from a user, so the password
// is not encrypted and a few fields are not filled(e.g, Access).
func (m *mgr) addUser(info *authn.User) (int, error) {
	if info.ID == "" || info.Password == "" {
		return http.StatusBadRequest, errInvalidCredentials
	}
	// Validate user ID
	if !cos.IsAlphaNice(info.ID) {
		return http.StatusBadRequest, fmt.Errorf("user ID %q is invalid: %s", info.ID, cos.OnlyNice)
	}
	_, _, err := m.db.GetString(usersCollection, info.ID)
	if err == nil {
		return http.StatusConflict, cos.NewErrAlreadyExists(m, "user "+info.ID)
	}
	info.Password = encryptPassword(info.Password)
	return m.db.Set(usersCollection, info.ID, info)
}

// Deletes an existing user
func (m *mgr) delUser(userID string) (int, error) {
	if userID == adminUserID {
		return http.StatusForbidden, fmt.Errorf("cannot remove built-in %q account", adminUserID)
	}
	return m.db.Delete(usersCollection, userID)
}

// Updates an existing user. The function invalidates user tokens after
// successful update.
func (m *mgr) updateUser(userID string, updateReq *authn.User) (int, error) {
	uInfo := &authn.User{}
	code, err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return code, err
	}
	if userID == adminUserID && len(updateReq.Roles) != 0 {
		return http.StatusForbidden, errors.New("cannot change administrator's role")
	}

	if updateReq.Password != "" {
		uInfo.Password = encryptPassword(updateReq.Password)
	}
	if len(updateReq.Roles) != 0 {
		uInfo.Roles = updateReq.Roles
	}
	return m.db.Set(usersCollection, userID, uInfo)
}

func (m *mgr) lookupUser(userID string) (*authn.User, int, error) {
	uInfo := &authn.User{}
	code, err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return nil, code, err
	}
	return uInfo, http.StatusOK, nil
}

func (m *mgr) userList() (map[string]*authn.User, int, error) {
	recs, code, err := m.db.GetAll(usersCollection, "")
	if err != nil {
		return nil, code, err
	}
	users := make(map[string]*authn.User, 4)
	for _, str := range recs {
		uInfo := &authn.User{}
		err := jsoniter.Unmarshal([]byte(str), uInfo)
		cos.AssertNoErr(err)
		users[uInfo.ID] = uInfo
	}
	return users, http.StatusOK, nil
}

//
// roles ============================================================
//

// Registers a new role
func (m *mgr) addRole(info *authn.Role) (int, error) {
	if info.Name == "" {
		return http.StatusBadRequest, errors.New("role name is undefined")
	}
	// Validate role name
	if !cos.IsAlphaNice(info.Name) {
		return http.StatusBadRequest, fmt.Errorf("role name %q is invalid: %s", info.Name, cos.OnlyNice)
	}
	if info.IsAdmin {
		return http.StatusForbidden, fmt.Errorf("only built-in roles can have %q permissions", adminUserID)
	}
	_, _, err := m.db.GetString(rolesCollection, info.Name)
	if err == nil {
		return http.StatusConflict, cos.NewErrAlreadyExists(m, "role "+info.Name)
	}
	return m.db.Set(rolesCollection, info.Name, info)
}

// Deletes an existing role
func (m *mgr) delRole(role string) (int, error) {
	if role == authn.AdminRole {
		return http.StatusForbidden, fmt.Errorf("cannot remove built-in %q role", authn.AdminRole)
	}
	return m.db.Delete(rolesCollection, role)
}

// Updates an existing role
func (m *mgr) updateRole(role string, updateReq *authn.Role) (int, error) {
	if role == authn.AdminRole {
		return http.StatusForbidden, fmt.Errorf("cannot modify built-in %q role", authn.AdminRole)
	}
	rInfo := &authn.Role{}
	code, err := m.db.Get(rolesCollection, role, rInfo)
	if err != nil {
		return code, err
	}

	if updateReq.Description != "" {
		rInfo.Description = updateReq.Description
	}
	rInfo.ClusterACLs = mergeClusterACLs(rInfo.ClusterACLs, updateReq.ClusterACLs, "")
	rInfo.BucketACLs = mergeBckACLs(rInfo.BucketACLs, updateReq.BucketACLs, "")

	return m.db.Set(rolesCollection, role, rInfo)
}

func (m *mgr) lookupRole(roleID string) (*authn.Role, int, error) {
	rInfo := &authn.Role{}
	code, err := m.db.Get(rolesCollection, roleID, rInfo)
	if err != nil {
		return nil, code, err
	}
	return rInfo, http.StatusOK, nil
}

func (m *mgr) roleList() ([]*authn.Role, int, error) {
	recs, code, err := m.db.GetAll(rolesCollection, "")
	if err != nil {
		return nil, code, err
	}
	roles := make([]*authn.Role, 0, len(recs))
	for _, str := range recs {
		role := &authn.Role{}
		err := jsoniter.Unmarshal([]byte(str), role)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		roles = append(roles, role)
	}
	return roles, http.StatusOK, nil
}

// Creates predefined roles for just added clusters. Errors are logged and
// are not returned to a caller as it is not crucial.
func (m *mgr) createRolesForCluster(clu *authn.CluACL) (int, error) {
	for _, pr := range predefinedRoles {
		suffix := cos.Left(clu.Alias, clu.ID)
		uid := pr.prefix + "-" + suffix
		rInfo := &authn.Role{}
		if _, err := m.db.Get(rolesCollection, uid, rInfo); err == nil {
			continue
		}
		rInfo.Name = uid
		cluName := clu.ID
		if clu.Alias != "" {
			cluName += "[" + clu.Alias + "]"
		}
		rInfo.Description = fmt.Sprintf(pr.desc, cluName)
		rInfo.ClusterACLs = []*authn.CluACL{
			{ID: clu.ID, Access: pr.perms},
		}
		if code, err := m.db.Set(rolesCollection, uid, rInfo); err != nil {
			return code, cmn.NewErrFailedTo(m, "create", "role", err)
		}
	}
	return http.StatusOK, nil
}

//
// clusters ============================================================
//

func (m *mgr) clus() (map[string]*authn.CluACL, int, error) {
	clusters, code, err := m.db.GetAll(clustersCollection, "")
	if err != nil {
		return nil, code, err
	}
	clus := make(map[string]*authn.CluACL, len(clusters))
	for cid, s := range clusters {
		cInfo := &authn.CluACL{}
		if err := jsoniter.Unmarshal([]byte(s), cInfo); err != nil {
			nlog.Errorf("Failed to parse cluster %s info: %v", cid, err)
			continue
		}
		clus[cInfo.ID] = cInfo
	}
	return clus, http.StatusOK, nil
}

// Returns a cluster ID which ID or Alias equal cluID or cluAlias.
func (m *mgr) cluLookup(cluID, cluAlias string) string {
	clus, _, err := m.clus()
	if err != nil {
		return ""
	}
	for cid, cInfo := range clus {
		if cid != "" && (cid == cluID || cid == cluAlias) {
			return cid
		}
		if cluAlias == "" {
			continue
		}
		if cInfo.ID == cluAlias || cInfo.Alias == cluAlias {
			return cid
		}
	}
	return ""
}

// Get an existing cluster
func (m *mgr) getCluster(cluID string) (*authn.CluACL, int, error) {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return nil, http.StatusNotFound, cos.NewErrNotFound(m, "cluster "+cluID)
	}
	clu := &authn.CluACL{}
	code, err := m.db.Get(clustersCollection, cid, clu)
	return clu, code, err
}

// Registers a new cluster
func (m *mgr) addCluster(clu *authn.CluACL) (int, error) {
	if clu.ID == "" {
		return http.StatusBadRequest, errors.New("cluster UUID is undefined")
	}
	// Validate cluster alias
	if !cos.IsAlphaNice(clu.Alias) {
		return http.StatusBadRequest, fmt.Errorf("cluster alias %q is invalid: %s", clu.Alias, cos.OnlyNice)
	}
	cid := m.cluLookup(clu.ID, clu.Alias)
	if cid != "" {
		return http.StatusConflict, fmt.Errorf("cluster %s[%s] already registered", clu.ID, cid)
	}

	// secret handshake
	if err := m.validateSecret(clu); err != nil {
		return http.StatusInternalServerError, err
	}

	if code, err := m.db.Set(clustersCollection, clu.ID, clu); err != nil {
		return code, err
	}
	m.createRolesForCluster(clu)

	go m.syncTokenList(clu)
	return http.StatusOK, nil
}

func (m *mgr) updateCluster(cluID string, info *authn.CluACL) (int, error) {
	if info.ID == "" {
		return http.StatusBadRequest, errors.New("cluster ID is undefined")
	}
	clu := &authn.CluACL{}
	if code, err := m.db.Get(clustersCollection, cluID, clu); err != nil {
		return code, err
	}
	if info.Alias != "" {
		// Validate cluster alias if user is changing it
		if !cos.IsAlphaNice(info.Alias) {
			return http.StatusBadRequest, fmt.Errorf("cluster alias %q is invalid: %s", info.Alias, cos.OnlyNice)
		}
		cid := m.cluLookup("", info.Alias)
		if cid != "" && cid != clu.ID {
			return http.StatusConflict, fmt.Errorf("alias %q is used for cluster %q", info.Alias, cid)
		}
		clu.Alias = info.Alias
	}
	if len(info.URLs) != 0 {
		clu.URLs = info.URLs
	}

	// secret handshake
	if err := m.validateSecret(clu); err != nil {
		return http.StatusInternalServerError, err
	}

	return m.db.Set(clustersCollection, cluID, clu)
}

// Unregister an existing cluster
func (m *mgr) delCluster(cluID string) (int, error) {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return http.StatusNotFound, cos.NewErrNotFound(m, "cluster "+cluID)
	}
	return m.db.Delete(clustersCollection, cid)
}

//
// tokens ============================================================
//

// Generates a token for a user if user credentials are valid. If the token is
// already generated and is not expired yet the existing token is returned.
// Token includes user ID, permissions, and token expiration time.
// If a new token was generated then it sends the proxy a new valid token list
func (m *mgr) issueToken(uid, pwd string, msg *authn.LoginMsg) (token string, code int, err error) {
	var (
		uInfo   = &authn.User{}
		cid     string
		cluACLs []*authn.CluACL
		bckACLs []*authn.BckACL
	)
	_, err = m.db.Get(usersCollection, uid, uInfo)
	if err != nil {
		nlog.Errorln(err)
		return "", http.StatusUnauthorized, errInvalidCredentials
	}

	debug.Assert(uid == uInfo.ID, uid, " vs ", uInfo.ID)

	if !isSamePassword(pwd, uInfo.Password) {
		return "", http.StatusUnauthorized, errInvalidCredentials
	}

	// update ACLs with roles' ones
	for _, role := range uInfo.Roles {
		cluACLs = mergeClusterACLs(cluACLs, role.ClusterACLs, cid)
		bckACLs = mergeBckACLs(bckACLs, role.BucketACLs, cid)
	}

	// generate token
	token, err = m._token(msg, uInfo, cluACLs, bckACLs)
	if err != nil {
		return "", http.StatusInternalServerError, err
	}
	return token, http.StatusOK, nil
}

func (m *mgr) _token(msg *authn.LoginMsg, uInfo *authn.User, cluACLs []*authn.CluACL, bckACLs []*authn.BckACL) (token string, err error) {
	expDelta := Conf.Expire()
	if msg.ExpiresIn != nil {
		expDelta = *msg.ExpiresIn
	}
	if expDelta == 0 {
		expDelta = foreverTokenTime
	}

	// put all useful info into token: who owns the token, when it was issued,
	// when it expires and credentials to log in AWS, GCP etc.
	// If a user is a super user, it is enough to pass only isAdmin marker
	expires := time.Now().Add(expDelta)
	uid := uInfo.ID
	if uInfo.IsAdmin() {
		token, err = tok.AdminJWT(expires, uid, Conf.Secret())
	} else {
		m.fixClusterIDs(cluACLs)
		token, err = tok.JWT(expires, uid, bckACLs, cluACLs, Conf.Secret())
	}
	return token, err
}

// Before putting a list of cluster permissions to a token, cluster aliases
// must be replaced with their IDs.
func (m *mgr) fixClusterIDs(lst []*authn.CluACL) {
	clus, _, err := m.clus()
	if err != nil {
		return
	}
	for _, cInfo := range lst {
		if _, ok := clus[cInfo.ID]; ok {
			continue
		}
		for _, clu := range clus {
			if clu.Alias == cInfo.ID {
				cInfo.ID = clu.ID
			}
		}
	}
}

// Delete existing token, a.k.a log out
// If the token was removed successfully then it sends the proxy a new valid token list
func (m *mgr) revokeToken(token string) (int, error) {
	code, err := m.db.Set(revokedCollection, token, "!")
	if err != nil {
		return code, err
	}

	// send the token in all case to allow an admin to revoke
	// an existing token even after cluster restart
	go m.broadcastRevoked(token)
	return http.StatusOK, nil
}

// Create a list of non-expired and valid revoked tokens.
// Obsolete and invalid tokens are removed from the database.
func (m *mgr) generateRevokedTokenList() ([]string, int, error) {
	tokens, code, err := m.db.List(revokedCollection, "")
	if err != nil {
		debug.AssertNoErr(err)
		return nil, code, err
	}

	now := time.Now()
	revokeList := make([]string, 0, len(tokens))
	secret := Conf.Secret()
	for _, token := range tokens {
		tk, err := tok.DecryptToken(token, secret)
		if err != nil {
			m.db.Delete(revokedCollection, token)
			continue
		}
		if tk.Expires.Before(now) {
			nlog.Infof("removing %s", tk)
			m.db.Delete(revokedCollection, token)
			continue
		}
		revokeList = append(revokeList, token)
	}
	return revokeList, http.StatusOK, nil
}

//
// private helpers ============================================================
//

func encryptPassword(password string) string {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	cos.AssertNoErr(err)
	return hex.EncodeToString(b)
}

func isSamePassword(password, hashed string) bool {
	b, err := hex.DecodeString(hashed)
	if err != nil {
		return false
	}
	return bcrypt.CompareHashAndPassword(b, []byte(password)) == nil
}

// If the DB is empty, the function prefills some data
func initializeDB(driver kvdb.Driver) (int, error) {
	users, code, err := driver.List(usersCollection, "")
	if err != nil || len(users) != 0 {
		// Return on errors or when DB is already initialized
		return code, err
	}

	// Create the admin role
	role := &authn.Role{
		Name:        authn.AdminRole,
		Description: "AuthN administrator",
		IsAdmin:     true,
	}

	if code, err := driver.Set(rolesCollection, authn.AdminRole, role); err != nil {
		return code, err
	}

	// environment override
	userName := cos.Right(adminUserID, os.Getenv(env.AisAuthAdminUsername))
	password, exists := os.LookupEnv(env.AisAuthAdminPassword)
	if !exists || password == "" {
		return 0, fmt.Errorf("failed to initialize DB, no password provided for admin user. Set with %q", env.AisAuthAdminPassword)
	}

	// Create the admin user
	su := &authn.User{
		ID:       userName,
		Password: encryptPassword(password),
		Roles:    []*authn.Role{role},
	}

	return driver.Set(usersCollection, userName, su)
}

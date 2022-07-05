// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/dbdriver"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/crypto/bcrypt"
)

type mgr struct {
	clientHTTP  *http.Client
	clientHTTPS *http.Client
	db          dbdriver.Driver
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
func newMgr(driver dbdriver.Driver) (*mgr, error) {
	timeout := time.Duration(Conf.Timeout.Default)
	clientHTTP := cmn.NewClient(cmn.TransportArgs{Timeout: timeout})
	clientHTTPS := cmn.NewClient(cmn.TransportArgs{
		Timeout:    timeout,
		UseHTTPS:   true,
		SkipVerify: true,
	})
	mgr := &mgr{
		clientHTTP:  clientHTTP,
		clientHTTPS: clientHTTPS,
		db:          driver,
	}
	err := initializeDB(driver)
	return mgr, err
}

//
// users ============================================================
//

// Registers a new user. It is info from a user, so the password
// is not encrypted and a few fields are not filled(e.g, Access).
func (m *mgr) addUser(info *authn.User) error {
	if info.ID == "" || info.Password == "" {
		return errInvalidCredentials
	}

	_, err := m.db.GetString(usersCollection, info.ID)
	if err == nil {
		return fmt.Errorf("user %q already registered", info.ID)
	}
	info.Password = encryptPassword(info.Password)
	return m.db.Set(usersCollection, info.ID, info)
}

// Deletes an existing user
func (m *mgr) delUser(userID string) error {
	if userID == adminUserID {
		return fmt.Errorf("cannot remove built-in %q account", adminUserID)
	}
	return m.db.Delete(usersCollection, userID)
}

// Updates an existing user. The function invalidates user tokens after
// successful update.
func (m *mgr) updateUser(userID string, updateReq *authn.User) error {
	uInfo := &authn.User{}
	err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return cmn.NewErrNotFound("%s: user %q", svcName, userID)
	}
	if userID == adminUserID && len(updateReq.Roles) != 0 {
		return errors.New("cannot change administrator's role")
	}

	if updateReq.Password != "" {
		uInfo.Password = encryptPassword(updateReq.Password)
	}
	if len(updateReq.Roles) != 0 {
		uInfo.Roles = updateReq.Roles
	}
	uInfo.ClusterACLs = MergeClusterACLs(uInfo.ClusterACLs, updateReq.ClusterACLs)
	uInfo.BucketACLs = MergeBckACLs(uInfo.BucketACLs, updateReq.BucketACLs)

	return m.db.Set(usersCollection, userID, uInfo)
}

func (m *mgr) lookupUser(userID string) (*authn.User, error) {
	uInfo := &authn.User{}
	err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return nil, err
	}

	// update ACLs with roles's ones
	for _, role := range uInfo.Roles {
		rInfo := &authn.Role{}
		err := m.db.Get(rolesCollection, role, rInfo)
		if err != nil {
			continue
		}
		uInfo.ClusterACLs = MergeClusterACLs(uInfo.ClusterACLs, rInfo.ClusterACLs)
		uInfo.BucketACLs = MergeBckACLs(uInfo.BucketACLs, rInfo.BucketACLs)
	}

	return uInfo, nil
}

func (m *mgr) userList() (map[string]*authn.User, error) {
	recs, err := m.db.GetAll(usersCollection, "")
	if err != nil {
		return nil, err
	}
	users := make(map[string]*authn.User, 4)
	for _, str := range recs {
		uInfo := &authn.User{}
		err := jsoniter.Unmarshal([]byte(str), uInfo)
		cos.AssertNoErr(err)
		users[uInfo.ID] = uInfo
	}
	return users, nil
}

//
// roles ============================================================
//

// Registers a new role
func (m *mgr) addRole(info *authn.Role) error {
	if info.ID == "" {
		return errors.New("role name is undefined")
	}
	if info.IsAdmin {
		return fmt.Errorf("only built-in roles can have %q permissions", adminUserID)
	}

	_, err := m.db.GetString(rolesCollection, info.ID)
	if err == nil {
		return fmt.Errorf("role %q already exists", info.ID)
	}
	return m.db.Set(rolesCollection, info.ID, info)
}

// Deletes an existing role
func (m *mgr) delRole(role string) error {
	if role == authn.AdminRole {
		return fmt.Errorf("cannot remove built-in %q role", authn.AdminRole)
	}
	return m.db.Delete(rolesCollection, role)
}

// Updates an existing role
func (m *mgr) updateRole(role string, updateReq *authn.Role) error {
	if role == authn.AdminRole {
		return fmt.Errorf("cannot modify built-in %q role", authn.AdminRole)
	}
	rInfo := &authn.Role{}
	err := m.db.Get(rolesCollection, role, rInfo)
	if err != nil {
		return cmn.NewErrNotFound("%s: role %q", svcName, role)
	}

	if updateReq.Desc != "" {
		rInfo.Desc = updateReq.Desc
	}
	if len(updateReq.Roles) != 0 {
		rInfo.Roles = updateReq.Roles
	}
	rInfo.ClusterACLs = MergeClusterACLs(rInfo.ClusterACLs, updateReq.ClusterACLs)
	rInfo.BucketACLs = MergeBckACLs(rInfo.BucketACLs, updateReq.BucketACLs)

	return m.db.Set(rolesCollection, role, rInfo)
}

func (m *mgr) lookupRole(roleID string) (*authn.Role, error) {
	rInfo := &authn.Role{}
	err := m.db.Get(rolesCollection, roleID, rInfo)
	if err != nil {
		return nil, err
	}
	return rInfo, nil
}

func (m *mgr) roleList() ([]*authn.Role, error) {
	recs, err := m.db.GetAll(rolesCollection, "")
	if err != nil {
		return nil, err
	}
	roles := make([]*authn.Role, 0, len(recs))
	for _, str := range recs {
		role := &authn.Role{}
		err := jsoniter.Unmarshal([]byte(str), role)
		if err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	return roles, nil
}

// Creates predefined roles for just added clusters. Errors are logged and
// are not returned to a caller as it is not crucial.
func (m *mgr) createRolesForCluster(clu *authn.CluACL) {
	for _, pr := range predefinedRoles {
		suffix := cos.Either(clu.Alias, clu.ID)
		uid := pr.prefix + "-" + suffix
		rInfo := &authn.Role{}
		if err := m.db.Get(rolesCollection, uid, rInfo); err == nil {
			continue
		}
		rInfo.ID = uid
		cluName := clu.ID
		if clu.Alias != "" {
			cluName += "[" + clu.Alias + "]"
		}
		rInfo.Desc = fmt.Sprintf(pr.desc, cluName)
		rInfo.ClusterACLs = []*authn.CluACL{
			{ID: clu.ID, Access: pr.perms},
		}
		if err := m.db.Set(rolesCollection, uid, rInfo); err != nil {
			glog.Errorf("Failed to create role %s: %v", uid, err)
		}
	}
}

//
// clusters ============================================================
//

func (m *mgr) clus() (map[string]*authn.CluACL, error) {
	clusters, err := m.db.GetAll(clustersCollection, "")
	if err != nil {
		return nil, err
	}
	clus := make(map[string]*authn.CluACL, len(clusters))
	for cid, s := range clusters {
		cInfo := &authn.CluACL{}
		if err := jsoniter.Unmarshal([]byte(s), cInfo); err != nil {
			glog.Errorf("Failed to parse cluster %s info: %v", cid, err)
			continue
		}
		clus[cInfo.ID] = cInfo
	}
	return clus, nil
}

// Returns a cluster ID which ID or Alias equal cluID or cluAlias.
func (m *mgr) cluLookup(cluID, cluAlias string) string {
	clus, err := m.clus()
	if err != nil {
		return ""
	}
	for cid, cInfo := range clus {
		if cid == cluID || cid == cluAlias {
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
func (m *mgr) getCluster(cluID string) (*authn.CluACL, error) {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return nil, cmn.NewErrNotFound("%s: cluster %q", svcName, cluID)
	}
	clu := &authn.CluACL{}
	err := m.db.Get(clustersCollection, cid, clu)
	return clu, err
}

// Registers a new cluster
func (m *mgr) addCluster(clu *authn.CluACL) error {
	if clu.ID == "" {
		return errors.New("cluster UUID is undefined")
	}

	cid := m.cluLookup(clu.ID, clu.Alias)
	if cid != "" {
		return fmt.Errorf("cluster %s[%s] already registered", clu.ID, cid)
	}

	// secret handshake
	if err := m.validateSecret(clu); err != nil {
		return err
	}

	if err := m.db.Set(clustersCollection, clu.ID, clu); err != nil {
		return err
	}
	m.createRolesForCluster(clu)

	go m.syncTokenList(clu)
	return nil
}

func (m *mgr) updateCluster(cluID string, info *authn.CluACL) error {
	if info.ID == "" {
		return errors.New("cluster ID is undefined")
	}
	clu := &authn.CluACL{}
	if err := m.db.Get(clustersCollection, cluID, clu); err != nil {
		return err
	}
	if info.Alias != "" {
		cid := m.cluLookup("", info.Alias)
		if cid != "" && cid != clu.ID {
			return fmt.Errorf("alias %q is used for cluster %q", info.Alias, cid)
		}
		clu.Alias = info.Alias
	}
	if len(info.URLs) != 0 {
		clu.URLs = info.URLs
	}

	// secret handshake
	if err := m.validateSecret(clu); err != nil {
		return err
	}

	return m.db.Set(clustersCollection, cluID, clu)
}

// Unregister an existing cluster
func (m *mgr) delCluster(cluID string) error {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return cmn.NewErrNotFound("%s: cluster %q", svcName, cluID)
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
func (m *mgr) issueToken(userID, pwd string, ttl *time.Duration) (string, error) {
	var (
		err     error
		expires time.Time
		token   string
		uInfo   = &authn.User{}
	)
	err = m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		glog.Error(err)
		return "", errInvalidCredentials
	}
	if !isSamePassword(pwd, uInfo.Password) {
		return "", errInvalidCredentials
	}

	// update ACLs with roles's ones
	for _, role := range uInfo.Roles {
		rInfo := &authn.Role{}
		err := m.db.Get(rolesCollection, role, rInfo)
		if err != nil {
			continue
		}
		uInfo.ClusterACLs = MergeClusterACLs(uInfo.ClusterACLs, rInfo.ClusterACLs)
		uInfo.BucketACLs = MergeBckACLs(uInfo.BucketACLs, rInfo.BucketACLs)
	}

	// generate token
	Conf.RLock()
	defer Conf.RUnlock()
	issued := time.Now()
	expDelta := time.Duration(Conf.Server.ExpirePeriod)
	if ttl != nil {
		expDelta = *ttl
	}
	if expDelta == 0 {
		expDelta = foreverTokenTime
	}
	expires = issued.Add(expDelta)

	// put all useful info into token: who owns the token, when it was issued,
	// when it expires and credentials to log in AWS, GCP etc.
	// If a user is a super user, it is enough to pass only isAdmin marker
	if uInfo.IsAdmin() {
		token, err = tok.IssueAdminJWT(expires, userID, Conf.Server.Secret)
	} else {
		m.fixClusterIDs(uInfo.ClusterACLs)
		token, err = tok.IssueJWT(expires, userID, uInfo.BucketACLs, uInfo.ClusterACLs, Conf.Server.Secret)
	}
	return token, err
}

// Before putting a list of cluster permissions to a token, cluster aliases
// must be replaced with their IDs.
func (m *mgr) fixClusterIDs(lst []*authn.CluACL) {
	clus, err := m.clus()
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
func (m *mgr) revokeToken(token string) error {
	err := m.db.Set(revokedCollection, token, "!")
	if err != nil {
		return err
	}

	// send the token in all case to allow an admin to revoke
	// an existing token even after cluster restart
	go m.broadcastRevoked(token)
	return nil
}

// Create a list of non-expired and valid revoked tokens.
// Obsolete and invalid tokens are removed from the database.
func (m *mgr) generateRevokedTokenList() ([]string, error) {
	tokens, err := m.db.List(revokedCollection, "")
	if err != nil {
		debug.AssertNoErr(err)
		return nil, err
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
			glog.Infof("removing %s", tk)
			m.db.Delete(revokedCollection, token)
			continue
		}
		revokeList = append(revokeList, token)
	}
	return revokeList, nil
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
func initializeDB(driver dbdriver.Driver) error {
	users, err := driver.List(usersCollection, "")
	if err != nil || len(users) != 0 {
		// return on erros or when DB is already initialized
		return err
	}

	role := &authn.Role{
		ID:      authn.AdminRole,
		Desc:    "AuthN administrator",
		IsAdmin: true,
	}
	if err := driver.Set(rolesCollection, authn.AdminRole, role); err != nil {
		return err
	}

	su := &authn.User{
		ID:       adminUserID,
		Password: encryptPassword(adminUserPass),
		Roles:    []string{authn.AdminRole},
	}
	return driver.Set(usersCollection, adminUserID, su)
}

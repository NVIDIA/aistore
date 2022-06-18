// Package authn - authorization server for AIStore.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 *
 */
package authn

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/golang-jwt/jwt/v4"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/crypto/bcrypt"
)

const (
	usersCollection    = "user"
	rolesCollection    = "role"
	revokedCollection  = "revoked"
	clustersCollection = "cluster"

	adminID   = "admin"
	adminPass = "admin"

	proxyTimeout     = 2 * time.Minute           // maximum time for syncing Authn data with primary proxy
	proxyRetryTime   = 5 * time.Second           // an interval between primary proxy detection attempts
	foreverTokenTime = 24 * 365 * 20 * time.Hour // kind of never-expired token
)

type (
	UserManager struct {
		clientHTTP  *http.Client
		clientHTTPS *http.Client
		db          dbdriver.Driver
	}
)

var (
	errInvalidCredentials = errors.New("invalid credentials")

	predefinedRoles = []struct {
		prefix string
		desc   string
		perms  apc.AccessAttrs
	}{
		{ClusterOwnerRole, "Full access to cluster %s", apc.AccessAll},
		{BucketOwnerRole, "Full access to buckets of cluster %s", apc.AccessRW},
		{GuestRole, "Read-only access to buckets of cluster %s", apc.AccessRO},
	}
)

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

	role := &Role{
		Name:    AdminRole,
		Desc:    "AuthN administrator",
		IsAdmin: true,
	}
	if err := driver.Set(rolesCollection, AdminRole, role); err != nil {
		return err
	}

	su := &User{
		ID:       adminID,
		Password: encryptPassword(adminPass),
		Roles:    []string{AdminRole},
	}
	return driver.Set(usersCollection, adminID, su)
}

// Creates a new user manager. If user DB exists, it loads the data from the
// file and decrypts passwords
func NewUserManager(driver dbdriver.Driver) (*UserManager, error) {
	timeout := time.Duration(Conf.Timeout.Default)
	clientHTTP := cmn.NewClient(cmn.TransportArgs{Timeout: timeout})
	clientHTTPS := cmn.NewClient(cmn.TransportArgs{
		Timeout:    timeout,
		UseHTTPS:   true,
		SkipVerify: true,
	})

	mgr := &UserManager{
		clientHTTP:  clientHTTP,
		clientHTTPS: clientHTTPS,
		db:          driver,
	}
	err := initializeDB(driver)
	return mgr, err
}

// Registers a new user. It is info from a user, so the password
// is not encrypted and a few fields are not filled(e.g, Access).
func (m *UserManager) addUser(info *User) error {
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

// Registers a new role
func (m *UserManager) addRole(info *Role) error {
	if info.Name == "" {
		return errors.New("role name is undefined")
	}
	if info.IsAdmin {
		return errors.New("only built-in roles can have administrator permissions")
	}

	_, err := m.db.GetString(rolesCollection, info.Name)
	if err == nil {
		return fmt.Errorf("role %q already exists", info.Name)
	}
	return m.db.Set(rolesCollection, info.Name, info)
}

func (m *UserManager) clusterList() (map[string]*Cluster, error) {
	clusters, err := m.db.GetAll(clustersCollection, "")
	if err != nil {
		return nil, err
	}
	cluList := make(map[string]*Cluster, len(clusters))
	for cid, s := range clusters {
		cInfo := &Cluster{}
		if err := jsoniter.Unmarshal([]byte(s), cInfo); err != nil {
			glog.Errorf("Failed to parse cluster %s info: %v", cid, err)
			continue
		}
		cluList[cInfo.ID] = cInfo
	}
	return cluList, nil
}

// Returns a cluster ID which ID or Alias equal cluID or cluAlias.
func (m *UserManager) cluLookup(cluID, cluAlias string) string {
	cluList, err := m.clusterList()
	if err != nil {
		return ""
	}
	for cid, cInfo := range cluList {
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
func (m *UserManager) getCluster(cluID string) (*Cluster, error) {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return nil, cmn.NewErrNotFound("user-manager: %s cluster %q", svcName, cluID)
	}
	clu := &Cluster{}
	err := m.db.Get(clustersCollection, cid, clu)
	return clu, err
}

// Registers a new cluster
func (m *UserManager) addCluster(info *Cluster) error {
	if info.ID == "" {
		return errors.New("cluster UUID is undefined")
	}

	cid := m.cluLookup(info.ID, info.Alias)
	if cid != "" {
		return fmt.Errorf("cluster %s[%s] already registered", info.ID, cid)
	}
	if err := m.db.Set(clustersCollection, info.ID, info); err != nil {
		return err
	}
	m.createRolesForCluster(info)

	go m.syncTokenList(info)
	return nil
}

// Deletes an existing user
func (m *UserManager) delUser(userID string) error {
	if userID == adminID {
		return errors.New("cannot remove built-in administrator account")
	}
	return m.db.Delete(usersCollection, userID)
}

// Unregister an existing cluster
func (m *UserManager) delCluster(cluID string) error {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return cmn.NewErrNotFound("user-manager: %s cluster %q", svcName, cluID)
	}
	return m.db.Delete(clustersCollection, cid)
}

// Deletes an existing role
func (m *UserManager) delRole(role string) error {
	if role == AdminRole {
		return errors.New("cannot remove built-in administrator role")
	}
	return m.db.Delete(rolesCollection, role)
}

// Updates an existing user. The function invalidates user tokens after
// successful update.
func (m *UserManager) updateUser(userID string, updateReq *User) error {
	uInfo := &User{}
	err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return cmn.NewErrNotFound("user-manager: %s user %q", svcName, userID)
	}
	if userID == adminID && len(updateReq.Roles) != 0 {
		return errors.New("cannot change administrator's role")
	}

	if updateReq.Password != "" {
		uInfo.Password = encryptPassword(updateReq.Password)
	}
	if len(updateReq.Roles) != 0 {
		uInfo.Roles = updateReq.Roles
	}
	uInfo.Clusters = MergeClusterACLs(uInfo.Clusters, updateReq.Clusters)
	uInfo.Buckets = MergeBckACLs(uInfo.Buckets, updateReq.Buckets)

	return m.db.Set(usersCollection, userID, uInfo)
}

// Updates an existing role
func (m *UserManager) updateRole(role string, updateReq *Role) error {
	if role == AdminRole {
		return errors.New("cannot modify built-in administrator role")
	}
	rInfo := &Role{}
	err := m.db.Get(rolesCollection, role, rInfo)
	if err != nil {
		return cmn.NewErrNotFound("user-manager: %s role %q", svcName, role)
	}

	if updateReq.Desc != "" {
		rInfo.Desc = updateReq.Desc
	}
	if len(updateReq.Roles) != 0 {
		rInfo.Roles = updateReq.Roles
	}
	rInfo.Clusters = MergeClusterACLs(rInfo.Clusters, updateReq.Clusters)
	rInfo.Buckets = MergeBckACLs(rInfo.Buckets, updateReq.Buckets)

	return m.db.Set(rolesCollection, role, rInfo)
}

func (m *UserManager) lookupRole(roleID string) (*Role, error) {
	rInfo := &Role{}
	err := m.db.Get(rolesCollection, roleID, rInfo)
	if err != nil {
		return nil, err
	}
	return rInfo, nil
}

func (m *UserManager) updateCluster(cluID string, info *Cluster) error {
	if info.ID == "" {
		return errors.New("cluster ID is undefined")
	}
	clu := &Cluster{}
	err := m.db.Get(clustersCollection, cluID, clu)
	if err != nil {
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
	return m.db.Set(clustersCollection, cluID, clu)
}

// Before putting a list of cluster permissions to a token, cluster aliases
// must be replaced with their IDs.
func (m *UserManager) fixClusterIDs(lst []*Cluster) {
	cluList, err := m.clusterList()
	if err != nil {
		return
	}
	for _, cInfo := range lst {
		if _, ok := cluList[cInfo.ID]; ok {
			continue
		}
		for _, clu := range cluList {
			if clu.Alias == cInfo.ID {
				cInfo.ID = clu.ID
			}
		}
	}
}

// Generates a token for a user if user credentials are valid. If the token is
// already generated and is not expired yet the existing token is returned.
// Token includes user ID, permissions, and token expiration time.
// If a new token was generated then it sends the proxy a new valid token list
func (m *UserManager) issueToken(userID, pwd string, ttl *time.Duration) (string, error) {
	var (
		err     error
		expires time.Time
	)

	uInfo := &User{}
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
		rInfo := &Role{}
		err := m.db.Get(rolesCollection, role, rInfo)
		if err != nil {
			continue
		}
		uInfo.Clusters = MergeClusterACLs(uInfo.Clusters, rInfo.Clusters)
		uInfo.Buckets = MergeBckACLs(uInfo.Buckets, rInfo.Buckets)
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
	var t *jwt.Token
	if uInfo.IsAdmin() {
		t = jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"expires":  expires,
			"username": userID,
			"admin":    true,
		})
	} else {
		m.fixClusterIDs(uInfo.Clusters)
		t = jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"expires":  expires,
			"username": userID,
			"buckets":  uInfo.Buckets,
			"clusters": uInfo.Clusters,
		})
	}
	tokenString, err := t.SignedString([]byte(Conf.Server.Secret))
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %v", err)
	}
	return tokenString, err
}

// Delete existing token, a.k.a log out
// If the token was removed successfully then it sends the proxy a new valid token list
func (m *UserManager) revokeToken(token string) error {
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
func (m *UserManager) generateRevokedTokenList() ([]string, error) {
	tokens, err := m.db.List(revokedCollection, "")
	if err != nil {
		return nil, err
	}
	now := time.Now()
	revokeList := make([]string, 0)
	secret := Conf.Secret()
	for _, t := range tokens {
		token, err := DecryptToken(t, secret)
		shortInfo := t
		if len(t) > 32 {
			shortInfo = t[len(t)-32:]
		}
		if err != nil {
			glog.Infof("removing invalid token: %s", shortInfo)
			m.db.Delete(revokedCollection, t)
			continue
		}
		if token.Expires.Before(now) {
			glog.Infof("removing expired token: %s", shortInfo)
			m.db.Delete(revokedCollection, t)
			continue
		}
		revokeList = append(revokeList, t)
	}
	return revokeList, nil
}

func (m *UserManager) lookupUser(userID string) (*User, error) {
	uInfo := &User{}
	err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return nil, err
	}

	// update ACLs with roles's ones
	for _, role := range uInfo.Roles {
		rInfo := &Role{}
		err := m.db.Get(rolesCollection, role, rInfo)
		if err != nil {
			continue
		}
		uInfo.Clusters = MergeClusterACLs(uInfo.Clusters, rInfo.Clusters)
		uInfo.Buckets = MergeBckACLs(uInfo.Buckets, rInfo.Buckets)
	}

	return uInfo, nil
}

func (m *UserManager) userList() (map[string]*User, error) {
	recs, err := m.db.GetAll(usersCollection, "")
	if err != nil {
		return nil, err
	}
	users := make(map[string]*User, 4)
	for _, str := range recs {
		uInfo := &User{}
		err := jsoniter.Unmarshal([]byte(str), uInfo)
		cos.AssertNoErr(err)
		users[uInfo.ID] = uInfo
	}
	return users, nil
}

func (m *UserManager) roleList() ([]*Role, error) {
	recs, err := m.db.GetAll(rolesCollection, "")
	if err != nil {
		return nil, err
	}
	roles := make([]*Role, 0, len(recs))
	for _, str := range recs {
		role := &Role{}
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
func (m *UserManager) createRolesForCluster(clu *Cluster) {
	for _, pr := range predefinedRoles {
		suffix := cos.Either(clu.Alias, clu.ID)
		uid := pr.prefix + "-" + suffix
		rInfo := &Role{}
		if err := m.db.Get(rolesCollection, uid, rInfo); err == nil {
			continue
		}
		rInfo.Name = uid
		cluName := clu.ID
		if clu.Alias != "" {
			cluName += "[" + clu.Alias + "]"
		}
		rInfo.Desc = fmt.Sprintf(pr.desc, cluName)
		rInfo.Clusters = []*Cluster{
			{ID: clu.ID, Access: pr.perms},
		}
		if err := m.db.Set(rolesCollection, uid, rInfo); err != nil {
			glog.Errorf("Failed to create role %s: %v", uid, err)
		}
	}
}

// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	jwt "github.com/dgrijalva/jwt-go"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/crypto/bcrypt"
)

const (
	authDB            = "authn.db"
	usersCollection   = "user"
	rolesCollection   = "role"
	tokensCollection  = "token"
	revokedCollection = "revoked"

	adminID   = "admin"
	adminPass = "admin"

	proxyTimeout     = 2 * time.Minute           // maximum time for syncing Authn data with primary proxy
	proxyRetryTime   = 5 * time.Second           // an interval between primary proxy detection attempts
	foreverTokenTime = 24 * 365 * 20 * time.Hour // kind of never-expired token
)

type (
	userManager struct {
		clientHTTP  *http.Client
		clientHTTPS *http.Client
		db          dbdriver.Driver
	}
)

var (
	errInvalidCredentials = errors.New("invalid credentials")
	errTokenNotFound      = errors.New("token not found")
	errTokenExpired       = errors.New("token expired")

	predefinedRoles = []struct {
		prefix string
		desc   string
		perms  cmn.AccessAttrs
	}{
		{cmn.AuthClusterOwnerRole, "Full access to cluster ", cmn.AllAccess()},
		{cmn.AuthBucketOwnerRole, "Full access to buckets ", cmn.ReadWriteAccess()},
		{cmn.AuthGuestRole, "Read-only access to buckets", cmn.ReadOnlyAccess()},
	}
)

func encryptPassword(password string) string {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	cmn.AssertNoErr(err)
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

	su := &cmn.AuthUser{
		ID:       adminID,
		Password: encryptPassword(adminPass),
		Roles:    []string{cmn.AuthAdminRole},
	}
	return driver.Set(usersCollection, adminID, su)
}

// Creates a new user manager. If user DB exists, it loads the data from the
// file and decrypts passwords
func newUserManager(driver dbdriver.Driver) (*userManager, error) {
	clientHTTP := cmn.NewClient(cmn.TransportArgs{Timeout: conf.Timeout.Default})
	clientHTTPS := cmn.NewClient(cmn.TransportArgs{
		Timeout:    conf.Timeout.Default,
		UseHTTPS:   true,
		SkipVerify: true,
	})

	mgr := &userManager{
		clientHTTP:  clientHTTP,
		clientHTTPS: clientHTTPS,
		db:          driver,
	}
	err := initializeDB(driver)
	return mgr, err
}

// Registers a new user. It is info from a user, so the password
// is not encrypted and a few fields are not filled(e.g, Access).
func (m *userManager) addUser(info *cmn.AuthUser) error {
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
func (m *userManager) delUser(userID string) error {
	// TODO: allow delete an admin only if there is another one in DN
	token := &cmn.AuthToken{}
	errToken := m.db.Get(tokensCollection, userID, token)
	err := m.db.Delete(usersCollection, userID)
	if err != nil {
		return err
	}

	if errToken == nil {
		go m.broadcastRevoked(token.Token)
		_ = m.db.Delete(tokensCollection, userID)
		err = m.db.SetString(revokedCollection, token.Token, userID)
	}

	return err
}

// Retunrs user info by user's ID
func (m *userManager) getUser(userID string) (*cmn.AuthUser, error) {
	uInfo := &cmn.AuthUser{}
	err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return nil, fmt.Errorf("user %q not found", userID)
	}
	return uInfo, nil
}

// Updates an existing user. The function invalidates user tokens after
// successful update.
func (m *userManager) updateUser(userID string, updateReq *cmn.AuthUser) error {
	uInfo := &cmn.AuthUser{}
	err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return fmt.Errorf("user %q not found", userID)
	}

	if updateReq.Password != "" {
		uInfo.Password = encryptPassword(updateReq.Password)
	}
	uInfo.MergeClusterACLs(updateReq.Clusters)
	uInfo.MergeBckACLs(updateReq.Buckets)

	return m.db.Set(usersCollection, userID, uInfo)
}

// Generates a token for a user if user credentials are valid. If the token is
// already generated and is not expired yet the existing token is returned.
// Token includes information about userID, AWS/GCP creds and expire token time.
// If a new token was generated then it sends the proxy a new valid token list
func (m *userManager) issueToken(userID, pwd string, ttl ...time.Duration) (string, error) {
	var (
		err     error
		expires time.Time
	)

	uInfo := &cmn.AuthUser{}
	err = m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		glog.Error(err)
		return "", errInvalidCredentials
	}
	if !isSamePassword(pwd, uInfo.Password) {
		return "", errInvalidCredentials
	}

	tInfo := &cmn.AuthToken{}
	err = m.db.Get(tokensCollection, userID, tInfo)
	if err == nil && tInfo.Expires.After(time.Now()) {
		return tInfo.Token, nil
	}

	// generate token
	issued := time.Now()
	expDelta := conf.Auth.ExpirePeriod
	if len(ttl) != 0 {
		expDelta = ttl[0]
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
		t = jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"expires":  expires,
			"username": userID,
			"buckets":  uInfo.Buckets,
			"clusters": uInfo.Clusters,
		})
	}
	tokenString, err := t.SignedString([]byte(conf.Auth.Secret))
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %v", err)
	}

	// TODO: multiple tokens per user
	tInfo = &cmn.AuthToken{
		UserID:  userID,
		Expires: expires,
		Token:   tokenString,
	}
	err = m.db.Set(tokensCollection, userID, tInfo)
	return tokenString, err
}

// Delete existing token, a.k.a log out
// If the token was removed successfully then it sends the proxy a new valid token list
func (m *userManager) revokeToken(token string) error {
	err := m.db.Set(revokedCollection, token, "!")
	if err != nil {
		return err
	}

	// send the token in all case to allow an admin to revoke
	// an existing token even after cluster restart
	go m.broadcastRevoked(token)

	uInfo, err := m.userByToken(token)
	if err == nil {
		if err := m.db.Delete(tokensCollection, uInfo.ID); err != nil {
			glog.Errorf("Failed to clean up token for %s", uInfo.ID)
		}
	}

	return nil
}

func (m *userManager) userByToken(token string) (*cmn.AuthUser, error) {
	recs, err := m.db.GetAll(tokensCollection, "")
	if err != nil {
		return nil, err
	}
	for uid, tkn := range recs {
		tInfo := &cmn.AuthToken{}
		err := jsoniter.Unmarshal([]byte(tkn), tInfo)
		cmn.AssertNoErr(err)
		if token != tInfo.Token {
			continue
		}
		if tInfo.Expires.Before(time.Now()) {
			// autocleanup expired tokens
			_ = m.db.Delete(tokensCollection, uid)
			return nil, errTokenExpired
		}
		uInfo := &cmn.AuthUser{}
		err = m.db.Get(usersCollection, uid, uInfo)
		return uInfo, err
	}
	return nil, errTokenNotFound
}

func (m *userManager) userList() (map[string]*cmn.AuthUser, error) {
	recs, err := m.db.GetAll(usersCollection, "")
	if err != nil {
		return nil, err
	}
	users := make(map[string]*cmn.AuthUser, 4)
	for _, str := range recs {
		uInfo := &cmn.AuthUser{}
		err := jsoniter.Unmarshal([]byte(str), uInfo)
		cmn.AssertNoErr(err)
		users[uInfo.ID] = uInfo
	}
	return users, nil
}

func (m *userManager) tokenByUser(userID string) (*cmn.AuthToken, error) {
	tInfo := &cmn.AuthToken{}
	err := m.db.Get(tokensCollection, userID, tInfo)
	return tInfo, err
}

func (m *userManager) roleList() ([]*cmn.AuthRole, error) {
	recs, err := m.db.GetAll(rolesCollection, "")
	if err != nil {
		return nil, err
	}
	roles := make([]*cmn.AuthRole, 0, len(recs))
	for _, str := range recs {
		role := &cmn.AuthRole{}
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
func (m *userManager) createRolesForClusters(cluList map[string][]string) {
	for clu := range cluList {
		for _, pr := range predefinedRoles {
			uid := pr.prefix + "-" + clu
			rInfo := &cmn.AuthRole{}
			if err := m.db.Get(rolesCollection, uid, rInfo); err == nil {
				continue
			}
			rInfo.Name = uid
			rInfo.Desc = pr.desc
			rInfo.Clusters = []*cmn.AuthCluster{
				{ID: clu, Access: pr.perms},
			}
			if err := m.db.Set(rolesCollection, uid, rInfo); err != nil {
				glog.Errorf("Failed to create role %s: %v", uid, err)
			}
		}
	}
}

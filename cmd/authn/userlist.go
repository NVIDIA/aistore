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
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/OneOfOne/xxhash"
	jwt "github.com/dgrijalva/jwt-go"
	jsoniter "github.com/json-iterator/go"
)

const (
	authDB            = "authn.db"
	usersCollection   = "user"
	rolesCollection   = "role"
	tokensCollection  = "token"
	revokedCollection = "revoked"

	guestUserID = "guest"

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

	fullAccessRole  = &cmn.AuthRole{Name: cmn.AuthPowerUserRole, Access: cmn.AllAccess(), Desc: "Full access to cluster"}
	readWriteRole   = &cmn.AuthRole{Name: cmn.AuthBucketOwnerRole, Access: cmn.ReadWriteAccess(), Desc: "Full access to buckets"}
	readOnlyRole    = &cmn.AuthRole{Name: cmn.AuthGuestRole, Access: cmn.ReadOnlyAccess(), Desc: "Read-only access to buckets"}
	predefinedRoles = []*cmn.AuthRole{fullAccessRole, readWriteRole, readOnlyRole}
)

func encryptPassword(salt, password string) string {
	h := xxhash.New64()
	h.WriteString(salt)
	h.WriteString(password)
	h.WriteString(password)
	h.WriteString(salt)
	return hex.EncodeToString(h.Sum(nil))
}

// If the DB is empty, the function prefills some data
func initializeDB(driver dbdriver.Driver) error {
	roles, err := driver.List(rolesCollection, "")
	if err != nil || len(roles) != 0 {
		// return on erros or when DB is already initialized
		return err
	}

	// DB seems empty (at least role collection is empty):
	// 1. Add SuperUser credentials
	// 2. Add standard roles
	if conf.Auth.Username != "" {
		// always overwrite SU record for now (in case someone changes config)
		su := &cmn.AuthUser{
			UserID:   conf.Auth.Username,
			Password: encryptPassword(conf.Auth.Username, conf.Auth.Password),
			Access:   cmn.AllAccess(),
			SU:       true,
		}
		err = driver.Set(usersCollection, conf.Auth.Username, su)
		if err != nil {
			return err
		}
	}
	for _, role := range predefinedRoles {
		err = driver.Set(rolesCollection, role.Name, role)
		if err != nil {
			return err
		}
	}
	// add default guest account
	guest := &cmn.AuthUser{
		UserID:   guestUserID,
		Role:     cmn.AuthGuestRole,
		Password: encryptPassword(guestUserID, "guest"),
		Access:   cmn.ReadOnlyAccess(),
	}
	err = driver.Set(usersCollection, guestUserID, guest)
	return err
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

func (m *userManager) roleDesc(role string) (*cmn.AuthRole, error) {
	rInfo := &cmn.AuthRole{}
	err := m.db.Get(rolesCollection, role, rInfo)
	return rInfo, err
}

// Registers a new user. It is info from a user, so the password
// is not encrypted and a few fields are not filled(e.g, Access).
func (m *userManager) addUser(info *cmn.AuthUser) error {
	if info.UserID == "" || info.Password == "" {
		return errInvalidCredentials
	}

	_, err := m.db.GetString(usersCollection, info.UserID)
	if err == nil {
		return fmt.Errorf("user %q already registered", info.UserID)
	}
	info.Password = encryptPassword(info.UserID, info.Password)
	rInfo, err := m.roleDesc(info.Role)
	if err != nil {
		return err
	}
	info.Access = rInfo.Access
	info.Buckets = rInfo.Buckets
	return m.db.Set(usersCollection, info.UserID, info)
}

// Deletes an existing user
func (m *userManager) delUser(userID string) error {
	if userID == conf.Auth.Username {
		return errors.New("superuser cannot be deleted")
	}

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

// Updates an existing user. The function invalidates user tokens after
// successful update.
func (m *userManager) updateUser(userID string, updateReq *cmn.AuthUser) error {
	if userID == conf.Auth.Username {
		return errors.New("superuser cannot be updated")
	}

	uInfo := &cmn.AuthUser{}
	err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return fmt.Errorf("user %q not found", userID)
	}

	changed := false
	if updateReq.Role != "" && updateReq.Role != uInfo.Role {
		rInfo, err := m.roleDesc(updateReq.Role)
		if err != nil {
			return err
		}
		uInfo.Access = rInfo.Access
		uInfo.Buckets = rInfo.Buckets
		changed = true
	}
	if updateReq.Password != "" {
		newPass := encryptPassword(userID, updateReq.Password)
		if newPass != uInfo.Password {
			uInfo.Password = newPass
			changed = true
		}
	}
	uInfo.MergeBckACLs(updateReq.Buckets)

	if changed {
		return m.db.Set(usersCollection, userID, uInfo)
	}

	return nil
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
	pass := encryptPassword(userID, pwd)
	if pass != uInfo.Password {
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
	// when it expires and credentials to log in AWS, GCP etc
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"issued":   issued,
		"expires":  expires,
		"username": userID,
		"perm":     strconv.FormatUint(uInfo.Access, 10),
		"buckets":  uInfo.Buckets,
	})
	tokenString, err := t.SignedString([]byte(conf.Auth.Secret))
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %v", err)
	}

	tInfo = &cmn.AuthToken{
		UserID:  userID,
		Issued:  issued,
		Expires: expires,
		Token:   tokenString,
		Access:  uInfo.Access,
		Buckets: uInfo.Buckets,
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
		if err := m.db.Delete(tokensCollection, uInfo.UserID); err != nil {
			glog.Errorf("Failed to clean up token for %s", uInfo.UserID)
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
	for uid, str := range recs {
		uInfo := &cmn.AuthUser{}
		err := jsoniter.Unmarshal([]byte(str), uInfo)
		cmn.AssertNoErr(err)
		// do not include SuperUsers
		if !uInfo.SU {
			users[uid] = uInfo
		}
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

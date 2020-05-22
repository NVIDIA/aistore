// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	jwt "github.com/dgrijalva/jwt-go"
	jsoniter "github.com/json-iterator/go"
)

const (
	authDB          = "authn.db"
	usersCollection = "users"
	// TODO: rolesCollection   = "roles" // nolint:unparam // unused now but will be used late
	tokensCollection  = "tokens"
	revokedCollection = "revoked"
	suRole            = "SU"
	defaultRole       = "admin"

	proxyTimeout     = 2 * time.Minute           // maximum time for syncing Authn data with primary proxy
	proxyRetryTime   = 5 * time.Second           // an interval between primary proxy detection attempts
	foreverTokenTime = 24 * 365 * 20 * time.Hour // kind of never-expired token
)

type (
	userInfo struct {
		UserID   string `json:"name"`
		Password string `json:"password,omitempty"`
		Role     string
	}
	tokenInfo struct {
		UserID  string    `json:"username"`
		Issued  time.Time `json:"issued"`
		Expires time.Time `json:"expires"`
		Token   string    `json:"token"`
	}
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
)

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

	var err error
	if conf.Auth.Username != "" {
		// always overwrite SU record for now (in case someone changes config)
		su := &userInfo{
			Role:     suRole,
			UserID:   conf.Auth.Username,
			Password: base64.StdEncoding.EncodeToString([]byte(conf.Auth.Password)),
		}
		err = driver.Set(usersCollection, conf.Auth.Username, su)
	}
	return mgr, err
}

// Registers a new user
func (m *userManager) addUser(userID, userPass string) error {
	if userID == "" || userPass == "" {
		return errInvalidCredentials
	}

	_, err := m.db.GetString(usersCollection, userID)
	if err == nil {
		return fmt.Errorf("user %q already registered", userID)
	}
	info := &userInfo{
		UserID:   userID,
		Password: base64.StdEncoding.EncodeToString([]byte(userPass)),
		Role:     defaultRole, // TODO
	}
	return m.db.Set(usersCollection, userID, info)
}

// Deletes an existing user
func (m *userManager) delUser(userID string) error {
	if userID == conf.Auth.Username {
		return errors.New("superuser cannot be deleted")
	}

	token := &tokenInfo{}
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

// Generates a token for a user if user credentials are valid. If the token is
// already generated and is not expired yet the existing token is returned.
// Token includes information about userID, AWS/GCP creds and expire token time.
// If a new token was generated then it sends the proxy a new valid token list
func (m *userManager) issueToken(userID, pwd string, ttl ...time.Duration) (string, error) {
	var (
		err     error
		expires time.Time
	)

	uInfo := &userInfo{}
	err = m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		glog.Error(err)
		return "", errInvalidCredentials
	}
	pass := base64.StdEncoding.EncodeToString([]byte(pwd))
	if pass != uInfo.Password {
		return "", errInvalidCredentials
	}

	tInfo := &tokenInfo{}
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
		"issued":   issued.Format(time.RFC822),
		"expires":  expires.Format(time.RFC822),
		"username": userID,
	})
	tokenString, err := t.SignedString([]byte(conf.Auth.Secret))
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %v", err)
	}

	tInfo = &tokenInfo{
		UserID:  userID,
		Issued:  issued,
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
		if err := m.db.Delete(tokensCollection, uInfo.UserID); err != nil {
			glog.Errorf("Failed to clean up token for %s", uInfo.UserID)
		}
	}

	return nil
}

func (m *userManager) userByToken(token string) (*userInfo, error) {
	recs, err := m.db.GetAll(tokensCollection, "")
	if err != nil {
		return nil, err
	}
	for uid, tkn := range recs {
		tInfo := &tokenInfo{}
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
		uInfo := &userInfo{}
		err = m.db.Get(usersCollection, uid, uInfo)
		return uInfo, err
	}
	return nil, errTokenNotFound
}

func (m *userManager) userList() (map[string]*userInfo, error) {
	recs, err := m.db.GetAll(usersCollection, "")
	if err != nil {
		return nil, err
	}
	users := make(map[string]*userInfo, 4)
	for uid, str := range recs {
		uInfo := &userInfo{}
		err := jsoniter.Unmarshal([]byte(str), uInfo)
		cmn.AssertNoErr(err)
		// do not include SuperUser
		if uid != conf.Auth.Username {
			users[uid] = uInfo
		}
	}
	return users, nil
}

func (m *userManager) tokenByUser(userID string) (*tokenInfo, error) {
	tInfo := &tokenInfo{}
	err := m.db.Get(tokensCollection, userID, tInfo)
	return tInfo, err
}

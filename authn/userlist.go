// Authorization server for AIStore. See /authn/README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/cmn"
	jwt "github.com/dgrijalva/jwt-go"
)

const (
	userListFile   = "users.json"
	proxyTimeout   = time.Minute * 2 // maximum time for syncing Authn data with primary proxy
	proxyRetryTime = time.Second * 5 // an interval between primary proxy detection attempts
)

type (
	userInfo struct {
		UserID          string            `json:"name"`
		Password        string            `json:"password,omitempty"`
		Creds           map[string]string `json:"creds,omitempty"`
		passwordDecoded string
	}
	tokenInfo struct {
		UserID  string    `json:"username"`
		Issued  time.Time `json:"issued"`
		Expires time.Time `json:"expires"`
		Token   string    `json:"token"`
	}
	userManager struct {
		mtx    sync.Mutex
		Path   string               `json:"-"`
		Users  map[string]*userInfo `json:"users"`
		tokens map[string]*tokenInfo
		client *http.Client
		proxy  *proxy
	}
)

// Creates a new user manager. If user DB exists, it loads the data from the
// file and decrypts passwords
func newUserManager(dbPath string, proxy *proxy) *userManager {
	var (
		err   error
		bytes []byte
	)
	client := cmn.NewClient(cmn.ClientArgs{
		Timeout: conf.Timeout.Default,
	})
	mgr := &userManager{
		Path:   dbPath,
		Users:  make(map[string]*userInfo, 10),
		tokens: make(map[string]*tokenInfo, 10),
		client: client,
		proxy:  proxy,
	}
	if _, err = os.Stat(dbPath); err != nil {
		if !os.IsNotExist(err) {
			glog.Fatalf("Failed to load user list: %v\n", err)
		}
		return mgr
	}

	if err = cmn.LocalLoad(dbPath, &mgr.Users); err != nil {
		glog.Fatalf("Failed to load user list: %v\n", err)
	}
	// update loaded list: create empty map for users who do not have credentials in saved file
	for _, uinfo := range mgr.Users {
		if uinfo.Creds == nil {
			uinfo.Creds = make(map[string]string, 10)
		}
	}

	for _, info := range mgr.Users {
		if bytes, err = base64.StdEncoding.DecodeString(info.Password); err != nil {
			glog.Fatalf("Failed to read user list: %v\n", err)
		}
		info.passwordDecoded = string(bytes)
	}

	return mgr
}

// save new user list to file
// It is called from functions of this module that acquire lock, so this
//    function needs no locks
func (m *userManager) saveUsers() (err error) {
	if err = cmn.LocalSave(m.Path, &m.Users); err != nil {
		err = fmt.Errorf("UserManager: Failed to save user list: %v", err)
	}
	return err
}

// Registers a new user
func (m *userManager) addUser(userID, userPass string) error {
	if userID == "" || userPass == "" {
		return fmt.Errorf("invalid credentials")
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := m.Users[userID]; ok {
		return fmt.Errorf("user '%s' already registered", userID)
	}
	m.Users[userID] = &userInfo{
		UserID:          userID,
		passwordDecoded: userPass,
		Password:        base64.StdEncoding.EncodeToString([]byte(userPass)),
		Creds:           make(map[string]string, 10),
	}

	return m.saveUsers()
}

// Deletes an existing user
func (m *userManager) delUser(userID string) error {
	m.mtx.Lock()
	if _, ok := m.Users[userID]; !ok {
		m.mtx.Unlock()
		return fmt.Errorf("user %s %s", userID, cmn.DoesNotExist)
	}
	delete(m.Users, userID)
	token, ok := m.tokens[userID]
	delete(m.tokens, userID)
	err := m.saveUsers()
	m.mtx.Unlock()

	if ok {
		go m.sendRevokedTokensToProxy(token.Token)
	}

	return err
}

// Generates a token for a user if user credentials are valid. If the token is
// already generated and is not expired yet the existing token is returned.
// Token includes information about userID, AWS/GCP creds and expire token time.
// If a new token was generated then it sends the proxy a new valid token list
func (m *userManager) issueToken(userID, pwd string) (string, error) {
	var (
		user  *userInfo
		token *tokenInfo
		ok    bool
		err   error
	)

	// check user name and pass in DB
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if user, ok = m.Users[userID]; !ok {
		return "", fmt.Errorf("invalid credentials")
	}
	passwordDecoded := user.passwordDecoded
	creds := user.Creds

	if passwordDecoded != pwd {
		return "", fmt.Errorf("invalid username or password")
	}

	// check if a user is already has got token. If existing token expired then
	// delete it and reissue a new token
	if token, ok = m.tokens[userID]; ok {
		if token.Expires.After(time.Now()) {
			return token.Token, nil
		}
		delete(m.tokens, userID)
	}

	// generate token
	issued := time.Now()
	expires := issued.Add(conf.Auth.ExpirePeriod)

	// put all useful info into token: who owns the token, when it was issued,
	// when it expires and credentials to log in AWS, GCP etc
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"issued":   issued.Format(time.RFC822),
		"expires":  expires.Format(time.RFC822),
		"username": userID,
		"creds":    creds,
	})
	tokenString, err := t.SignedString([]byte(conf.Auth.Secret))
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %v", err)
	}

	token = &tokenInfo{
		UserID:  userID,
		Issued:  issued,
		Expires: expires,
		Token:   tokenString,
	}
	m.tokens[userID] = token

	return tokenString, nil
}

// Delete existing token, a.k.a log out
// If the token was removed successfully then it sends the proxy a new valid token list
func (m *userManager) revokeToken(token string) {
	m.mtx.Lock()
	for id, info := range m.tokens {
		if info.Token == token {
			delete(m.tokens, id)
			break
		}
	}
	m.mtx.Unlock()

	// send the token in all case to allow an admin to revoke
	// an existing token even after cluster restart
	go m.sendRevokedTokensToProxy(token)
}

// update list of valid token on a proxy
func (m *userManager) sendRevokedTokensToProxy(tokens ...string) {
	if len(tokens) == 0 {
		return
	}
	if m.proxy.URL == "" {
		glog.Warning("Primary proxy is not defined")
		return
	}

	tokenList := ais.TokenList{Tokens: tokens}
	body := cmn.MustMarshal(tokenList)
	if err := m.proxyRequest(http.MethodDelete, cmn.Tokens, body); err != nil {
		glog.Errorf("Failed to send token list: %v", err)
	}
}

func (m *userManager) userByToken(token string) (*userInfo, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for id, info := range m.tokens {
		if info.Token == token {
			if info.Expires.Before(time.Now()) {
				delete(m.tokens, id)
				return nil, fmt.Errorf("token expired")
			}

			user, ok := m.Users[id]
			if !ok {
				return nil, fmt.Errorf("invalid token")
			}

			return user, nil
		}
	}

	return nil, fmt.Errorf("token not found")
}

// Generic function to send everything to primary proxy
// It can detect primary proxy change and sent to the new one on the fly
func (m *userManager) proxyRequest(method, path string, injson []byte) error {
	startRequest := time.Now()
	for {
		url := m.proxy.URL + cmn.URLPath(cmn.Version, path)
		request, err := http.NewRequest(method, url, bytes.NewBuffer(injson))
		if err != nil {
			// Fatal - interrupt the loop
			return err
		}

		request.Header.Set("Content-Type", "application/json")
		response, err := m.client.Do(request)
		var respCode int
		if response != nil {
			respCode = response.StatusCode
			if response.Body != nil {
				response.Body.Close()
			}
		}
		if err == nil && respCode < http.StatusBadRequest {
			return nil
		}

		glog.Errorf("Failed to http-call %s %s: error %v", method, url, err)

		err = m.proxy.detectPrimary()
		if err != nil {
			// primary change is not detected or failed - interrupt the loop
			return err
		}

		if time.Since(startRequest) > proxyTimeout {
			return fmt.Errorf("sending data to primary proxy timed out")
		}

		time.Sleep(proxyRetryTime)
	}
}

func (m *userManager) updateCredentials(userID, provider, userCreds string) (bool, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if !isValidProvider(provider) {
		return false, fmt.Errorf("invalid cloud provider: %s", provider)
	}

	user, ok := m.Users[userID]
	if !ok {
		err := fmt.Errorf("user %s %s", userID, cmn.DoesNotExist)
		return false, err
	}

	changed := user.Creds[provider] != userCreds
	if changed {
		user.Creds[provider] = userCreds
		if token, ok := m.tokens[userID]; ok {
			delete(m.tokens, userID)
			go m.sendRevokedTokensToProxy(token.Token)
		}
	}

	if changed {
		if err := m.saveUsers(); err != nil {
			glog.Errorf("Delete credentials failed to save user list: %v", err)
		}
	}

	return changed, nil
}

func (m *userManager) deleteCredentials(userID, provider string) (bool, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if !isValidProvider(provider) {
		return false, fmt.Errorf("invalid cloud provider: %s", provider)
	}

	user, ok := m.Users[userID]
	if !ok {
		return false, fmt.Errorf("user %s %s", userID, cmn.DoesNotExist)
	}
	if _, ok = user.Creds[provider]; ok {
		delete(user.Creds, provider)
		if err := m.saveUsers(); err != nil {
			glog.Errorf("Delete credentials failed to save user list: %v", err)
		}
		return true, nil
	}

	return false, nil
}

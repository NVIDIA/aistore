package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/dgrijalva/jwt-go"
	"github.com/golang/glog"
)

const (
	dbFile = "users.json"
)

type (
	userInfo struct {
		UserID          string `json:"name"`
		Password        string `json:"password,omitempty"`
		passwordDecoded string
		Creds           map[string]string `json:"creds,omitempty"` //TODO: aws?gcp?
	}
	tokenInfo struct {
		UserID  string    `json:"username"`
		Issued  time.Time `json:"issued"`
		Expires time.Time `json:"expires"`
		Token   string    `json:"token"`
	}
	tokenList struct {
		Tokens []*tokenInfo `json:"tokens"`
	}
	userManager struct {
		userMtx  sync.Mutex
		tokenMtx sync.Mutex
		Path     string               `json:"-"`
		Users    map[string]*userInfo `json:"users"`
		tokens   map[string]*tokenInfo
		client   *http.Client
	}
)

// borrowed from DFC
func createHTTPClient() *http.Client {
	defaultTransport := http.DefaultTransport.(*http.Transport)
	transport := &http.Transport{
		// defaults
		Proxy: defaultTransport.Proxy,
		DialContext: (&net.Dialer{ // defaultTransport.DialContext,
			Timeout:   30 * time.Second, // must be reduced & configurable
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		IdleConnTimeout:       defaultTransport.IdleConnTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		// custom
		MaxIdleConnsPerHost: defaultTransport.MaxIdleConnsPerHost,
		MaxIdleConns:        defaultTransport.MaxIdleConns,
	}
	return &http.Client{Transport: transport, Timeout: conf.Timeout.Default}
}

// Creates a new user manager. If user DB exists, it loads the data from the
// file and decrypts passwords
func newUserManager(dbPath string) *userManager {
	var (
		err   error
		bytes []byte
	)
	mgr := &userManager{
		Path:   dbPath,
		Users:  make(map[string]*userInfo, 0),
		tokens: make(map[string]*tokenInfo, 0),
		client: createHTTPClient(),
	}
	if _, err = os.Stat(dbPath); err != nil {
		if !os.IsNotExist(err) {
			glog.Fatalf("Failed to load user list: %v\n", err)
		}
		return mgr
	}

	if err = dfc.LocalLoad(dbPath, &mgr.Users); err != nil {
		glog.Fatalf("Failed to load user list: %v\n", err)
	}

	for _, info := range mgr.Users {
		if bytes, err = base64.StdEncoding.DecodeString(info.Password); err != nil {
			glog.Fatalf("Failed to read user list: %v\n", err)
		}
		info.passwordDecoded = string(bytes)
	}

	return mgr
}

// save new user list to user DB
func (m *userManager) saveUsers() (err error) {
	m.userMtx.Lock()
	defer m.userMtx.Unlock()
	if err = dfc.LocalSave(m.Path, &m.Users); err != nil {
		err = fmt.Errorf("UserManager: Failed to save user list: %v", err)
	}
	return err
}

// Registers a new user
func (m *userManager) addUser(userID, userPass string) error {
	if userID == "" || userPass == "" {
		return fmt.Errorf("Invalid credentials")
	}

	m.userMtx.Lock()
	if _, ok := m.Users[userID]; ok {
		m.userMtx.Unlock()
		return fmt.Errorf("User '%s' already registered", userID)
	}
	m.Users[userID] = &userInfo{
		UserID:          userID,
		passwordDecoded: userPass,
		Password:        base64.StdEncoding.EncodeToString([]byte(userPass)),
	}
	m.userMtx.Unlock()

	// clean up in case of there is an old token issued for the same UserID
	m.tokenMtx.Lock()
	delete(m.tokens, userID)
	m.tokenMtx.Unlock()

	return m.saveUsers()
}

// Deletes an existing user
func (m *userManager) delUser(userID string) error {
	m.userMtx.Lock()
	if _, ok := m.Users[userID]; !ok {
		m.userMtx.Unlock()
		return fmt.Errorf("User %s does not exist", userID)
	}
	delete(m.Users, userID)
	m.userMtx.Unlock()

	m.tokenMtx.Lock()
	_, ok := m.tokens[userID]
	delete(m.tokens, userID)
	m.tokenMtx.Unlock()
	if ok {
		go m.sendTokensToProxy()
	}

	return m.saveUsers()
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
	m.userMtx.Lock()
	if user, ok = m.Users[userID]; !ok {
		m.userMtx.Unlock()
		return "", fmt.Errorf("Invalid credentials")
	}
	passwordDecoded := user.passwordDecoded
	creds := user.Creds
	m.userMtx.Unlock()

	if passwordDecoded != pwd {
		return "", fmt.Errorf("Invalid username or password")
	}

	// check if a user is already has got token. If existing token expired then
	// delete it and reissue a new token
	m.tokenMtx.Lock()
	if token, ok = m.tokens[userID]; ok {
		if token.Expires.After(time.Now()) {
			m.tokenMtx.Unlock()
			return token.Token, nil
		}
		delete(m.tokens, userID)
	}
	m.tokenMtx.Unlock()

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
	m.tokenMtx.Lock()
	m.tokens[userID] = token
	m.tokenMtx.Unlock()
	go m.sendTokensToProxy()

	return tokenString, nil
}

// Delete existing token, a.k.a log out
// If the token was removed successfully then it sends the proxy a new valid token list
func (m *userManager) revokeToken(token string) {
	tokenDeleted := false
	m.tokenMtx.Lock()
	for id, info := range m.tokens {
		if info.Token == token {
			delete(m.tokens, id)
			tokenDeleted = true
			break
		}
	}
	m.tokenMtx.Unlock()

	if tokenDeleted {
		go m.sendTokensToProxy()
	}
}

// update list of valid token on a proxy
func (m *userManager) sendTokensToProxy() {
	if conf.Proxy.URL == "" {
		glog.Error("Proxy is not defined")
		return
	}

	tokenList := &dfc.TokenList{Tokens: make([]string, 0, len(m.tokens))}
	m.tokenMtx.Lock()
	for userID, tokenRec := range m.tokens {
		if tokenRec.Expires.Before(time.Now()) {
			// remove expired token
			delete(m.tokens, userID)
			continue
		}

		tokenList.Tokens = append(tokenList.Tokens, tokenRec.Token)
	}
	m.tokenMtx.Unlock()

	method := http.MethodPost
	url := fmt.Sprintf("%s/%s/%s", conf.Proxy.URL, dfc.Rversion, dfc.Rtokens)
	injson, _ := json.Marshal(tokenList)
	request, err := http.NewRequest(method, url, bytes.NewBuffer(injson))
	if err != nil {
		glog.Error(err)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := m.client.Do(request)
	if err != nil {
		glog.Errorf("Failed to http-call %s %s: error %v", method, url, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest {
		glog.Errorf("Failed to http-call %s %s: error code %d", method, url, response.StatusCode)
	}
}

func (m *userManager) userByToken(token string) (*userInfo, error) {
	m.tokenMtx.Lock()
	defer m.tokenMtx.Unlock()
	for id, info := range m.tokens {
		if info.Token == token {
			if info.Expires.Before(time.Now()) {
				delete(m.tokens, id)
				return nil, fmt.Errorf("Token expired")
			}

			m.userMtx.Lock()
			defer m.userMtx.Unlock()
			user, ok := m.Users[id]
			if !ok {
				return nil, fmt.Errorf("Invalid token")
			}

			return user, nil
		}
	}

	return nil, fmt.Errorf("Token not found")
}

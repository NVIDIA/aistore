// Authorization server for DFC
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	pathUsers  = "users"
	pathTokens = "tokens"
	smapConfig = "smap.json"
)

// a message to generate token
// POST: <version>/<pathUsers>/<username>
//		Body: <loginMsg>
//	Returns: <tokenMsg>
type loginMsg struct {
	Password string `json:"password"`
}

// a message to test token validity and to revoke existing token
// check: GET <version>/<pathTokens>
//		Body: <tokenMsg>
// revoke: DEL <version>/<pathTokens>
//		Body: <tokenMsg>
type tokenMsg struct {
	Token string `json:"token"`
}

//-------------------------------------
// global functions (borrowed from DFC)
//-------------------------------------
func isSyscallWriteError(err error) bool {
	switch e := err.(type) {
	case *url.Error:
		return isSyscallWriteError(e.Err)
	case *net.OpError:
		return e.Op == "write" && isSyscallWriteError(e.Err)
	case *os.SyscallError:
		return e.Syscall == "write"
	default:
		return false
	}
}

func isValidProvider(prov string) bool {
	return prov == cmn.ProviderAmazon || prov == cmn.ProviderGoogle || prov == cmn.ProviderDFC
}

func checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int, items ...string) ([]string, error) {
	items, err := cmn.MatchRESTItems(r.URL.Path, itemsAfter, true, items...)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return nil, err
	}

	return items, err
}

//-------------------------------------
// auth server
//-------------------------------------
type authServ struct {
	mux   *http.ServeMux
	h     *http.Server
	users *userManager
}

func newAuthServ(mgr *userManager) *authServ {
	srv := &authServ{users: mgr}
	srv.mux = http.NewServeMux()

	return srv
}

// Starts two HTTP servers:
// Public one: it can be HTTP or HTTPS (config dependent). Used to manage
//	users and generate tokens
// Internal one: it can be only HTTP and supports only one function - check
//	if a token is valid. Can be called, e.g, from a targer. Now it is not used
//  and can be removed
func (a *authServ) run() error {
	// Run public server to manage users and generate tokens
	portstring := fmt.Sprintf(":%d", conf.Net.HTTP.Port)
	glog.Infof("Launching public server at %s", portstring)
	a.registerPublicHandlers()

	a.h = &http.Server{Addr: portstring, Handler: a.mux}
	if conf.Net.HTTP.UseHTTPS {
		if err := a.h.ListenAndServeTLS(conf.Net.HTTP.Certificate, conf.Net.HTTP.Key); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated with err: %v", err)
				return err
			}
		}
	} else {
		if err := a.h.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				glog.Errorf("Terminated with err: %v", err)
				return err
			}
		}
	}

	return nil
}

func (a *authServ) registerHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	a.mux.HandleFunc(path, handler)
	if !strings.HasSuffix(path, "/") {
		a.mux.HandleFunc(path+"/", handler)
	}
}

func (a *authServ) registerPublicHandlers() {
	a.registerHandler(cmn.URLPath(cmn.Version, pathUsers), a.userHandler)
	a.registerHandler(cmn.URLPath(cmn.Version, pathTokens), a.tokenHandler)
}

func (a *authServ) userHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		a.httpUserDel(w, r)
	case http.MethodPost:
		a.httpUserPost(w, r)
	case http.MethodPut:
		a.httpUserPut(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "Unsupported method for /users handler")
	}
}

func (a *authServ) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		a.httpRevokeToken(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "Unsupported method for /token handler")
	}
}

// Deletes existing token, a.k.a log out
func (a *authServ) httpRevokeToken(w http.ResponseWriter, r *http.Request) {
	if _, err := checkRESTItems(w, r, 0, cmn.Version, pathTokens); err != nil {
		return
	}

	msg := &tokenMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil || msg.Token == "" {
		glog.Errorf("Failed to read request: %v\n", err)
		return
	}

	a.users.revokeToken(msg.Token)
}

func (a *authServ) httpUserDel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, pathUsers)
	if err != nil {
		return
	}

	if err := a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v\n", err)
		return
	}

	if len(apiItems) == 1 {
		if err := a.users.delUser(apiItems[0]); err != nil {
			glog.Errorf("Failed to delete user: %v\n", err)
			cmn.InvalidHandlerWithMsg(w, r, "Failed to delete user")
		}
	} else {
		a.userRemoveCredentials(w, r)
	}
}

func (a *authServ) httpUserPost(w http.ResponseWriter, r *http.Request) {
	if apiItems, err := checkRESTItems(w, r, 0, cmn.Version, pathUsers); err != nil {
		return
	} else if len(apiItems) == 0 {
		a.userAdd(w, r)
	} else {
		a.userLogin(w, r)
	}
}

// Updates user credentials
// If user did not have credentials before updating or the credentials changes
//   then new user list is saved and sent to the proxy to update the cluster
func (a *authServ) httpUserPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 2, cmn.Version, pathUsers)
	if err != nil {
		return
	}
	if err = a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v\n", err)
		return
	}

	userID := apiItems[0]
	provider := apiItems[1]

	b, err := ioutil.ReadAll(r.Body)
	if err != nil || len(b) == 0 {
		cmn.InvalidHandlerWithMsg(w, r, "Invalid request")
		return
	}

	if glog.V(4) {
		glog.Infof("Received credentials for %s\n", userID)
	}

	if _, err := a.users.updateCredentials(userID, provider, string(b)); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("Failed to update credentials: %v", err), http.StatusInternalServerError)
		return
	}

	a.writeJSON(w, r, []byte("Credentials updated successfully"), "update credentials")
}

// Adds a new user to user list
func (a *authServ) userAdd(w http.ResponseWriter, r *http.Request) {
	if err := a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v\n", err)
		return
	}

	info := &userInfo{}
	if err := cmn.ReadJSON(w, r, info); err != nil {
		glog.Errorf("Failed to read credentials: %v\n", err)
		return
	}

	if err := a.users.addUser(info.UserID, info.Password); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("Failed to add user: %v", err), http.StatusInternalServerError)
		return
	}
	if glog.V(4) {
		glog.Infof("Added a user %s\n", info.UserID)
	}

	msg := []byte("User created successfully")
	a.writeJSON(w, r, msg, "create user")
}

// Checks if the request header contains super-user credentials and they are
// valid. Super-user is a user created at deployment time that cannot be
// deleted/created via REST API
func (a *authServ) checkAuthorization(w http.ResponseWriter, r *http.Request) error {
	s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("Invalid header")
	}

	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("Invalid header authorization")
	}

	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("Invalid header authorization")
	}

	if pair[0] != conf.Auth.Username || pair[1] != conf.Auth.Password {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("Invalid credentials")
	}

	return nil
}

// Generate a token for a user if provided credentials are valid.
// If a token is already issued and it is not expired yet then the old
// token is returned
func (a *authServ) userLogin(w http.ResponseWriter, r *http.Request) {
	var err error

	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, pathUsers)
	if err != nil {
		return
	}

	msg := &loginMsg{}
	if err = cmn.ReadJSON(w, r, msg); err != nil {
		glog.Errorf("Failed to read request body: %v\n", err)
		return
	}

	if msg.Password == "" {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return
	}

	userID := apiItems[0]
	pass := msg.Password
	if glog.V(4) {
		glog.Infof("User: %s, pass: %s\n", userID, pass)
	}

	tokenString, err := a.users.issueToken(userID, pass)
	if err != nil {
		glog.Errorf("Failed to generate token: %v\n", err)
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return
	}

	repl := fmt.Sprintf(`{"token": "%s"}`, tokenString)
	a.writeJSON(w, r, []byte(repl), "auth")
}

// Borrowed from DFC (modified cmn.InvalidHandler calls)
func (a *authServ) writeJSON(w http.ResponseWriter, r *http.Request, jsbytes []byte, tag string) (ok bool) {
	w.Header().Set("Content-Type", "application/json")
	var err error
	if _, err = w.Write(jsbytes); err == nil {
		ok = true
		return
	}
	if isSyscallWriteError(err) {
		// apparently, cannot write to this w: broken-pipe and similar
		s := "isSyscallWriteError: " + r.Method + " " + r.URL.Path + " from " + r.RemoteAddr
		glog.Errorf("isSyscallWriteError: %v [%s]", err, s)
		return
	}
	errstr := fmt.Sprintf("%s: Failed to write json, err: %v", tag, err)
	cmn.InvalidHandlerWithMsg(w, r, errstr, http.StatusInternalServerError)
	return
}

// Removes user credentials
// On successful update the function sends new credentials list to primary
//   proxy to update the cluster
func (a *authServ) userRemoveCredentials(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 2, cmn.Version, pathUsers)
	if err != nil {
		return
	}

	userID := apiItems[0]
	provider := apiItems[1]
	if !isValidProvider(provider) {
		errmsg := fmt.Sprintf("Invalid cloud provider: %s", provider)
		cmn.InvalidHandlerWithMsg(w, r, errmsg, http.StatusBadRequest)
		return
	}

	if glog.V(4) {
		glog.Infof("Removing %s credentials for %s\n", provider, userID)
	}

	if _, err = a.users.deleteCredentials(userID, provider); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("Failed to delete credentials: %v", err), http.StatusBadRequest)
		return
	}

	a.writeJSON(w, r, []byte("Credentials updated successfully"), "update credentials")
}

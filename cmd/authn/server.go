// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

const (
	pathRoles    = "roles"
	pathUsers    = "users"
	pathTokens   = "tokens"
	pathClusters = "clusters"
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
	a.registerHandler(cmn.URLPath(cmn.Version, pathClusters), a.clusterHandler)
	a.registerHandler(cmn.URLPath(cmn.Version, pathRoles), a.roleHandler)
}

func (a *authServ) userHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		a.httpUserDel(w, r)
	case http.MethodPost:
		a.httpUserPost(w, r)
	case http.MethodPut:
		a.httpUserPut(w, r)
	case http.MethodGet:
		a.httpUserGet(w, r)
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

func (a *authServ) clusterHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.httpSrvGet(w, r)
	case http.MethodPost:
		a.httpSrvPost(w, r)
	case http.MethodPut:
		a.httpSrvPut(w, r)
	case http.MethodDelete:
		a.httpSrvDelete(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "Unsupported method for /cluster handler")
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

	if len(apiItems) == 0 {
		cmn.InvalidHandlerWithMsg(w, r, "User name is not defined")
		return
	}
	if err := a.users.delUser(apiItems[0]); err != nil {
		glog.Errorf("Failed to delete user: %v\n", err)
		cmn.InvalidHandlerWithMsg(w, r, "Failed to delete user: "+err.Error())
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
	updateReq := &cmn.AuthUser{}
	err = jsoniter.NewDecoder(r.Body).Decode(updateReq)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, "Invalid request")
		return
	}

	if glog.V(4) {
		glog.Infof("Received credentials for %s\n", userID)
	}
	if err := a.users.updateUser(userID, updateReq); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
}

// Adds a new user to user list
func (a *authServ) userAdd(w http.ResponseWriter, r *http.Request) {
	if err := a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v\n", err)
		return
	}

	info := &cmn.AuthUser{}
	if err := cmn.ReadJSON(w, r, info); err != nil {
		glog.Errorf("Failed to read credentials: %v\n", err)
		return
	}

	if err := a.users.addUser(info); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("Failed to add user: %v", err), http.StatusInternalServerError)
		return
	}
	if glog.V(4) {
		glog.Infof("Added a user %s\n", info.UserID)
	}
}

// Returns list of users (without superusers)
func (a *authServ) httpUserGet(w http.ResponseWriter, r *http.Request) {
	_, err := checkRESTItems(w, r, 0, cmn.Version, pathUsers)
	if err != nil {
		return
	}

	var users map[string]*cmn.AuthUser
	if users, err = a.users.userList(); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}

	a.writeJSON(w, users, "list users")
}

// Checks if the request header contains super-user credentials and they are
// valid. Super-user is a user created at deployment time that cannot be
// deleted/created via REST API
func (a *authServ) checkAuthorization(w http.ResponseWriter, r *http.Request) error {
	s := strings.SplitN(r.Header.Get(cmn.HeaderAuthorization), " ", 2)
	if len(s) != 2 {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("invalid header")
	}

	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("invalid header authorization")
	}

	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("invalid header authorization")
	}

	if pair[0] != conf.Auth.Username || pair[1] != conf.Auth.Password {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return fmt.Errorf("invalid credentials")
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
	a.writeBytes(w, []byte(repl), "auth")
}

// Borrowed from ais (modified cmn.InvalidHandler calls)
func (a *authServ) writeJSON(w http.ResponseWriter, val interface{}, tag string) {
	w.Header().Set("Content-Type", "application/json")
	err := jsoniter.NewEncoder(w).Encode(val)
	a.processError(tag, err)
}

// Borrowed from ais (modified cmn.InvalidHandler calls)
func (a *authServ) writeBytes(w http.ResponseWriter, jsbytes []byte, tag string) {
	w.Header().Set("Content-Type", "application/json")
	var err error
	if _, err = w.Write(jsbytes); err == nil {
		return
	}
	a.processError(tag, err)
}
func (a *authServ) processError(tag string, err error) {
	glog.Errorf("%s: failed to write json, err: %v", tag, err)
}

func (a *authServ) httpSrvPost(w http.ResponseWriter, r *http.Request) {
	if _, err := checkRESTItems(w, r, 0, cmn.Version, pathClusters); err != nil {
		return
	}
	cluConf := &clusterConfig{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		glog.Errorf("Failed to read request body: %v\n", err)
		return
	}
	if len(cluConf.Conf) == 0 {
		cmn.InvalidHandlerWithMsg(w, r, "Empty cluster list", http.StatusBadRequest)
		return
	}
	conf.Cluster.mtx.Lock()
	defer conf.Cluster.mtx.Unlock()
	for cid := range cluConf.Conf {
		if conf.clusterExists(cid) {
			msg := fmt.Sprintf("Cluster %q already registered", cid)
			cmn.InvalidHandlerWithMsg(w, r, msg, http.StatusConflict)
			return
		}
	}
	if err := conf.updateClusters(cluConf); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := conf.save(); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *authServ) httpSrvPut(w http.ResponseWriter, r *http.Request) {
	if _, err := checkRESTItems(w, r, 0, cmn.Version, pathClusters); err != nil {
		return
	}
	cluConf := &clusterConfig{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		glog.Errorf("Failed to read request body: %v\n", err)
		return
	}
	if len(cluConf.Conf) == 0 {
		cmn.InvalidHandlerWithMsg(w, r, "Empty cluster list", http.StatusBadRequest)
		return
	}
	conf.Cluster.mtx.Lock()
	defer conf.Cluster.mtx.Unlock()
	for cid := range cluConf.Conf {
		if !conf.clusterExists(cid) {
			msg := fmt.Sprintf("Cluster %q not found", cid)
			cmn.InvalidHandlerWithMsg(w, r, msg, http.StatusNotFound)
			return
		}
	}
	if err := conf.updateClusters(cluConf); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := conf.save(); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *authServ) httpSrvDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, cmn.Version, pathClusters)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		cmn.InvalidHandlerWithMsg(w, r, "Cluster name or ID is not defined", http.StatusInternalServerError)
		return
	}
	conf.Cluster.mtx.Lock()
	defer conf.Cluster.mtx.Unlock()
	if !conf.clusterExists(apiItems[0]) {
		msg := fmt.Sprintf("Cluster %q not found", apiItems[0])
		cmn.InvalidHandlerWithMsg(w, r, msg, http.StatusNotFound)
		return
	}
	conf.deleteCluster(apiItems[0])
	if err := conf.save(); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *authServ) httpSrvGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, cmn.Version, pathClusters)
	if err != nil {
		return
	}
	conf.Cluster.mtx.RLock()
	defer conf.Cluster.mtx.RUnlock()
	cluList := &conf.Cluster
	if len(apiItems) != 0 {
		cid := apiItems[0]
		urls, ok := cluList.Conf[cid]
		if !ok {
			msg := fmt.Sprintf("Cluster %q not found", cid)
			cmn.InvalidHandlerWithMsg(w, r, msg, http.StatusNotFound)
			return
		}
		cluList = &clusterConfig{Conf: map[string][]string{cid: urls}}
	}
	a.writeJSON(w, cluList, "auth")
}

func (a *authServ) roleHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.httpRoleList(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "Unsupported method for /users handler")
	}
}

func (a *authServ) httpRoleList(w http.ResponseWriter, r *http.Request) {
	roles, err := a.users.roleList()
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	a.writeJSON(w, roles, "rolelist")
}

// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

func checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int, items []string) ([]string, error) {
	items, err := cmn.MatchRESTItems(r.URL.Path, itemsAfter, true, items)
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
	a.registerHandler(cmn.URLPathUsers.S, a.userHandler)
	a.registerHandler(cmn.URLPathTokens.S, a.tokenHandler)
	a.registerHandler(cmn.URLPathClusters.S, a.clusterHandler)
	a.registerHandler(cmn.URLPathRoles.S, a.roleHandler)
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
	if _, err := checkRESTItems(w, r, 0, cmn.URLPathTokens.L); err != nil {
		return
	}

	msg := &cmn.TokenMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil || msg.Token == "" {
		glog.Errorf("Failed to read request: %v\n", err)
		return
	}

	_, err := cmn.DecryptToken(msg.Token, conf.Auth.Secret)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}

	a.users.revokeToken(msg.Token)
}

func (a *authServ) httpUserDel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, cmn.URLPathUsers.L)
	if err != nil {
		return
	}

	if err = a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v\n", err)
		return
	}
	if err := a.users.delUser(apiItems[0]); err != nil {
		glog.Errorf("Failed to delete user: %v\n", err)
		cmn.InvalidHandlerWithMsg(w, r, "Failed to delete user: "+err.Error())
	}
}

func (a *authServ) httpUserPost(w http.ResponseWriter, r *http.Request) {
	if apiItems, err := checkRESTItems(w, r, 0, cmn.URLPathUsers.L); err != nil {
		return
	} else if len(apiItems) == 0 {
		a.userAdd(w, r)
	} else {
		a.userLogin(w, r)
	}
}

// Updates user credentials
func (a *authServ) httpUserPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, cmn.URLPathUsers.L)
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
		glog.Infof("Added a user %s\n", info.ID)
	}
}

// Returns list of users (without superusers)
func (a *authServ) httpUserGet(w http.ResponseWriter, r *http.Request) {
	items, err := checkRESTItems(w, r, 0, cmn.URLPathUsers.L)
	if err != nil {
		return
	}

	if len(items) > 1 {
		cmn.InvalidHandlerWithMsg(w, r, "invalid request")
		return
	}

	var users map[string]*cmn.AuthUser
	if len(items) == 0 {
		if users, err = a.users.userList(); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
		for _, uInfo := range users {
			uInfo.Password = ""
		}
		a.writeJSON(w, users, "list users")
		return
	}

	uInfo, err := a.users.lookupUser(items[0])
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	uInfo.Password = ""
	clusters, err := a.users.clusterList()
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	for _, clu := range uInfo.Clusters {
		if cInfo, ok := clusters[clu.ID]; ok {
			clu.Alias = cInfo.Alias
		}
	}
	a.writeJSON(w, uInfo, "user info")
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
	secret := conf.Auth.Secret
	token, err := cmn.DecryptToken(s[1], secret)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return err
	}
	if !token.IsAdmin {
		msg := "insufficient permissions"
		cmn.InvalidHandlerWithMsg(w, r, msg, http.StatusUnauthorized)
		return errors.New(msg)
	}

	return nil
}

// Generate a token for a user if provided credentials are valid.
// If a token is already issued and it is not expired yet then the old
// token is returned
func (a *authServ) userLogin(w http.ResponseWriter, r *http.Request) {
	var err error

	apiItems, err := checkRESTItems(w, r, 1, cmn.URLPathUsers.L)
	if err != nil {
		return
	}

	msg := &cmn.LoginMsg{}
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

	tokenString, err := a.users.issueToken(userID, pass, msg.ExpiresIn)
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
	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	var err error
	if err = jsoniter.NewEncoder(w).Encode(val); err == nil {
		return
	}
	glog.Errorf("%s: failed to write json, err: %v", tag, err)
}

// Borrowed from ais (modified cmn.InvalidHandler calls)
func (a *authServ) writeBytes(w http.ResponseWriter, jsbytes []byte, tag string) {
	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	var err error
	if _, err = w.Write(jsbytes); err == nil {
		return
	}
	glog.Errorf("%s: failed to write json, err: %v", tag, err)
}

func (a *authServ) httpSrvPost(w http.ResponseWriter, r *http.Request) {
	if _, err := checkRESTItems(w, r, 0, cmn.URLPathClusters.L); err != nil {
		return
	}
	if err := a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v", err)
		return
	}

	cluConf := &cmn.AuthCluster{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		glog.Errorf("Failed to read request body: %v\n", err)
		return
	}
	if err := a.users.addCluster(cluConf); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *authServ) httpSrvPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, cmn.URLPathClusters.L)
	if err != nil {
		return
	}
	if err := a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v", err)
		return
	}

	cluID := apiItems[0]
	cluConf := &cmn.AuthCluster{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		glog.Errorf("Failed to read request body: %v\n", err)
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := a.users.updateCluster(cluID, cluConf); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
	}
}

func (a *authServ) httpSrvDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, cmn.URLPathClusters.L)
	if err != nil {
		return
	}
	if err = a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v", err)
		return
	}

	if len(apiItems) == 0 {
		cmn.InvalidHandlerWithMsg(w, r, "Cluster name or ID is not defined", http.StatusInternalServerError)
		return
	}
	if err := a.users.delCluster(apiItems[0]); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
	}
}

func (a *authServ) httpSrvGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, cmn.URLPathClusters.L)
	if err != nil {
		return
	}
	var cluList *cmn.AuthClusterList
	if len(apiItems) != 0 {
		cid := apiItems[0]
		clu, ok := cluList.Clusters[cid]
		if !ok {
			msg := fmt.Sprintf("Cluster %q not found", cid)
			cmn.InvalidHandlerWithMsg(w, r, msg, http.StatusNotFound)
			return
		}
		cluList = &cmn.AuthClusterList{
			Clusters: map[string]*cmn.AuthCluster{cid: {ID: cid, URLs: clu.URLs}},
		}
	} else {
		clusters, err := a.users.clusterList()
		if err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
			return
		}
		cluList = &cmn.AuthClusterList{Clusters: clusters}
	}
	a.writeJSON(w, cluList, "auth")
}

func (a *authServ) roleHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		a.httpRolePost(w, r)
	case http.MethodPut:
		a.httpRolePut(w, r)
	case http.MethodDelete:
		a.httpRoleDel(w, r)
	case http.MethodGet:
		a.httpRoleGet(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "Unsupported method for /roles handler")
	}
}

func (a *authServ) httpRoleGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, cmn.URLPathRoles.L)
	if err != nil {
		return
	}
	if len(apiItems) > 1 {
		cmn.InvalidHandlerWithMsg(w, r, "invalid request")
		return
	}

	if len(apiItems) == 0 {
		roles, err := a.users.roleList()
		if err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
		a.writeJSON(w, roles, "rolelist")
		return
	}

	role, err := a.users.lookupRole(apiItems[0])
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	clusters, err := a.users.clusterList()
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	for _, clu := range role.Clusters {
		if cInfo, ok := clusters[clu.ID]; ok {
			clu.Alias = cInfo.Alias
		}
	}
	a.writeJSON(w, role, "role")
}

func (a *authServ) httpRoleDel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, cmn.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v", err)
		return
	}

	roleID := apiItems[0]
	if err = a.users.delRole(roleID); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
	}
}

func (a *authServ) httpRolePost(w http.ResponseWriter, r *http.Request) {
	_, err := checkRESTItems(w, r, 0, cmn.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v", err)
		return
	}

	info := &cmn.AuthRole{}
	if err := cmn.ReadJSON(w, r, info); err != nil {
		glog.Errorf("Failed to read role: %v", err)
		return
	}

	if err := a.users.addRole(info); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("Failed to add role: %v", err), http.StatusInternalServerError)
	}
}

func (a *authServ) httpRolePut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, cmn.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = a.checkAuthorization(w, r); err != nil {
		glog.Errorf("Not authorized: %v", err)
		return
	}

	role := apiItems[0]
	updateReq := &cmn.AuthRole{}
	err = jsoniter.NewDecoder(r.Body).Decode(updateReq)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, "Invalid request")
		return
	}

	if glog.V(4) {
		glog.Infof("Update role %s\n", role)
	}
	if err := a.users.updateRole(role, updateReq); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
	}
}

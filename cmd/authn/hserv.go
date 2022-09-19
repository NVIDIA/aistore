// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

const svcName = "AuthN"

type hserv struct {
	mux *http.ServeMux
	s   *http.Server
	mgr *mgr
}

func newServer(mgr *mgr) *hserv {
	srv := &hserv{mgr: mgr}
	srv.mux = http.NewServeMux()

	return srv
}

// Run public server to manage users and generate tokens
func (h *hserv) Run() (err error) {
	portstring := fmt.Sprintf(":%d", Conf.Net.HTTP.Port)
	glog.Infof("Listening on *:%s", portstring)

	h.registerPublicHandlers()
	h.s = &http.Server{Addr: portstring, Handler: h.mux}
	if Conf.Net.HTTP.UseHTTPS {
		if err = h.s.ListenAndServeTLS(Conf.Net.HTTP.Certificate, Conf.Net.HTTP.Key); err == nil {
			return nil
		}
		goto rerr
	}
	if err = h.s.ListenAndServe(); err == nil {
		return nil
	}
rerr:
	if err != http.ErrServerClosed {
		glog.Errorf("Terminated with err: %v", err)
		return err
	}
	return nil
}

func (h *hserv) registerHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	h.mux.HandleFunc(path, handler)
	if !strings.HasSuffix(path, "/") {
		h.mux.HandleFunc(path+"/", handler)
	}
}

func (h *hserv) registerPublicHandlers() {
	h.registerHandler(apc.URLPathUsers.S, h.userHandler)
	h.registerHandler(apc.URLPathTokens.S, h.tokenHandler)
	h.registerHandler(apc.URLPathClusters.S, h.clusterHandler)
	h.registerHandler(apc.URLPathRoles.S, h.roleHandler)
	h.registerHandler(apc.URLPathDae.S, configHandler)
}

func (h *hserv) userHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		h.httpUserDel(w, r)
	case http.MethodPost:
		h.httpUserPost(w, r)
	case http.MethodPut:
		h.httpUserPut(w, r)
	case http.MethodGet:
		h.httpUserGet(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

func (h *hserv) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		h.httpRevokeToken(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete)
	}
}

func (h *hserv) clusterHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.httpSrvGet(w, r)
	case http.MethodPost:
		h.httpSrvPost(w, r)
	case http.MethodPut:
		h.httpSrvPut(w, r)
	case http.MethodDelete:
		h.httpSrvDelete(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

// Deletes existing token, h.k.h log out
func (h *hserv) httpRevokeToken(w http.ResponseWriter, r *http.Request) {
	if _, err := checkRESTItems(w, r, 0, apc.URLPathTokens.L); err != nil {
		return
	}
	msg := &authn.TokenMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Token == "" {
		cmn.WriteErrMsg(w, r, "empty token")
		return
	}
	secret := Conf.Secret()
	if _, err := tok.DecryptToken(msg.Token, secret); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	h.mgr.revokeToken(msg.Token)
}

func (h *hserv) httpUserDel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	if err = validateAdminPerms(w, r); err != nil {
		return
	}
	if err := h.mgr.delUser(apiItems[0]); err != nil {
		glog.Errorf("Failed to delete user: %v\n", err)
		cmn.WriteErrMsg(w, r, "Failed to delete user: "+err.Error())
	}
}

func (h *hserv) httpUserPost(w http.ResponseWriter, r *http.Request) {
	if apiItems, err := checkRESTItems(w, r, 0, apc.URLPathUsers.L); err != nil {
		return
	} else if len(apiItems) == 0 {
		h.userAdd(w, r)
	} else {
		h.userLogin(w, r)
	}
}

// Updates user credentials
func (h *hserv) httpUserPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	if err = validateAdminPerms(w, r); err != nil {
		return
	}

	userID := apiItems[0]
	updateReq := &authn.User{}
	err = jsoniter.NewDecoder(r.Body).Decode(updateReq)
	if err != nil {
		cmn.WriteErrMsg(w, r, "Invalid request")
		return
	}
	if glog.V(4) {
		glog.Infof("PUT user %q", userID)
	}
	if err := h.mgr.updateUser(userID, updateReq); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
}

// Adds h new user to user list
func (h *hserv) userAdd(w http.ResponseWriter, r *http.Request) {
	if err := validateAdminPerms(w, r); err != nil {
		return
	}
	info := &authn.User{}
	if err := cmn.ReadJSON(w, r, info); err != nil {
		return
	}
	if err := h.mgr.addUser(info); err != nil {
		cmn.WriteErrMsg(w, r, fmt.Sprintf("Failed to add user: %v", err), http.StatusInternalServerError)
		return
	}
	if glog.V(4) {
		glog.Infof("Add user %q", info.ID)
	}
}

// Returns list of users (without superusers)
func (h *hserv) httpUserGet(w http.ResponseWriter, r *http.Request) {
	items, err := checkRESTItems(w, r, 0, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	if len(items) > 1 {
		cmn.WriteErrMsg(w, r, "invalid request")
		return
	}

	var users map[string]*authn.User
	if len(items) == 0 {
		if users, err = h.mgr.userList(); err != nil {
			cmn.WriteErr(w, r, err)
			return
		}
		for _, uInfo := range users {
			uInfo.Password = ""
		}
		writeJSON(w, users, "list users")
		return
	}

	uInfo, err := h.mgr.lookupUser(items[0])
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	uInfo.Password = ""
	clus, err := h.mgr.clus()
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	for _, clu := range uInfo.ClusterACLs {
		if cInfo, ok := clus[clu.ID]; ok {
			clu.Alias = cInfo.Alias
		}
	}
	writeJSON(w, uInfo, "user info")
}

// Checks if the request header contains valid admin credentials.
// (admin is created at deployment time and cannot be modified via API)
func validateAdminPerms(w http.ResponseWriter, r *http.Request) error {
	token, err := tok.ExtractToken(r.Header)
	if err != nil {
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return err
	}
	secret := Conf.Secret()
	tk, err := tok.DecryptToken(token, secret)
	if err != nil {
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return err
	}
	if tk.Expires.Before(time.Now()) {
		err := fmt.Errorf("not authorized: %s", tk)
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return err
	}
	if !tk.IsAdmin {
		err := fmt.Errorf("not authorized: requires admin (%s)", tk)
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return err
	}
	return nil
}

// Generate h token for h user if provided credentials are valid.
// If h token is already issued and it is not expired yet then the old
// token is returned
func (h *hserv) userLogin(w http.ResponseWriter, r *http.Request) {
	var err error
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	msg := &authn.LoginMsg{}
	if err = cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Password == "" {
		cmn.WriteErrMsg(w, r, "Not authorized", http.StatusUnauthorized)
		return
	}
	userID := apiItems[0]
	pass := msg.Password

	tokenString, err := h.mgr.issueToken(userID, pass, msg)
	if err != nil {
		glog.Errorf("Failed to generate token for user %q: %v\n", userID, err)
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return
	}

	repl := fmt.Sprintf(`{"token": %q}`, tokenString)
	writeBytes(w, []byte(repl), "auth")
}

func writeJSON(w http.ResponseWriter, val any, tag string) {
	w.Header().Set(cos.HdrContentType, cos.ContentJSON)
	var err error
	if err = jsoniter.NewEncoder(w).Encode(val); err == nil {
		return
	}
	glog.Errorf("%s: failed to write json, err: %v", tag, err)
}

func writeBytes(w http.ResponseWriter, jsbytes []byte, tag string) {
	w.Header().Set(cos.HdrContentType, cos.ContentJSON)
	var err error
	if _, err = w.Write(jsbytes); err == nil {
		return
	}
	glog.Errorf("%s: failed to write json, err: %v", tag, err)
}

func (h *hserv) httpSrvPost(w http.ResponseWriter, r *http.Request) {
	if _, err := checkRESTItems(w, r, 0, apc.URLPathClusters.L); err != nil {
		return
	}
	if err := validateAdminPerms(w, r); err != nil {
		return
	}
	cluConf := &authn.CluACL{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		return
	}
	if err := h.mgr.addCluster(cluConf); err != nil {
		cmn.WriteErr(w, r, err, http.StatusInternalServerError)
	}
}

func (h *hserv) httpSrvPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathClusters.L)
	if err != nil {
		return
	}
	if err := validateAdminPerms(w, r); err != nil {
		return
	}
	cluConf := &authn.CluACL{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		return
	}
	cluID := apiItems[0]
	if err := h.mgr.updateCluster(cluID, cluConf); err != nil {
		cmn.WriteErr(w, r, err, http.StatusInternalServerError)
	}
}

func (h *hserv) httpSrvDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, apc.URLPathClusters.L)
	if err != nil {
		return
	}
	if err = validateAdminPerms(w, r); err != nil {
		return
	}

	if len(apiItems) == 0 {
		cmn.WriteErrMsg(w, r, "cluster name or ID is not defined", http.StatusInternalServerError)
		return
	}
	if err := h.mgr.delCluster(apiItems[0]); err != nil {
		if cmn.IsErrNotFound(err) {
			cmn.WriteErr(w, r, err, http.StatusNotFound)
		} else {
			cmn.WriteErr(w, r, err, http.StatusInternalServerError)
		}
	}
}

func (h *hserv) httpSrvGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, apc.URLPathClusters.L)
	if err != nil {
		return
	}
	var cluList *authn.RegisteredClusters
	if len(apiItems) != 0 {
		cid := apiItems[0]
		clu, err := h.mgr.getCluster(cid)
		if err != nil {
			if cmn.IsErrNotFound(err) {
				cmn.WriteErr(w, r, err, http.StatusNotFound)
			} else {
				cmn.WriteErr(w, r, err, http.StatusInternalServerError)
			}
			return
		}
		cluList = &authn.RegisteredClusters{
			M: map[string]*authn.CluACL{clu.ID: clu},
		}
	} else {
		clus, err := h.mgr.clus()
		if err != nil {
			cmn.WriteErr(w, r, err, http.StatusInternalServerError)
			return
		}
		cluList = &authn.RegisteredClusters{M: clus}
	}
	writeJSON(w, cluList, "auth")
}

func (h *hserv) roleHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.httpRolePost(w, r)
	case http.MethodPut:
		h.httpRolePut(w, r)
	case http.MethodDelete:
		h.httpRoleDel(w, r)
	case http.MethodGet:
		h.httpRoleGet(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

func (h *hserv) httpRoleGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if len(apiItems) > 1 {
		cmn.WriteErrMsg(w, r, "invalid request")
		return
	}

	if len(apiItems) == 0 {
		roles, err := h.mgr.roleList()
		if err != nil {
			cmn.WriteErr(w, r, err)
			return
		}
		writeJSON(w, roles, "rolelist")
		return
	}

	role, err := h.mgr.lookupRole(apiItems[0])
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	clus, err := h.mgr.clus()
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	for _, clu := range role.ClusterACLs {
		if cInfo, ok := clus[clu.ID]; ok {
			clu.Alias = cInfo.Alias
		}
	}
	writeJSON(w, role, "role")
}

func (h *hserv) httpRoleDel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = validateAdminPerms(w, r); err != nil {
		return
	}

	roleID := apiItems[0]
	if err = h.mgr.delRole(roleID); err != nil {
		cmn.WriteErr(w, r, err)
	}
}

func (h *hserv) httpRolePost(w http.ResponseWriter, r *http.Request) {
	_, err := checkRESTItems(w, r, 0, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = validateAdminPerms(w, r); err != nil {
		return
	}
	info := &authn.Role{}
	if err := cmn.ReadJSON(w, r, info); err != nil {
		return
	}
	if err := h.mgr.addRole(info); err != nil {
		cmn.WriteErrMsg(w, r, fmt.Sprintf("Failed to add role: %v", err), http.StatusInternalServerError)
	}
}

func (h *hserv) httpRolePut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = validateAdminPerms(w, r); err != nil {
		return
	}

	role := apiItems[0]
	updateReq := &authn.Role{}
	err = jsoniter.NewDecoder(r.Body).Decode(updateReq)
	if err != nil {
		cmn.WriteErrMsg(w, r, "Invalid request")
		return
	}
	if glog.V(4) {
		glog.Infof("PUT role %q\n", role)
	}
	if err := h.mgr.updateRole(role, updateReq); err != nil {
		if cmn.IsErrNotFound(err) {
			cmn.WriteErr(w, r, err, http.StatusNotFound)
		} else {
			cmn.WriteErr(w, r, err)
		}
	}
}

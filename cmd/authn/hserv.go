// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"

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

func parseURL(w http.ResponseWriter, r *http.Request, itemsAfter int, items []string) ([]string, error) {
	items, err := cmn.ParseURL(r.URL.Path, items, itemsAfter, true)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return nil, err
	}
	return items, err
}

func (h *hserv) failAction(w http.ResponseWriter, r *http.Request, action, what string, err error, code int) {
	err = cmn.NewErrFailedTo(h.mgr, action, what, err, code)
	cmn.WriteErr(w, r, err, code)
}

// Run public server to manage users and generate tokens
func (h *hserv) Run() error {
	var (
		portStr    string
		err        error
		useHTTPS   bool
		serverCert string
		serverKey  string
	)

	// Retrieve and set the port
	portStr = os.Getenv(env.AisAuthPort)
	if portStr == "" {
		portStr = fmt.Sprintf(":%d", Conf.Net.HTTP.Port)
	} else {
		portStr = ":" + portStr
	}
	nlog.Infof("Listening on %s", portStr)

	h.registerPublicHandlers()
	h.s = &http.Server{
		Addr:              portStr,
		Handler:           h.mux,
		ReadHeaderTimeout: apc.ReadHeaderTimeout,
	}
	if timeout, isSet := cmn.ParseReadHeaderTimeout(); isSet { // optional env var
		h.s.ReadHeaderTimeout = timeout
	}

	// Retrieve and set HTTPS configuration with environment variables taking precedence
	useHTTPS, err = cos.IsParseEnvBoolOrDefault(env.AisAuthUseHTTPS, Conf.Net.HTTP.UseHTTPS)
	if err != nil {
		nlog.Errorf("Failed to parse %s: %v. Defaulting to false", env.AisAuthUseHTTPS, err)
	}
	serverCert = cos.GetEnvOrDefault(env.AisAuthServerCrt, Conf.Net.HTTP.Certificate)
	serverKey = cos.GetEnvOrDefault(env.AisAuthServerKey, Conf.Net.HTTP.Key)

	// Start the appropriate server based on the configuration
	if useHTTPS {
		nlog.Infof("Starting HTTPS server on port%s", portStr)
		nlog.Infof("Certificate: %s", serverCert)
		nlog.Infof("Key: %s", serverKey)
		err = h.s.ListenAndServeTLS(serverCert, serverKey)
	} else {
		nlog.Infof("Starting HTTP server on port%s", portStr)
		err = h.s.ListenAndServe()
	}

	if err != nil && err != http.ErrServerClosed {
		nlog.Errorf("Server terminated with error: %v", err)
		return err
	}
	return nil
}

func (h *hserv) registerHandler(path string, handler func(http.ResponseWriter, *http.Request)) {
	h.mux.HandleFunc(path, handler)
	if !cos.IsLastB(path, '/') {
		h.mux.HandleFunc(path+"/", handler)
	}
}

func (h *hserv) registerPublicHandlers() {
	h.registerHandler(apc.URLPathUsers.S, h.userHandler)
	h.registerHandler(apc.URLPathTokens.S, h.tokenHandler)
	h.registerHandler(apc.URLPathClusters.S, h.clusterHandler)
	h.registerHandler(apc.URLPathRoles.S, h.roleHandler)
	h.registerHandler(apc.URLPathDae.S, h.configHandler)
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
	if _, err := parseURL(w, r, 0, apc.URLPathTokens.L); err != nil {
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
	if _, err := h.mgr.tkParser.ValidateToken(r.Context(), msg.Token); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	code, err := h.mgr.revokeToken(msg.Token)
	if err != nil {
		h.failAction(w, r, "revoke token", msg.Token, err, code)
	}
}

func (h *hserv) httpUserDel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 1, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	if err = h.validateAdminPerms(w, r); err != nil {
		return
	}
	userID := apiItems[0]
	if code, err := h.mgr.delUser(userID); err != nil {
		h.failAction(w, r, "delete user", userID, err, code)
	}
}

func (h *hserv) httpUserPost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 0, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		h.userAdd(w, r)
	} else {
		h.userLogin(w, r)
	}
}

// Updates user credentials
func (h *hserv) httpUserPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 1, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	var (
		userID    = apiItems[0]
		updateReq = &authn.User{}
	)
	err = jsoniter.NewDecoder(r.Body).Decode(updateReq)
	if err != nil {
		cmn.WriteErrMsg(w, r, "Invalid request")
		return
	}
	if err = h.validateUpdatePerms(w, r, userID, updateReq); err != nil {
		return
	}
	if Conf.Verbose() {
		nlog.Infof("PUT user %q", userID)
	}
	if code, err := h.mgr.updateUser(userID, updateReq); err != nil {
		h.failAction(w, r, "update user", userID, err, code)
	}
}

// Adds h new user to user list
func (h *hserv) userAdd(w http.ResponseWriter, r *http.Request) {
	if err := h.validateAdminPerms(w, r); err != nil {
		return
	}
	info := &authn.User{}
	if err := cmn.ReadJSON(w, r, info); err != nil {
		return
	}
	if code, err := h.mgr.addUser(info); err != nil {
		h.failAction(w, r, "add user", info.ID, err, code)
		return
	}
	if Conf.Verbose() {
		nlog.Infof("Add user %q", info.ID)
	}
}

// Returns list of users (without superusers)
func (h *hserv) httpUserGet(w http.ResponseWriter, r *http.Request) {
	items, err := parseURL(w, r, 0, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	if len(items) > 1 {
		cmn.WriteErrMsg(w, r, "invalid request")
		return
	}
	var (
		users map[string]*authn.User
		code  int
	)
	if len(items) == 0 {
		if err := h.validateAdminPerms(w, r); err != nil {
			return
		}
		if users, code, err = h.mgr.userList(); err != nil {
			cmn.WriteErr(w, r, err, code)
			return
		}
		for _, uInfo := range users {
			uInfo.Password = ""
		}
		writeJSON(w, users, "list users")
		return
	}
	tk, err := h.getToken(r)
	if err != nil {
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return
	}
	reqUser := items[0]
	if !tk.IsAdmin && !tk.IsUser(reqUser) {
		err := errors.New("not authorized: requires admin or self")
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return
	}
	uInfo, code, err := h.mgr.lookupUser(items[0])
	if err != nil {
		cmn.WriteErr(w, r, err, code)
		return
	}
	uInfo.Password = ""
	writeJSON(w, uInfo, "get user")
}

func (h *hserv) getToken(r *http.Request) (*tok.AISClaims, error) {
	tokenHdr, err := tok.ExtractToken(r.Header)
	if err != nil {
		return nil, err
	}
	claims, err := h.mgr.tkParser.ValidateToken(r.Context(), tokenHdr.Token)
	if err != nil {
		if errors.Is(err, tok.ErrInvalidToken) {
			return nil, fmt.Errorf("not authorized (token expired): %q", tokenHdr.Token)
		}
		return nil, err
	}
	return claims, nil
}

// Checks if the request header contains valid admin credentials.
// (admin is created at deployment time and cannot be modified via API)
func (h *hserv) validateAdminPerms(w http.ResponseWriter, r *http.Request) error {
	tk, err := h.getToken(r)
	if err != nil {
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

func (h *hserv) validateUpdatePerms(w http.ResponseWriter, r *http.Request, userID string, updateReq *authn.User) error {
	tk, err := h.getToken(r)
	if err != nil {
		cmn.WriteErr(w, r, err, http.StatusUnauthorized)
		return err
	}
	if tk.IsAdmin {
		return nil
	}
	if tk.IsUser(userID) && len(updateReq.Roles) == 0 {
		return nil
	}
	err = fmt.Errorf("not authorized: (%s)", tk)
	cmn.WriteErr(w, r, err, http.StatusUnauthorized)
	return err
}

// Generate h token for h user if provided credentials are valid.
// If h token is already issued and it is not expired yet then the old
// token is returned
func (h *hserv) userLogin(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 1, apc.URLPathUsers.L)
	if err != nil {
		return
	}
	msg := &authn.LoginMsg{}
	if err = cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Password == "" {
		cmn.WriteErrMsg(w, r, "empty password", http.StatusUnauthorized)
		return
	}

	var (
		token  string
		code   int
		userID = apiItems[0]
	)
	if token, code, err = h.mgr.issueToken(userID, msg.Password, msg); err != nil {
		h.failAction(w, r, "generate token for", userID, err, code)
		return
	}

	repl := fmt.Sprintf(`{"token": %q}`, token)
	writeBytes(w, cos.UnsafeB(repl), "login")
}

func writeJSON(w http.ResponseWriter, val any, tag string) {
	w.Header().Set(cos.HdrContentType, cos.ContentJSON)
	if err := jsoniter.NewEncoder(w).Encode(val); err != nil {
		nlog.Errorf("%s: failed to write response: %v", tag, err)
	}
}

func writeBytes(w http.ResponseWriter, jsbytes []byte, tag string) {
	w.Header().Set(cos.HdrContentType, cos.ContentJSON)
	if _, err := w.Write(jsbytes); err != nil {
		nlog.Errorf("%s: failed to write response: %v", tag, err)
	}
}

func (h *hserv) httpSrvPost(w http.ResponseWriter, r *http.Request) {
	if _, err := parseURL(w, r, 0, apc.URLPathClusters.L); err != nil {
		return
	}
	if err := h.validateAdminPerms(w, r); err != nil {
		return
	}
	cluConf := &authn.CluACL{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		return
	}
	if code, err := h.mgr.addCluster(r.Context(), cluConf); err != nil {
		h.failAction(w, r, "add cluster", cluConf.ID, err, code)
	}
}

func (h *hserv) httpSrvPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 1, apc.URLPathClusters.L)
	if err != nil {
		return
	}
	if err := h.validateAdminPerms(w, r); err != nil {
		return
	}
	cluConf := &authn.CluACL{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		return
	}
	cluID := apiItems[0]
	if code, err := h.mgr.updateCluster(cluID, cluConf); err != nil {
		h.failAction(w, r, "update cluster", cluID, err, code)
	}
}

func (h *hserv) httpSrvDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 0, apc.URLPathClusters.L)
	if err != nil {
		return
	}
	if err = h.validateAdminPerms(w, r); err != nil {
		return
	}
	cluID := apiItems[0]
	if len(apiItems) == 0 {
		err = errors.New("cluster name or ID not defined")
		h.failAction(w, r, "delete cluster", cluID, err, http.StatusBadRequest)
		return
	}
	if code, err := h.mgr.delCluster(cluID); err != nil {
		h.failAction(w, r, "delete cluster", cluID, err, code)
	}
}

func (h *hserv) httpSrvGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 0, apc.URLPathClusters.L)
	if err != nil {
		return
	}
	var cluList *authn.RegisteredClusters
	if len(apiItems) != 0 {
		cid := apiItems[0]
		clu, code, err := h.mgr.getCluster(cid)
		if err != nil {
			cmn.WriteErr(w, r, err, code)
			return
		}
		cluList = &authn.RegisteredClusters{
			Clusters: map[string]*authn.CluACL{clu.ID: clu},
		}
	} else {
		clus, code, err := h.mgr.clus()
		if err != nil {
			cmn.WriteErr(w, r, err, code)
			return
		}
		cluList = &authn.RegisteredClusters{Clusters: clus}
	}
	writeJSON(w, cluList, "get cluster")
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
	apiItems, err := parseURL(w, r, 0, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if len(apiItems) > 1 {
		cmn.WriteErrMsg(w, r, "invalid request")
		return
	}

	if len(apiItems) == 0 {
		roles, code, err := h.mgr.roleList()
		if err != nil {
			cmn.WriteErr(w, r, err, code)
			return
		}
		writeJSON(w, roles, "list roles")
		return
	}

	role, code, err := h.mgr.lookupRole(apiItems[0])
	if err != nil {
		cmn.WriteErr(w, r, err, code)
		return
	}
	clus, code, err := h.mgr.clus()
	if err != nil {
		cmn.WriteErr(w, r, err, code)
		return
	}
	for _, clu := range role.ClusterACLs {
		if cInfo, ok := clus[clu.ID]; ok {
			clu.Alias = cInfo.Alias
		}
	}
	writeJSON(w, role, "get role")
}

func (h *hserv) httpRoleDel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 1, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = h.validateAdminPerms(w, r); err != nil {
		return
	}

	roleID := apiItems[0]
	if code, err := h.mgr.delRole(roleID); err != nil {
		h.failAction(w, r, "delete role", roleID, err, code)
	}
}

func (h *hserv) httpRolePost(w http.ResponseWriter, r *http.Request) {
	_, err := parseURL(w, r, 0, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = h.validateAdminPerms(w, r); err != nil {
		return
	}
	info := &authn.Role{}
	if err := cmn.ReadJSON(w, r, info); err != nil {
		return
	}
	if code, err := h.mgr.addRole(info); err != nil {
		h.failAction(w, r, "add role", info.Name, err, code)
	}
}

func (h *hserv) httpRolePut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 1, apc.URLPathRoles.L)
	if err != nil {
		return
	}
	if err = h.validateAdminPerms(w, r); err != nil {
		return
	}

	role := apiItems[0]
	updateReq := &authn.Role{}
	err = jsoniter.NewDecoder(r.Body).Decode(updateReq)
	if err != nil {
		cmn.WriteErrMsg(w, r, "Invalid request")
		return
	}
	if Conf.Verbose() {
		nlog.Infof("PUT role %q\n", role)
	}
	if code, err := h.mgr.updateRole(role, updateReq); err != nil {
		h.failAction(w, r, "update role", role, err, code)
	}
}

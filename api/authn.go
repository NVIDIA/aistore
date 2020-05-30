// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"errors"
	"net/http"
	"sort"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

type (
	AuthnSpec struct {
		AdminName     string
		AdminPassword string
	}

	ClusterSpec struct {
		AdminName     string
		AdminPassword string
		ClusterID     string
		URLs          []string
	}

	loginRec struct {
		Password string `json:"password"`
	}

	AuthCreds struct {
		Token string `json:"token"`
	}

	authClusterReg struct {
		Conf map[string][]string `json:"conf"`
	}

	AuthClusterRec struct {
		ID   string
		URLs []string
	}
)

func AddUser(baseParams BaseParams, spec AuthnSpec, newUser *cmn.AuthUser) error {
	msg, err := jsoniter.Marshal(newUser)
	if err != nil {
		return err
	}

	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Users),
		Body:       msg,
		User:       spec.AdminName,
		Password:   spec.AdminPassword,
	})
}

func DeleteUser(baseParams BaseParams, spec AuthnSpec, userID string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Users, userID),
		User:       spec.AdminName,
		Password:   spec.AdminPassword,
	})
}

func LoginUser(baseParams BaseParams, userID, pass string) (token *AuthCreds, err error) {
	baseParams.Method = http.MethodPost

	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Users, userID),
		Body:       cmn.MustMarshal(loginRec{Password: pass}),
	}, &token)
	if err != nil {
		return nil, err
	}

	if token.Token == "" {
		return nil, errors.New("login failed: empty response from AuthN server")
	}
	return token, nil
}

func RegisterClusterAuthN(baseParams BaseParams, spec ClusterSpec) error {
	req := authClusterReg{Conf: make(map[string][]string, 1)}
	req.Conf[spec.ClusterID] = spec.URLs
	msg, err := jsoniter.Marshal(req)
	if err != nil {
		return err
	}

	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Clusters),
		Body:       msg,
		User:       spec.AdminName,
		Password:   spec.AdminPassword,
	})
}

func UpdateClusterAuthN(baseParams BaseParams, spec ClusterSpec) error {
	req := authClusterReg{Conf: make(map[string][]string, 1)}
	req.Conf[spec.ClusterID] = spec.URLs
	msg, err := jsoniter.Marshal(req)
	if err != nil {
		return err
	}

	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Clusters),
		Body:       msg,
		User:       spec.AdminName,
		Password:   spec.AdminPassword,
	})
}

func UnregisterClusterAuthN(baseParams BaseParams, spec ClusterSpec) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Clusters, spec.ClusterID),
		User:       spec.AdminName,
		Password:   spec.AdminPassword,
	})
}

func GetClusterAuthN(baseParams BaseParams, spec ClusterSpec) ([]*AuthClusterRec, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Clusters)
	if spec.ClusterID != "" {
		path = cmn.URLPath(path, spec.ClusterID)
	}
	clusters := &authClusterReg{}
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       path,
	}, clusters)
	rec := make([]*AuthClusterRec, 0, len(clusters.Conf))
	for cid, urls := range clusters.Conf {
		clu := &AuthClusterRec{ID: cid, URLs: urls}
		rec = append(rec, clu)
	}
	less := func(i, j int) bool { return rec[i].ID < rec[j].ID }
	sort.Slice(rec, less)
	return rec, err
}

func GetRolesAuthN(baseParams BaseParams, spec ClusterSpec) ([]*cmn.AuthRole, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Roles)
	if spec.ClusterID != "" {
		path = cmn.URLPath(path, spec.ClusterID)
	}
	roles := make([]*cmn.AuthRole, 0)
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       path,
	}, &roles)
	return roles, err
}

func GetUsersAuthN(baseParams BaseParams) ([]*cmn.AuthUser, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Users)
	users := make(map[string]*cmn.AuthUser, 4)
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       path,
	}, &users)

	list := make([]*cmn.AuthUser, 0, len(users))
	for _, info := range users {
		list = append(list, info)
	}

	// TODO: better sorting
	less := func(i, j int) bool { return list[i].ID < list[j].ID }
	sort.Slice(list, less)

	return list, err
}

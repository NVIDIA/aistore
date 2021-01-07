// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

	loginRec struct {
		Password string `json:"password"`
	}

	AuthCreds struct {
		Token string `json:"token"`
	}
)

func AddUser(baseParams BaseParams, newUser *cmn.AuthUser) error {
	msg, err := jsoniter.Marshal(newUser)
	if err != nil {
		return err
	}

	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathUsers.S, Body: msg})
}

func UpdateUser(baseParams BaseParams, newUser *cmn.AuthUser) error {
	msg := cmn.MustMarshal(newUser)
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.URLPathUsers.S, newUser.ID),
		Body:       msg,
	})
}

func DeleteUser(baseParams BaseParams, userID string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.JoinWords(cmn.URLPathUsers.S, userID)})
}

func LoginUser(baseParams BaseParams, userID, pass string) (token *AuthCreds, err error) {
	baseParams.Method = http.MethodPost

	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.URLPathUsers.S, userID),
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

func RegisterClusterAuthN(baseParams BaseParams, cluSpec cmn.AuthCluster) error {
	msg := cmn.MustMarshal(cluSpec)
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusters.S, Body: msg})
}

func UpdateClusterAuthN(baseParams BaseParams, cluSpec cmn.AuthCluster) error {
	msg := cmn.MustMarshal(cluSpec)
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.URLPathClusters.S, cluSpec.ID),
		Body:       msg,
	})
}

func UnregisterClusterAuthN(baseParams BaseParams, spec cmn.AuthCluster) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.URLPathClusters.S, spec.ID),
	})
}

func GetClusterAuthN(baseParams BaseParams, spec cmn.AuthCluster) ([]*cmn.AuthCluster, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPathClusters.S
	if spec.ID != "" {
		path = cmn.JoinWords(path, spec.ID)
	}
	clusters := &cmn.AuthClusterList{}
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       path,
	}, clusters)
	rec := make([]*cmn.AuthCluster, 0, len(clusters.Clusters))
	for _, clu := range clusters.Clusters {
		rec = append(rec, clu)
	}
	less := func(i, j int) bool { return rec[i].ID < rec[j].ID }
	sort.Slice(rec, less)
	return rec, err
}

func GetRolesAuthN(baseParams BaseParams) ([]*cmn.AuthRole, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPathRoles.S
	roles := make([]*cmn.AuthRole, 0)
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       path,
	}, &roles)
	less := func(i, j int) bool { return roles[i].Name < roles[j].Name }
	sort.Slice(roles, less)
	return roles, err
}

func GetUsersAuthN(baseParams BaseParams) ([]*cmn.AuthUser, error) {
	baseParams.Method = http.MethodGet
	users := make(map[string]*cmn.AuthUser, 4)
	err := DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathUsers.S}, &users)

	list := make([]*cmn.AuthUser, 0, len(users))
	for _, info := range users {
		list = append(list, info)
	}

	// TODO: better sorting
	less := func(i, j int) bool { return list[i].ID < list[j].ID }
	sort.Slice(list, less)

	return list, err
}

func AddRoleAuthN(baseParams BaseParams, roleSpec *cmn.AuthRole) error {
	msg := cmn.MustMarshal(roleSpec)
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.URLPathRoles.S, Body: msg})
}

func DeleteRoleAuthN(baseParams BaseParams, role string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.URLPathRoles.S, role),
	})
}

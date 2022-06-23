// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

type AuthnSpec struct {
	AdminName     string
	AdminPassword string
}

func AddUserAuthN(baseParams BaseParams, newUser *authn.User) error {
	msg, err := jsoniter.Marshal(newUser)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UpdateUserAuthN(baseParams BaseParams, user *authn.User) error {
	msg := cos.MustMarshal(user)
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.Join(user.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func DeleteUserAuthN(baseParams BaseParams, userID string) error {
	baseParams.Method = http.MethodDelete
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.Join(userID)
	}
	return reqParams.DoHTTPRequest()
}

// Authorize a user and return a user token in case of success.
// The token expires in `expire` time. If `expire` is `nil` the expiration
// time is set by AuthN (default AuthN expiration time is 24 hours)
func LoginUserAuthN(baseParams BaseParams, userID, pass string, expire *time.Duration) (token *authn.TokenMsg, err error) {
	baseParams.Method = http.MethodPost
	rec := authn.LoginMsg{Password: pass, ExpiresIn: expire}
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.Join(userID)
		reqParams.Body = cos.MustMarshal(rec)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&token)
	if err != nil {
		return nil, err
	}

	if token.Token == "" {
		return nil, errors.New("login failed: empty response from AuthN server")
	}
	return token, nil
}

func RegisterClusterAuthN(baseParams BaseParams, cluSpec authn.Cluster) error {
	msg := cos.MustMarshal(cluSpec)
	baseParams.Method = http.MethodPost
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClusters.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UpdateClusterAuthN(baseParams BaseParams, cluSpec authn.Cluster) error {
	msg := cos.MustMarshal(cluSpec)
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClusters.Join(cluSpec.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UnregisterClusterAuthN(baseParams BaseParams, spec authn.Cluster) error {
	baseParams.Method = http.MethodDelete
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClusters.Join(spec.ID)
	}
	return reqParams.DoHTTPRequest()
}

func GetClusterAuthN(baseParams BaseParams, spec authn.Cluster) ([]*authn.Cluster, error) {
	baseParams.Method = http.MethodGet
	path := apc.URLPathClusters.S
	if spec.ID != "" {
		path = cos.JoinWords(path, spec.ID)
	}
	clusters := &authn.ClusterList{}
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
	}
	err := reqParams.DoHTTPReqResp(clusters)

	rec := make([]*authn.Cluster, 0, len(clusters.Clusters))
	for _, clu := range clusters.Clusters {
		rec = append(rec, clu)
	}
	less := func(i, j int) bool { return rec[i].ID < rec[j].ID }
	sort.Slice(rec, less)
	return rec, err
}

func GetRoleAuthN(baseParams BaseParams, roleID string) (*authn.Role, error) {
	if roleID == "" {
		return nil, errors.New("missing role ID")
	}
	rInfo := &authn.Role{}
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cos.JoinWords(apc.URLPathRoles.S, roleID)
	}
	err := reqParams.DoHTTPReqResp(&rInfo)
	return rInfo, err
}

func GetAllRolesAuthN(baseParams BaseParams) ([]*authn.Role, error) {
	baseParams.Method = http.MethodGet
	path := apc.URLPathRoles.S
	roles := make([]*authn.Role, 0)
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
	}
	err := reqParams.DoHTTPReqResp(&roles)

	less := func(i, j int) bool { return roles[i].ID < roles[j].ID }
	sort.Slice(roles, less)
	return roles, err
}

func GetAllUsersAuthN(baseParams BaseParams) ([]*authn.User, error) {
	baseParams.Method = http.MethodGet
	users := make(map[string]*authn.User, 4)
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.S
	}
	err := reqParams.DoHTTPReqResp(&users)

	list := make([]*authn.User, 0, len(users))
	for _, info := range users {
		list = append(list, info)
	}

	less := func(i, j int) bool { return list[i].ID < list[j].ID }
	sort.Slice(list, less)

	return list, err
}

func GetUserAuthN(baseParams BaseParams, userID string) (*authn.User, error) {
	if userID == "" {
		return nil, errors.New("missing user ID")
	}
	uInfo := &authn.User{}
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cos.JoinWords(apc.URLPathUsers.S, userID)
	}
	err := reqParams.DoHTTPReqResp(&uInfo)
	return uInfo, err
}

func AddRoleAuthN(baseParams BaseParams, roleSpec *authn.Role) error {
	msg := cos.MustMarshal(roleSpec)
	baseParams.Method = http.MethodPost
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathRoles.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UpdateRoleAuthN(baseParams BaseParams, roleSpec *authn.Role) error {
	msg := cos.MustMarshal(roleSpec)
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathRoles.Join(roleSpec.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func DeleteRoleAuthN(baseParams BaseParams, role string) error {
	baseParams.Method = http.MethodDelete
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathRoles.Join(role)
	}
	return reqParams.DoHTTPRequest()
}

func RevokeTokenAuthN(baseParams BaseParams, token string) error {
	baseParams.Method = http.MethodDelete
	msg := &authn.TokenMsg{Token: token}
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathTokens.S
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func GetConfigAuthN(baseParams BaseParams) (*authn.Config, error) {
	conf := &authn.Config{}
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathDae.S
	}
	err := reqParams.DoHTTPReqResp(&conf)
	return conf, err
}

func SetConfigAuthN(baseParams BaseParams, conf *authn.ConfigToUpdate) error {
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	defer freeRp(reqParams)
	{
		reqParams.Body = cos.MustMarshal(conf)
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathDae.S
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

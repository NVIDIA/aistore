// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

func AddUser(baseParams api.BaseParams, newUser *User) error {
	msg, err := jsoniter.Marshal(newUser)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UpdateUser(baseParams api.BaseParams, user *User) error {
	msg := cos.MustMarshal(user)
	baseParams.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.Join(user.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func DeleteUser(baseParams api.BaseParams, userID string) error {
	baseParams.Method = http.MethodDelete
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.Join(userID)
	}
	return reqParams.DoHTTPRequest()
}

// Authorize a user and return a user token in case of success.
// The token expires in `expire` time. If `expire` is `nil` the expiration
// time is set by AuthN (default AuthN expiration time is 24 hours)
func LoginUser(baseParams api.BaseParams, userID, pass string, expire *time.Duration) (token *TokenMsg, err error) {
	baseParams.Method = http.MethodPost
	rec := LoginMsg{Password: pass, ExpiresIn: expire}
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.Join(userID)
		reqParams.Body = cos.MustMarshal(rec)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
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

func RegisterCluster(baseParams api.BaseParams, cluSpec CluACL) error {
	msg := cos.MustMarshal(cluSpec)
	baseParams.Method = http.MethodPost
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClusters.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UpdateCluster(baseParams api.BaseParams, cluSpec CluACL) error {
	msg := cos.MustMarshal(cluSpec)
	baseParams.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClusters.Join(cluSpec.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UnregisterCluster(baseParams api.BaseParams, spec CluACL) error {
	baseParams.Method = http.MethodDelete
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathClusters.Join(spec.ID)
	}
	return reqParams.DoHTTPRequest()
}

func GetRegisteredClusters(baseParams api.BaseParams, spec CluACL) ([]*CluACL, error) {
	baseParams.Method = http.MethodGet
	path := apc.URLPathClusters.S
	if spec.ID != "" {
		path = cos.JoinWords(path, spec.ID)
	}
	clusters := &RegisteredClusters{}
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
	}
	err := reqParams.DoHTTPReqResp(clusters)

	rec := make([]*CluACL, 0, len(clusters.M))
	for _, clu := range clusters.M {
		rec = append(rec, clu)
	}
	less := func(i, j int) bool { return rec[i].ID < rec[j].ID }
	sort.Slice(rec, less)
	return rec, err
}

func GetRole(baseParams api.BaseParams, roleID string) (*Role, error) {
	if roleID == "" {
		return nil, errors.New("missing role ID")
	}
	rInfo := &Role{}
	baseParams.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cos.JoinWords(apc.URLPathRoles.S, roleID)
	}
	err := reqParams.DoHTTPReqResp(&rInfo)
	return rInfo, err
}

func GetAllRoles(baseParams api.BaseParams) ([]*Role, error) {
	baseParams.Method = http.MethodGet
	path := apc.URLPathRoles.S
	roles := make([]*Role, 0)
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = path
	}
	err := reqParams.DoHTTPReqResp(&roles)

	less := func(i, j int) bool { return roles[i].ID < roles[j].ID }
	sort.Slice(roles, less)
	return roles, err
}

func GetAllUsers(baseParams api.BaseParams) ([]*User, error) {
	baseParams.Method = http.MethodGet
	users := make(map[string]*User, 4)
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathUsers.S
	}
	err := reqParams.DoHTTPReqResp(&users)

	list := make([]*User, 0, len(users))
	for _, info := range users {
		list = append(list, info)
	}

	less := func(i, j int) bool { return list[i].ID < list[j].ID }
	sort.Slice(list, less)

	return list, err
}

func GetUser(baseParams api.BaseParams, userID string) (*User, error) {
	if userID == "" {
		return nil, errors.New("missing user ID")
	}
	uInfo := &User{}
	baseParams.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cos.JoinWords(apc.URLPathUsers.S, userID)
	}
	err := reqParams.DoHTTPReqResp(&uInfo)
	return uInfo, err
}

func AddRole(baseParams api.BaseParams, roleSpec *Role) error {
	msg := cos.MustMarshal(roleSpec)
	baseParams.Method = http.MethodPost
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathRoles.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func UpdateRole(baseParams api.BaseParams, roleSpec *Role) error {
	msg := cos.MustMarshal(roleSpec)
	baseParams.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathRoles.Join(roleSpec.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func DeleteRole(baseParams api.BaseParams, role string) error {
	baseParams.Method = http.MethodDelete
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathRoles.Join(role)
	}
	return reqParams.DoHTTPRequest()
}

func RevokeToken(baseParams api.BaseParams, token string) error {
	baseParams.Method = http.MethodDelete
	msg := &TokenMsg{Token: token}
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathTokens.S
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

func GetConfig(baseParams api.BaseParams) (*Config, error) {
	conf := &Config{}
	baseParams.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathDae.S
	}
	err := reqParams.DoHTTPReqResp(&conf)
	return conf, err
}

func SetConfig(baseParams api.BaseParams, conf *ConfigToUpdate) error {
	baseParams.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.Body = cos.MustMarshal(conf)
		reqParams.BaseParams = baseParams
		reqParams.Path = apc.URLPathDae.S
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoHTTPRequest()
}

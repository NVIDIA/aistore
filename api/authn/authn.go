// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

func AddUser(bp api.BaseParams, newUser *User) error {
	msg, err := jsoniter.Marshal(newUser)
	if err != nil {
		return err
	}
	bp.Method = http.MethodPost
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathUsers.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

func UpdateUser(bp api.BaseParams, user *User) error {
	msg := cos.MustMarshal(user)
	bp.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathUsers.Join(user.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

func DeleteUser(bp api.BaseParams, userID string) error {
	bp.Method = http.MethodDelete
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathUsers.Join(userID)
	}
	return reqParams.DoRequest()
}

// Authorize a user and return a user token in case of success.
// The token expires in `expire` time. If `expire` is `nil` the expiration
// time is set by AuthN (default AuthN expiration time is 24 hours)
func LoginUser(bp api.BaseParams, userID, pass string, expire *time.Duration) (token *TokenMsg, err error) {
	bp.Method = http.MethodPost
	rec := LoginMsg{Password: pass, ExpiresIn: expire}
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathUsers.Join(userID)
		reqParams.Body = cos.MustMarshal(rec)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	if _, err = reqParams.DoReqAny(&token); err != nil {
		return nil, err
	}
	if token.Token == "" {
		return nil, errors.New("login failed: empty response from AuthN server")
	}
	return token, nil
}

func RegisterCluster(bp api.BaseParams, cluSpec CluACL) error {
	msg := cos.MustMarshal(cluSpec)
	bp.Method = http.MethodPost
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClusters.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

func UpdateCluster(bp api.BaseParams, cluSpec CluACL) error {
	msg := cos.MustMarshal(cluSpec)
	bp.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClusters.Join(cluSpec.ID)
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

func UnregisterCluster(bp api.BaseParams, spec CluACL) error {
	bp.Method = http.MethodDelete
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClusters.Join(spec.ID)
	}
	return reqParams.DoRequest()
}

func GetRegisteredClusters(bp api.BaseParams, spec CluACL) ([]*CluACL, error) {
	bp.Method = http.MethodGet
	path := apc.URLPathClusters.S
	if spec.ID != "" {
		path = cos.JoinWords(path, spec.ID)
	}
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
	}

	clusters := &RegisteredClusters{}
	_, err := reqParams.DoReqAny(clusters)
	rec := make([]*CluACL, 0, len(clusters.Clusters))
	for _, clu := range clusters.Clusters {
		rec = append(rec, clu)
	}
	less := func(i, j int) bool { return rec[i].ID < rec[j].ID }
	sort.Slice(rec, less)
	return rec, err
}

func GetRole(bp api.BaseParams, roleID string) (*Role, error) {
	if roleID == "" {
		return nil, errors.New("missing role ID")
	}
	bp.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = cos.JoinWords(apc.URLPathRoles.S, roleID)
	}

	rInfo := &Role{}
	_, err := reqParams.DoReqAny(&rInfo)
	return rInfo, err
}

func GetAllRoles(bp api.BaseParams) ([]*Role, error) {
	bp.Method = http.MethodGet
	path := apc.URLPathRoles.S
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
	}

	roles := make([]*Role, 0)
	_, err := reqParams.DoReqAny(&roles)

	less := func(i, j int) bool { return roles[i].Name < roles[j].Name }
	sort.Slice(roles, less)
	return roles, err
}

func GetAllUsers(bp api.BaseParams) ([]*User, error) {
	bp.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathUsers.S
	}

	users := make(map[string]*User, 4)
	_, err := reqParams.DoReqAny(&users)

	list := make([]*User, 0, len(users))
	for _, info := range users {
		list = append(list, info)
	}

	less := func(i, j int) bool { return list[i].ID < list[j].ID }
	sort.Slice(list, less)

	return list, err
}

func GetUser(bp api.BaseParams, userID string) (*User, error) {
	if userID == "" {
		return nil, errors.New("missing user ID")
	}
	bp.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = cos.JoinWords(apc.URLPathUsers.S, userID)
	}

	uInfo := &User{}
	_, err := reqParams.DoReqAny(&uInfo)
	return uInfo, err
}

func AddRole(bp api.BaseParams, roleSpec *Role) error {
	msg := cos.MustMarshal(roleSpec)
	bp.Method = http.MethodPost
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathRoles.S
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

func UpdateRole(bp api.BaseParams, roleSpec *Role) error {
	msg := cos.MustMarshal(roleSpec)
	bp.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathRoles.Join(roleSpec.Name)
		reqParams.Body = msg
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

func DeleteRole(bp api.BaseParams, role string) error {
	bp.Method = http.MethodDelete
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathRoles.Join(role)
	}
	return reqParams.DoRequest()
}

func RevokeToken(bp api.BaseParams, token string) error {
	bp.Method = http.MethodDelete
	msg := &TokenMsg{Token: token}
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathTokens.S
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

func GetConfig(bp api.BaseParams) (*Config, error) {
	bp.Method = http.MethodGet
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDae.S
	}

	conf := &Config{}
	_, err := reqParams.DoReqAny(&conf)
	return conf, err
}

func SetConfig(bp api.BaseParams, conf *ConfigToUpdate) error {
	bp.Method = http.MethodPut
	reqParams := api.AllocRp()
	defer api.FreeRp(reqParams)
	{
		reqParams.Body = cos.MustMarshal(conf)
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDae.S
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	return reqParams.DoRequest()
}

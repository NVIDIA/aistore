package api

import (
	"errors"
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

type AuthnSpec struct {
	AdminName     string
	AdminPassword string
	UserName      string
	UserPassword  string
}

type userRec struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

type loginRec struct {
	Password string `json:"password"`
}

type AuthCreds struct {
	Token string `json:"token"`
}

func AddUser(baseParams *BaseParams, spec AuthnSpec) error {
	req := userRec{Name: spec.UserName, Password: spec.UserPassword}
	msg, err := jsoniter.Marshal(req)
	if err != nil {
		return err
	}

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Users)
	optParams := OptionalParams{User: spec.AdminName, Password: spec.AdminPassword}
	_, err = DoHTTPRequest(baseParams, path, msg, optParams)
	return err
}

func DeleteUser(baseParams *BaseParams, spec AuthnSpec) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Users, spec.UserName)
	optParams := OptionalParams{User: spec.AdminName, Password: spec.AdminPassword}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}

func LoginUser(baseParams *BaseParams, spec AuthnSpec) (*AuthCreds, error) {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Users, spec.UserName)
	optParams := OptionalParams{User: spec.AdminName, Password: spec.AdminPassword}

	req := loginRec{Password: spec.UserPassword}
	msg, err := jsoniter.Marshal(req)
	if err != nil {
		return nil, err
	}

	b, err := DoHTTPRequest(baseParams, path, msg, optParams)
	if err != nil {
		return nil, err
	}
	token := &AuthCreds{}
	if len(b) == 0 {
		return nil, errors.New("Login failed: empty response from AuthN server")
	}

	err = jsoniter.Unmarshal(b, token)
	if err != nil {
		return nil, err
	}
	if token.Token == "" {
		return nil, errors.New("Login failed: empty response from AuthN server")
	}
	return token, nil
}

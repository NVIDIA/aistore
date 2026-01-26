// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
)

const (
	SigningMethodHS256 = "HS256"
	SigningMethodRS256 = "RS256"
)

// OIDCConfiguration -- Partial implementation of OIDC spec: https://openid.net/specs/openid-connect-discovery-1_0.html
type OIDCConfiguration struct {
	Issuer                           string   `json:"issuer"`
	TokenEndpoint                    string   `json:"token_endpoint"`
	UserinfoEndpoint                 string   `json:"userinfo_endpoint,omitempty"`
	JWKSURI                          string   `json:"jwks_uri"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
}

func NewOIDCConfiguration(base *url.URL) *OIDCConfiguration {
	return &OIDCConfiguration{
		Issuer:                           base.String(),
		TokenEndpoint:                    base.JoinPath(apc.URLPathTokens.S).String(),
		UserinfoEndpoint:                 base.JoinPath(apc.URLPathUsers.S).String(),
		JWKSURI:                          base.JoinPath(apc.URLPathJWKS.S).String(),
		IDTokenSigningAlgValuesSupported: []string{SigningMethodHS256, SigningMethodRS256},
	}
}

// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// LoadToken retrieves the authentication token from the specified tokenFile,
// environment variables, or default location (CLI config).
func LoadToken(tokenFile string) (string /*token value*/, error) {
	// token value directly from environment
	if tokenFile == "" {
		if tokenEnv := os.Getenv(env.AuthN.Token); tokenEnv != "" {
			return tokenEnv, nil
		}
	}

	var token TokenMsg

	// token filename from environment
	if tokenFile == "" {
		tokenFile = os.Getenv(env.AuthN.TokenFile)
	}

	// or, default token filename
	if tokenFile == "" {
		// Default location when generated via CLI without the `-f` option:
		// $HOME/.config/ais/cli/<fname.Token>
		tokenFile = filepath.Join(cos.HomeConfigDir(fname.HomeCLI), fname.Token)
	}

	// load
	_, err := jsp.LoadMeta(tokenFile, &token)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("token file %q does not exist", tokenFile)
		}
		return "", fmt.Errorf("failed to load token from %q: %v", tokenFile, err)
	}

	return token.Token, nil
}

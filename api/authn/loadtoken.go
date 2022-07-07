// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// NOTE: must load when tokenFile != ""
func LoadToken(tokenFile string) string {
	var (
		token    TokenMsg
		mustLoad = true
	)
	if tokenFile == "" {
		tokenFile = os.Getenv(env.AuthN.TokenFile)
	}
	if tokenFile == "" {
		// when generated via CLI (and without the `-f` option) - the location:
		// $HOME/.config/ais/cli/<fname.Token>
		tokenFile = filepath.Join(cos.HomeConfigDir(fname.HomeCLI), fname.Token)
		mustLoad = false
	}
	_, err := jsp.LoadMeta(tokenFile, &token)
	if err != nil && (mustLoad || !os.IsNotExist(err)) {
		debug.AssertMsg(mustLoad && os.IsNotExist(err), err.Error())
		cos.Errorf("Failed to load token from %q: %v", tokenFile, err)
	}
	return token.Token
}

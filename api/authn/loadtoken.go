// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// NOTE: must load when tokenFile != ""
func LoadToken(tokenFile string) string {
	var (
		token    TokenMsg
		mustLoad = true
	)
	if tokenFile == "" {
		tokenFile = filepath.Join(jsp.DefaultAppConfigDir(), cmn.TokenFname)
		mustLoad = false
	}
	_, err := jsp.LoadMeta(tokenFile, &token)
	if err != nil && (mustLoad || !os.IsNotExist(err)) {
		debug.AssertNoErr(err)
		cos.Errorf("Failed to load token from %q: %v", tokenFile, err)
	}
	return token.Token
}

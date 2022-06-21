// Package authn - authorization server for AIStore.
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

func LoadToken() string {
	var (
		tokenPath string
		token     TokenMsg
		mustLoad  = true
	)
	if tokenPath = os.Getenv(EnvVars.TokenFile); tokenPath == "" {
		tokenPath = filepath.Join(jsp.DefaultAppConfigDir(), cmn.TokenFname)
		mustLoad = false
	}
	_, err := jsp.LoadMeta(tokenPath, &token)
	if err != nil && (mustLoad || !os.IsNotExist(err)) {
		debug.AssertNoErr(err)
		cos.Errorf("Failed to load token from %q: %v", tokenPath, err)
	}
	return token.Token
}

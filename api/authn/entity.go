// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const (
	AdminRole = "Admin"
)

type (
	User struct {
		ID          string    `json:"id"`
		Password    string    `json:"pass,omitempty"`
		Roles       []string  `json:"roles"`
		ClusterACLs []*CluACL `json:"clusters"`
		BucketACLs  []*BckACL `json:"buckets"` // list of buckets with special permissions
	}
	CluACL struct {
		ID     string          `json:"id"`
		Alias  string          `json:"alias,omitempty"`
		Access apc.AccessAttrs `json:"perm,string,omitempty"`
		URLs   []string        `json:"urls,omitempty"`
	}
	BckACL struct {
		Bck    cmn.Bck         `json:"bck"`
		Access apc.AccessAttrs `json:"perm,string"`
	}
	TokenMsg struct {
		Token string `json:"token"`
	}
	LoginMsg struct {
		Password  string         `json:"password"`
		ExpiresIn *time.Duration `json:"expires_in"`
	}
	RegisteredClusters struct {
		M map[string]*CluACL `json:"clusters,omitempty"`
	}
	Role struct {
		ID          string    `json:"name"`
		Desc        string    `json:"desc"`
		Roles       []string  `json:"roles"`
		ClusterACLs []*CluACL `json:"clusters"`
		BucketACLs  []*BckACL `json:"buckets"`
		IsAdmin     bool      `json:"admin"`
	}
)

//////////
// User //
//////////
func (uInfo *User) IsAdmin() bool {
	for _, r := range uInfo.Roles {
		if r == AdminRole {
			return true
		}
	}
	return false
}

////////////
// CluACL //
////////////
func (clu *CluACL) String() string {
	uuid := "[" + clu.ID + "]"
	if clu.Alias != "" && clu.Alias != clu.ID {
		return clu.Alias + uuid
	}
	if len(clu.URLs) > 0 {
		return clu.URLs[0] + uuid
	}
	return uuid
}

//////////////
// TokenMsg //
//////////////
var _ jsp.Opts = (*TokenMsg)(nil)

func (*TokenMsg) JspOpts() jsp.Options { return authtokJspOpts }

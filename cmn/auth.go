// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/dgrijalva/jwt-go"
)

const (
	AuthAdminRole        = "Admin"
	AuthClusterOwnerRole = "ClusterOwner"
	AuthBucketOwnerRole  = "BucketOwner"
	AuthGuestRole        = "Guest"
)

type (
	// A registered user
	AuthUser struct {
		ID       string         `json:"id"`
		Password string         `json:"pass,omitempty"`
		Roles    []string       `json:"roles"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets"` // list of buckets with special permissions
	}
	// Default permissions for a cluster
	AuthCluster struct {
		ID     string      `json:"id"`
		Alias  string      `json:"alias,omitempty"`
		Access AccessAttrs `json:"perm,string,omitempty"`
		URLs   []string    `json:"urls,omitempty"`
	}
	// Permissions for a single bucket
	AuthBucket struct {
		Bck    Bck         `json:"bck"`
		Access AccessAttrs `json:"perm,string"`
	}
	AuthRole struct {
		Name     string         `json:"name"`
		Desc     string         `json:"desc"`
		Roles    []string       `json:"roles"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets"`
		IsAdmin  bool           `json:"admin"`
	}
	AuthToken struct {
		UserID   string         `json:"username"`
		Expires  time.Time      `json:"expires"`
		Token    string         `json:"token"`
		Clusters []*AuthCluster `json:"clusters"`
		Buckets  []*AuthBucket  `json:"buckets,omitempty"`
		IsAdmin  bool           `json:"admin"`
	}
	AuthClusterList struct {
		Clusters map[string]*AuthCluster `json:"clusters,omitempty"`
	}
	LoginMsg struct {
		Password  string         `json:"password"`
		ExpiresIn *time.Duration `json:"expires_in"`
	}
	TokenMsg struct {
		Token string `json:"token"`
	}
)

/////////////////////
// authn jsp stuff //
/////////////////////
var (
	_ jsp.Opts = (*AuthNConfig)(nil)
	_ jsp.Opts = (*TokenMsg)(nil)

	authcfgJspOpts = jsp.Plain() // TODO -- FIXME: use CCSign(MetaverAuthNConfig)
	authtokJspOpts = jsp.Plain() // ditto MetaverAuthTokens
)

func (*AuthNConfig) JspOpts() jsp.Options { return authcfgJspOpts }
func (*TokenMsg) JspOpts() jsp.Options    { return authtokJspOpts }

// authn api helpers and errors

var (
	ErrNoPermissions = errors.New("insufficient permissions")
	ErrInvalidToken  = errors.New("invalid token")
	ErrTokenExpired  = errors.New("token expired")
)

func (tk *AuthToken) aclForCluster(clusterID string) (perms AccessAttrs, ok bool) {
	for _, pm := range tk.Clusters {
		if pm.ID == clusterID {
			return pm.Access, true
		}
	}
	return 0, false
}

func (tk *AuthToken) aclForBucket(clusterID string, bck *Bck) (perms AccessAttrs, ok bool) {
	for _, b := range tk.Buckets {
		tbBck := b.Bck
		if tbBck.Ns.UUID != clusterID {
			continue
		}
		// For AuthN all buckets are external, so they have UUIDs. To correctly
		// compare with local bucket, token's bucket should be fixed.
		tbBck.Ns.UUID = ""
		if b.Bck.Equal(*bck) {
			return b.Access, true
		}
	}
	return 0, false
}

// A user has two-level permissions: cluster-wide and on per bucket basis.
// To be able to access data, a user must have either permission. This
// allows creating users, e.g, with read-only access to the entire cluster,
// and read-write access to a single bucket.
// Per-bucket ACL overrides cluster-wide one.
func (tk *AuthToken) CheckPermissions(clusterID string, bck *Bck, perms AccessAttrs) error {
	if tk.IsAdmin {
		return nil
	}
	debug.AssertMsg(perms != 0, "Empty permissions requested")
	cluPerms := perms & AccessCluster
	objPerms := perms &^ AccessCluster
	cluACL, cluOk := tk.aclForCluster(clusterID)
	if cluPerms != 0 {
		// Cluster-wide permissions requested
		if !cluOk {
			return ErrNoPermissions
		}
		debug.AssertMsg(clusterID != "", "Requested cluster permissions without cluster ID")
		if !cluACL.Has(cluPerms) {
			return ErrNoPermissions
		}
	}
	if objPerms == 0 {
		return nil
	}

	// Check only bucket specific permissions.
	debug.AssertMsg(bck != nil, "Requested bucket permissions without bucket")
	bckACL, bckOk := tk.aclForBucket(clusterID, bck)
	if bckOk {
		if bckACL.Has(objPerms) {
			return nil
		}
		return ErrNoPermissions
	}
	if !cluOk || !cluACL.Has(objPerms) {
		return ErrNoPermissions
	}
	return nil
}

func (uInfo *AuthUser) IsAdmin() bool {
	for _, r := range uInfo.Roles {
		if r == AuthAdminRole {
			return true
		}
	}
	return false
}

func MergeBckACLs(oldACLs, newACLs []*AuthBucket) []*AuthBucket {
	for _, n := range newACLs {
		found := false
		for _, o := range oldACLs {
			if o.Bck.Equal(n.Bck) {
				found = true
				o.Access = n.Access
				break
			}
			if !found {
				oldACLs = append(oldACLs, n)
			}
		}
	}
	return oldACLs
}

func MergeClusterACLs(oldACLs, newACLs []*AuthCluster) []*AuthCluster {
	for _, n := range newACLs {
		found := false
		for _, o := range oldACLs {
			if o.ID == n.ID {
				found = true
				o.Access = n.Access
				break
			}
		}
		if !found {
			oldACLs = append(oldACLs, n)
		}
	}
	return oldACLs
}

func DecryptToken(tokenStr, secret string) (*AuthToken, error) {
	token, err := jwt.Parse(tokenStr, func(tk *jwt.Token) (interface{}, error) {
		if _, ok := tk.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", tk.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}
	tInfo := &AuthToken{}
	if err := cos.MorphMarshal(claims, tInfo); err != nil {
		return nil, ErrInvalidToken
	}
	if tInfo.Expires.Before(time.Now()) {
		return nil, ErrTokenExpired
	}
	return tInfo, nil
}

// Package authn - authorization server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"errors"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/golang-jwt/jwt/v4"
)

const (
	AdminRole        = "Admin"
	ClusterOwnerRole = "ClusterOwner"
	BucketOwnerRole  = "BucketOwner"
	GuestRole        = "Guest"
)

type (
	// A registered user
	User struct {
		ID       string     `json:"id"`
		Password string     `json:"pass,omitempty"`
		Roles    []string   `json:"roles"`
		Clusters []*Cluster `json:"clusters"`
		Buckets  []*Bucket  `json:"buckets"` // list of buckets with special permissions
	}
	// Default permissions for a cluster
	Cluster struct {
		ID     string          `json:"id"`
		Alias  string          `json:"alias,omitempty"`
		Access apc.AccessAttrs `json:"perm,string,omitempty"`
		URLs   []string        `json:"urls,omitempty"`
	}
	// Permissions for a single bucket
	Bucket struct {
		Bck    cmn.Bck         `json:"bck"`
		Access apc.AccessAttrs `json:"perm,string"`
	}
	Role struct {
		Name     string     `json:"name"`
		Desc     string     `json:"desc"`
		Roles    []string   `json:"roles"`
		Clusters []*Cluster `json:"clusters"`
		Buckets  []*Bucket  `json:"buckets"`
		IsAdmin  bool       `json:"admin"`
	}
	Token struct {
		UserID   string     `json:"username"`
		Expires  time.Time  `json:"expires"`
		Token    string     `json:"token"`
		Clusters []*Cluster `json:"clusters"`
		Buckets  []*Bucket  `json:"buckets,omitempty"`
		IsAdmin  bool       `json:"admin"`
	}
	ClusterList struct {
		Clusters map[string]*Cluster `json:"clusters,omitempty"`
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
	_ jsp.Opts = (*Config)(nil)
	_ jsp.Opts = (*TokenMsg)(nil)

	authcfgJspOpts = jsp.Plain() // TODO: use CCSign(MetaverAuthNConfig)
	authtokJspOpts = jsp.Plain() // ditto MetaverTokens
)

func (*Config) JspOpts() jsp.Options   { return authcfgJspOpts }
func (*TokenMsg) JspOpts() jsp.Options { return authtokJspOpts }

// authn api helpers and errors

var (
	ErrNoPermissions = errors.New("insufficient permissions")
	ErrInvalidToken  = errors.New("invalid token")
	ErrNoToken       = errors.New("token required")
	ErrTokenExpired  = errors.New("token expired")
)

///////////
// Token //
///////////

func (tk *Token) String() string {
	return fmt.Sprintf("user %s, %s", tk.UserID, expiresIn(tk.Expires))
}

func expiresIn(tm time.Time) string {
	now := time.Now()
	if !now.Before(tm) {
		return "TOKEN EXPIRED"
	}
	// round up
	d := tm.Sub(now) / time.Second
	d *= time.Second
	return "token expires in " + d.String()
}

func (tk *Token) aclForCluster(clusterID string) (perms apc.AccessAttrs, ok bool) {
	for _, pm := range tk.Clusters {
		if pm.ID == clusterID {
			return pm.Access, true
		}
	}
	return 0, false
}

func (tk *Token) aclForBucket(clusterID string, bck *cmn.Bck) (perms apc.AccessAttrs, ok bool) {
	for _, b := range tk.Buckets {
		tbBck := b.Bck
		if tbBck.Ns.UUID != clusterID {
			continue
		}
		// For AuthN all buckets are external, so they have UUIDs. To correctly
		// compare with local bucket, token's bucket should be fixed.
		tbBck.Ns.UUID = ""
		if b.Bck.Equal(bck) {
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
func (tk *Token) CheckPermissions(clusterID string, bck *cmn.Bck, perms apc.AccessAttrs) error {
	if tk.IsAdmin {
		return nil
	}
	if perms == 0 {
		return errors.New("Empty permissions requested")
	}
	cluPerms := perms & apc.AccessCluster
	objPerms := perms &^ apc.AccessCluster
	cluACL, cluOk := tk.aclForCluster(clusterID)
	if cluPerms != 0 {
		// Cluster-wide permissions requested
		if !cluOk {
			return ErrNoPermissions
		}
		if clusterID == "" {
			return errors.New("Requested cluster permissions without cluster ID")
		}
		if !cluACL.Has(cluPerms) {
			return fmt.Errorf("%v: [cluster %s, %s, access(%s)]", ErrNoPermissions, clusterID, tk, cluACL.Describe())
		}
	}
	if objPerms == 0 {
		return nil
	}

	// Check only bucket specific permissions.
	if bck == nil {
		return errors.New("Requested bucket permissions without a bucket")
	}
	bckACL, bckOk := tk.aclForBucket(clusterID, bck)
	if bckOk {
		if bckACL.Has(objPerms) {
			return nil
		}
		return fmt.Errorf("%v: [%s, bucket %s, access(%s)]", ErrNoPermissions, tk, bck.String(), bckACL.Describe())
	}
	if !cluOk || !cluACL.Has(objPerms) {
		return ErrNoPermissions
	}
	return nil
}

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

//
// utils
//

func MergeBckACLs(oldACLs, newACLs []*Bucket) []*Bucket {
	for _, n := range newACLs {
		found := false
		for _, o := range oldACLs {
			if o.Bck.Equal(&n.Bck) {
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

func MergeClusterACLs(oldACLs, newACLs []*Cluster) []*Cluster {
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

func DecryptToken(tokenStr, secret string) (*Token, error) {
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
	tInfo := &Token{}
	if err := cos.MorphMarshal(claims, tInfo); err != nil {
		return nil, ErrInvalidToken
	}
	if tInfo.Expires.Before(time.Now()) {
		return nil, ErrTokenExpired
	}
	return tInfo, nil
}

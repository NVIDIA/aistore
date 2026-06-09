// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/signing"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"

	"github.com/golang-jwt/jwt/v5"
	jsoniter "github.com/json-iterator/go"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"golang.org/x/crypto/bcrypt"
)

type (
	mgr struct {
		clientH   *http.Client
		clientTLS *http.Client
		db        kvdb.Driver
		cm        *config.ConfManager
		sb        atomic.Pointer[signerBundle]

		authzMu sync.Mutex
	}

	signerBundle struct {
		signer tok.Signer
		parser tok.Parser
	}
)

var (
	errInvalidCredentials  = errors.New("invalid credentials")
	errInvalidRequestedExp = errors.New("invalid requested expiry date")
	errJWKSUnavailable     = errors.New("JWKS not available for current signing method")
	errInvalidRotation     = errors.New("rotation not supported for current signing method")

	predefinedRoles = []struct {
		prefix string
		desc   string
		perms  apc.AccessAttrs
	}{
		{ClusterOwnerRole, "Admin access to %s", apc.AccessAll},
		{BucketOwnerRole, "Full access to buckets in %s", apc.AccessRW},
		{GuestRole, "Read-only access to buckets in %s", apc.AccessRO},
	}
)

// If user DB exists, loads the data from the file and decrypts passwords
func newMgr(cm *config.ConfManager, signer tok.Signer, driver kvdb.Driver) (m *mgr, code int, err error) {
	m = &mgr{
		db: driver,
		cm: cm,
	}
	m.updateSignerBundle(signer)
	m.clientH, m.clientTLS = cmn.NewDefaultClients(cm.GetDefaultTimeout())
	code, err = initializeDB(driver)
	if err != nil {
		return
	}
	if err = m.ensureRoleUsersIndex(); err != nil {
		code = http.StatusInternalServerError
		return
	}
	return
}

func (m *mgr) getSigner() tok.Signer { return m.sb.Load().signer }
func (m *mgr) getParser() tok.Parser { return m.sb.Load().parser }

func (*mgr) String() string { return config.ServiceName }

func (m *mgr) updateSignerBundle(signer tok.Signer) {
	parser := tok.NewTokenParser(signer, nil)
	m.sb.Store(&signerBundle{signer: signer, parser: parser})
}

func (m *mgr) validateToken(ctx context.Context, token string) (*tok.AISClaims, error) {
	return m.getParser().ValidateToken(ctx, token)
}

func (m *mgr) getPubKey() (string, error) {
	provider, ok := m.getSigner().(signing.AsymmetricKeySigner)
	if !ok {
		return "", errors.New("current signing method does not have a public key")
	}
	return provider.GetPubKey(), nil
}

func (m *mgr) getJWKS() (jwk.Set, error) {
	provider, ok := m.getSigner().(signing.JWKSProvider)
	if !ok {
		return nil, errJWKSUnavailable
	}
	return provider.GetJWKS()
}

func (m *mgr) getJWKSMaxAge() int {
	return m.cm.GetJWKSMaxAge()
}

func (m *mgr) rotateKey() error {
	signer, ok := m.getSigner().(signing.AsymmetricKeySigner)
	if !ok {
		return errInvalidRotation
	}
	return signer.RotateKey()
}

//
// users ============================================================
//

// Registers a new user. It is info from a user, so the password
// is not encrypted and a few fields are not filled (e.g, Access).
func (m *mgr) addUser(info *authn.User) (int, error) {
	if info.ID == "" || info.Password == "" {
		return http.StatusBadRequest, errInvalidCredentials
	}
	if !cos.IsAlphaNice(info.ID) {
		return http.StatusBadRequest, fmt.Errorf("user ID %q is invalid: %s", info.ID, cos.OnlyNice)
	}
	encPass := encryptPassword(info.Password)

	m.authzMu.Lock()
	defer m.authzMu.Unlock()

	_, _, err := m.db.GetString(usersCollection, info.ID)
	if err == nil {
		return http.StatusConflict, cos.NewErrAlreadyExists(m, "user "+info.ID)
	}
	roles, code, err := m.fetchUserRoles(info.Roles)
	if err != nil {
		return code, err
	}
	info.Roles = roles
	info.Password = encPass
	if err = m.syncUserRoleIndex(info.ID, info.Roles); err != nil {
		nlog.Errorf("failed to sync role index for user %q: %v", info.ID, err)
		return http.StatusInternalServerError, err
	}
	if code, err = m.db.Set(usersCollection, info.ID, info); err != nil {
		return code, err
	}
	return http.StatusOK, nil
}

// Deletes an existing user
func (m *mgr) delUser(userID string) (int, error) {
	if userID == adminUserID {
		return http.StatusForbidden, fmt.Errorf("cannot remove built-in %q account", adminUserID)
	}

	m.authzMu.Lock()
	defer m.authzMu.Unlock()

	uInfo, code, err := m.lookupUser(userID)
	if err != nil {
		return code, err
	}
	if code, err = m.db.Delete(usersCollection, userID); err != nil {
		return code, err
	}
	if err = m.clearUserFromRoleIndex(userID, uInfo.Roles); err != nil {
		nlog.Errorf("failed to clear role index for user %q: %v", userID, err)
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

// Updates an existing user.
func (m *mgr) updateUser(userID string, updateReq *authn.User) (int, error) {
	if userID == adminUserID && len(updateReq.Roles) != 0 {
		return http.StatusForbidden, errors.New("cannot change administrator's role")
	}

	var (
		encPass     string
		rolesUpdate = len(updateReq.Roles) != 0
	)
	if updateReq.Password != "" {
		encPass = encryptPassword(updateReq.Password)
	}

	m.authzMu.Lock()
	defer m.authzMu.Unlock()

	uInfo := &authn.User{}
	code, err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return code, err
	}
	if encPass != "" {
		uInfo.Password = encPass
	}
	var oldRoles []*authn.Role
	if rolesUpdate {
		newRoles, code, err := m.fetchUserRoles(updateReq.Roles)
		if err != nil {
			return code, err
		}
		oldRoles = uInfo.Roles
		uInfo.Roles = newRoles
	}
	if code, err := m.db.Set(usersCollection, userID, uInfo); err != nil {
		return code, err
	}
	if rolesUpdate {
		if err = m.updateUserRoleIndex(userID, oldRoles, uInfo.Roles); err != nil {
			nlog.Errorf("failed to update role index for user %q: %v", userID, err)
			return http.StatusInternalServerError, err
		}
	}
	return http.StatusOK, nil
}

func (m *mgr) lookupUser(userID string) (*authn.User, int, error) {
	uInfo := &authn.User{}
	code, err := m.db.Get(usersCollection, userID, uInfo)
	if err != nil {
		return nil, code, err
	}
	return uInfo, http.StatusOK, nil
}

func (m *mgr) userList() (map[string]*authn.User, int, error) {
	recs, code, err := m.db.GetAll(usersCollection, "")
	if err != nil {
		return nil, code, err
	}
	users := make(map[string]*authn.User, 4)
	for _, str := range recs {
		uInfo := &authn.User{}
		err := jsoniter.Unmarshal([]byte(str), uInfo)
		cos.AssertNoErr(err)
		users[uInfo.ID] = uInfo
	}
	return users, http.StatusOK, nil
}

//
// roles ============================================================
//

// Registers a new role
func (m *mgr) addRole(info *authn.Role) (int, error) {
	if info.Name == "" {
		return http.StatusBadRequest, errors.New("role name is undefined")
	}
	if !cos.IsAlphaNice(info.Name) {
		return http.StatusBadRequest, fmt.Errorf("role name %q is invalid: %s", info.Name, cos.OnlyNice)
	}
	if info.IsAdmin {
		return http.StatusForbidden, fmt.Errorf("only built-in roles can have %q permissions", adminUserID)
	}

	m.authzMu.Lock()
	defer m.authzMu.Unlock()

	_, _, err := m.db.GetString(rolesCollection, info.Name)
	if err == nil {
		return http.StatusConflict, cos.NewErrAlreadyExists(m, "role "+info.Name)
	}
	return m.db.Set(rolesCollection, info.Name, info)
}

// Deletes an existing role
func (m *mgr) delRole(role string) (int, error) {
	if role == authn.AdminRole {
		return http.StatusForbidden, fmt.Errorf("cannot remove built-in %q role", authn.AdminRole)
	}

	m.authzMu.Lock()
	defer m.authzMu.Unlock()

	if code, err := m.db.Delete(rolesCollection, role); err != nil {
		return code, err
	}
	return m.removeRoleFromAllUsers(role)
}

// Updates an existing role
func (m *mgr) updateRole(role string, updateReq *authn.Role) (int, error) {
	if role == authn.AdminRole {
		return http.StatusForbidden, fmt.Errorf("cannot modify built-in %q role", authn.AdminRole)
	}

	m.authzMu.Lock()
	defer m.authzMu.Unlock()

	rInfo := &authn.Role{}
	code, err := m.db.Get(rolesCollection, role, rInfo)
	if err != nil {
		return code, err
	}

	if updateReq.Description != "" {
		rInfo.Description = updateReq.Description
	}
	rInfo.ClusterACLs = mergeClusterACLs(rInfo.ClusterACLs, updateReq.ClusterACLs, "")
	rInfo.BucketACLs = mergeBckACLs(rInfo.BucketACLs, updateReq.BucketACLs, "")

	if code, err := m.db.Set(rolesCollection, role, rInfo); err != nil {
		return code, err
	}
	return m.propagateRoleToUsers(role, rInfo)
}

func (m *mgr) lookupRole(roleID string) (*authn.Role, int, error) {
	rInfo := &authn.Role{}
	code, err := m.db.Get(rolesCollection, roleID, rInfo)
	if err != nil {
		return nil, code, err
	}
	return rInfo, http.StatusOK, nil
}

func (m *mgr) roleList() ([]*authn.Role, int, error) {
	recs, code, err := m.db.GetAll(rolesCollection, "")
	if err != nil {
		return nil, code, err
	}
	roles := make([]*authn.Role, 0, len(recs))
	for _, str := range recs {
		role := &authn.Role{}
		err := jsoniter.Unmarshal([]byte(str), role)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		roles = append(roles, role)
	}
	return roles, http.StatusOK, nil
}

// Creates predefined roles for newly-added clusters
func (m *mgr) createRolesForCluster(clu *authn.CluACL) (int, error) {
	for _, pr := range predefinedRoles {
		suffix := cos.Left(clu.Alias, clu.ID)
		uid := pr.prefix + "-" + suffix
		rInfo := &authn.Role{}
		if _, err := m.db.Get(rolesCollection, uid, rInfo); err == nil {
			continue
		}
		rInfo.Name = uid
		cluName := clu.ID
		if clu.Alias != "" {
			cluName += "[" + clu.Alias + "]"
		}
		rInfo.Description = fmt.Sprintf(pr.desc, cluName)
		rInfo.ClusterACLs = []*authn.CluACL{
			{ID: clu.ID, Access: pr.perms},
		}
		if code, err := m.db.Set(rolesCollection, uid, rInfo); err != nil {
			return code, cmn.NewErrFailedTo(m, "create", "role", err)
		}
	}
	return http.StatusOK, nil
}

//
// clusters ============================================================
//

func (m *mgr) clus() (map[string]*authn.CluACL, int, error) {
	clusters, code, err := m.db.GetAll(clustersCollection, "")
	if err != nil {
		return nil, code, err
	}
	clus := make(map[string]*authn.CluACL, len(clusters))
	for cid, s := range clusters {
		cInfo := &authn.CluACL{}
		if err := jsoniter.Unmarshal([]byte(s), cInfo); err != nil {
			nlog.Errorf("Failed to parse cluster %s info: %v", cid, err)
			continue
		}
		clus[cInfo.ID] = cInfo
	}
	return clus, http.StatusOK, nil
}

// Returns a cluster ID which ID or Alias equal cluID or cluAlias.
func (m *mgr) cluLookup(cluID, cluAlias string) string {
	clus, _, err := m.clus()
	if err != nil {
		return ""
	}
	for cid, cInfo := range clus {
		if cid != "" && (cid == cluID || cid == cluAlias) {
			return cid
		}
		if cluAlias == "" {
			continue
		}
		if cInfo.ID == cluAlias || cInfo.Alias == cluAlias {
			return cid
		}
	}
	return ""
}

// Get an existing cluster
func (m *mgr) getCluster(cluID string) (*authn.CluACL, int, error) {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return nil, http.StatusNotFound, cos.NewErrNotFound(m, "cluster "+cluID)
	}
	clu := &authn.CluACL{}
	code, err := m.db.Get(clustersCollection, cid, clu)
	return clu, code, err
}

// Registers a new cluster
func (m *mgr) registerCluster(ctx context.Context, clu *authn.CluACL) (int, error) {
	if clu.ID == "" {
		return http.StatusBadRequest, errors.New("cluster UUID is undefined")
	}
	// Validate cluster alias
	if !cos.IsAlphaNice(clu.Alias) {
		return http.StatusBadRequest, fmt.Errorf("cluster alias %q is invalid: %s", clu.Alias, cos.OnlyNice)
	}
	cid := m.cluLookup(clu.ID, clu.Alias)
	if cid != "" {
		return http.StatusConflict, fmt.Errorf("cluster %s[%s] already registered", clu.ID, cid)
	}

	// Before registering a cluster:
	// 1. Ensure it will accept JWTs issued by this authN
	if code, err := m.validateCluster(ctx, clu); err != nil {
		return code, err
	}
	// 2. Ensure it knows all revoked tokens
	if code, err := m.syncRevokedTokens(ctx, clu); err != nil {
		return code, err
	}
	return m.addClusterWithRoles(clu)
}

func (m *mgr) addClusterWithRoles(clu *authn.CluACL) (int, error) {
	if code, err := m.db.Set(clustersCollection, clu.ID, clu); err != nil {
		return code, err
	}
	code, err := m.createRolesForCluster(clu)
	// Role creation is best-effort, log error if failure but keep created cluster
	if err != nil {
		nlog.Errorf("failed to create roles for cluster %s: %v", clu.ID, err)
	}
	return code, nil
}

func (m *mgr) updateCluster(ctx context.Context, cluID string, info *authn.CluACL) (int, error) {
	if info.ID == "" {
		return http.StatusBadRequest, errors.New("cluster ID is undefined")
	}
	clu := &authn.CluACL{}
	if code, err := m.db.Get(clustersCollection, cluID, clu); err != nil {
		return code, err
	}
	if info.Alias != "" {
		// Validate cluster alias if user is changing it
		if !cos.IsAlphaNice(info.Alias) {
			return http.StatusBadRequest, fmt.Errorf("cluster alias %q is invalid: %s", info.Alias, cos.OnlyNice)
		}
		cid := m.cluLookup("", info.Alias)
		if cid != "" && cid != clu.ID {
			return http.StatusConflict, fmt.Errorf("alias %q is used for cluster %q", info.Alias, cid)
		}
		clu.Alias = info.Alias
	}
	if len(info.URLs) != 0 {
		clu.URLs = info.URLs
	}

	// secret handshake
	if code, err := m.validateCluster(ctx, clu); err != nil {
		return code, err
	}

	return m.db.Set(clustersCollection, cluID, clu)
}

// Unregister an existing cluster
func (m *mgr) delCluster(cluID string) (int, error) {
	cid := m.cluLookup(cluID, cluID)
	if cid == "" {
		return http.StatusNotFound, cos.NewErrNotFound(m, "cluster "+cluID)
	}
	return m.db.Delete(clustersCollection, cid)
}

//
// tokens ============================================================
//

// Generates a token for a user if user credentials are valid. If the token is
// already generated and is not expired yet the existing token is returned.
// AISClaims includes user ID, permissions, and token expiration time.
// If a new token was generated then it sends the proxy a new valid token list
func (m *mgr) issueToken(uid, pwd string, msg *authn.LoginMsg) (token string, code int, err error) {
	var (
		uInfo   = &authn.User{}
		cid     string
		cluACLs []*authn.CluACL
		bckACLs []*authn.BckACL
	)
	_, err = m.db.Get(usersCollection, uid, uInfo)
	if err != nil {
		nlog.Errorln(err)
		return "", http.StatusUnauthorized, errInvalidCredentials
	}

	debug.Assert(uid == uInfo.ID, uid, " vs ", uInfo.ID)

	if !isSamePassword(pwd, uInfo.Password) {
		return "", http.StatusUnauthorized, errInvalidCredentials
	}

	// update ACLs with roles' ones
	for _, role := range uInfo.Roles {
		cluACLs = mergeClusterACLs(cluACLs, role.ClusterACLs, cid)
		bckACLs = mergeBckACLs(bckACLs, role.BucketACLs, cid)
	}

	claims, err := m.buildClaims(msg, uInfo, cluACLs, bckACLs)
	if err != nil {
		if errors.Is(err, errInvalidRequestedExp) {
			return "", http.StatusBadRequest, err
		}
		return "", http.StatusInternalServerError, err
	}
	token, err = m.getSigner().SignToken(claims)
	if err != nil {
		return "", http.StatusInternalServerError, err
	}
	return token, http.StatusOK, nil
}

func (m *mgr) buildClaims(msg *authn.LoginMsg, uInfo *authn.User, cluACLs []*authn.CluACL, bckACLs []*authn.BckACL) (*tok.AISClaims, error) {
	now := time.Now().UTC()
	exp, expErr := m.getExp(now, msg)
	if expErr != nil {
		return nil, expErr
	}
	// Standard claims: iss, sub, exp, iat
	regClaims := &jwt.RegisteredClaims{
		Issuer:    m.cm.GetExternalURL().String(),
		Subject:   uInfo.ID,
		ExpiresAt: exp,
		IssuedAt:  jwt.NewNumericDate(now),
	}
	// Create with appropriate AIS ACLs
	if uInfo.IsAdmin() {
		// Admin tokens contain aud claims for ALL registered clusters
		// Note: requires re-issued token if new cluster is added with required aud claims
		cluIDs, err := m.getAllClusterIDs()
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster IDs for aud claim: %v", err)
		}
		regClaims.Audience = cluIDs
		return tok.AdminClaims(regClaims), nil
	}
	if cluACLs != nil {
		if err := m.fixClusterIDs(cluACLs); err != nil {
			return nil, fmt.Errorf("failed to build cluster ACLs: %v", err)
		}
	}
	regClaims.Audience = getAud(bckACLs, cluACLs)
	return tok.StandardClaims(regClaims, bckACLs, cluACLs), nil
}

// Get the expiry for a newly signed JWT based on the login data
func (m *mgr) getExp(now time.Time, msg *authn.LoginMsg) (*jwt.NumericDate, error) {
	// If not specified in request, return default
	if msg.ExpiresIn == nil {
		return jwt.NewNumericDate(now.Add(m.cm.GetExpiry())), nil
	}
	// User-requested expiration
	delta := *msg.ExpiresIn
	maxAge := m.cm.GetMaxTokenAge()

	if delta == 0 {
		delta = maxAge.D()
	} else if delta < authn.MinAuthExpiration.D() {
		return nil, fmt.Errorf("%w: must be 0 or >= %s", errInvalidRequestedExp, authn.MinAuthExpiration)
	}

	if delta > maxAge.D() {
		return nil, fmt.Errorf("%w: cannot exceed configured max token age: %s", errInvalidRequestedExp, maxAge.String())
	}
	return jwt.NewNumericDate(now.Add(delta)), nil
}

// Get the audience to include in new JWT -- AIS can optionally verify this
func getAud(bckACLs []*authn.BckACL, cluACLs []*authn.CluACL) []string {
	seen := make(map[string]struct{}, len(cluACLs)+len(bckACLs))
	for _, acl := range cluACLs {
		if acl.ID != "" {
			seen[acl.ID] = struct{}{}
		}
	}
	for _, acl := range bckACLs {
		if uuid := acl.Bck.Ns.UUID; uuid != "" {
			seen[uuid] = struct{}{}
		}
	}
	aud := make([]string, 0, len(seen))
	for id := range seen {
		aud = append(aud, id)
	}
	slices.Sort(aud)
	return aud
}

func (m *mgr) updateConf(cu *authn.ConfigToUpdate) error {
	oldSecret := m.cm.GetSecret()
	if err := m.cm.UpdateConf(cu); err != nil {
		return err
	}
	newSecret := m.cm.GetSecret()
	if oldSecret != newSecret {
		signer := signing.NewHMACSigner(newSecret)
		m.updateSignerBundle(signer)
	}
	return nil
}

// Before putting a list of cluster permissions to a token, cluster aliases
// must be replaced with their IDs.
func (m *mgr) fixClusterIDs(lst []*authn.CluACL) error {
	clus, _, err := m.clus()
	if err != nil {
		return err
	}
	for _, cInfo := range lst {
		if _, ok := clus[cInfo.ID]; ok {
			continue
		}
		for _, clu := range clus {
			if clu.Alias == cInfo.ID {
				cInfo.ID = clu.ID
			}
		}
	}
	return nil
}

func (m *mgr) getAllClusterIDs() ([]string, error) {
	clus, _, err := m.clus()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(clus))
	for id := range clus {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, nil
}

// Add the given token to the revoked list within AuthN
// In the background, attempt to delete the token from all clusters
func (m *mgr) revokeToken(token string) (int, error) {
	code, err := m.db.Set(revokedCollection, token, "!")
	if err != nil {
		return code, err
	}

	// best-effort request to all registered clusters to remove the token
	go m.broadcastRevoked(context.Background(), token)
	return http.StatusOK, nil
}

// Create a list of non-expired and valid revoked tokens.
// Obsolete and invalid tokens are removed from the database.
func (m *mgr) generateRevokedTokenList(ctx context.Context) ([]string, int, error) {
	tokens, code, err := m.db.List(revokedCollection, "")
	if err != nil {
		debug.AssertNoErr(err)
		return nil, code, err
	}

	revokeList := make([]string, 0, len(tokens))
	for _, token := range tokens {
		_, err = m.validateToken(ctx, token)
		if err != nil {
			nlog.Infof("removing invalid token %q due to validation error %v", token, err)
			_, err = m.db.Delete(revokedCollection, token)
			if err != nil {
				nlog.Errorf("failed to delete token %q due to error %v", token, err)
			}
			continue
		}
		revokeList = append(revokeList, token)
	}
	return revokeList, http.StatusOK, nil
}

//
// private helpers ============================================================
//

func encryptPassword(password string) string {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	cos.AssertNoErr(err)
	return hex.EncodeToString(b)
}

func isSamePassword(password, hashed string) bool {
	b, err := hex.DecodeString(hashed)
	if err != nil {
		return false
	}
	return bcrypt.CompareHashAndPassword(b, []byte(password)) == nil
}

// If the DB is empty, the function prefills some data
func initializeDB(driver kvdb.Driver) (int, error) {
	users, code, err := driver.List(usersCollection, "")
	if err != nil || len(users) != 0 {
		// Return on errors or when DB is already initialized
		return code, err
	}

	// Create the admin role
	role := &authn.Role{
		Name:        authn.AdminRole,
		Description: "AuthN administrator",
		IsAdmin:     true,
	}

	if code, err := driver.Set(rolesCollection, authn.AdminRole, role); err != nil {
		return code, err
	}

	// environment override
	userName := cos.Right(adminUserID, os.Getenv(env.AisAuthAdminUsername))
	password, exists := os.LookupEnv(env.AisAuthAdminPassword)
	if !exists || password == "" {
		return 0, fmt.Errorf("failed to initialize DB, no password provided for admin user. Set with %q", env.AisAuthAdminPassword)
	}

	// Create the admin user
	su := &authn.User{
		ID:       userName,
		Password: encryptPassword(password),
		Roles:    []*authn.Role{role},
	}

	return driver.Set(usersCollection, userName, su)
}

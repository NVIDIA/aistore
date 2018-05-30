// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Authentication flow:
// 1. If AuthN server is disabled or directory with user credentials is not set:
//    Token in HTTP request header is ignored.
//    All user credentials are read from default files and environment variables.
//    AWS: file ~/.aws/credentials
//    GCP: file ~/gcp_creds.json and GOOGLE_CLOUD_PROJECT variable
//         a user should be logged in to GCP before running any DFC operation
//         that touches cloud objects
// 2. AuthN server is enabled and everything is set up
//    - DFC reads userID from HTTP request header: 'Authorization: Bearer <token>'.
//    - A user credentials is loaded for the userID
//      AWS: CredDir points to a directory that contains a file with user
//       credentials. Authn looks for a file with name 'credentials' or it takes
//       the first file found in the directory. The file must contain a section
//       for the userID in a form:
//       [userId]
//       region = us-east-1
//       aws_access_key_id = USERACCESSKEY
//       aws_secret_access_key = USERSECRETKEY
//      GCP: CredDir points to a directory that contains user files. A file per
//       a user. The file name must be 'userID' + '.json'
//    - Besides user credentials it loads a projectId for GCP (required by
//      bucketnames call)
//    - After successful loading and checking the user's credentials it opens
//      a new session with loaded from file data
//    - Extra step for GCP: if creating session for a given user fails, it tries
//      to start a session with default parameters
package dfc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	ctxUserID    = "userID"      // a field name of a context that contains userID
	ctxCredsDir  = "credDir"     // a field of a context that contains path to directory with credentials
	awsCredsFile = "credentials" // a default AWS file with user credentials
)

type (
	// TokenList is a list of tokens pushed by authn after any token change
	TokenList struct {
		Tokens []string `json:"tokens"`
	}

	authRec struct {
		userID  string
		issued  time.Time
		expires time.Time
		creds   map[string]string // TODO: what to keep in this field and how
	}

	authList map[string]*authRec

	authManager struct {
		// decrypted token information from TokenList
		sync.Mutex
		tokens authList
	}
)

// Decrypts JWT token and returns all encrypted information.
// Used by proxy - to check a user access and token validity(e.g, expiration),
// and by target - only to get a user name for AWS/GCP access
func decryptToken(tokenStr string) (*authRec, error) {
	var (
		issueStr, expireStr string
		invalTokenErr       = fmt.Errorf("Invalid token")
	)
	rec := &authRec{}
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		return []byte(ctx.config.Auth.Secret), nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, invalTokenErr
	}
	if rec.userID, ok = claims["username"].(string); !ok {
		return nil, invalTokenErr
	}
	if issueStr, ok = claims["issued"].(string); !ok {
		return nil, invalTokenErr
	}
	if rec.issued, err = time.Parse(time.RFC822, issueStr); err != nil {
		return nil, invalTokenErr
	}
	if expireStr, ok = claims["expires"].(string); !ok {
		return nil, invalTokenErr
	}
	if rec.expires, err = time.Parse(time.RFC822, expireStr); err != nil {
		return nil, invalTokenErr
	}
	if rec.creds, ok = claims["creds"].(map[string]string); !ok {
		rec.creds = make(map[string]string, 0)
	}

	return rec, nil
}

// Converts token list sent by authn and checks for correct format
func newAuthList(tokenList *TokenList) (authList, error) {
	auth := make(map[string]*authRec)
	if tokenList == nil || len(tokenList.Tokens) == 0 {
		return auth, nil
	}

	for _, tokenStr := range tokenList.Tokens {
		rec, err := decryptToken(tokenStr)
		if err != nil {
			return nil, err
		}
		auth[tokenStr] = rec
	}

	return auth, nil
}

// Retreives a userID from context or empty string if nothing found
func userIDFromContext(ct context.Context) string {
	userIf := ct.Value(ctxUserID)
	if userIf == nil {
		return ""
	}

	userID, ok := userIf.(string)
	if !ok {
		return ""
	}

	return userID
}

// Reads a directory with user credentials files.
// userID should be empty for AWS and set for GCP.
// If it is found it does additional checks:
// - it is a directory, not a file
// - it contains a file with user credentials (for AWS it looks for 'credentials' file, for GCP - "${userID}.json"
// Returns a full path to file with credentials or error
func userCredsPathFromContext(ct context.Context, userID string) (string, error) {
	dirIf := ct.Value(ctxCredsDir)
	if dirIf == nil {
		return "", fmt.Errorf("Directory is not defined")
	}

	credDir, ok := dirIf.(string)
	if !ok {
		return "", fmt.Errorf("%s expected string type but it is %T (%v)", ctxCredsDir, dirIf, dirIf)
	}

	stat, err := os.Stat(credDir)
	if err != nil {
		return "", fmt.Errorf("Invalid directory: %v", err)
	}
	if !stat.IsDir() {
		return "", fmt.Errorf("%s is not a directory", credDir)
	}

	var credPath string
	if userID == "" {
		// AWS way - one file for all users
		credPath = filepath.Join(credDir, awsCredsFile)
	} else {
		// GCP way - every user in a separate file
		credPath = filepath.Join(credDir, userID+".json")
	}

	stat, err = os.Stat(credPath)
	if err != nil {
		glog.Errorf("Failed to open credential file: %v", err)
		return "", fmt.Errorf("Failed to open credentials file")
	}

	if stat.IsDir() {
		return "", fmt.Errorf("A file expected but %s is a directory", credPath)
	}

	return credPath, nil
}

// Looks for a token in the list of valid tokens and returns information
// about a user for whom the token was issued
func (a *authManager) validateToken(token string) (*authRec, error) {
	a.Lock()
	defer a.Unlock()
	auth, ok := a.tokens[token]
	if !ok {
		glog.Errorf("Token not found: %s", token)
		return nil, fmt.Errorf("Token not found")
	}

	if auth.expires.Before(time.Now()) {
		glog.Errorf("Expired token was used: %s", token)
		delete(a.tokens, token)
		return nil, fmt.Errorf("Token expired")
	}

	return auth, nil
}

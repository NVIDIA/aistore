package dfc

import (
	"fmt"
	"sync"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/golang/glog"
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

// Converts token list sent by authn and checks for correct format
func newAuthList(tokenList *TokenList) (authList, error) {
	var (
		issueStr, expireStr string
		invalTokenErr       = fmt.Errorf("Invalid token")
	)
	auth := make(map[string]*authRec)
	if tokenList == nil || len(tokenList.Tokens) == 0 {
		return auth, nil
	}

	for _, tokenStr := range tokenList.Tokens {
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

		auth[tokenStr] = rec
	}

	return auth, nil
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

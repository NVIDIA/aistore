// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func testJWT(payload string) string {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return hdr + "." + body + ".sig"
}

func setupWhoamiOutput(t *testing.T, w *bytes.Buffer) {
	t.Helper()
	prevCyan, prevGreen, prevWriter := fcyan, fgreen, teb.Writer
	fcyan, fgreen, teb.Writer = fmt.Sprint, fmt.Sprint, w
	t.Cleanup(func() {
		fcyan, fgreen, teb.Writer = prevCyan, prevGreen, prevWriter
	})
}

func TestParseWhoamiClaims(t *testing.T) {
	t.Parallel()

	exp := time.Now().Add(time.Hour).Unix()
	token := testJWT(fmt.Sprintf(
		`{"sub":"alice","iss":"http://authn","admin":false,"exp":%d,"clusters":[{"id":"clu1","alias":"test","perm":"%d"}]}`,
		exp, apc.ClusterAccessRO,
	))

	claims, err := parseWhoamiClaims(token)
	if err != nil {
		t.Fatal(err)
	}
	tassert.Fatal(t, claims.Subject == "alice", "subject")
	tassert.Fatal(t, claims.Issuer == "http://authn", "issuer")
	tassert.Fatal(t, !claims.Admin, "admin")
	tassert.Fatal(t, len(claims.ClusterACLs) == 1, "cluster ACLs")
	tassert.Fatal(t, claims.ClusterACLs[0].Alias == "test", "cluster alias")
}

func TestParseWhoamiClaimsAdmin(t *testing.T) {
	t.Parallel()

	exp := time.Now().Add(time.Hour).Unix()
	token := testJWT(fmt.Sprintf(`{"sub":"admin","admin":true,"iss":"http://authn","exp":%d}`, exp))
	claims, err := parseWhoamiClaims(token)
	if err != nil {
		t.Fatal(err)
	}
	tassert.Fatal(t, claims.Admin, "expected admin claim")
}

func TestParseWhoamiClaimsEmpty(t *testing.T) {
	t.Parallel()

	if _, err := parseWhoamiClaims(""); err != authn.ErrNoToken {
		t.Fatalf("expected ErrNoToken, got %v", err)
	}
}

func TestParseWhoamiClaimsBucket(t *testing.T) {
	t.Parallel()

	exp := time.Now().Add(time.Hour).Unix()
	token := testJWT(fmt.Sprintf(
		`{"sub":"ubck","iss":"http://authn","admin":false,"exp":%d,"buckets":[{"bck":{"name":"project-1","provider":"ais","namespace":{"uuid":"cid1"}},"perm":"%d"}]}`,
		exp, apc.AccessRO,
	))

	claims, err := parseWhoamiClaims(token)
	if err != nil {
		t.Fatal(err)
	}
	tassert.Fatal(t, len(claims.BucketACLs) == 1, "bucket ACLs")
	tassert.Fatal(t, claims.BucketACLs[0].Access == apc.AccessRO, "bucket access")
	tassert.Fatal(t, claims.BucketACLs[0].Bck.Name == "project-1", "bucket name")
}

func TestWhoamiBckName(t *testing.T) {
	t.Parallel()

	bck := cmn.Bck{Name: "project-1", Provider: apc.AIS, Ns: cmn.Ns{UUID: "TQtCnRSOE"}}
	tassert.Fatal(t, whoamiBckName(bck) == "ais://project-1", "whoami bucket name")
}

func TestWhoamiAccessStale(t *testing.T) {
	bck := cmn.Bck{Name: "b1", Provider: apc.AIS, Ns: cmn.Ns{UUID: "cid1"}}
	claims := &whoamiClaims{
		ClusterACLs: []*authn.CluACL{{ID: "cid1", Alias: "test", Access: apc.AccessRO}},
		BucketACLs:  []*authn.BckACL{{Bck: bck, Access: apc.AccessRW}},
	}
	user := &authn.User{Roles: []*authn.Role{
		{ClusterACLs: []*authn.CluACL{{ID: "test", Access: apc.AccessRO}}},
		{BucketACLs: []*authn.BckACL{{Bck: bck, Access: apc.AccessRW}}},
	}}
	tassert.Fatal(t, !whoamiAccessStale(claims, user), "matching effective access must be current")

	user.Roles[0].ClusterACLs[0].Access = apc.AccessRW
	tassert.Fatal(t, whoamiAccessStale(claims, user), "changed permission must be stale")

	// AuthN unions permissions across roles for the same cluster (union=true in mergeClusterACLs).
	user.Roles[0].ClusterACLs[0].Access = apc.AccessRO
	tassert.Fatal(t, !whoamiAccessStale(claims, user), "union of matching permissions must be current")

	user.Roles = append(user.Roles, &authn.Role{ClusterACLs: []*authn.CluACL{{ID: "cid2", Access: apc.AccessRO}}})
	tassert.Fatal(t, whoamiAccessStale(claims, user), "new resource grant must be stale")

	adminClaims := &whoamiClaims{Admin: true}
	admin := &authn.User{Roles: []*authn.Role{{Name: authn.AdminRole}}}
	tassert.Fatal(t, !whoamiAccessStale(adminClaims, admin), "matching admin access must be current")
	tassert.Fatal(t, whoamiAccessStale(adminClaims, &authn.User{}), "removed admin access must be stale")
}

func TestPrintWhoamiUsesTokenAccessAndWarnsWhenStale(t *testing.T) {
	var stdout, stderr bytes.Buffer
	setupWhoamiOutput(t, &stdout)
	app := cli.NewApp()
	app.Writer = &stdout
	app.ErrWriter = &stderr
	c := cli.NewContext(app, nil, nil)

	claims := &whoamiClaims{
		Subject:     "alice",
		ClusterACLs: []*authn.CluACL{{ID: "cid1", Alias: "test", Access: apc.AceGET}},
	}
	user := &authn.User{ID: "alice", Roles: []*authn.Role{{
		Name:        "writers",
		ClusterACLs: []*authn.CluACL{{ID: "cid1", Alias: "test", Access: apc.AcePUT}},
	}}}

	if err := printWhoami(c, claims, user); err != nil {
		t.Fatal(err)
	}
	out := stdout.String()
	tassert.Fatal(t, strings.Contains(out, "Roles\n"), "roles heading")
	tassert.Fatal(t, strings.Contains(out, "Access\n"), "access heading")
	tassert.Fatal(t, !strings.Contains(out, "(server)"), "roles source label must be hidden")
	tassert.Fatal(t, !strings.Contains(out, "(token)"), "access source label must be hidden")
	tassert.Fatal(t, strings.Contains(out, "GET"), "token permission must be printed")
	tassert.Fatal(t, !strings.Contains(out, "PUT"), "server permission must not replace token permission")
	warning := stderr.String()
	tassert.Fatal(t, strings.Contains(warning,
		"your roles have been updated and are not reflected in your token's access list shown above."),
		"stale access warning")
	tassert.Fatal(t, strings.Contains(warning, "Log in again to refresh your token."), "token refresh instruction")
}

func TestPrintWhoamiAdminAccess(t *testing.T) {
	var stdout bytes.Buffer
	setupWhoamiOutput(t, &stdout)
	app := cli.NewApp()
	app.Writer = &stdout
	c := cli.NewContext(app, nil, nil)

	claims := &whoamiClaims{Subject: "admin", Admin: true}
	user := &authn.User{ID: "admin", Roles: []*authn.Role{{Name: authn.AdminRole}}}
	if err := printWhoami(c, claims, user); err != nil {
		t.Fatal(err)
	}
	tassert.Fatal(t, strings.Contains(stdout.String(), "Access\nFull admin access"), "full admin access")
}

func TestPrintWhoamiDefaultAndNoAccess(t *testing.T) {
	var stdout bytes.Buffer
	setupWhoamiOutput(t, &stdout)

	if err := printWhoamiAccess(&stdout, []*authn.CluACL{{Access: apc.AccessNone}}, nil); err != nil {
		t.Fatal(err)
	}
	tassert.Fatal(t, strings.Contains(stdout.String(), "all clusters"), "default-cluster ACL label")
	tassert.Fatal(t, strings.Contains(stdout.String(), "none"), "explicit no-access permission")
}

func TestWhoamiHandlerExpiredToken(t *testing.T) {
	prev := loggedUserToken
	defer func() { loggedUserToken = prev }()

	exp := time.Now().Add(-time.Hour).Unix()
	loggedUserToken = testJWT(fmt.Sprintf(`{"sub":"admin","admin":true,"iss":"http://authn","exp":%d}`, exp))

	app := cli.NewApp()
	c := cli.NewContext(app, nil, nil)

	err := whoamiHandler(c)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
	tassert.Fatal(t, strings.Contains(err.Error(), "token expired at"), "expired-token error")
}

func TestTokenExpiredBoundary(t *testing.T) {
	expires := time.Unix(1000, 0)
	tests := []struct {
		name    string
		now     time.Time
		expires time.Time
		expired bool
	}{
		{name: "before", now: expires.Add(-time.Nanosecond), expires: expires},
		{name: "equal", now: expires, expires: expires, expired: true},
		{name: "after", now: expires.Add(time.Nanosecond), expires: expires, expired: true},
		{name: "missing expiration", now: expires, expires: time.Time{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := tokenExpired(test.now, test.expires); actual != test.expired {
				t.Fatalf("expected expired=%t, got %t", test.expired, actual)
			}
		})
	}
}

func TestPrintWhoamiNoStaleWarningWhenFresh(t *testing.T) {
	var stdout, stderr bytes.Buffer
	setupWhoamiOutput(t, &stdout)
	app := cli.NewApp()
	app.Writer = &stdout
	app.ErrWriter = &stderr
	c := cli.NewContext(app, nil, nil)

	// Token and server side carry the same effective access (alias "prod" resolves to "cid1").
	claims := &whoamiClaims{
		Subject:     "alice",
		ClusterACLs: []*authn.CluACL{{ID: "cid1", Alias: "prod", Access: apc.AceGET}},
	}
	user := &authn.User{ID: "alice", Roles: []*authn.Role{{
		Name:        "readers",
		ClusterACLs: []*authn.CluACL{{ID: "prod", Access: apc.AceGET}},
	}}}

	if err := printWhoami(c, claims, user); err != nil {
		t.Fatal(err)
	}
	tassert.Fatal(t, stderr.String() == "", "no stale warning expected for a current token")
}

func TestWrapWhoamiNotLoggedInBeforeAuthConfig(t *testing.T) {
	prev := loggedUserToken
	defer func() { loggedUserToken = prev }()
	loggedUserToken = ""

	app := cli.NewApp()
	c := cli.NewContext(app, nil, nil)
	err := wrapWhoami(whoamiHandler)(c)
	if err != errWhoamiNotLoggedIn {
		t.Fatalf("expected not-logged-in error, got %v", err)
	}
}

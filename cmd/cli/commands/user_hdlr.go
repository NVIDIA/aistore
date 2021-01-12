// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that create entities in the cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/urfave/cli"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	tokenFile = "auth.token"

	authnServerURL = "AUTHN_URL"
	authnTokenPath = "AUTHN_TOKEN_FILE"

	flagsAuthUserLogin = "user_login"
	flagsAuthRoleAdd   = "role_add"
)

var (
	authFlags = map[string][]cli.Flag{
		flagsAuthUserLogin: {tokenFileFlag, passwordFlag},
		subcmdAuthUser:     {passwordFlag},
		flagsAuthRoleAdd:   {descriptionFlag},
	}
	authCmds = []cli.Command{
		{
			Name:  commandAuth,
			Usage: "manage AutnN server",
			Subcommands: []cli.Command{
				{
					Name:  subcmdAuthAdd,
					Usage: "add entity to auth",
					Subcommands: []cli.Command{
						{
							Name:         subcmdAuthUser,
							Usage:        "add a new user",
							ArgsUsage:    addUserArgument,
							Flags:        authFlags[subcmdAuthUser],
							Action:       wrapAuthN(addUserHandler),
							BashComplete: multiRoleCompletions,
						},
						{
							Name:      subcmdAuthCluster,
							Usage:     "register a new cluster",
							ArgsUsage: addAuthClusterArgument,
							Action:    wrapAuthN(addAuthClusterHandler),
						},
						{
							Name:         subcmdAuthRole,
							Usage:        "create a new role",
							ArgsUsage:    addAuthRoleArgument,
							Flags:        authFlags[flagsAuthRoleAdd],
							Action:       wrapAuthN(addAuthRoleHandler),
							BashComplete: roleCluPermCompletions,
						},
					},
				},
				{
					Name:  subcmdAuthRemove,
					Usage: "remove entity from auth",
					Subcommands: []cli.Command{
						{
							Name:         subcmdAuthUser,
							Usage:        "remove an existing user",
							ArgsUsage:    deleteUserArgument,
							Action:       wrapAuthN(deleteUserHandler),
							BashComplete: oneUserCompletions,
						},
						{
							Name:         subcmdAuthCluster,
							Usage:        "unregister a cluster",
							ArgsUsage:    deleteAuthClusterArgument,
							Action:       wrapAuthN(deleteAuthClusterHandler),
							BashComplete: oneClusterCompletions,
						},
						{
							Name:         subcmdAuthRole,
							Usage:        "remove an existing role",
							ArgsUsage:    deleteRoleArgument,
							Action:       wrapAuthN(deleteRoleHandler),
							BashComplete: oneRoleCompletions,
						},
					},
				},
				{
					Name: subcmdAuthUpdate,
					Subcommands: []cli.Command{
						{
							Name:      subcmdAuthCluster,
							Usage:     "update registered cluster config",
							ArgsUsage: addAuthClusterArgument,
							Action:    wrapAuthN(updateAuthClusterHandler),
						},
						{
							Name:         subcmdAuthUser,
							Usage:        "update an existing user",
							ArgsUsage:    addUserArgument,
							Flags:        authFlags[subcmdAuthUser],
							Action:       wrapAuthN(updateUserHandler),
							BashComplete: multiRoleCompletions,
						},
					},
				},
				{
					Name:  subcmdAuthShow,
					Usage: "show entity in authn",
					Subcommands: []cli.Command{
						{
							Name:      subcmdAuthCluster,
							Usage:     "show registered clusters",
							ArgsUsage: showAuthClusterArgument,
							Action:    wrapAuthN(showAuthClusterHandler),
						},
						{
							Name:      subcmdAuthRole,
							Usage:     "show existing user roles",
							ArgsUsage: showAuthRoleArgument,
							Action:    wrapAuthN(showAuthRoleHandler),
						},
						{
							Name:   subcmdAuthUser,
							Usage:  "show user list",
							Action: wrapAuthN(showUserHandler),
						},
					},
				},
				{
					Name:      subcmdAuthLogin,
					Usage:     "log in with existing user credentials",
					Flags:     authFlags[flagsAuthUserLogin],
					ArgsUsage: userLoginArgument,
					Action:    wrapAuthN(loginUserHandler),
				},
				{
					Name:   subcmdAuthLogout,
					Usage:  "log out",
					Action: wrapAuthN(logoutUserHandler),
				},
			},
		},
	}

	loggedUserToken api.AuthCreds
)

// Use the function to wrap every AuthN handler that does API calls.
// The function verifies that AuthN is up and running before doing the API call
// and augments API errors if needed.
func wrapAuthN(f cli.ActionFunc) cli.ActionFunc {
	const authnUnreacable = "AuthN unreachable at %s. You may need to update AIS CLI configuration or environment variable %s"
	return func(c *cli.Context) error {
		if authnHTTPClient == nil {
			return fmt.Errorf("AuthN URL is not set") // nolint:golint // name of the service
		}
		err := f(c)
		if err != nil && isUnreachableError(err) {
			err = fmt.Errorf(authnUnreacable, authParams.URL, authnServerURL)
		}
		return err
	}
}

func readMasked(c *cli.Context, prompt string) string {
	fmt.Fprintf(c.App.Writer, prompt+": ")
	bytePass, err := terminal.ReadPassword(0)
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(string(bytePass), "\n")
}

func cliAuthnURL(cfg *config.Config) string {
	authURL := os.Getenv(authnServerURL)
	if authURL == "" {
		authURL = cfg.Auth.URL
	}
	return authURL
}

func cliAuthnUserName(c *cli.Context) string {
	name := c.Args().First()
	if name == "" {
		name = readValue(c, "User login")
	}
	return name
}

func cliAuthnUserPassword(c *cli.Context) string {
	pass := parseStrFlag(c, passwordFlag)
	if pass == "" {
		pass = readMasked(c, "User password")
	}
	return pass
}

func updateUserHandler(c *cli.Context) (err error) {
	user := parseAuthUser(c)
	return api.UpdateUser(authParams, user)
}

func addUserHandler(c *cli.Context) (err error) {
	user := parseAuthUser(c)
	return api.AddUser(authParams, user)
}

func deleteUserHandler(c *cli.Context) (err error) {
	userName := c.Args().First()
	if userName == "" {
		return missingArgumentsError(c, "user name")
	}
	return api.DeleteUser(authParams, userName)
}

func deleteRoleHandler(c *cli.Context) (err error) {
	role := c.Args().Get(0)
	if role == "" {
		return missingArgumentsError(c, "role")
	}
	return api.DeleteRoleAuthN(authParams, role)
}

func loginUserHandler(c *cli.Context) (err error) {
	const tokenSaveFailFmt = "successfully logged in, but failed to save token: %v"
	name := cliAuthnUserName(c)
	password := cliAuthnUserPassword(c)
	token, err := api.LoginUser(authParams, name, password)
	if err != nil {
		return err
	}

	tokenPath := parseStrFlag(c, tokenFileFlag)
	userPathUsed := tokenPath != ""
	if tokenPath == "" {
		tokenPath = filepath.Join(config.ConfigDirPath, tokenFile)
	}
	err = cmn.CreateDir(filepath.Dir(config.ConfigDirPath))
	if err != nil {
		fmt.Fprintf(c.App.Writer, "Token:\n%s\n", token.Token)
		return fmt.Errorf(tokenSaveFailFmt, err)
	}
	err = jsp.Save(tokenPath, token, jsp.Plain())
	if err != nil {
		fmt.Fprintf(c.App.Writer, "Token:\n%s\n", token.Token)
		return fmt.Errorf(tokenSaveFailFmt, err)
	}

	if userPathUsed {
		fmt.Fprintf(c.App.Writer, "Token saved to %s\n", tokenPath)
	} else {
		fmt.Fprintf(c.App.Writer, "Token(%s):\n%s\n", tokenPath, token.Token)
	}
	return nil
}

func logoutUserHandler(c *cli.Context) (err error) {
	const logoutFailFmt = "logging out failed: %v"
	tokenPath := filepath.Join(config.ConfigDirPath, tokenFile)
	if err = os.Remove(tokenPath); os.IsNotExist(err) {
		return fmt.Errorf(logoutFailFmt, err)
	}
	fmt.Fprintln(c.App.Writer, "Logged out")
	return nil
}

func addAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec, err := parseClusterSpecs(c)
	if err != nil {
		return
	}
	return api.RegisterClusterAuthN(authParams, cluSpec)
}

func updateAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec, err := parseClusterSpecs(c)
	if err != nil {
		return
	}

	return api.UpdateClusterAuthN(authParams, cluSpec)
}

func deleteAuthClusterHandler(c *cli.Context) (err error) {
	cid := c.Args().Get(0)
	if cid == "" {
		return missingArgumentsError(c, "cluster id")
	}
	cluSpec := cmn.AuthCluster{ID: cid}
	return api.UnregisterClusterAuthN(authParams, cluSpec)
}

func showAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec := cmn.AuthCluster{
		ID: c.Args().Get(0),
	}
	list, err := api.GetClusterAuthN(authParams, cluSpec)
	if err != nil {
		return err
	}

	return templates.DisplayOutput(list, c.App.Writer, templates.AuthNClusterTmpl)
}

func showAuthRoleHandler(c *cli.Context) (err error) {
	list, err := api.GetRolesAuthN(authParams)
	if err != nil {
		return err
	}

	return templates.DisplayOutput(list, c.App.Writer, templates.AuthNRoleTmpl)
}

func showUserHandler(c *cli.Context) (err error) {
	list, err := api.GetUsersAuthN(authParams)
	if err != nil {
		return err
	}

	return templates.DisplayOutput(list, c.App.Writer, templates.AuthNUserTmpl)
}

// TODO: it is a basic parser for permissions.
// Need better one that supports parsing specific permissions, not only
// these three predefined ones. After the accurate pareser is implemeneted
// the function will move to cmn/api_access.go
func parseAccess(acc string) (cmn.AccessAttrs, error) {
	switch acc {
	case "admin":
		return cmn.AllAccess(), nil
	case "rw":
		return cmn.ReadWriteAccess(), nil
	case "ro":
		return cmn.ReadOnlyAccess(), nil
	case "no":
		return cmn.NoAccess(), nil
	default:
		return 0, fmt.Errorf("invalid access %s", acc)
	}
}

func addAuthRoleHandler(c *cli.Context) (err error) {
	args := c.Args()
	role := args.First()
	if role == "" {
		return missingArgumentsError(c, "role name")
	}
	kvs := args.Tail()
	cluList, err := makePairs(kvs)
	if err != nil {
		return err
	}
	rInfo := &cmn.AuthRole{
		Name: role,
		Desc: parseStrFlag(c, descriptionFlag),
	}
	if len(cluList) != 0 {
		cluPerms := make([]*cmn.AuthCluster, 0, len(cluList))
		for k, v := range cluList {
			access, err := parseAccess(v)
			if err != nil {
				return err
			}
			ac := &cmn.AuthCluster{ID: k, Access: access}
			cluPerms = append(cluPerms, ac)
		}
		rInfo.Clusters = cluPerms
	}
	return api.AddRoleAuthN(authParams, rInfo)
}

func parseAuthUser(c *cli.Context) *cmn.AuthUser {
	username := cliAuthnUserName(c)
	userpass := cliAuthnUserPassword(c)
	roles := c.Args().Tail()
	user := &cmn.AuthUser{
		ID:       username,
		Password: userpass,
		Roles:    roles,
	}
	return user
}

func parseClusterSpecs(c *cli.Context) (cluSpec cmn.AuthCluster, err error) {
	cid := c.Args().Get(0)
	if cid == "" {
		err = missingArgumentsError(c, "cluster id")
	}
	urlList := make([]string, 0)
	alias := c.Args().Get(1)
	if strings.HasPrefix(alias, "http") {
		urlList = append(urlList, alias)
		alias = ""
	}
	for idx := 2; idx < c.NArg(); idx++ {
		url := c.Args().Get(idx)
		if strings.HasPrefix(url, "-") {
			break
		}
		if !strings.HasPrefix(url, "http") {
			err = fmt.Errorf("URL %q does not contain protocol", url)
		}
		urlList = append(urlList, url)
	}
	cluSpec = cmn.AuthCluster{
		ID:    cid,
		Alias: alias,
		URLs:  urlList,
	}
	return
}

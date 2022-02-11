// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that create entities in the cluster.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/authn"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"golang.org/x/term"
)

const (
	tokenFile = "auth.token"

	authnServerURL = "AUTHN_URL"
	authnTokenPath = "AUTHN_TOKEN_FILE"

	flagsAuthUserLogin   = "user_login"
	flagsAuthUserShow    = "user_show"
	flagsAuthRoleAdd     = "role_add"
	flagsAuthRevokeToken = "revoke_token"
	flagsAuthRoleShow    = "role_show"
	flagsAuthConfShow    = "conf_show"
)

const authnUnreachable = `AuthN unreachable at %s. You may need to update AIS CLI configuration or environment variable %s`

var (
	authFlags = map[string][]cli.Flag{
		flagsAuthUserLogin:   {tokenFileFlag, passwordFlag, expireFlag},
		subcmdAuthUser:       {passwordFlag},
		flagsAuthRoleAdd:     {descriptionFlag},
		flagsAuthRevokeToken: {tokenFileFlag},
		flagsAuthUserShow:    {verboseFlag},
		flagsAuthRoleShow:    {verboseFlag},
		flagsAuthConfShow:    {jsonFlag},
	}

	// define separately to allow for aliasing (see alias_hdlr.go)
	authCmdShow = cli.Command{
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
				Name:         subcmdAuthRole,
				Usage:        "show existing user roles",
				ArgsUsage:    showAuthRoleArgument,
				Flags:        authFlags[flagsAuthRoleShow],
				Action:       wrapAuthN(showAuthRoleHandler),
				BashComplete: oneRoleCompletions,
			},
			{
				Name:      subcmdAuthUser,
				Usage:     "show user list or user details",
				Flags:     authFlags[flagsAuthUserShow],
				ArgsUsage: showUserListArgument,
				Action:    wrapAuthN(showUserHandler),
			},
			{
				Name:   subcmdAuthConfig,
				Usage:  "show AuthN server configuration",
				Flags:  authFlags[flagsAuthConfShow],
				Action: wrapAuthN(showAuthConfigHandler),
			},
		},
	}

	authCmd = cli.Command{
		Name:  commandAuth,
		Usage: "add/remove/show users, manage user roles, manage access to remote clusters",
		Subcommands: []cli.Command{
			authCmdShow,
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
					{
						Name:      subcmdAuthToken,
						Usage:     "revoke an authorization token",
						Flags:     authFlags[flagsAuthRevokeToken],
						ArgsUsage: deleteTokenArgument,
						Action:    wrapAuthN(revokeTokenHandler),
					},
				},
			},
			{
				Name:  subcmdAuthUpdate,
				Usage: "update users and cluster configurations",
				Subcommands: []cli.Command{
					{
						Name:      subcmdAuthCluster,
						Usage:     "update registered cluster configuration",
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
			{
				Name:  subcmdAuthSet,
				Usage: "set entity properties",
				Subcommands: []cli.Command{
					{
						Name:         subcmdAuthConfig,
						Usage:        "set AuthN server configuration",
						Action:       wrapAuthN(setAuthConfigHandler),
						BashComplete: suggestUpdatableAuthNConfig,
					},
				},
			},
		},
	}

	loggedUserToken authn.TokenMsg
)

// Use the function to wrap every AuthN handler that does API calls.
// The function verifies that AuthN is up and running before doing the API call
// and augments API errors if needed.
func wrapAuthN(f cli.ActionFunc) cli.ActionFunc {
	return func(c *cli.Context) error {
		if authnHTTPClient == nil {
			return errors.New("AuthN URL is not set") // nolint:golint // name of the service
		}
		err := f(c)
		if err != nil {
			if msg, unreachable := isUnreachableError(err); unreachable {
				err = fmt.Errorf(authnUnreachable, authParams.URL+" (detailed error: "+msg+")", authnServerURL)
			}
		}
		return err
	}
}

func readMasked(c *cli.Context, prompt string) string {
	fmt.Fprintf(c.App.Writer, prompt+": ")
	bytePass, err := term.ReadPassword(0)
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

func cliAuthnUserPassword(c *cli.Context, omitEmpty bool) string {
	pass := parseStrFlag(c, passwordFlag)
	if pass == "" && !omitEmpty {
		pass = readMasked(c, "User password")
	}
	return pass
}

func updateUserHandler(c *cli.Context) (err error) {
	user := parseAuthUser(c, true)
	return api.UpdateUser(authParams, user)
}

func addUserHandler(c *cli.Context) (err error) {
	user := parseAuthUser(c, false)
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
	var (
		expireIn *time.Duration
		name     = cliAuthnUserName(c)
		password = cliAuthnUserPassword(c, false)
	)
	if flagIsSet(c, expireFlag) {
		expireIn = api.Duration(parseDurationFlag(c, expireFlag))
	}
	token, err := api.LoginUser(authParams, name, password, expireIn)
	if err != nil {
		return err
	}

	tokenPath := parseStrFlag(c, tokenFileFlag)
	userPathUsed := tokenPath != ""
	if tokenPath == "" {
		tokenPath = filepath.Join(config.ConfigDirPath, tokenFile)
	}
	err = cos.CreateDir(filepath.Dir(config.ConfigDirPath))
	if err != nil {
		fmt.Fprintf(c.App.Writer, "Token:\n%s\n", token.Token)
		return fmt.Errorf(tokenSaveFailFmt, err)
	}
	err = jsp.Save(tokenPath, token, jsp.Plain(), nil)
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
	var smap *cluster.Smap
	if len(cluSpec.URLs) == 0 {
		smap, err = api.GetClusterMap(defaultAPIParams)
		cluSpec.URLs = append(cluSpec.URLs, clusterURL)
	} else {
		baseParams := api.BaseParams{
			Client: defaultHTTPClient,
			URL:    cluSpec.URLs[0],
			Token:  loggedUserToken.Token,
		}
		smap, err = api.GetClusterMap(baseParams)
		if err != nil {
			err = fmt.Errorf("could not connect cluster %q", cluSpec.URLs[0])
		}
	}
	if err != nil {
		return err
	}
	cluSpec.ID = smap.UUID
	return api.RegisterClusterAuthN(authParams, cluSpec)
}

func updateAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec, err := parseClusterSpecs(c)
	if err != nil {
		return
	}
	if cluSpec.Alias == "" && cluSpec.ID == "" {
		return missingArgumentsError(c, "cluster ID")
	}
	list, err := api.GetClusterAuthN(authParams, cluSpec)
	if err != nil {
		return err
	}
	if cluSpec.ID == "" {
		for _, cluster := range list {
			if cluSpec.Alias == cluster.ID {
				cluSpec.ID = cluSpec.Alias
				break
			}
			if cluSpec.Alias == cluster.Alias {
				cluSpec.ID = cluster.ID
				break
			}
		}
	}
	if cluSpec.ID == "" {
		return fmt.Errorf("cluster %q not found", cluSpec.Alias)
	}

	return api.UpdateClusterAuthN(authParams, cluSpec)
}

func deleteAuthClusterHandler(c *cli.Context) (err error) {
	cid := c.Args().Get(0)
	if cid == "" {
		return missingArgumentsError(c, "cluster id")
	}
	cluSpec := authn.Cluster{ID: cid}
	return api.UnregisterClusterAuthN(authParams, cluSpec)
}

func showAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec := authn.Cluster{
		ID: c.Args().Get(0),
	}
	list, err := api.GetClusterAuthN(authParams, cluSpec)
	if err != nil {
		return err
	}

	return templates.DisplayOutput(list, c.App.Writer, templates.AuthNClusterTmpl)
}

func showAuthRoleHandler(c *cli.Context) (err error) {
	roleID := c.Args().First()
	if roleID == "" {
		list, err := api.GetRolesAuthN(authParams)
		if err != nil {
			return err
		}

		return templates.DisplayOutput(list, c.App.Writer, templates.AuthNRoleTmpl)
	}

	rInfo, err := api.GetRoleAuthN(authParams, roleID)
	if err != nil {
		return err
	}

	if !flagIsSet(c, verboseFlag) {
		return templates.DisplayOutput([]*authn.Role{rInfo}, c.App.Writer, templates.AuthNRoleTmpl)
	}

	return templates.DisplayOutput(rInfo, c.App.Writer, templates.AuthNRoleVerboseTmpl)
}

func showUserHandler(c *cli.Context) (err error) {
	userID := c.Args().First()
	if userID == "" {
		list, err := api.GetUsersAuthN(authParams)
		if err != nil {
			return err
		}

		return templates.DisplayOutput(list, c.App.Writer, templates.AuthNUserTmpl)
	}

	uInfo, err := api.GetUserAuthN(authParams, userID)
	if err != nil {
		return err
	}

	if !flagIsSet(c, verboseFlag) {
		return templates.DisplayOutput([]*authn.User{uInfo}, c.App.Writer, templates.AuthNUserTmpl)
	}

	return templates.DisplayOutput(uInfo, c.App.Writer, templates.AuthNUserVerboseTmpl)
}

func addAuthRoleHandler(c *cli.Context) (err error) {
	args := c.Args()
	role := args.First()
	if role == "" {
		return missingArgumentsError(c, "role name")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "cluster ID")
	} else if c.NArg() < 3 {
		return missingArgumentsError(c, "permissions")
	}

	cluster := args.Get(1)
	alias := ""
	cluList, err := api.GetClusterAuthN(authParams, authn.Cluster{})
	if err != nil {
		return
	}
	found := false
	for _, clu := range cluList {
		if cluster == clu.Alias {
			alias = cluster
			cluster = clu.ID
			found = true
			break
		}
		if cluster == clu.ID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("cluster %q not found", cluster)
	}

	perms := cmn.AccessNone
	for i := 2; i < c.NArg(); i++ {
		p, err := cmn.StrToAccess(args.Get(i))
		if err != nil {
			return err
		}
		perms |= p
	}

	cluPerms := []*authn.Cluster{
		{
			ID:     cluster,
			Alias:  alias,
			Access: perms,
		},
	}
	rInfo := &authn.Role{
		Name:     role,
		Desc:     parseStrFlag(c, descriptionFlag),
		Clusters: cluPerms,
	}
	return api.AddRoleAuthN(authParams, rInfo)
}

func parseAuthUser(c *cli.Context, omitEmpty bool) *authn.User {
	username := cliAuthnUserName(c)
	userpass := cliAuthnUserPassword(c, omitEmpty)
	roles := c.Args().Tail()
	user := &authn.User{
		ID:       username,
		Password: userpass,
		Roles:    roles,
	}
	return user
}

func parseClusterSpecs(c *cli.Context) (cluSpec authn.Cluster, err error) {
	cluSpec.URLs = make([]string, 0, 1)
	for idx := 0; idx < c.NArg(); idx++ {
		arg := c.Args().Get(idx)
		if strings.HasPrefix(arg, "http:") || strings.HasPrefix(arg, "https:") {
			cluSpec.URLs = append(cluSpec.URLs, arg)
			continue
		}
		if cluSpec.Alias != "" {
			err := fmt.Errorf("either invalid URL or duplicated alias %q", arg)
			return cluSpec, err
		}
		cluSpec.Alias = arg
	}
	return cluSpec, nil
}

func revokeTokenHandler(c *cli.Context) (err error) {
	token := c.Args().Get(0)
	tokenFile := parseStrFlag(c, tokenFileFlag)
	if token != "" && tokenFile != "" {
		return fmt.Errorf("defined either a token or token filename")
	}
	if tokenFile != "" {
		bt, err := os.ReadFile(tokenFile)
		if err != nil {
			return err
		}
		creds := &authn.TokenMsg{}
		if err = jsoniter.Unmarshal(bt, creds); err != nil {
			return fmt.Errorf("invalid token file format")
		}
		token = creds.Token
	}
	if token == "" {
		return missingArgumentsError(c, "token or token filename")
	}
	return api.RevokeToken(authParams, token)
}

func showAuthConfigHandler(c *cli.Context) (err error) {
	conf, err := api.GetAuthNConfig(authParams)
	if err != nil {
		return err
	}

	list, err := authNConfPairs(conf, c.Args().Get(0))
	if err != nil {
		return err
	}
	useJSON := flagIsSet(c, jsonFlag)
	if useJSON {
		return templates.DisplayOutput(conf, c.App.Writer, templates.PropsSimpleTmpl, useJSON)
	}
	return templates.DisplayOutput(list, c.App.Writer, templates.PropsSimpleTmpl, useJSON)
}

func authNConfigFromArgs(c *cli.Context) (conf *authn.ConfigToUpdate, err error) {
	conf = &authn.ConfigToUpdate{Server: &authn.ServerConfToUpdate{}}
	items := c.Args()
	for i := 0; i < len(items); {
		name, value := items.Get(i), items.Get(i+1)
		if idx := strings.Index(name, "="); idx > 0 {
			name = name[:idx]
			value = name[idx+1:]
			i++
		} else {
			i += 2
		}
		if value == "" {
			return nil, fmt.Errorf("no value for %q", name)
		}
		if err := cmn.UpdateFieldValue(conf, name, value); err != nil {
			return nil, err
		}
	}
	return conf, nil
}

func setAuthConfigHandler(c *cli.Context) (err error) {
	conf, err := authNConfigFromArgs(c)
	if err != nil {
		return err
	}
	return api.SetAuthNConfig(authParams, conf)
}

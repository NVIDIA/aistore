// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that create entities in the cluster.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/fatih/color"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"golang.org/x/term"
)

const (
	flagsAuthUserLogin   = "user_login"
	flagsAuthUserLogout  = "user_logout"
	flagsAuthUserShow    = "user_show"
	flagsAuthRoleAddSet  = "role_add_set"
	flagsAuthRevokeToken = "revoke_token"
	flagsAuthRoleShow    = "role_show"
	flagsAuthConfShow    = "conf_show"
)

const authnUnreachable = `AuthN unreachable at %s. You may need to update AIS CLI configuration or environment variable %s`

var (
	authFlags = map[string][]cli.Flag{
		flagsAuthUserLogin:   {tokenFileFlag, passwordFlag, expireFlag, clusterTokenFlag},
		flagsAuthUserLogout:  {tokenFileFlag},
		cmdAuthUser:          {passwordFlag},
		flagsAuthRoleAddSet:  {descRoleFlag, clusterRoleFlag, bucketRoleFlag},
		flagsAuthRevokeToken: {tokenFileFlag},
		flagsAuthUserShow:    {nonverboseFlag, verboseFlag},
		flagsAuthRoleShow:    {nonverboseFlag, verboseFlag, clusterFilterFlag},
		flagsAuthConfShow:    {jsonFlag},
	}

	// define separately to allow for aliasing (see alias_hdlr.go)
	authCmdShow = cli.Command{
		Name:  cmdAuthShow,
		Usage: "show entity in authn",
		Subcommands: []cli.Command{
			{
				Name:      cmdAuthCluster,
				Usage:     "show AIS clusters managed by this AuthN instance",
				ArgsUsage: showAuthClusterArgument,
				Action:    wrapAuthN(showAuthClusterHandler),
			},
			{
				Name:         cmdAuthRole,
				Usage:        "show existing AuthN roles",
				ArgsUsage:    showAuthRoleArgument,
				Flags:        authFlags[flagsAuthRoleShow],
				Action:       wrapAuthN(showAuthRoleHandler),
				BashComplete: oneRoleCompletions,
			},
			{
				Name:      cmdAuthUser,
				Usage:     "show user list and details",
				Flags:     authFlags[flagsAuthUserShow],
				ArgsUsage: showAuthUserListArgument,
				Action:    wrapAuthN(showAuthUserHandler),
			},
			{
				Name:   cmdAuthConfig,
				Usage:  "show AuthN server configuration",
				Flags:  authFlags[flagsAuthConfShow],
				Action: wrapAuthN(showAuthConfigHandler),
			},
		},
	}

	authCmd = cli.Command{
		Name:  commandAuth,
		Usage: "add/remove/show users, manage user roles, manage access to AIS clusters",
		Subcommands: []cli.Command{
			// show
			authCmdShow,
			// add
			{
				Name:  cmdAuthAdd,
				Usage: "add AuthN entity: user, role, AIS cluster",
				Subcommands: []cli.Command{
					{
						Name:         cmdAuthUser,
						Usage:        "add a new user",
						ArgsUsage:    addAuthUserArgument,
						Flags:        authFlags[cmdAuthUser],
						Action:       wrapAuthN(addAuthUserHandler),
						BashComplete: oneRoleCompletions,
					},
					{
						Name:      cmdAuthCluster,
						Usage:     "add AIS cluster (to authenticate access to buckets and to the cluster)",
						ArgsUsage: addAuthClusterArgument,
						Action:    wrapAuthN(addAuthClusterHandler),
					},
					{
						Name:         cmdAuthRole,
						Usage:        "create a new role",
						ArgsUsage:    addSetAuthRoleArgument,
						Flags:        authFlags[flagsAuthRoleAddSet],
						Action:       wrapAuthN(addAuthRoleHandler),
						BashComplete: addRoleCompletions,
					},
				},
			},
			// rm
			{
				Name:  cmdAuthRemove,
				Usage: "remove an entity from AuthN",
				Subcommands: []cli.Command{
					{
						Name:         cmdAuthUser,
						Usage:        "remove an existing user",
						ArgsUsage:    deleteAuthUserArgument,
						Action:       wrapAuthN(deleteUserHandler),
						BashComplete: oneUserCompletions,
					},
					{
						Name:         cmdAuthCluster,
						Usage:        "remove AIS cluster",
						ArgsUsage:    deleteAuthClusterArgument,
						Action:       wrapAuthN(deleteAuthClusterHandler),
						BashComplete: oneClusterCompletions,
					},
					{
						Name:         cmdAuthRole,
						Usage:        "remove an existing role",
						ArgsUsage:    deleteAuthRoleArgument,
						Action:       wrapAuthN(deleteRoleHandler),
						BashComplete: oneRoleCompletions,
					},
					{
						Name:      cmdAuthToken,
						Usage:     "revoke AuthN token",
						Flags:     authFlags[flagsAuthRevokeToken],
						ArgsUsage: deleteAuthTokenArgument,
						Action:    wrapAuthN(revokeTokenHandler),
					},
				},
			},
			// set
			{
				Name:  cmdAuthSet,
				Usage: "update AuthN configuration and its entities: users, roles, and AIS clusters",
				Subcommands: []cli.Command{
					{
						Name:         cmdAuthConfig,
						Usage:        "update AuthN server configuration",
						Action:       wrapAuthN(setAuthConfigHandler),
						BashComplete: suggestUpdatableAuthNConfig,
					},
					{
						Name:         cmdAuthCluster,
						Usage:        "update AIS cluster configuration (the cluster must be previously added to AuthN)",
						ArgsUsage:    addAuthClusterArgument,
						Action:       wrapAuthN(updateAuthClusterHandler),
						BashComplete: oneClusterCompletions,
					},
					{
						Name:         cmdAuthUser,
						Usage:        "update an existing user",
						ArgsUsage:    addAuthUserArgument,
						Flags:        authFlags[cmdAuthUser],
						Action:       wrapAuthN(updateAuthUserHandler),
						BashComplete: oneUserCompletionsWithRoles,
					},
					{
						Name:         cmdAuthRole,
						Usage:        "update an existing role for all users that have it",
						ArgsUsage:    addSetAuthRoleArgument,
						Flags:        authFlags[flagsAuthRoleAddSet],
						Action:       wrapAuthN(updateAuthRoleHandler),
						BashComplete: setRoleCompletions,
					},
				},
			},
			// login, logout
			{
				Name:      cmdAuthLogin,
				Usage:     "log in with existing user ID and password",
				Flags:     authFlags[flagsAuthUserLogin],
				ArgsUsage: userLoginArgument,
				Action:    wrapAuthN(loginUserHandler),
			},
			{
				Name:   cmdAuthLogout,
				Usage:  "log out",
				Flags:  authFlags[flagsAuthUserLogout],
				Action: wrapAuthN(logoutUserHandler),
			},
		},
	}
)

// Use the function to wrap every AuthN handler that does API calls.
// The function verifies that AuthN is up and running before doing the API call
// and augments API errors if needed.
func wrapAuthN(f cli.ActionFunc) cli.ActionFunc {
	return func(c *cli.Context) error {
		if authParams.Client == nil {
			return errors.New(env.AuthN.URL + " is not set")
		}
		err := f(c)
		if err != nil {
			if msg, unreachable := isUnreachableError(err); unreachable {
				err = fmt.Errorf(authnUnreachable, authParams.URL+" (detailed error: "+msg+")",
					env.AuthN.URL)
			}
		}
		return err
	}
}

func readMasked(c *cli.Context, prompt string) string {
	fmt.Fprint(c.App.Writer, prompt+": ")
	bytePass, err := term.ReadPassword(0)
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(string(bytePass), "\n")
}

func cliAuthnURL(cfg *config.Config) string {
	return cos.Right(cfg.Auth.URL, os.Getenv(env.AuthN.URL))
}

func lookupClusterID(cluID string) (string, error) {
	cluList, err := authn.GetRegisteredClusters(authParams, authn.CluACL{})
	if err != nil {
		return "", err
	}
	for _, clu := range cluList {
		if clu.ID == cluID {
			return cluID, nil
		}
		if clu.Alias == cluID {
			return clu.ID, nil
		}
	}
	return "", fmt.Errorf("cluster %q not found", cluID)
}

func filterRolesByCluster(roles []*authn.Role, clusters []string) ([]*authn.Role, error) {
	cluIDs := clusters[:0]
	for _, clu := range clusters {
		// Empty ID is a special case: the role is applied to all clusters.
		if clu == "" {
			cluIDs = append(cluIDs, clu)
			continue
		}
		cluID, err := lookupClusterID(clu)
		if err != nil {
			return nil, err
		}
		cluIDs = append(cluIDs, cluID)
	}
	filtered := roles[:0]
	for _, role := range roles {
		for _, clu := range role.ClusterACLs {
			if cos.StringInSlice(clu.ID, cluIDs) {
				filtered = append(filtered, role)
				break
			}
		}
		for _, bck := range role.BucketACLs {
			if cos.StringInSlice(bck.Bck.Ns.UUID, cluIDs) {
				filtered = append(filtered, role)
				break
			}
		}
	}
	return filtered, nil
}

func cliAuthnUserName(c *cli.Context) string {
	name := c.Args().Get(0)
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

func updateAuthUserHandler(c *cli.Context) error {
	user, err := userFromArgsOrStdin(c, true)
	if err != nil {
		return err
	}
	return authn.UpdateUser(authParams, user)
}

func addAuthUserHandler(c *cli.Context) error {
	user, err := userFromArgsOrStdin(c, false)
	if err != nil {
		return err
	}
	existingUsers, err := authn.GetAllUsers(authParams)
	if err != nil {
		return err
	}
	for _, u := range existingUsers {
		if u.ID == user.ID {
			return fmt.Errorf("user %q already exists", u.ID)
		}
	}
	fmt.Fprintln(c.App.Writer)
	return authn.AddUser(authParams, user)
}

func deleteUserHandler(c *cli.Context) (err error) {
	userName := c.Args().Get(0)
	if userName == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	return authn.DeleteUser(authParams, userName)
}

func deleteRoleHandler(c *cli.Context) (err error) {
	role := c.Args().Get(0)
	if role == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	return authn.DeleteRole(authParams, role)
}

func loginUserHandler(c *cli.Context) (err error) {
	var (
		expireIn *time.Duration
		name     = cliAuthnUserName(c)
		password = cliAuthnUserPassword(c, false)
		cluID    = parseStrFlag(c, clusterTokenFlag)
	)
	if flagIsSet(c, expireFlag) {
		expireIn = apc.Ptr(parseDurationFlag(c, expireFlag))
	}
	if cluID != "" {
		if _, err := authn.GetRegisteredClusters(authParams, authn.CluACL{ID: cluID}); err != nil {
			return err
		}
	}
	token, err := authn.LoginUser(authParams, name, password, expireIn)
	if err != nil {
		return err
	}
	tokenFilePath, err := getTokenFilePath(c)
	if err != nil {
		return err
	}
	if err := jsp.Save(tokenFilePath, token, jsp.Plain(), nil); err != nil {
		return fmt.Errorf("failed to write token %q: %v", tokenFilePath, err)
	}
	fmt.Fprintf(c.App.Writer, "Logged in (%s)\n", tokenFilePath)
	return nil
}

func logoutUserHandler(c *cli.Context) (err error) {
	tokenFilePath, err := getTokenFilePath(c)
	if err != nil {
		return fmt.Errorf("cannot logout: %v", err)
	}
	if err := revokeTokenHandler(c); err != nil {
		return err
	}
	if err := cos.RemoveFile(tokenFilePath); err != nil {
		return fmt.Errorf("failed to logout, could not remove token file %q: %v", tokenFilePath, err)
	}
	fmt.Fprintf(c.App.Writer, "Logged out (removed/revoked %q)\n", tokenFilePath)
	return nil
}

func addAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec, err := parseClusterSpecs(c)
	if err != nil {
		return
	}
	var smap *meta.Smap
	if len(cluSpec.URLs) == 0 {
		smap, err = getClusterMap(c)
		cluSpec.URLs = append(cluSpec.URLs, clusterURL)
	} else {
		bp := api.BaseParams{
			URL:   cluSpec.URLs[0],
			Token: loggedUserToken,
			UA:    ua,
		}
		if cos.IsHTTPS(bp.URL) {
			bp.Client = clientTLS
		} else {
			bp.Client = clientH
		}
		smap, err = api.GetClusterMap(bp)
		if err != nil {
			err = fmt.Errorf("failed to add cluster %q(%q, %s): %v", cluSpec.ID, cluSpec.Alias, cluSpec.URLs[0], err)
		}
	}
	if err != nil {
		return err
	}
	cluSpec.ID = smap.UUID
	return authn.RegisterCluster(authParams, cluSpec)
}

func updateAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec, err := parseClusterSpecs(c)
	if err != nil {
		return
	}
	if cluSpec.Alias == "" && cluSpec.ID == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	list, err := authn.GetRegisteredClusters(authParams, cluSpec)
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

	return authn.UpdateCluster(authParams, cluSpec)
}

func deleteAuthClusterHandler(c *cli.Context) (err error) {
	cid := c.Args().Get(0)
	if cid == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	cluSpec := authn.CluACL{ID: cid}
	return authn.UnregisterCluster(authParams, cluSpec)
}

func showAuthClusterHandler(c *cli.Context) (err error) {
	cluSpec := authn.CluACL{
		ID: c.Args().Get(0),
	}
	list, err := authn.GetRegisteredClusters(authParams, cluSpec)
	if err != nil {
		return err
	}

	return teb.Print(list, teb.AuthNClusterTmpl)
}

func showAuthSingleRole(c *cli.Context, roleID string) error {
	rInfo, err := authn.GetRole(authParams, roleID)
	if err != nil {
		return err
	}
	// verbose is the implicit default when showing one
	if flagIsSet(c, nonverboseFlag) {
		return teb.Print([]*authn.Role{rInfo}, teb.AuthNRoleTmpl)
	}
	return teb.Print(rInfo, teb.AuthNRoleVerboseTmpl)
}
func showAuthAllRoles(c *cli.Context) error {
	list, err := authn.GetAllRoles(authParams)
	if err != nil {
		return err
	}
	cluID := parseStrFlag(c, clusterFilterFlag)
	if cluID != "" {
		l := splitCsv(cluID)
		if list, err = filterRolesByCluster(list, l); err != nil {
			return err
		}
	}
	// non-verbose is the implicit default when showing all
	if flagIsSet(c, verboseFlag) {
		for i, role := range list {
			rInfo, err := authn.GetRole(authParams, role.Name)
			if err != nil {
				color.New(color.FgRed).Fprintf(c.App.Writer, "%s: %v\n", role.Name, err)
			} else {
				if i > 0 {
					fmt.Fprintln(c.App.Writer)
				}
				teb.Print(rInfo, teb.AuthNRoleVerboseTmpl)
			}
		}
		return nil
	}
	return teb.Print(list, teb.AuthNRoleTmpl)
}

func showAuthRoleHandler(c *cli.Context) (err error) {
	roleID := c.Args().Get(0)
	if roleID != "" {
		return showAuthSingleRole(c, roleID)
	}
	return showAuthAllRoles(c)
}

func showAuthUserHandler(c *cli.Context) (err error) {
	userID := c.Args().Get(0)
	if userID == "" {
		list, err := authn.GetAllUsers(authParams)
		if err != nil {
			return err
		}
		// non-verbose is the implicit default when showing all
		if flagIsSet(c, verboseFlag) {
			for i, user := range list {
				if uInfo, err := authn.GetUser(authParams, user.ID); err != nil {
					color.New(color.FgRed).Fprintf(c.App.Writer, "%s: %v\n", user.ID, err)
				} else {
					if i > 0 {
						fmt.Fprintln(c.App.Writer)
					}
					teb.Print(uInfo, teb.AuthNUserVerboseTmpl)
				}
			}
			return nil
		}
		return teb.Print(list, teb.AuthNUserTmpl)
	}
	uInfo, err := authn.GetUser(authParams, userID)
	if err != nil {
		return err
	}
	// verbose is the implicit default when showing one
	if flagIsSet(c, nonverboseFlag) {
		return teb.Print([]*authn.User{uInfo}, teb.AuthNUserTmpl)
	}
	return teb.Print(uInfo, teb.AuthNUserVerboseTmpl)
}

func addAuthRoleHandler(c *cli.Context) error {
	rInfo, err := addOrUpdateRole(c)
	if err != nil {
		return err
	}
	return authn.AddRole(authParams, rInfo)
}

func updateAuthRoleHandler(c *cli.Context) error {
	rInfo, err := addOrUpdateRole(c)
	if err != nil {
		return err
	}
	return authn.UpdateRole(authParams, rInfo)
}

// TODO: bucket permissions
func addOrUpdateRole(c *cli.Context) (*authn.Role, error) {
	var (
		alias   string
		args    = c.Args()
		cluster = parseStrFlag(c, clusterRoleFlag)
		bucket  = parseStrFlag(c, bucketRoleFlag)
		role    = args.Get(0)
	)
	if bucket != "" && cluster == "" {
		return nil, fmt.Errorf("flag %s requires %s to be specified", qflprn(bucketRoleFlag), qflprn(clusterRoleFlag))
	}

	if cluster != "" {
		cluList, err := authn.GetRegisteredClusters(authParams, authn.CluACL{})
		if err != nil {
			return nil, err
		}
		var found bool
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
			return nil, fmt.Errorf("cluster %q not found", cluster)
		}
	}

	perms := apc.AccessNone
	for i := 1; i < c.NArg(); i++ {
		p, err := apc.StrToAccess(args.Get(i))
		if err != nil {
			return nil, err
		}
		perms |= p
	}
	roleACL := &authn.Role{
		Name:        role,
		Description: parseStrFlag(c, descRoleFlag),
	}
	if bucket != "" {
		bck, err := parseBckURI(c, bucket, false)
		if err != nil {
			return nil, err
		}
		bck.Ns.UUID = cluster
		roleACL.BucketACLs = []*authn.BckACL{
			{
				Bck:    bck,
				Access: perms,
			},
		}
	} else {
		roleACL.ClusterACLs = []*authn.CluACL{
			{
				ID:     cluster,
				Alias:  alias,
				Access: perms,
			},
		}
	}
	return roleACL, nil
}

func userFromArgsOrStdin(c *cli.Context, omitEmpty bool) (*authn.User, error) {
	var (
		username = cliAuthnUserName(c)
		userpass = cliAuthnUserPassword(c, omitEmpty)
		args     = c.Args().Tail()
	)

	roles := make([]*authn.Role, 0, len(args))
	for _, roleName := range args {
		roleName = strings.TrimSpace(roleName)
		roleInfo, err := authn.GetRole(authParams, roleName)
		if err != nil {
			fmt.Fprintf(c.App.Writer, "Role %q not found\n", roleName)
			return nil, err
		}
		roles = append(roles, roleInfo)
	}
	return &authn.User{ID: username, Password: userpass, Roles: roles}, nil
}

func parseClusterSpecs(c *cli.Context) (cluSpec authn.CluACL, err error) {
	cluSpec.URLs = make([]string, 0, 1)
	for idx := range c.NArg() {
		arg := c.Args().Get(idx)
		if strings.HasPrefix(arg, "http:") || strings.HasPrefix(arg, "https:") ||
			strings.HasPrefix(arg, "HTTP:") || strings.HasPrefix(arg, "HTTPS:") {
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
	tokenFilePath, err := getTokenFilePath(c)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return fmt.Errorf("failed to read token %q: %v", tokenFilePath, err)
	}
	msg := &authn.TokenMsg{}
	if err := jsoniter.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("invalid token %q format: %v", tokenFilePath, err)
	}
	return authn.RevokeToken(authParams, msg.Token)
}
func showAuthConfigHandler(c *cli.Context) (err error) {
	conf, err := authn.GetConfig(authParams)
	if err != nil {
		return err
	}

	list, err := authNConfPairs(conf, c.Args().Get(0))
	if err != nil {
		return err
	}
	usejs := flagIsSet(c, jsonFlag)
	switch {
	case usejs:
		return teb.Print(conf, teb.PropValTmpl, teb.Jopts(usejs))
	case flagIsSet(c, noHeaderFlag):
		return teb.Print(list, teb.PropValTmplNoHdr)
	default:
		return teb.Print(list, teb.PropValTmpl)
	}
}

func authNConfigFromArgs(c *cli.Context) (conf *authn.ConfigToUpdate, err error) {
	conf = &authn.ConfigToUpdate{Server: &authn.ServerConfToSet{}}
	items := c.Args()
	for i := 0; i < len(items); {
		name, value := items.Get(i), items.Get(i+1)
		if idx := strings.Index(name, "="); idx > 0 {
			value = strings.TrimSpace(name[idx+1:])
			name = strings.TrimSpace(name[:idx])
			if value == "" {
				return nil, fmt.Errorf("'auth set config %q': missing value in %q", name, items.Get(i))
			}
			if name == "" {
				return nil, fmt.Errorf("'auth set config %q': empty name in %q", name, items.Get(i))
			}
			i++
		} else {
			i += 2
			if value == "" {
				return nil, fmt.Errorf("'auth set config %q': missing value", name)
			}
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
	return authn.SetConfig(authParams, conf)
}

// getTokenFilePath retrieves the file path for the authentication token.
// It checks the token file flag, environment variable, and default location in order of precedence.
// If the file does not exist, it attempts to create the necessary directories.
func getTokenFilePath(c *cli.Context) (string, error) {
	tokenFilePath := parseStrFlag(c, tokenFileFlag)
	if tokenFilePath == "" {
		tokenFilePath = os.Getenv(env.AuthN.TokenFile)
	}
	if tokenFilePath == "" {
		tokenFilePath = filepath.Join(config.ConfigDir, fname.Token)
	}
	if err := cos.Stat(tokenFilePath); err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("failed to access token file %q: %v", tokenFilePath, err)
		}
		if createErr := cos.CreateDir(filepath.Dir(tokenFilePath)); createErr != nil {
			return "", fmt.Errorf("failed to create directory for token file %q: %v", tokenFilePath, createErr)
		}
	}
	return tokenFilePath, nil
}

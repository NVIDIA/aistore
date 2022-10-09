// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides aliases to frequently used commands that are inside other top level commands.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/urfave/cli"
)

// NOTE: for built-in aliases, see `DefaultAliasConfig` (cmd/cli/config/config.go)

const invalidAlias = "alias must start with a letter and can only contain letters, numbers, hyphens (-), and underscores (_)"

func (aisCLI *AISCLI) getAliasCmd() cli.Command {
	aliasCmd := cli.Command{
		Name:   commandAlias,
		Usage:  "manage top-level aliases",
		Action: showCLIAliasHandler,
		Subcommands: []cli.Command{
			{
				Name:   subcmdCLIAliasShow,
				Usage:  "display list of aliases",
				Action: showCLIAliasHandler,
			},
			{
				Name:      subcmdCLIAliasRm,
				Usage:     "remove existing alias",
				ArgsUsage: aliasCmdArgument,
				Action:    rmCLIAliasHandler,
			},
			{
				Name:   subcmdCLIAliasReset,
				Usage:  "reset aliases to default",
				Action: resetCLIAliasHandler,
			},
			{
				Name:      subcmdCLIAliasSet,
				Usage:     "add new or update existing alias",
				ArgsUsage: aliasSetCmdArgument,
				Action:    aisCLI.setCLIAliasHandler,
			},
		},
	}
	return aliasCmd
}

// initAliases reads cfg.Aliases and returns all aliases.
func (aisCLI *AISCLI) initAliases() (aliasCmds []cli.Command) {
	for alias, orig := range cfg.Aliases {
		cmd := aisCLI.resolveCmd(orig)

		if cmd != nil {
			aliasCmds = append(aliasCmds, makeAlias(*cmd, orig, false, alias))
		}
	}
	return
}

func validateAlias(alias string) (matched bool) {
	matched, _ = regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9_-]*$`, alias)
	return
}

// resolveCmd() traverses the command tree and returns the cli.Command matching `args`
// similar to cli.App.Command(), but looking through subcommands as well.
func (aisCLI *AISCLI) resolveCmd(command string) *cli.Command {
	if command == "" {
		return nil
	}
	var (
		app      = aisCLI.app
		args     = strings.Split(command, " ")
		toplevel = args[0]
		tlCmd    = app.Command(toplevel)
	)
	if len(args) == 1 {
		return tlCmd
	}

	currCmd := tlCmd
	for _, token := range args[1:] {
		consumed := false
		for i := range currCmd.Subcommands {
			c := &currCmd.Subcommands[i]
			if c.HasName(token) {
				currCmd = c
				consumed = true
				break
			}
		}
		if !consumed {
			return nil
		}
	}
	return currCmd
}

// makeAlias returns a copy of cmd with some changes:
// 1. command name is changed if provided.
// 2. category set to "ALIASES" if specified.
// 3. "alias for" message added to usage if not a silent alias.
func makeAlias(cmd cli.Command, aliasFor string, silentAlias bool, newName ...string) cli.Command {
	if len(newName) != 0 {
		cmd.Name = newName[0]
	}
	if !silentAlias {
		cmd.Category = "ALIASES"
		cmd.Usage = fmt.Sprintf("(alias for %q) %s", aliasFor, cmd.Usage)
	}

	// help is already added to the original, remove from cmd and all subcmds
	cmd.HideHelp = true
	if len(cmd.Subcommands) != 0 {
		aliasSubcmds := make([]cli.Command, len(cmd.Subcommands))
		for i := range cmd.Subcommands {
			aliasSubcmds[i] = makeAlias(cmd.Subcommands[i], "", true)
		}
		cmd.Subcommands = aliasSubcmds
	}

	return cmd
}

func appendSubcommand(cmd, subCmd cli.Command) cli.Command {
	cmd.Subcommands = append(cmd.Subcommands, subCmd)
	return cmd
}

func resetCLIAliasHandler(c *cli.Context) (err error) {
	cfg.Aliases = config.DefaultAliasConfig
	if err := config.Save(cfg); err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, "Aliases reset to default")
	return
}

func showCLIAliasHandler(c *cli.Context) (err error) {
	aliases := make(nvpairList, 0, len(cfg.Aliases))
	for k, v := range cfg.Aliases {
		aliases = append(aliases, nvpair{Name: k, Value: v})
	}
	sort.Slice(aliases, func(i, j int) bool {
		return aliases[i].Name < aliases[j].Name
	})
	return tmpls.Print(aliases, c.App.Writer, tmpls.AliasTemplate, nil, false)
}

func rmCLIAliasHandler(c *cli.Context) (err error) {
	alias := c.Args().First()
	if alias == "" {
		return missingArgumentsError(c, "alias")
	}
	if _, ok := cfg.Aliases[alias]; !ok {
		return fmt.Errorf("alias %q does not exist", alias)
	}
	delete(cfg.Aliases, alias)
	return config.Save(cfg)
}

func (aisCLI *AISCLI) setCLIAliasHandler(c *cli.Context) (err error) {
	alias := c.Args().First()
	if alias == "" {
		return missingArgumentsError(c, "alias")
	}
	if !validateAlias(alias) {
		return fmt.Errorf(invalidAlias)
	}

	if c.NArg() < 2 {
		return missingArgumentsError(c, "command")
	}
	oldCmd, ok := cfg.Aliases[alias]
	newCmd := ""
	for _, arg := range c.Args().Tail() {
		if newCmd != "" {
			newCmd += " "
		}
		newCmd += arg
	}
	if cmd := aisCLI.resolveCmd(newCmd); cmd == nil {
		return fmt.Errorf("%q is not AIS command", newCmd)
	}
	cfg.Aliases[alias] = newCmd
	if ok {
		fmt.Fprintf(c.App.Writer, "Alias %q new command %q (was: %q)\n", alias, newCmd, oldCmd)
	} else {
		fmt.Fprintf(c.App.Writer, "Aliased %q = %q\n", newCmd, alias)
	}
	return config.Save(cfg)
}

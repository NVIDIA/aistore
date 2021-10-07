// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides aliases to frequently used commands that are inside other top level commands.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/urfave/cli"
)

// NOTE: for built-in aliases, see `DefaultAliasConfig` (cmd/cli/config/config.go)

const invalidAlias = "alias must start with a letter and can only contain letters, numbers, hyphens (-), and underscores (_)"

func (aisCLI *AISCLI) getAliasCmd() cli.Command {
	return cli.Command{
		Name:      commandAlias,
		Usage:     "create top-level alias to a CLI command",
		ArgsUsage: aliasCmdArgument,
		Flags:     []cli.Flag{resetAliasFlag},
		Action:    aisCLI.aliasHandler,
	}
}

func (aisCLI *AISCLI) aliasHandler(c *cli.Context) (err error) {
	if flagIsSet(c, resetAliasFlag) {
		return resetAliases(c)
	}
	if !c.Args().Present() {
		return listAliases(c)
	}
	return aisCLI.addAlias(c)
}

func listAliases(c *cli.Context) (err error) {
	return templates.DisplayOutput(cfg.Aliases, c.App.Writer, templates.AliasTemplate)
}

func resetAliases(c *cli.Context) (err error) {
	cfg.Aliases = config.DefaultAliasConfig
	if err := config.Save(cfg); err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, "aliases reset to default")
	return
}

func (aisCLI *AISCLI) addAlias(c *cli.Context) (err error) {
	var (
		input = strings.Split(strings.Join(c.Args(), " "), "=")
		alias = input[0]
		orig  = input[1]
	)
	if !validateAlias(alias) {
		return fmt.Errorf(invalidAlias)
	}
	if cmd := aisCLI.resolveCmd(orig); cmd == nil {
		return fmt.Errorf("command %q not found", orig)
	}

	cfg.Aliases[alias] = orig
	if err := config.Save(cfg); err != nil {
		return err
	}

	fmt.Fprintf(c.App.Writer, "aliased %q=%q\n", alias, orig)
	return
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

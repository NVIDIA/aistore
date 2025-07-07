// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/urfave/cli"
)

// makeAlias returns a shallow copy of the original `cmd` with specified
// modifications:
// 1. command name is changed if newName is provided
// 2. if `aliasFor` is not empty "alias for" message added to usage
// 3. flags can be added or removed as specified
// 4. help is hidden to avoid duplication with original command
//
// The function performs shallow copying of the command structure:
// while the top-level cli.Command struct is copied, nested slices and any pointer
// fields continue to reference the same underlying data as the original command.
//
// This approach relies on the following facts:
// - Commands are defined statically at startup and are never modified at runtime
// - The CLI framework does not mutate command structures after initialization
// - Flag slices are deep-copied only when modifications (add/remove) are requested
//
// [NOTE] for `makeAlias usage guidelines, please refer to [make_alias.md](https://github.com/NVIDIA/aistore/blob/main/cmd/cli/cli/make_alias.md)

type mkaliasOpts struct {
	newName  string
	aliasFor string
	addFlags []cli.Flag
	delFlags []cli.Flag
	replace  cos.StrKVs
}

func makeAlias(cmd *cli.Command, opts *mkaliasOpts) cli.Command {
	aliasCmd := *cmd
	if opts.newName != "" {
		aliasCmd.Name = opts.newName
	}
	if opts.aliasFor != "" {
		aliasCmd.Usage = fmt.Sprintf(aliasForPrefix+"%q) %s", opts.aliasFor, cmd.Usage)
	}

	// help is already added to the original
	aliasCmd.HideHelp = true

	// subcommand management (recursive)
	if len(aliasCmd.Subcommands) != 0 {
		aliasSub := make([]cli.Command, len(cmd.Subcommands))
		for i := range cmd.Subcommands {
			subCmdCopy := &cmd.Subcommands[i]
			aliasSub[i] = makeAlias(subCmdCopy, &mkaliasOpts{})
		}
		aliasCmd.Subcommands = aliasSub
	}

	// flag management: deep copy original flags and add/remove as needed
	if len(opts.addFlags) > 0 || len(opts.delFlags) > 0 {
		var nflags []cli.Flag
		if cmd.Flags != nil {
			nflags = make([]cli.Flag, len(cmd.Flags))
			if len(opts.delFlags) > 0 {
				nflags = rmFlags(cmd.Flags, opts.delFlags...)
			} else {
				copy(nflags, cmd.Flags)
			}
		} else {
			debug.Assert(len(opts.delFlags) == 0, "no flags to remove")
		}
		nflags = append(nflags, opts.addFlags...)
		aliasCmd.Flags = sortFlags(nflags)
	}

	// help text management
	if opts.replace != nil {
		_updAliasedHelp(&aliasCmd, opts.replace)
	}

	return aliasCmd
}

// make targeted replacements in help text of the aliased command
func _updAliasedHelp(cmd *cli.Command, replace cos.StrKVs) {
	for oldPath, newPath := range replace {
		cmd.Usage = strings.ReplaceAll(cmd.Usage, oldPath, newPath)
	}
	// Recursively update subcommands
	for i := range cmd.Subcommands {
		_updAliasedHelp(&cmd.Subcommands[i], replace)
	}
}

// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
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
	newName   string
	aliasFor  string
	addFlags  []cli.Flag
	delFlags  []cli.Flag
	replace   cos.StrKVs
	usage     string
	argsUsage string
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
			if len(opts.delFlags) > 0 {
				nflags = rmFlags(cmd.Flags, opts.delFlags...)
			} else {
				nflags = make([]cli.Flag, 0, len(cmd.Flags)+len(opts.addFlags))
				nflags = append(nflags, cmd.Flags...)
			}
		} else {
			debug.Assert(len(opts.delFlags) == 0, "no flags to remove")
		}
		nflags = append(nflags, opts.addFlags...)
		aliasCmd.Flags = sortFlags(nflags)
	}

	// help text management
	if opts.usage != "" {
		debug.Assert(opts.replace == nil)
		aliasCmd.Usage = opts.usage
	} else if opts.replace != nil {
		_updAliasedHelp(&aliasCmd, opts.replace)
	}
	if opts.argsUsage != "" {
		aliasCmd.ArgsUsage = opts.argsUsage
	}

	return aliasCmd
}

// make targeted replacements in help text of the aliased command
func _updAliasedHelp(cmd *cli.Command, replace cos.StrKVs) {
	usage := cmd.Usage
	offset := 0

	if strings.HasPrefix(usage, aliasForPrefix) {
		if end := strings.Index(usage, ") "); end != -1 {
			offset = end + 2
		}
	}

	prefix := usage[:offset]
	suffix := usage[offset:]

	for old, new := range replace {
		suffix = strings.ReplaceAll(suffix, old, new)
	}

	cmd.Usage = prefix + suffix

	for i := range cmd.Subcommands {
		_updAliasedHelp(&cmd.Subcommands[i], replace)
	}
}

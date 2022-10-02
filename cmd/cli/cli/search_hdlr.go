// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides commands that remove various entities from the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

var (
	searchCmdFlags = []cli.Flag{
		regexFlag,
	}

	searchCommands []cli.Command

	similarWords = map[string][]string{
		commandMountpath: {"mount", "unmount", "umount"},
		commandList:      {"list", "dir"},
		commandSet:       {"update", "assign", "modify"},
		commandShow:      {"view", "display", "list"},
		commandRemove:    {"remove", "delete", "del", "evict", "destroy"},
		commandRename:    {"move", "rename"},
		commandCopy:      {"copy", "replicate"},
		commandGet:       {"fetch", "read"},
		commandPrefetch:  {"load", "preload", "warmup", "cache"},
		commandMirror:    {"protect", "replicate"},
		commandECEncode:  {"protect", "encode", "replicate", "erasure-code"},
		commandStart:     {"do", "run", "execute"},
		commandStop:      {"abort", "termnate"},
		commandPut:       {"update", "write", "promote", "modify"},
		commandCreate:    {"add", "new"},
		commandObject:    {"file"},
		commandStorage:   {"disk", "mountpath", "capacity", "used", "available"},
		commandBucket:    {"dir", "directory"},
		commandJob:       {"xaction", "batch", "async"},
		commandArch:      {"serialize", "format", "reformat", "tar", "zip", "gzip"},
		//
		subcmdAuthAdd:  {"register", "create"},
		subcmdDownload: {"load"},
	}

	// app state
	cmdStrs    []string
	keywordMap map[string][]string // mapping of synonym to actual
	invIndex   map[string][]int    // inverted index key: [commands]
)

func initSearch(app *cli.App) {
	searchCommands = []cli.Command{
		{
			Name:         commandSearch,
			Usage:        "search ais commands",
			ArgsUsage:    searchArgument,
			Action:       searchCmdHdlr,
			Flags:        searchCmdFlags,
			BashComplete: searchBashCmplt,
		},
	}

	cmdStrs = getFullCmdNames(app.Name, app.Commands)
	populateKeyMapInvIdx()
}

func populateKeyMapInvIdx() {
	invIndex = make(map[string][]int)
	keywordMap = invertMap(similarWords)
	for i := range cmdStrs {
		keywords := strings.Split(cmdStrs[i], " ")
		for _, word := range keywords {
			keywordMap[word] = append(keywordMap[word], word)
			invIndex[word] = append(invIndex[word], i)
		}
	}
}

func findCmdByKey(key string) (result cos.StrSet) {
	result = make(cos.StrSet)
	if resKeys, ok := keywordMap[key]; ok {
		for _, resKey := range resKeys {
			for _, idx := range invIndex[resKey] {
				result.Add(cmdStrs[idx])
			}
		}
	}
	return
}

func findCmdMultiKey(keys []string) []string {
	resultSet := findCmdByKey(keys[0])
	for _, key := range keys[1:] {
		cmds := findCmdByKey(key)
		resultSet = resultSet.Intersection(cmds)
	}

	result := resultSet.ToSlice()
	sort.Strings(result)
	return result
}

func findCmdMatching(pattern string) []string {
	result := make([]string, 0)
	for _, cmd := range cmdStrs {
		if cond, _ := regexp.MatchString(pattern, cmd); cond {
			result = append(result, cmd)
		}
	}
	return result
}

func searchCmdHdlr(c *cli.Context) error {
	if !flagIsSet(c, regexFlag) && c.NArg() == 0 {
		return missingArgumentsError(c, "keyword")
	}
	var commands []string

	if flagIsSet(c, regexFlag) {
		pattern := parseStrFlag(c, regexFlag)
		commands = findCmdMatching(pattern)
	} else {
		commands = findCmdMultiKey(c.Args())
	}

	return tmpls.DisplayOutput(commands, c.App.Writer, tmpls.SearchTmpl, nil, false)
}

func searchBashCmplt(_ *cli.Context) {
	for key := range keywordMap {
		fmt.Println(key)
	}
}

func invertMap(inp map[string][]string) map[string][]string {
	inv := make(map[string][]string)
	for key := range inp {
		for _, v := range inp[key] {
			inv[v] = append(inv[v], key)
		}
	}
	return inv
}

func getFullCmdNames(base string, cmds cli.Commands) []string {
	names := make([]string, 0)

	for i := range cmds {
		cmd := &cmds[i]
		fullCmd := fmt.Sprintf("%s %s", base, cmd.FullName())

		if len(cmd.Subcommands) == 0 {
			names = append(names, fullCmd)
			continue
		}

		output := getFullCmdNames(fullCmd, cmd.Subcommands)
		names = append(names, output...)
	}

	return names
}

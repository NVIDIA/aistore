// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides commands that remove various entities from the cluster.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

const searchUsage = "search " + cliName + " commands, e.g.:\n" +
	indent1 + "\t - 'ais search log' - commands containing 'log' subcommand\n" +
	indent1 + "\t - 'ais search --regex log' - include all subcommands that contain 'log' substring\n" +
	indent1 + "\t - 'ais search --regex \"\\blog\"' - slightly narrow the search to those commands that have 'log' on a word boundary, etc."

var (
	searchCmdFlags = []cli.Flag{
		regexFlag,
	}

	searchCommands []cli.Command

	similarWords = map[string][]string{
		cmdMountpath:    {"mount", "unmount", "umount", "disk"},
		commandList:     {"list", "dir", "contents"},
		commandSet:      {"update", "assign", "modify"},
		commandShow:     {"view", "display", "list"},
		commandRemove:   {"remove", "delete", "del", "evict", "destroy", "cleanup"},
		commandRename:   {"move", "rename", "ren"},
		commandCopy:     {"copy", "replicate", "backup"},
		commandGet:      {"fetch", "read", "download"},
		commandPrefetch: {"load", "preload", "warmup", "cache", "get"},
		commandMirror:   {"protect", "replicate", "copy", "n-way", "backup", "redundancy"},
		commandECEncode: {"protect", "encode", "replicate", "erasure-code", "backup", "redundancy"},
		commandStart:    {"do", "run", "execute"},
		commandStop:     {"abort", "terminate"},
		commandPut:      {"update", "write", "promote", "modify", "upload"},
		commandCreate:   {"add", "new"},
		commandObject:   {"file"},
		commandStorage:  {"disk", "mountpath", "capacity", "used", "available"},
		commandBucket:   {"dir", "directory", "container"},
		commandJob:      {"batch", "async"},
		commandArch:     {"serialize", "format", "reformat", "compress", "tar", "zip", "gzip"},
		cmdAuthAdd:      {"register", "create"},
		cmdStgCleanup:   {"remove", "delete", "evict"},
		cmdDownload:     {"load", "populate", "copy", "cp"},
		commandTLS:      {"x509", "X509", "X.509", "certificate", "https"},
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
			Usage:        searchUsage,
			ArgsUsage:    searchArgument,
			Action:       searchCmdHdlr,
			Flags:        sortFlags(searchCmdFlags),
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

func findCmdByKey(key string) cos.StrSet {
	result := make(cos.StrSet)
	_find(key, result)

	// in addition:
	for w := range keywordMap {
		if strings.HasPrefix(w, key+"-") { // e.g., ("reset-stats", "reset")
			_find(w, result)
		}
	}
	return result
}

func _find(key string, result cos.StrSet) {
	resKeys, ok := keywordMap[key]
	if !ok {
		return
	}
	for _, resKey := range resKeys {
		for _, idx := range invIndex[resKey] {
			result.Add(cmdStrs[idx])
		}
	}
}

// (compare w/ findCmdMultiKeyAlt)
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

func findCmdMultiKeyAlt(keys ...string) []string {
	var (
		result    []string
		resultSet = findCmdByKey(keys[0])
	)
outer:
	for cmd := range resultSet {
		for _, key := range keys[1:] {
			if !strings.Contains(cmd, " "+key+" ") && !strings.HasSuffix(cmd, " "+key) {
				continue outer
			}
		}
		result = append(result, cmd)
	}

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

//
// see also: actionIsHandler
//

func searchCmdHdlr(c *cli.Context) (err error) {
	var commands []string
	if !flagIsSet(c, regexFlag) && c.NArg() == 0 {
		return missingArgumentsError(c, "keyword")
	}
	if flagIsSet(c, regexFlag) {
		pattern := parseStrFlag(c, regexFlag)
		commands = findCmdMatching(pattern)
	} else {
		if c.NArg() > 1 {
			for word, similar := range similarWords {
				if !slices.Contains(c.Args(), word) {
					continue
				}
				for _, word2 := range similar {
					if slices.Contains(c.Args(), word2) {
						warn := fmt.Sprintf("%q and %q are \"similar\"", word, word2)
						actionWarn(c, warn+" (search results may include either/or combinations)")
					}
				}
			}
		}
		commands = findCmdMultiKey(c.Args())
	}

	if len(commands) > 0 {
		err = teb.Print(commands, teb.SearchTmpl)
	}
	if len(commands) == 0 && err == nil && !flagIsSet(c, regexFlag) {
		// tip
		msg := fmt.Sprintf("No matches (use %s to include more results, e.g.: '%s %s %s %s')\n",
			qflprn(regexFlag), cliName, commandSearch, flprn(regexFlag), c.Args().Get(0))
		actionDone(c, msg)
	}
	return err
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

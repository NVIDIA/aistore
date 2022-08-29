// Package aisfs - command-line mounting utility for aisfs.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"strings"

	"github.com/urfave/cli"
)

type flags struct {
	// Control
	Wait bool

	// File System
	AdditionalMountOptions map[string]string
	UID                    int32
	GID                    int32
}

func parseFlags(c *cli.Context) *flags {
	flags := &flags{
		// Control
		Wait: c.Bool("wait"),

		// File System
		AdditionalMountOptions: parseAdditionalMountOptions(c),
		UID:                    int32(c.Int("uid")),
		GID:                    int32(c.Int("gid")),
	}

	return flags
}

// Advanced mounting with additional mount options.
//
// mount command accepts options in the format: `-o OPTIONS`,
// where OPTIONS is a comma-separated list of `KEY[=VALUE]`.
// See `man 8 mount` for more details.
//
// It is assumed that neither KEY nor VALUE contain a comma!
//
// Also, only the first `=` is treated as a separator, so in
// the case of `-o a=b=c` KEY is `a`, and VALUE is `b=c`.
//
// VALUE is optional. Example: `-o noatime,nodev,nosuid`
// provides three options that do not require a value.
//
// There can be multiple `-o OPTIONS` on the command line.

func parseAdditionalMountOptions(c *cli.Context) map[string]string {
	options := make(map[string]string)

	for _, list := range c.StringSlice("o") {
		for _, pair := range strings.Split(list, ",") {
			key, value := splitOnFirst(pair, "=")
			options[key] = value
		}
	}

	return options
}

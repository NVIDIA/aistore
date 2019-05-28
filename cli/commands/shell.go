// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles bash completions for the CLI
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

// Bash Completion
func daemonList(_ *cli.Context) {
	baseParams := cliAPIParams(ClusterURL)
	smap, _ := api.GetClusterMap(baseParams)

	for dae := range smap.Pmap {
		fmt.Println(dae)
	}
	for dae := range smap.Tmap {
		fmt.Println(dae)
	}
}

func targetList(_ *cli.Context) {
	baseParams := cliAPIParams(ClusterURL)
	smap, _ := api.GetClusterMap(baseParams)

	for dae := range smap.Tmap {
		fmt.Println(dae)
	}
}

// Returns flags for command
func flagList(c *cli.Context) {
	for _, flag := range c.Command.Flags {
		fmt.Printf("--%s\n", cleanFlag(flag.GetName()))
	}
}

// The function will list bucket names if the first argument to the command was not yet specified, otherwise it will
// list flags and everything that `additionalCompletions` list.
// By default it tries to read `provider` from flag `--provider` or AIS_BUCKET_PROVIDER env variable. If none
// is provided it lists all buckets.
// Optional parameter `provider` can be used to specify which buckets will be listed - only local or only cloud.
func bucketList(additionalCompletions []cli.BashCompleteFunc, provider ...string) cli.BashCompleteFunc {
	bckProvider := ""
	if len(provider) > 0 {
		bckProvider = provider[0]
	}

	// Completions for bucket names based on bucket provider
	return func(c *cli.Context) {
		// Don't list buckets if one is provided via env variable
		if c.NArg() >= 1 {
			for _, f := range additionalCompletions {
				f(c)
			}
			flagList(c)
			return
		}

		// If not specified, try to get provider from flag or env variable
		if bckProvider == "" {
			bckProvider = parseStrFlag(c, bckProviderFlag)
			if bckProvider == "" {
				bckProvider = os.Getenv(aisBucketProviderEnvVar)
			}

			var err error
			bckProvider, err = cmn.BckProviderFromStr(bckProvider)
			if err != nil {
				bckProvider = ""
			}
		}

		baseParams := cliAPIParams(ClusterURL)
		bucketNames, err := api.GetBucketNames(baseParams, bckProvider)
		if err != nil {
			return
		}

		if bckProvider == cmn.LocalBs || bckProvider == "" {
			for _, bucket := range bucketNames.Local {
				fmt.Println(bucket)
			}
		}
		if bckProvider == cmn.CloudBs || bckProvider == "" {
			for _, bucket := range bucketNames.Cloud {
				fmt.Println(bucket)
			}
		}
	}
}

// Xaction list
func xactList(_ *cli.Context) {
	for key := range cmn.XactKind {
		fmt.Println(key)
	}
}

func propList(_ *cli.Context) {
	for prop, readonly := range cmn.BucketPropList {
		if !readonly {
			fmt.Println(prop)
		}
	}
}

func configPropList(_ *cli.Context) {
	for prop, readonly := range cmn.ConfigPropList {
		if !readonly {
			fmt.Println(prop)
		}
	}
}

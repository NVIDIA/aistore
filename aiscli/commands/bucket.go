// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with buckets in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	BucketCreate = "create"
	BucketNames  = "names"
)

var (
	BaseBucketFlags = []cli.Flag{
		bucketFlag,
		bckProviderFlag,
	}

	BucketFlags = map[string][]cli.Flag{
		BucketCreate: []cli.Flag{bucketFlag},
		BucketNames:  []cli.Flag{regexFlag, bckProviderFlag},
		CommandDel:   []cli.Flag{bucketFlag},
		CommandRename: append(
			[]cli.Flag{
				newBucketFlag,
			},
			BaseBucketFlags...),
		CommandList: append(
			[]cli.Flag{
				regexFlag,
				prefixFlag,
				pageSizeFlag,
				objPropsFlag,
				objLimitFlag,
			},
			BaseBucketFlags...),
	}

	BucketCreateDelList = "aiscli bucket %s --bucket <value>"
	BucketCreateText    = fmt.Sprintf(BucketCreateDelList, BucketCreate)
	BucketDelText       = fmt.Sprintf(BucketCreateDelList, CommandDel)
	BucketListText      = fmt.Sprintf(BucketCreateDelList, CommandList)
	BucketRenameText    = fmt.Sprintf("aiscli bucket %s --bucket <value> --newbucket <value>", CommandRename)
	BucketNamesText     = fmt.Sprintf("aiscli bucket %s", BucketNames)
)

func BucketHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)

	command := c.Command.Name
	switch command {
	case BucketCreate:
		if err = checkFlags(c, bucketFlag.Name); err != nil {
			return err
		}

		bucket := parseFlag(c, bucketFlag.Name)
		if err = api.CreateLocalBucket(baseParams, bucket); err != nil {
			return err
		}

		fmt.Printf("%s bucket created\n", bucket)
	case CommandDel:
		if err = checkFlags(c, bucketFlag.Name); err != nil {
			return err
		}

		bucket := parseFlag(c, bucketFlag.Name)
		if err = api.DestroyLocalBucket(baseParams, bucket); err != nil {
			return err
		}
		fmt.Printf("%s bucket deleted\n", bucket)
	case CommandRename:
		if err = checkFlags(c, bucketFlag.Name, newBucketFlag.Name); err != nil {
			return err
		}

		bucket := parseFlag(c, bucketFlag.Name)
		newBucket := parseFlag(c, newBucketFlag.Name)
		if err = api.RenameLocalBucket(baseParams, bucket, newBucket); err != nil {
			return err
		}
		fmt.Printf("%s bucket renamed to %s\n", bucket, newBucket)
	case BucketNames:
		bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
		if err != nil {
			return err
		}
		bucketNames, err := api.GetBucketNames(baseParams, bckProvider)
		if err != nil {
			return err
		}
		printBucketNames(bucketNames, parseFlag(c, regexFlag.Name), bckProvider)
	default:
		return fmt.Errorf("invalid command name '%s'", command)
	}
	return err
}

func ListBucket(c *cli.Context) error {
	baseParams := cliAPIParams(ClusterURL)
	if err := checkFlags(c, bucketFlag.Name); err != nil {
		return err
	}
	regex := parseFlag(c, regexFlag.Name)
	r, err := regexp.Compile(regex)
	if err != nil {
		return err
	}

	prefix := parseFlag(c, prefixFlag.Name)
	bucket := parseFlag(c, bucketFlag.Name)
	props := parseFlag(c, objPropsFlag.Name)
	pagesize, err := strconv.Atoi(parseFlag(c, pageSizeFlag.Name))
	if err != nil {
		return err
	}
	limit, err := strconv.Atoi(parseFlag(c, objLimitFlag.Name))
	if err != nil {
		return err
	}

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, parseFlag(c, bckProviderFlag.Name))
	query.Add(cmn.URLParamPrefix, prefix)

	msg := &cmn.GetMsg{GetPageSize: pagesize, GetProps: props}
	objList, err := api.ListBucket(baseParams, bucket, msg, limit, query)
	if err != nil {
		return err
	}

	for _, obj := range objList.Entries {
		if r.MatchString(obj.Name) {
			fmt.Printf("%+v\n", *obj)
		}
	}
	return nil
}

func printBucketNames(bucketNames *cmn.BucketNames, regex, bckProvider string) {
	if bckProvider == cmn.LocalBs || bckProvider == "" {
		localBuckets := regexFilter(regex, bucketNames.Local)
		fmt.Printf("Local Buckets (%d)\n", len(localBuckets))
		for _, bucket := range localBuckets {
			fmt.Println(bucket)
		}
		if bckProvider == cmn.LocalBs {
			return
		}
		fmt.Println()
	}

	cloudBuckets := regexFilter(regex, bucketNames.Cloud)
	fmt.Printf("Cloud Buckets (%d)\n", len(cloudBuckets))
	for _, bucket := range cloudBuckets {
		fmt.Println(bucket)
	}
}

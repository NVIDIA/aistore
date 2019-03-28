// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with buckets in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	BucketCreate  = "create"
	BucketDestroy = "destroy"
	BucketNames   = "names"
)

var (
	BaseBucketFlags = []cli.Flag{
		bucketFlag,
		bckProviderFlag,
	}

	BucketFlags = map[string][]cli.Flag{
		BucketCreate:  []cli.Flag{bucketFlag},
		BucketNames:   []cli.Flag{regexFlag, bckProviderFlag},
		BucketDestroy: []cli.Flag{bucketFlag},
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
		CommandSetProps: append(
			[]cli.Flag{jsonFlag},
			BaseBucketFlags...),
	}

	BucketCreateDelList = "%s bucket %s --bucket <value>"
	BucketCreateText    = fmt.Sprintf(BucketCreateDelList, cliName, BucketCreate)
	BucketDelText       = fmt.Sprintf(BucketCreateDelList, cliName, BucketDestroy)
	BucketListText      = fmt.Sprintf(BucketCreateDelList, cliName, CommandList)
	BucketNamesText     = fmt.Sprintf("%s bucket %s", cliName, BucketNames)
	BucketRenameText    = fmt.Sprintf("%s bucket %s --bucket <value> --newbucket <value>", cliName, CommandRename)
	BucketPropsText     = fmt.Sprintf("%s bucket %s --bucket <value> key=value ...", cliName, CommandSetProps)
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
	case BucketDestroy:
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

	msg := &cmn.SelectMsg{PageSize: pagesize, Props: props}
	objList, err := api.ListBucket(baseParams, bucket, msg, limit, query)
	if err != nil {
		return err
	}
	err = printObjectProps(objList.Entries, r, props)
	return err
}

func SetBucketProps(c *cli.Context) error {
	if err := checkFlags(c, bucketFlag.Name); err != nil {
		return err
	}

	if c.NArg() < 1 {
		return errors.New("expected at least one argument")
	}

	baseParams := cliAPIParams(ClusterURL)
	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	bckName := parseFlag(c, bucketFlag.Name)

	if err := bucketExists(baseParams, bckName, bckProvider); err != nil {
		return err
	}

	// For setting bucket props (multiple)
	if flagIsSet(c, jsonFlag.Name) {
		props := cmn.BucketProps{}
		inputProps := []byte(c.Args().First())
		if err := json.Unmarshal(inputProps, &props); err != nil {
			return err
		}
		if err = api.SetBucketProps(baseParams, bckName, props, query); err != nil {
			return err
		}

		fmt.Printf("Bucket props set for %s bucket\n", bckName)
		return nil
	}

	// For setting bucket prop (single)
	keyVals := c.Args()
	for _, ele := range keyVals {
		pairs := makeList(ele, "=")
		if err = api.SetBucketProp(baseParams, bckName, pairs[0], pairs[1], query); err != nil {
			return err
		}
		fmt.Printf("%s set to %v\n", pairs[0], pairs[1])
	}

	fmt.Printf("%d properties set for %s bucket\n", c.NArg(), bckName)
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

func printObjectProps(entries []*cmn.BucketEntry, r *regexp.Regexp, props string) error {
	propsList := makeList(props, ",")
	bodyStr := "{{$obj := . }}"
	for _, field := range propsList {
		if _, ok := templates.ObjectPropsMap[field]; !ok {
			return fmt.Errorf("'%s' is not a valid property", field)
		}
		bodyStr += templates.ObjectPropsMap[field]
	}
	bodyStr += "\n"

	templates.DisplayOutput(propsList, templates.ObjectPropsHeader)
	for _, obj := range entries {
		if r.MatchString(obj.Name) {
			templates.DisplayOutput(obj, bodyStr)
		}
	}
	return nil
}

func bucketExists(baseParams *api.BaseParams, bckName, bckProvider string) error {
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	if _, err := api.HeadBucket(baseParams, bckName, query); err != nil {
		return fmt.Errorf("%s bucket does not exist", bckName)
	}
	return nil
}

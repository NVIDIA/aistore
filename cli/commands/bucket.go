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
	bucketCreate   = "create"
	bucketDestroy  = "destroy"
	bucketNames    = "names"
	bucketSetProps = "setprops"
)

var (
	baseBucketFlags = []cli.Flag{
		bucketFlag,
		bckProviderFlag,
	}

	bucketFlags = map[string][]cli.Flag{
		bucketCreate:  []cli.Flag{bucketFlag},
		bucketNames:   []cli.Flag{regexFlag, bckProviderFlag},
		bucketDestroy: []cli.Flag{bucketFlag},
		commandRename: append(
			[]cli.Flag{
				newBucketFlag,
			},
			baseBucketFlags...),
		commandList: append(
			[]cli.Flag{
				regexFlag,
				prefixFlag,
				pageSizeFlag,
				objPropsFlag,
				objLimitFlag,
			},
			baseBucketFlags...),
		bucketSetProps: append(
			[]cli.Flag{jsonFlag},
			baseBucketFlags...),
	}

	bucketCreateDelList = "%s bucket %s --bucket <value>"
	bucketCreateText    = fmt.Sprintf(bucketCreateDelList, cliName, bucketCreate)
	bucketDelText       = fmt.Sprintf(bucketCreateDelList, cliName, bucketDestroy)
	bucketListText      = fmt.Sprintf(bucketCreateDelList, cliName, commandList)
	bucketNamesText     = fmt.Sprintf("%s bucket %s", cliName, bucketNames)
	bucketRenameText    = fmt.Sprintf("%s bucket %s --bucket <value> --newbucket <value>", cliName, commandRename)
	bucketPropsText     = fmt.Sprintf("%s bucket %s --bucket <value> key=value ...", cliName, bucketSetProps)

	BucketCmds = []cli.Command{
		{
			Name:  cmn.URLParamBucket,
			Usage: "commands that interact with objects",
			Flags: baseBucketFlags,
			Subcommands: []cli.Command{
				{
					Name:         bucketCreate,
					Usage:        "creates the local bucket",
					UsageText:    bucketCreateText,
					Flags:        bucketFlags[bucketCreate],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketDestroy,
					Usage:        "destroys the local bucket",
					UsageText:    bucketDelText,
					Flags:        bucketFlags[bucketDestroy],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         commandRename,
					Usage:        "renames the local bucket",
					UsageText:    bucketRenameText,
					Flags:        bucketFlags[commandRename],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketNames,
					Usage:        "returns all bucket names",
					UsageText:    bucketNamesText,
					Flags:        bucketFlags[bucketNames],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         commandList,
					Usage:        "returns all objects from bucket",
					UsageText:    bucketListText,
					Flags:        bucketFlags[commandList],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketSetProps,
					Usage:        "sets bucket properties",
					UsageText:    bucketPropsText,
					Flags:        bucketFlags[bucketSetProps],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
			},
		},
	}
)

func bucketHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	command := c.Command.Name
	if err = checkFlags(c, bucketFlag.Name); err != nil && command != bucketNames {
		return err
	}

	bucket := parseFlag(c, bucketFlag.Name)

	switch command {
	case bucketCreate:
		err = createBucket(c, baseParams, bucket)
	case bucketDestroy:
		err = destroyBucket(c, baseParams, bucket)
	case commandRename:
		err = renameBucket(c, baseParams, bucket)
	case bucketNames:
		err = listBucketNames(c, baseParams)
	case commandList:
		err = listBucketObj(c, baseParams, bucket)
	case bucketSetProps:
		err = setBucketProps(c, baseParams, bucket)
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	return err
}

// Creates new local bucket
func createBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = api.CreateLocalBucket(baseParams, bucket); err != nil {
		return err
	}
	fmt.Printf("%s bucket created\n", bucket)
	return
}

// Destroy local bucket
func destroyBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = api.DestroyLocalBucket(baseParams, bucket); err != nil {
		return err
	}
	fmt.Printf("%s bucket destroyed\n", bucket)
	return
}

// Rename local bucket
func renameBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = checkFlags(c, newBucketFlag.Name); err != nil {
		return err
	}
	newBucket := parseFlag(c, newBucketFlag.Name)
	if err = api.RenameLocalBucket(baseParams, bucket, newBucket); err != nil {
		return err
	}
	fmt.Printf("%s bucket renamed to %s\n", bucket, newBucket)
	return
}

// Lists bucket names
func listBucketNames(c *cli.Context, baseParams *api.BaseParams) (err error) {
	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}
	bucketNames, err := api.GetBucketNames(baseParams, bckProvider)
	if err != nil {
		return err
	}
	printBucketNames(bucketNames, parseFlag(c, regexFlag.Name), bckProvider)
	return
}

// Lists objects in bucket
func listBucketObj(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	regex := parseFlag(c, regexFlag.Name)
	r, err := regexp.Compile(regex)
	if err != nil {
		return err
	}

	prefix := parseFlag(c, prefixFlag.Name)
	props := "name," + parseFlag(c, objPropsFlag.Name)
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
	return printObjectProps(objList.Entries, r, props)

}

// Sets bucket properties
func setBucketProps(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if c.NArg() < 1 {
		return errors.New("expected at least one argument")
	}

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
	filteredEntries := []cmn.BucketEntry{}
	propsList := makeList(props, ",")
	headStr := ""
	bodyStr := "{{range $obj := .}}"
	for _, field := range propsList {
		if _, ok := templates.ObjectPropsMap[field]; !ok {
			return fmt.Errorf("'%s' is not a valid property", field)
		}
		headStr += field + "\t"
		bodyStr += templates.ObjectPropsMap[field]
	}
	headStr += "\n"
	bodyStr += "\n{{end}}\n"
	for _, obj := range entries {
		if r.MatchString(obj.Name) {
			filteredEntries = append(filteredEntries, *obj)
		}
	}
	return templates.DisplayOutput(filteredEntries, headStr+bodyStr)
}

func bucketExists(baseParams *api.BaseParams, bckName, bckProvider string) error {
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	if _, err := api.HeadBucket(baseParams, bckName, query); err != nil {
		return fmt.Errorf("%s bucket does not exist", bckName)
	}
	return nil
}

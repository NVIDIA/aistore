// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with objects in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	objGet      = "get"
	objPut      = "put"
	objDel      = "delete"
	objPrefetch = cmn.ActPrefetch
	objEvict    = commandEvict
)

var (
	keyFlag      = cli.StringFlag{Name: "key", Usage: "name of object"}
	outFileFlag  = cli.StringFlag{Name: "outfile", Usage: "name of the file where the contents will be saved"}
	bodyFlag     = cli.StringFlag{Name: "body", Usage: "filename for content of the object"}
	newKeyFlag   = cli.StringFlag{Name: "newkey", Usage: "new name of object"}
	offsetFlag   = cli.StringFlag{Name: cmn.URLParamOffset, Usage: "object read offset"}
	lengthFlag   = cli.StringFlag{Name: cmn.URLParamLength, Usage: "object read length"}
	prefixFlag   = cli.StringFlag{Name: cmn.URLParamPrefix, Usage: "prefix for string matching"}
	listFlag     = cli.StringFlag{Name: "list", Usage: "comma separated list of object names, eg. 'o1,o2,o3'"}
	rangeFlag    = cli.StringFlag{Name: "range", Usage: "colon separated interval of object indices, eg. <START>:<STOP>"}
	deadlineFlag = cli.StringFlag{Name: "deadline", Usage: "amount of time (Go Duration string) before the request expires", Value: "0s"}
	cachedFlag   = cli.BoolFlag{Name: "cached", Usage: "check if an object is cached"}

	baseObjectFlags = []cli.Flag{
		bucketFlag,
		keyFlag,
		bckProviderFlag,
	}

	baseLstRngFlags = []cli.Flag{
		listFlag,
		rangeFlag,
		prefixFlag,
		regexFlag,
		waitFlag,
		deadlineFlag,
	}

	objectFlags = map[string][]cli.Flag{
		objPut: append(
			[]cli.Flag{bodyFlag},
			baseObjectFlags...),
		objGet: append(
			[]cli.Flag{
				outFileFlag,
				offsetFlag,
				lengthFlag,
				checksumFlag,
				propsFlag,
				cachedFlag,
			},
			baseObjectFlags...),
		commandRename: []cli.Flag{
			bucketFlag,
			newKeyFlag,
			keyFlag,
		},
		objDel: append(
			baseLstRngFlags,
			baseObjectFlags...),
		objPrefetch: append(
			[]cli.Flag{bucketFlag},
			baseLstRngFlags...),
		objEvict: append(
			[]cli.Flag{bucketFlag, keyFlag},
			baseLstRngFlags...),
	}

	objectDelGetText    = "%s object %s --bucket <value> --key <value>"
	objectGetUsage      = fmt.Sprintf(objectDelGetText, cliName, objGet)
	objectDelUsage      = fmt.Sprintf(objectDelGetText, cliName, objDel)
	objectPutUsage      = fmt.Sprintf("%s object %s --bucket <value> --key <value> --body <value>", cliName, objPut)
	objectRenameUsage   = fmt.Sprintf("%s object %s --bucket <value> --key <value> --newkey <value> ", cliName, commandRename)
	objectPrefetchUsage = fmt.Sprintf("%s object %s [--list <value>] [--range <value> --prefix <value> --regex <value>]", cliName, objPrefetch)
	objectEvictUsage    = fmt.Sprintf("%s object %s [--list <value>] [--range <value> --prefix <value> --regex <value>]", cliName, objEvict)

	ObjectCmds = []cli.Command{
		{
			Name:  "object",
			Usage: "interact with objects",
			Flags: baseObjectFlags,
			Subcommands: []cli.Command{
				{
					Name:         objGet,
					Usage:        "gets the object from the specified bucket",
					UsageText:    objectGetUsage,
					Flags:        objectFlags[objGet],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objPut,
					Usage:        "puts the object to the specified bucket",
					UsageText:    objectPutUsage,
					Flags:        objectFlags[objPut],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objDel,
					Usage:        "deletes the object from the specified bucket",
					UsageText:    objectDelUsage,
					Flags:        objectFlags[objDel],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         commandRename,
					Usage:        "renames the local object",
					UsageText:    objectRenameUsage,
					Flags:        objectFlags[commandRename],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objPrefetch,
					Usage:        "prefetches the object from the specified bucket",
					UsageText:    objectPrefetchUsage,
					Flags:        objectFlags[objPrefetch],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objEvict,
					Usage:        "evicts the object from the specified bucket",
					UsageText:    objectEvictUsage,
					Flags:        objectFlags[objEvict],
					Action:       objectHandler,
					BashComplete: flagList,
				},
			},
		},
	}
)

func objectHandler(c *cli.Context) (err error) {
	if err = checkFlags(c, bucketFlag); err != nil {
		return
	}

	baseParams := cliAPIParams(ClusterURL)
	bucket := parseFlag(c, bucketFlag)
	bckProvider, err := cmn.BckProviderFromStr(parseFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	commandName := c.Command.Name
	switch commandName {
	case objGet:
		err = retrieveObject(c, baseParams, bucket, bckProvider)
	case objPut:
		err = putObject(c, baseParams, bucket, bckProvider)
	case objDel:
		err = deleteObject(c, baseParams, bucket, bckProvider)
	case commandRename:
		err = renameObject(c, baseParams, bucket)
	case objPrefetch:
		err = prefetchObject(c, baseParams, bucket, bckProvider)
	case objEvict:
		err = evictObject(c, baseParams, bucket, bckProvider)
	default:
		return fmt.Errorf(invalidCmdMsg, commandName)
	}
	return errorHandler(err)
}

// Get object from bucket
func retrieveObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if err = checkFlags(c, keyFlag); err != nil {
		return
	}
	obj := parseFlag(c, keyFlag)
	var objLen int64
	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)
	query.Add(cmn.URLParamOffset, parseFlag(c, offsetFlag))
	query.Add(cmn.URLParamLength, parseFlag(c, lengthFlag))
	objArgs := api.GetObjectInput{Writer: os.Stdout, Query: query}

	// Output to user location
	if flagIsSet(c, outFileFlag) {
		outFile := parseFlag(c, outFileFlag)
		f, err := os.Create(outFile)
		if err != nil {
			return err
		}
		defer f.Close()
		objArgs = api.GetObjectInput{Writer: f, Query: query}
	}

	//Otherwise, saves to local cached of bucket
	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return fmt.Errorf("%s and %s flags both need to be set", lengthFlag.Name, offsetFlag.Name)
	}

	// Object Props and isCached
	if flagIsSet(c, propsFlag) || flagIsSet(c, cachedFlag) {
		objProps, err := api.HeadObject(baseParams, bucket, bckProvider, obj, flagIsSet(c, cachedFlag))
		if flagIsSet(c, cachedFlag) {
			if err != nil {
				if err.(*cmn.HTTPError).Status == http.StatusNotFound {
					fmt.Printf("Cached: %v\n", false)
					return nil
				}
				return errorHandler(err)
			}
			fmt.Printf("Cached: %v\n", true)
			return nil
		} else if err != nil {
			return errorHandler(err)
		}

		fmt.Printf("%s has size %s (%d B) and version %q\n",
			obj, cmn.B2S(int64(objProps.Size), 2), objProps.Size, objProps.Version)
		return nil
	}

	if flagIsSet(c, checksumFlag) {
		objLen, err = api.GetObjectWithValidation(baseParams, bucket, obj, objArgs)
	} else {
		objLen, err = api.GetObject(baseParams, bucket, obj, objArgs)
	}
	if err != nil {
		return
	}

	if flagIsSet(c, lengthFlag) {
		fmt.Printf("\nRead %s (%d B)\n", cmn.B2S(objLen, 2), objLen)
		return
	}
	fmt.Printf("%s has size %s (%d B)\n", obj, cmn.B2S(objLen, 2), objLen)
	return
}

// Put object into bucket
func putObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if err = checkFlags(c, bodyFlag, keyFlag); err != nil {
		return
	}
	source := parseFlag(c, bodyFlag)
	obj := parseFlag(c, keyFlag)
	path, err := filepath.Abs(source)
	if err != nil {
		return
	}
	reader, err := cmn.NewFileHandle(path)
	if err != nil {
		return
	}

	putArgs := api.PutObjectArgs{baseParams, bucket, bckProvider, obj, "", reader}
	if err = api.PutObject(putArgs); err != nil {
		return errorHandler(err)
	}
	fmt.Printf("%s put into %s bucket\n", obj, bucket)
	return
}

// Deletes object from bucket
func deleteObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, listFlag) && flagIsSet(c, rangeFlag) {
		return fmt.Errorf("cannot use both %s and %s", listFlag.Name, rangeFlag.Name)
	}

	// Normal usage
	if flagIsSet(c, keyFlag) {
		obj := parseFlag(c, keyFlag)
		if err = api.DeleteObject(baseParams, bucket, obj, bckProvider); err != nil {
			return
		}
		fmt.Printf("%s deleted from %s bucket\n", obj, bucket)
		return
	} else if flagIsSet(c, listFlag) {
		// List Delete
		return listOp(c, baseParams, objDel, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag) {
		// Range Delete
		return rangeOp(c, baseParams, objDel, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Prefetch operations
func prefetchObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, listFlag) {
		// List prefetch
		return listOp(c, baseParams, objPrefetch, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag) {
		// Range prefetch
		return rangeOp(c, baseParams, objPrefetch, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Evict operations
func evictObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, keyFlag) {
		// Key evict
		key := parseFlag(c, keyFlag)
		if err = api.EvictObject(baseParams, bucket, key); err != nil {
			return err
		}
		fmt.Printf("%s evicted from %s bucket\n", key, bucket)
		return
	} else if flagIsSet(c, listFlag) {
		// List evict
		return listOp(c, baseParams, objEvict, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag) {
		// Range evict
		return rangeOp(c, baseParams, objEvict, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Renames object
func renameObject(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = checkFlags(c, keyFlag, newKeyFlag); err != nil {
		return
	}
	obj := parseFlag(c, keyFlag)
	newName := parseFlag(c, newKeyFlag)
	if err = api.RenameObject(baseParams, bucket, obj, newName); err != nil {
		return
	}

	fmt.Printf("%s renamed to %s\n", obj, newName)
	return
}

// =======================HELPERS=========================
// List handler
func listOp(c *cli.Context, baseParams *api.BaseParams, command, bucket, bckProvider string) (err error) {
	fileList := makeList(parseFlag(c, listFlag), ",")
	wait := flagIsSet(c, waitFlag)
	deadline, err := time.ParseDuration(parseFlag(c, deadlineFlag))
	if err != nil {
		return
	}

	switch command {
	case objDel:
		err = api.DeleteList(baseParams, bucket, bckProvider, fileList, wait, deadline)
		command += "d"
	case objPrefetch:
		err = api.PrefetchList(baseParams, bucket, cmn.CloudBs, fileList, wait, deadline)
		command += "ed"
	case objEvict:
		err = api.EvictList(baseParams, bucket, cmn.CloudBs, fileList, wait, deadline)
		command += "ed"
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	if err != nil {
		return
	}
	fmt.Printf("%s %s from %s bucket\n", fileList, command, bucket)
	return
}

// Range handler
func rangeOp(c *cli.Context, baseParams *api.BaseParams, command, bucket, bckProvider string) (err error) {
	var (
		wait     = flagIsSet(c, waitFlag)
		prefix   = parseFlag(c, prefixFlag)
		regex    = parseFlag(c, regexFlag)
		rangeStr = parseFlag(c, rangeFlag)
	)

	deadline, err := time.ParseDuration(parseFlag(c, deadlineFlag))
	if err != nil {
		return
	}

	switch command {
	case objDel:
		err = api.DeleteRange(baseParams, bucket, bckProvider, prefix, regex, rangeStr, wait, deadline)
		command += "d"
	case objPrefetch:
		err = api.PrefetchRange(baseParams, bucket, cmn.CloudBs, prefix, regex, rangeStr, wait, deadline)
		command += "ed"
	case objEvict:
		err = api.EvictRange(baseParams, bucket, cmn.CloudBs, prefix, regex, rangeStr, wait, deadline)
		command += "ed"
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	if err != nil {
		return
	}
	fmt.Printf("%s files with prefix '%s' matching '%s' in the range '%s' from %s bucket\n",
		command, prefix, regex, rangeStr, bucket)
	return
}

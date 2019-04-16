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
	baseObjectFlags = []cli.Flag{
		bucketFlag,
		keyFlag,
		bckProviderFlag,
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
		objDel: append(
			[]cli.Flag{
				listFlag,
				rangeFlag,
				prefixFlag,
				regexFlag,
				waitFlag,
				deadlineFlag,
			},
			baseObjectFlags...),
		commandRename: []cli.Flag{
			bucketFlag,
			newKeyFlag,
			keyFlag,
		},
		objPrefetch: []cli.Flag{
			listFlag,
			rangeFlag,
			prefixFlag,
			regexFlag,
			waitFlag,
			deadlineFlag,
			bucketFlag,
		},
		objEvict: []cli.Flag{
			listFlag,
			rangeFlag,
			prefixFlag,
			regexFlag,
			waitFlag,
			deadlineFlag,
			bucketFlag,
			keyFlag,
		},
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
			Usage: "commands that interact with objects",
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
	if err = checkFlags(c, bucketFlag.Name); err != nil {
		return
	}

	baseParams := cliAPIParams(ClusterURL)
	bucket := parseFlag(c, bucketFlag.Name)
	bckProvider, err := cmn.BckProviderFromStr(parseFlag(c, bckProviderFlag.Name))
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
	if err = checkFlags(c, keyFlag.Name); err != nil {
		return
	}
	obj := parseFlag(c, keyFlag.Name)
	var objLen int64
	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)
	query.Add(cmn.URLParamOffset, parseFlag(c, offsetFlag.Name))
	query.Add(cmn.URLParamLength, parseFlag(c, lengthFlag.Name))
	objArgs := api.GetObjectInput{Writer: os.Stdout, Query: query}

	// Output to user location
	if flagIsSet(c, outFileFlag.Name) {
		outFile := parseFlag(c, outFileFlag.Name)
		f, err := os.Create(outFile)
		if err != nil {
			return err
		}
		defer f.Close()
		objArgs = api.GetObjectInput{Writer: f, Query: query}
	}

	//Otherwise, saves to local cached of bucket
	if flagIsSet(c, lengthFlag.Name) != flagIsSet(c, offsetFlag.Name) {
		return fmt.Errorf("%s and %s flags both need to be set", lengthFlag.Name, offsetFlag.Name)
	}

	// Object Props and isCached
	if flagIsSet(c, propsFlag.Name) || flagIsSet(c, cachedFlag.Name) {
		objProps, err := api.HeadObject(baseParams, bucket, bckProvider, obj, flagIsSet(c, cachedFlag.Name))
		if flagIsSet(c, cachedFlag.Name) {
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

	if flagIsSet(c, checksumFlag.Name) {
		objLen, err = api.GetObjectWithValidation(baseParams, bucket, obj, objArgs)
	} else {
		objLen, err = api.GetObject(baseParams, bucket, obj, objArgs)
	}
	if err != nil {
		return
	}

	if flagIsSet(c, lengthFlag.Name) {
		fmt.Printf("\nRead %s (%d B)\n", cmn.B2S(objLen, 2), objLen)
		return
	}
	fmt.Printf("%s has size %s (%d B)\n", obj, cmn.B2S(objLen, 2), objLen)
	return
}

// Put object into bucket
func putObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if err = checkFlags(c, bodyFlag.Name, keyFlag.Name); err != nil {
		return
	}
	source := parseFlag(c, bodyFlag.Name)
	obj := parseFlag(c, keyFlag.Name)
	path, err := filepath.Abs(source)
	if err != nil {
		return
	}
	reader, err := cmn.NewFileHandle(path)
	if err != nil {
		return
	}

	putArgs := api.PutObjectArgs{baseParams, bucket, bckProvider, obj, "", reader}
	err = api.PutObject(putArgs)
	if err != nil {
		return
	}
	fmt.Printf("%s put into %s bucket\n", obj, bucket)
	return
}

// Deletes object from bucket
func deleteObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, listFlag.Name) && flagIsSet(c, rangeFlag.Name) {
		return fmt.Errorf("cannot use both %s and %s", listFlag.Name, rangeFlag.Name)
	}

	// Normal usage
	if flagIsSet(c, keyFlag.Name) {
		obj := parseFlag(c, keyFlag.Name)
		if err = api.DeleteObject(baseParams, bucket, obj, bckProvider); err != nil {
			return
		}
		fmt.Printf("%s deleted from %s bucket\n", obj, bucket)
		return
	} else if flagIsSet(c, listFlag.Name) {
		// List Delete
		return listOp(c, baseParams, objDel, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag.Name) {
		// Range Delete
		return rangeOp(c, baseParams, objDel, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Prefetch operations
func prefetchObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, listFlag.Name) {
		// List prefetch
		return listOp(c, baseParams, objPrefetch, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag.Name) {
		// Range prefetch
		return rangeOp(c, baseParams, objPrefetch, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Evict operations
func evictObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, keyFlag.Name) {
		// Key evict
		key := parseFlag(c, keyFlag.Name)
		if err = api.EvictObject(baseParams, bucket, key); err != nil {
			return err
		}
		fmt.Printf("%s evicted from %s bucket\n", key, bucket)
		return
	} else if flagIsSet(c, listFlag.Name) {
		// List evict
		return listOp(c, baseParams, objEvict, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag.Name) {
		// Range evict
		return rangeOp(c, baseParams, objEvict, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Renames object
func renameObject(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = checkFlags(c, keyFlag.Name, newKeyFlag.Name); err != nil {
		return
	}
	obj := parseFlag(c, keyFlag.Name)
	newName := parseFlag(c, newKeyFlag.Name)
	if err = api.RenameObject(baseParams, bucket, obj, newName); err != nil {
		return
	}

	fmt.Printf("%s renamed to %s\n", obj, newName)
	return
}

// =======================HELPERS=========================
// List handler
func listOp(c *cli.Context, baseParams *api.BaseParams, command, bucket, bckProvider string) (err error) {
	fileList := makeList(parseFlag(c, listFlag.Name), ",")
	wait := flagIsSet(c, waitFlag.Name)
	deadline, err := time.ParseDuration(parseFlag(c, deadlineFlag.Name))
	if err != nil {
		return
	}

	switch command {
	case objDel:
		err = api.DeleteList(baseParams, bucket, bckProvider, fileList, wait, deadline)
		command = command + "d"
	case objPrefetch:
		err = api.PrefetchList(baseParams, bucket, cmn.CloudBs, fileList, wait, deadline)
		command = command + "ed"
	case objEvict:
		err = api.EvictList(baseParams, bucket, cmn.CloudBs, fileList, wait, deadline)
		command = command + "ed"
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
	wait := flagIsSet(c, waitFlag.Name)
	deadline, err := time.ParseDuration(parseFlag(c, deadlineFlag.Name))
	if err != nil {
		return
	}

	prefix := parseFlag(c, prefixFlag.Name)
	regex := parseFlag(c, regexFlag.Name)
	rangeStr := parseFlag(c, rangeFlag.Name)

	switch command {
	case objDel:
		err = api.DeleteRange(baseParams, bucket, bckProvider, prefix, regex, rangeStr, wait, deadline)
		command = command + "d"
	case objPrefetch:
		err = api.PrefetchRange(baseParams, bucket, cmn.CloudBs, prefix, regex, rangeStr, wait, deadline)
		command = command + "ed"
	case objEvict:
		err = api.EvictRange(baseParams, bucket, cmn.CloudBs, prefix, regex, rangeStr, wait, deadline)
		command = command + "ed"
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

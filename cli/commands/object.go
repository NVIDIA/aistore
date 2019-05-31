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
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cli/templates"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	outFileStdout = "-"
)

var (
	baseObjectFlags = []cli.Flag{
		bucketFlag,
		nameFlag,
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
			baseObjectFlags,
			fileFlag,
		),
		objGet: append(
			baseObjectFlags,
			outFileFlag,
			offsetFlag,
			lengthFlag,
			checksumFlag,
			propsFlag,
			cachedFlag,
		),
		subcommandRename: {
			nameFlag,
			newNameFlag,
			bucketFlag,
		},
		objDel: append(
			baseObjectFlags,
			baseLstRngFlags...,
		),
		objStat: append(
			baseObjectFlags,
			objPropsFlag,
			noHeaderFlag,
			jsonFlag,
		),
		objPrefetch: append(
			baseLstRngFlags,
			bucketFlag,
		),
		objEvict: append(
			baseLstRngFlags,
			bucketFlag,
			nameFlag,
		),
	}

	objectBasicUsageText = "%s object %s --bucket <value> --name <value>"
	objectGetUsage       = fmt.Sprintf(objectBasicUsageText, cliName, objGet)
	objectDelUsage       = fmt.Sprintf(objectBasicUsageText, cliName, objDel)
	objectStatUsage      = fmt.Sprintf(objectBasicUsageText, cliName, objStat)
	objectPutUsage       = fmt.Sprintf("%s object %s --bucket <value> --name <value> --file <value>", cliName, objPut)
	objectRenameUsage    = fmt.Sprintf("%s object %s --bucket <value> --name <value> --new-name <value> ", cliName, subcommandRename)
	objectPrefetchUsage  = fmt.Sprintf("%s object %s [--list <value>] [--range <value> --prefix <value> --regex <value>]", cliName, objPrefetch)
	objectEvictUsage     = fmt.Sprintf("%s object %s [--list <value>] [--range <value> --prefix <value> --regex <value>]", cliName, objEvict)

	objectCmds = []cli.Command{
		{
			Name:  commandObject,
			Usage: "interact with objects",
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
					Name:         objStat,
					Usage:        "displays basic information about the object",
					UsageText:    objectStatUsage,
					Flags:        objectFlags[objStat],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         subcommandRename,
					Usage:        "renames the local object",
					UsageText:    objectRenameUsage,
					Flags:        objectFlags[subcommandRename],
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
	if err = checkFlags(c, []cli.Flag{bucketFlag}); err != nil {
		return
	}

	baseParams := cliAPIParams(ClusterURL)
	bucket := parseStrFlag(c, bucketFlag)
	bckProvider, err := cmn.BckProviderFromStr(parseStrFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	commandName := c.Command.Name
	switch commandName {
	case objGet:
		err = objectRetrieve(c, baseParams, bucket, bckProvider)
	case objPut:
		err = objectPut(c, baseParams, bucket, bckProvider)
	case objDel:
		err = objectDelete(c, baseParams, bucket, bckProvider)
	case objStat:
		err = objectStat(c, baseParams, bucket, bckProvider)
	case subcommandRename:
		err = objectRename(c, baseParams, bucket)
	case objPrefetch:
		err = objectPrefetch(c, baseParams, bucket, bckProvider)
	case objEvict:
		err = objectEvict(c, baseParams, bucket, bckProvider)
	default:
		return fmt.Errorf(invalidCmdMsg, commandName)
	}
	return err
}

// Get object from bucket
func objectRetrieve(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	if flagIsSet(c, cachedFlag) {
		return objectCheckCached(c, baseParams, bucket, bckProvider)
	}

	if err := checkFlags(c, []cli.Flag{nameFlag, outFileFlag}); err != nil {
		return err
	}
	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return fmt.Errorf("%s and %s flags both need to be set", lengthFlag.Name, offsetFlag.Name)
	}

	offset, err := getByteFlagValue(c, offsetFlag)
	if err != nil {
		return err
	}
	length, err := getByteFlagValue(c, lengthFlag)
	if err != nil {
		return err
	}
	obj := parseStrFlag(c, nameFlag)
	outFile := parseStrFlag(c, outFileFlag)

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)
	query.Add(cmn.URLParamOffset, offset)
	query.Add(cmn.URLParamLength, length)

	var objArgs api.GetObjectInput

	if outFile == outFileStdout {
		objArgs = api.GetObjectInput{Writer: os.Stdout, Query: query}
	} else {
		// Output to user location
		f, err := os.Create(outFile)
		if err != nil {
			return err
		}
		defer f.Close()
		objArgs = api.GetObjectInput{Writer: f, Query: query}
	}

	var objLen int64
	if flagIsSet(c, checksumFlag) {
		objLen, err = api.GetObjectWithValidation(baseParams, bucket, obj, objArgs)
	} else {
		objLen, err = api.GetObject(baseParams, bucket, obj, objArgs)
	}
	if err != nil {
		return err
	}

	if flagIsSet(c, lengthFlag) {
		_, _ = fmt.Fprintf(c.App.ErrWriter, "Read %s (%d B)\n", cmn.B2S(objLen, 2), objLen)
		return err
	}
	_, _ = fmt.Fprintf(c.App.ErrWriter, "%s has size %s (%d B)\n", obj, cmn.B2S(objLen, 2), objLen)
	return err
}

func objectCheckCached(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	object := parseStrFlag(c, nameFlag)

	_, err := api.HeadObject(baseParams, bucket, bckProvider, object, true)
	if err != nil {
		if err.(*cmn.HTTPError).Status == http.StatusNotFound {
			_, _ = fmt.Fprintf(c.App.Writer, "Cached: %v\n", false)
			return nil
		}
		return err
	}

	_, _ = fmt.Fprintf(c.App.Writer, "Cached: %v\n", true)
	return nil
}

// Put object into bucket
func objectPut(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	if err := checkFlags(c, []cli.Flag{fileFlag}); err != nil {
		return err
	}

	source := parseStrFlag(c, fileFlag)

	objName := parseStrFlag(c, nameFlag)
	if objName == "" {
		// If name was not provided use the last element of the object's path as object's name
		objName = filepath.Base(source)
	}

	path, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	reader, err := cmn.NewFileHandle(path)
	if err != nil {
		return err
	}

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, BucketProvider: bckProvider, Object: objName, Reader: reader}
	if err := api.PutObject(putArgs); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(c.App.Writer, "%s put into %s bucket\n", objName, bucket)
	return nil
}

// Deletes object from bucket
func objectDelete(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, listFlag) && flagIsSet(c, rangeFlag) {
		return fmt.Errorf("cannot use both %s and %s", listFlag.Name, rangeFlag.Name)
	}

	// Normal usage
	if flagIsSet(c, nameFlag) {
		obj := parseStrFlag(c, nameFlag)
		if err = api.DeleteObject(baseParams, bucket, obj, bckProvider); err != nil {
			return
		}
		_, _ = fmt.Fprintf(c.App.Writer, "%s deleted from %s bucket\n", obj, bucket)
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

func buildObjStatTemplate(props string, showHeaders bool) string {
	var (
		headSb strings.Builder
		bodySb strings.Builder

		propsList = makeList(props, ",")
	)
	for _, field := range propsList {
		if _, ok := templates.ObjStatMap[field]; !ok {
			// just skip invalid props for now - here a prop string
			// from `bucket objects` can be used and how HEAD and LIST
			// request have different set of properties
			continue
		}
		headSb.WriteString(strings.Title(field) + "\t")
		bodySb.WriteString(templates.ObjStatMap[field])
	}
	headSb.WriteString("\n")
	bodySb.WriteString("\n")

	if showHeaders {
		return headSb.String() + bodySb.String() + "\n"
	}

	return bodySb.String()
}

// Displays object properties
func objectStat(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	if err := checkFlags(c, []cli.Flag{nameFlag}); err != nil {
		return err
	}

	props, propsFlag := "local,", ""
	if flagIsSet(c, objPropsFlag) {
		propsFlag = parseStrFlag(c, objPropsFlag)
	}
	if propsFlag == "all" || propsFlag == "" {
		props += strings.Join(cmn.GetPropsAll, ",")
	} else {
		props += parseStrFlag(c, objPropsFlag)
	}

	tmpl := buildObjStatTemplate(props, !flagIsSet(c, noHeaderFlag))
	name := parseStrFlag(c, nameFlag)
	objProps, err := api.HeadObject(baseParams, bucket, bckProvider, name)
	if err != nil {
		return handleObjHeadError(err, bucket, name)
	}

	return templates.DisplayOutput(objProps, c.App.Writer, tmpl, flagIsSet(c, jsonFlag))
}

// Prefetch operations
func objectPrefetch(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
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
func objectEvict(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, nameFlag) {
		// Name evict
		name := parseStrFlag(c, nameFlag)
		if err := api.EvictObject(baseParams, bucket, name); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c.App.Writer, "%s evicted from %s bucket\n", name, bucket)
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
func objectRename(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = checkFlags(c, []cli.Flag{nameFlag, newNameFlag}); err != nil {
		return
	}
	obj := parseStrFlag(c, nameFlag)
	newName := parseStrFlag(c, newNameFlag)
	if err = api.RenameObject(baseParams, bucket, obj, newName); err != nil {
		return
	}

	_, _ = fmt.Fprintf(c.App.Writer, "%s renamed to %s\n", obj, newName)
	return
}

// =======================HELPERS=========================
// List handler
func listOp(c *cli.Context, baseParams *api.BaseParams, command, bucket, bckProvider string) (err error) {
	fileList := makeList(parseStrFlag(c, listFlag), ",")
	wait := flagIsSet(c, waitFlag)
	deadline, err := time.ParseDuration(parseStrFlag(c, deadlineFlag))
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
	_, _ = fmt.Fprintf(c.App.Writer, "%s %s from %s bucket\n", fileList, command, bucket)
	return
}

// Range handler
func rangeOp(c *cli.Context, baseParams *api.BaseParams, command, bucket, bckProvider string) (err error) {
	var (
		wait     = flagIsSet(c, waitFlag)
		prefix   = parseStrFlag(c, prefixFlag)
		regex    = parseStrFlag(c, regexFlag)
		rangeStr = parseStrFlag(c, rangeFlag)
	)

	deadline, err := time.ParseDuration(parseStrFlag(c, deadlineFlag))
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
	_, _ = fmt.Fprintf(c.App.Writer, "%s files with prefix '%s' matching '%s' in the range '%s' from %s bucket\n",
		command, prefix, regex, rangeStr, bucket)
	return
}

// This function is needed to print a nice error message for the user
func handleObjHeadError(err error, bucket, object string) error {
	httpErr, ok := err.(*cmn.HTTPError)
	if !ok {
		return err
	}
	if httpErr.Status == http.StatusNotFound {
		return fmt.Errorf("no such object %q in bucket %q", object, bucket)
	}

	return err
}

// Returns a string containing the value of the `flag` in bytes, used for `offset` and `length` flags
func getByteFlagValue(c *cli.Context, flag cli.Flag) (string, error) {
	if flagIsSet(c, flag) {
		offsetInt, err := parseByteFlagToInt(c, flag)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(offsetInt, 10), nil
	}

	return "", nil
}

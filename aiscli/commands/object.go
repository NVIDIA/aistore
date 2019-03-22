// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with objects in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	ObjGet    = "get"
	ObjPut    = "put"
	ObjDel    = "delete"
	ObjRename = "rename"
)

var (
	BaseObjectFlags = []cli.Flag{bucketFlag, keyFlag, bckProviderFlag}

	ObjectFlags = map[string][]cli.Flag{
		ObjGet: append(
			[]cli.Flag{
				offsetFlag,
				lengthFlag,
				checksumFlag,
			},
			BaseObjectFlags...,
		),
		ObjDel: append(
			[]cli.Flag{
				listFlag,
				rangeFlag,
				prefixFlag,
				regexFlag,
				waitFlag,
				deadlineFlag,
			},
			BaseObjectFlags...,
		),
		ObjPut: BaseObjectFlags,
		ObjRename: append(
			[]cli.Flag{
				oldKeyFlag,
			},
			BaseObjectFlags...,
		),
	}
	objectGetPutUsage = "aiscli object [FLAGS...] %s [OPTIONAL FLAGS...] --bucket <value> --key <value>"
	ObjectGetUsage    = fmt.Sprintf(objectGetPutUsage, ObjGet)
	ObjectPutUsage    = fmt.Sprintf(objectGetPutUsage, ObjPut)
	ObjectDelUsage    = fmt.Sprintf("aiscli object [FLAGS...] %s --bucket <value> [DELETE FLAGS]", ObjDel)
	ObjectRenameUsage = fmt.Sprintf("aiscli object %s --bucket <value> --oldkey <value> --key <value>", ObjRename)
)

func ObjectHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
	)

	if c.NumFlags() < 2 {
		return errors.New(c.Command.UsageText)
	}
	if err := checkFlags(c, bucketFlag.Name, keyFlag.Name); err != nil {
		return err
	}

	obj := c.String(keyFlag.Name)
	bucket = c.String(bucketFlag.Name)
	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}

	commandName := c.Command.Name
	switch commandName {
	case ObjGet:
		var objLen int64
		query := url.Values{}
		query.Add(cmn.URLParamBckProvider, bckProvider)
		query.Add(cmn.URLParamOffset, c.String(offsetFlag.Name))
		query.Add(cmn.URLParamLength, c.String(lengthFlag.Name))
		objArgs := api.GetObjectInput{Writer: os.Stdout, Query: query}

		// XOR
		if flagIsSet(c, lengthFlag.Name) != flagIsSet(c, offsetFlag.Name) {
			return fmt.Errorf("%s and %s flags both need to be set", lengthFlag.Name, offsetFlag.Name)
		}

		// Checksum validation
		if c.Bool(checksumFlag.Name) {
			objLen, err = api.GetObjectWithValidation(baseParams, bucket, obj, objArgs)
		} else {
			objLen, err = api.GetObject(baseParams, bucket, obj, objArgs)
		}
		if err != nil {
			return err
		}

		if flagIsSet(c, lengthFlag.Name) {
			fmt.Printf("\nRead %d byte(s)\n", objLen)
			return nil
		}
		fmt.Printf("%s has size %d\n", obj, objLen)
	case ObjPut:
		path, err := filepath.Abs(obj)
		if err != nil {
			return err
		}
		reader, err := cmn.NewFileHandle(path)
		if err != nil {
			return err
		}
		putArgs := api.PutObjectArgs{baseParams, bucket, bckProvider, obj, "", reader}
		err = api.PutObject(putArgs)
		if err != nil {
			return err
		}
		fmt.Printf("%s put into %s bucket\n", obj, bucket)
	case ObjRename:
		if c.NumFlags() < 3 {
			return errors.New(c.Command.UsageText)
		}
		oldName := c.String(oldKeyFlag.Name)
		err := api.RenameObject(baseParams, bucket, oldName, obj)
		if err != nil {
			return err
		}
		fmt.Printf("%s renamed to %s\n", oldName, obj)
	default:
		return fmt.Errorf("invalid command name '%s'", commandName)
	}
	return nil
}

func DeleteObject(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
		wait       = c.Bool(waitFlag.Name)
	)

	if err := checkFlags(c, bucketFlag.Name); err != nil {
		return err
	}

	bucket = c.String(bucketFlag.Name)
	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}
	deadline, err := time.ParseDuration(c.String(deadlineFlag.Name))
	if err != nil {
		return err
	}

	if flagIsSet(c, listFlag.Name) && flagIsSet(c, rangeFlag.Name) {
		return fmt.Errorf("cannot use both %s and %s", listFlag.Name, rangeFlag.Name)
	}

	// Normal usage
	if flagIsSet(c, keyFlag.Name) {
		obj := c.String(keyFlag.Name)
		if err := api.DeleteObject(baseParams, bucket, obj, bckProvider); err != nil {
			return err
		}
		fmt.Printf("%s deleted from %s bucket\n", obj, bucket)
		return nil
	}

	// List Delete
	if flagIsSet(c, listFlag.Name) {
		fileList := makeList(c.String(listFlag.Name))
		if err := api.DeleteList(baseParams, bucket, bckProvider, fileList, wait, deadline); err != nil {
			return err
		}
		fmt.Printf("%s deleted from %s bucket\n", fileList, bucket)
		return nil
	}

	// Range Delete
	if flagIsSet(c, rangeFlag.Name) {
		prefix := c.String(prefixFlag.Name)
		regex := c.String(regexFlag.Name)
		rangeStr := c.String(rangeFlag.Name)
		if err := api.DeleteRange(baseParams, bucket, bckProvider, prefix, regex, rangeStr, wait, deadline); err != nil {
			return err
		}
		fmt.Printf("Deleted files with prefix '%s' matching '%s' in the range '%s' from %s bucket\n",
			prefix, regex, rangeStr, bucket)
		return nil
	}

	return errors.New(c.Command.UsageText)
}

// Users can pass in a comma separated list
func makeList(list string) []string {
	return strings.Split(list, ",")
}

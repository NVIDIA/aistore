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
	ObjectFlag = map[string][]cli.Flag{
		ObjGet: []cli.Flag{
			BucketFlag,
			KeyFlag,
			BckProviderFlag,
			OffsetFlag,
			LengthFlag,
			ChecksumFlag,
		},
		ObjDel: []cli.Flag{
			BucketFlag,
			KeyFlag,
			BckProviderFlag,
			ListFlag,
			RangeFlag,
			PrefixFlag,
			RegexFlag,
			WaitFlag,
			DeadlineFlag,
		},
		ObjPut: []cli.Flag{
			BucketFlag,
			KeyFlag,
			BckProviderFlag,
		},
		ObjRename: []cli.Flag{
			BucketFlag,
			OldKeyFlag,
			KeyFlag,
		},
	}
	ObjectGetPutUsage = "aiscli object [FLAGS...] %s [OPTIONAL FLAGS...] --bucket <value> --key <value> "
	ObjectDelUsage    = "aiscli object [FLAGS...] %s --bucket <value> [DELETE FLAGS]"
	ObjectRenameUsage = "aiscli object %s --bucket <value> --oldkey <value> --key <value>"
)

func ObjectHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
	)

	if c.NumFlags() < 2 {
		return errors.New(c.Command.UsageText)
	}
	if !c.IsSet(BucketFlag.Name) {
		return fmt.Errorf("%s flag is not set", BucketFlag.Name)
	}
	if !c.IsSet(KeyFlag.Name) {
		return fmt.Errorf("%s flag is not set", KeyFlag.Name)
	}

	obj := c.String(KeyFlag.Name)
	bucket = c.String(BucketFlag.Name)
	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, BckProviderFlag.Name))
	if err != nil {
		return err
	}

	commandName := c.Command.Name
	switch commandName {
	case ObjGet:
		var objLen int64
		query := url.Values{}
		query.Add(cmn.URLParamBckProvider, bckProvider)
		query.Add(cmn.URLParamOffset, c.String(OffsetFlag.Name))
		query.Add(cmn.URLParamLength, c.String(LengthFlag.Name))
		objArgs := api.GetObjectInput{Writer: os.Stdout, Query: query}

		// XOR
		if c.IsSet(LengthFlag.Name) != c.IsSet(OffsetFlag.Name) {
			return fmt.Errorf("%s and %s flags both need to be set", LengthFlag.Name, OffsetFlag.Name)
		}

		// Checksum validation
		if c.Bool(ChecksumFlag.Name) {
			objLen, err = api.GetObjectWithValidation(baseParams, bucket, obj, objArgs)
		} else {
			objLen, err = api.GetObject(baseParams, bucket, obj, objArgs)
		}
		if err != nil {
			return err
		}

		if c.IsSet(LengthFlag.Name) {
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
		oldName := c.String(OldKeyFlag.Name)
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
		wait       = c.Bool(WaitFlag.Name)
	)

	if !c.IsSet(BucketFlag.Name) {
		return fmt.Errorf("%s flag is not set", BucketFlag.Name)
	}

	bucket = c.String(BucketFlag.Name)
	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, BckProviderFlag.Name))
	if err != nil {
		return err
	}
	deadline, err := time.ParseDuration(c.String(DeadlineFlag.Name))
	if err != nil {
		return err
	}

	if c.IsSet(ListFlag.Name) && c.IsSet(RangeFlag.Name) {
		return fmt.Errorf("cannot use both %s and %s", ListFlag.Name, RangeFlag.Name)
	}

	// Normal usage
	if c.IsSet(KeyFlag.Name) {
		obj := c.String(KeyFlag.Name)
		if err := api.DeleteObject(baseParams, bucket, obj, bckProvider); err != nil {
			return err
		}
		fmt.Printf("%s deleted from %s bucket\n", obj, bucket)
		return nil
	}

	// List Delete
	if c.IsSet(ListFlag.Name) {
		fileList := makeList(c.String(ListFlag.Name))
		if err := api.DeleteList(baseParams, bucket, bckProvider, fileList, wait, deadline); err != nil {
			return err
		}
		fmt.Printf("%s deleted from %s bucket\n", fileList, bucket)
		return nil
	}

	// Range Delete
	if c.IsSet(RangeFlag.Name) {
		prefix := c.String(PrefixFlag.Name)
		regex := c.String(RegexFlag.Name)
		rangeStr := c.String(RangeFlag.Name)
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

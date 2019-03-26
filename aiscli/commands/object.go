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
	ObjGet = "get"
	ObjPut = "put"
)

var (
	BaseObjectFlags = []cli.Flag{
		bucketFlag,
		keyFlag,
		bckProviderFlag,
	}

	ObjectFlags = map[string][]cli.Flag{
		ObjPut: append(
			[]cli.Flag{bodyFlag},
			BaseObjectFlags...),
		ObjGet: append(
			[]cli.Flag{
				outFileFlag,
				offsetFlag,
				lengthFlag,
				checksumFlag,
				propsFlag,
			},
			BaseObjectFlags...),
		CommandDel: append(
			[]cli.Flag{
				listFlag,
				rangeFlag,
				prefixFlag,
				regexFlag,
				waitFlag,
				deadlineFlag,
			},
			BaseObjectFlags...),
		CommandRename: []cli.Flag{
			bucketFlag,
			newKeyFlag,
			keyFlag,
		},
	}

	ObjectDelGetText  = "aiscli object %s --bucket <value> --key <value>"
	ObjectGetUsage    = fmt.Sprintf(ObjectDelGetText, ObjGet)
	ObjectDelUsage    = fmt.Sprintf(ObjectDelGetText, CommandDel)
	ObjectPutUsage    = fmt.Sprintf("aiscli object %s --bucket <value> --key <value> --body <value>", ObjPut)
	ObjectRenameUsage = fmt.Sprintf("aiscli object %s --bucket <value> --key <value> --newkey <value> ", CommandRename)
)

func ObjectHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
	)

	if err := checkFlags(c, bucketFlag.Name, keyFlag.Name); err != nil {
		return err
	}

	obj := parseFlag(c, keyFlag.Name)
	bucket = parseFlag(c, bucketFlag.Name)
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

		// XOR
		if flagIsSet(c, lengthFlag.Name) != flagIsSet(c, offsetFlag.Name) {
			return fmt.Errorf("%s and %s flags both need to be set", lengthFlag.Name, offsetFlag.Name)
		}

		// Object Props
		if c.Bool(propsFlag.Name) {
			objProps, err := api.HeadObject(baseParams, bucket, bckProvider, obj)
			if err != nil {
				return err
			}
			fmt.Printf("%s has size %s (%d B) and version '%s'\n", obj, cmn.B2S(int64(objProps.Size), 2), objProps.Size, objProps.Version)
			return nil
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
			fmt.Printf("\nRead %s (%d B)\n", cmn.B2S(objLen, 2), objLen)
			return nil
		}
		fmt.Printf("%s has size %s (%d B)\n", obj, cmn.B2S(objLen, 2), objLen)
	case ObjPut:
		if err := checkFlags(c, bodyFlag.Name); err != nil {
			return err
		}
		source := parseFlag(c, bodyFlag.Name)
		path, err := filepath.Abs(source)
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
	case CommandRename:
		if err := checkFlags(c, newKeyFlag.Name, bucketFlag.Name, keyFlag.Name); err != nil {
			return err
		}
		newName := parseFlag(c, newKeyFlag.Name)
		err := api.RenameObject(baseParams, bucket, obj, newName)
		if err != nil {
			return err
		}
		fmt.Printf("%s renamed to %s\n", obj, newName)
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

	bucket = parseFlag(c, bucketFlag.Name)
	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}
	deadline, err := time.ParseDuration(parseFlag(c, deadlineFlag.Name))
	if err != nil {
		return err
	}

	if flagIsSet(c, listFlag.Name) && flagIsSet(c, rangeFlag.Name) {
		return fmt.Errorf("cannot use both %s and %s", listFlag.Name, rangeFlag.Name)
	}

	// Normal usage
	if flagIsSet(c, keyFlag.Name) {
		obj := parseFlag(c, keyFlag.Name)
		if err := api.DeleteObject(baseParams, bucket, obj, bckProvider); err != nil {
			return err
		}
		fmt.Printf("%s deleted from %s bucket\n", obj, bucket)
		return nil
	}

	// List Delete
	if flagIsSet(c, listFlag.Name) {
		fileList := makeList(parseFlag(c, listFlag.Name))
		if err := api.DeleteList(baseParams, bucket, bckProvider, fileList, wait, deadline); err != nil {
			return err
		}
		fmt.Printf("%s deleted from %s bucket\n", fileList, bucket)
		return nil
	}

	// Range Delete
	if flagIsSet(c, rangeFlag.Name) {
		prefix := parseFlag(c, prefixFlag.Name)
		regex := parseFlag(c, regexFlag.Name)
		rangeStr := parseFlag(c, rangeFlag.Name)
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

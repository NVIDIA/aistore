// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

func mptCreateHandler(c *cli.Context) error {
	if c.NArg() != 1 {
		return missingArgumentsError(c, "bucket/object_name")
	}

	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjURI(c, uri, false)
	if err != nil {
		return err
	}
	if objName == "" {
		return missingArgumentsError(c, "object name")
	}

	if shouldHeadRemote(c, bck) {
		if _, err = headBucket(bck, false /* don't add */); err != nil {
			return err
		}
	}

	// encode special symbols if requested
	var (
		warned     bool
		encObjName = warnEscapeObjName(c, objName, &warned)
	)
	uploadID, err := api.CreateMultipartUpload(apiBP, bck, encObjName)
	if err != nil {
		return err
	}

	if flagIsSet(c, verboseFlag) {
		fmt.Fprintf(c.App.Writer, "Created multipart upload for %s\n", bck.Cname(objName))
	}
	fmt.Fprintf(c.App.Writer, "Upload ID: %s\n", uploadID)
	return nil
}

func mptPutHandler(c *cli.Context) error {
	if c.NArg() < 2 {
		return missingArgumentsError(c, "arguments: "+mptPutArgument)
	}

	var (
		uri        = c.Args().Get(0)
		uploadID   = c.Args().Get(1)
		partNumber int
		filePath   string
		err        error
	)

	// Parse arguments: can be provided as separate args or via flags
	if c.NArg() >= 4 {
		// All arguments provided: bucket/object upload_id part_number file_path
		if partNumber, err = strconv.Atoi(c.Args().Get(2)); err != nil {
			return fmt.Errorf("invalid part number %q: %v", c.Args().Get(2), err)
		}
		filePath = c.Args().Get(3)
	} else {
		// Use flags for part number and file path
		if !flagIsSet(c, mptPartNumberFlag) {
			return missingArgumentsError(c, "part number (use --part-number flag or provide as argument)")
		}
		partNumber = parseIntFlag(c, mptPartNumberFlag)
		if c.NArg() < 3 {
			return missingArgumentsError(c, "file path")
		}
		filePath = c.Args().Get(2)
	}

	// Override with flags if provided
	if flagIsSet(c, mptUploadIDFlag) {
		uploadID = parseStrFlag(c, mptUploadIDFlag)
	}
	if flagIsSet(c, mptPartNumberFlag) {
		partNumber = parseIntFlag(c, mptPartNumberFlag)
	}

	if uploadID == "" {
		return missingArgumentsError(c, "upload ID")
	}
	if partNumber <= 0 {
		return incorrectUsageMsg(c, "part number must be positive")
	}

	// Parse bucket and object
	bck, objName, err := parseBckObjURI(c, uri, false)
	if err != nil {
		return err
	}
	if objName == "" {
		return missingArgumentsError(c, "object name")
	}
	// encode special symbols if requested
	var (
		warned     bool
		encObjName = warnEscapeObjName(c, objName, &warned)
	)
	// Get file info
	finfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to access file %q: %v", filePath, err)
	}
	if finfo.IsDir() {
		return fmt.Errorf("%q is a directory", filePath)
	}

	// Open file
	fh, err := cos.NewFileHandle(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %v", filePath, err)
	}
	defer fh.Close()

	// Upload part
	putPartArgs := &api.PutPartArgs{
		PutArgs: api.PutArgs{
			BaseParams: apiBP,
			Bck:        bck,
			ObjName:    encObjName,
			Reader:     fh,
			Size:       uint64(finfo.Size()),
		},
		UploadID:   uploadID,
		PartNumber: partNumber,
	}

	if flagIsSet(c, verboseFlag) {
		fmt.Fprintf(c.App.Writer, "Uploading part %d from %s (%s)...\n",
			partNumber, filePath, teb.FmtSize(finfo.Size(), "", 2))
	}

	err = api.UploadPart(putPartArgs)
	if err != nil {
		return fmt.Errorf("failed to upload part %d: %v", partNumber, err)
	}

	actionDone(c, fmt.Sprintf("Uploaded part %d for %s (upload ID: %s)",
		partNumber, bck.Cname(objName), uploadID))
	return nil
}

func mptCompleteHandler(c *cli.Context) error {
	if c.NArg() < 2 {
		return missingArgumentsError(c, "arguments: "+mptCompleteArgument)
	}

	var (
		uri         = c.Args().Get(0)
		uploadID    = c.Args().Get(1)
		partNumbers []int
		partNumsStr string
		err         error
	)

	// Parse part numbers from arguments or flags
	if c.NArg() >= 3 {
		partNumsStr = c.Args().Get(2)
	}
	if flagIsSet(c, mptUploadIDFlag) {
		uploadID = parseStrFlag(c, mptUploadIDFlag)
	}
	if flagIsSet(c, mptPartNumbersFlag) {
		partNumsStr = parseStrFlag(c, mptPartNumbersFlag)
	}

	if uploadID == "" {
		return missingArgumentsError(c, "upload ID")
	}
	if partNumsStr == "" {
		return missingArgumentsError(c, "part numbers (e.g., '1,2,3' or use --part-numbers flag)")
	}

	// Parse part numbers
	parts := strings.Split(partNumsStr, ",")
	partNumbers = make([]int, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if partNumbers[i], err = strconv.Atoi(part); err != nil {
			return fmt.Errorf("invalid part number %q: %v", part, err)
		}
		if partNumbers[i] <= 0 {
			return fmt.Errorf("part number must be positive: %d", partNumbers[i])
		}
	}

	// Parse bucket and object
	bck, objName, err := parseBckObjURI(c, uri, false)
	if err != nil {
		return err
	}
	if objName == "" {
		return missingArgumentsError(c, "object name")
	}

	if flagIsSet(c, verboseFlag) {
		fmt.Fprintf(c.App.Writer, "Completing multipart upload for %s with %d parts...\n",
			bck.Cname(objName), len(partNumbers))
	}

	err = api.CompleteMultipartUpload(apiBP, bck, objName, uploadID, partNumbers)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %v", err)
	}

	actionDone(c, "Successfully completed multipart upload for "+bck.Cname(objName))
	return nil
}

func mptAbortHandler(c *cli.Context) error {
	if c.NArg() < 2 {
		return missingArgumentsError(c, "arguments: "+mptAbortArgument)
	}

	var (
		uri      = c.Args().Get(0)
		uploadID = c.Args().Get(1)
	)

	// Override with flag if provided
	if flagIsSet(c, mptUploadIDFlag) {
		uploadID = parseStrFlag(c, mptUploadIDFlag)
	}

	if uploadID == "" {
		return missingArgumentsError(c, "upload ID")
	}

	// Parse bucket and object
	bck, objName, err := parseBckObjURI(c, uri, false)
	if err != nil {
		return err
	}
	if objName == "" {
		return missingArgumentsError(c, "object name")
	}

	if flagIsSet(c, verboseFlag) {
		fmt.Fprintf(c.App.Writer, "Aborting multipart upload for %s (upload ID: %s)...\n",
			bck.Cname(objName), uploadID)
	}

	err = api.AbortMultipartUpload(apiBP, bck, objName, uploadID)
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload: %v", err)
	}

	actionDone(c, "Successfully aborted multipart upload for %s"+bck.Cname(objName))
	return nil
}

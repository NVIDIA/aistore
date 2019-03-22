// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with objects in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	DownloadSingle = "single"
	DownloadRange  = "range"
	DownloadCloud  = "cloud"
	DownloadStatus = "status"
	DownloadCancel = "cancel"
)

var (
	BaseDownloadFlags = []cli.Flag{
		bucketFlag,
		bckProviderFlag,
		timeoutFlag,
	}

	DownloadFlags = map[string][]cli.Flag{
		DownloadSingle: append(
			[]cli.Flag{
				objNameFlag,
				linkFlag,
			},
			BaseDownloadFlags...,
		),
		DownloadRange: append(
			[]cli.Flag{
				baseFlag,
				templateFlag,
			},
			BaseDownloadFlags...,
		),
		DownloadCloud: append(
			[]cli.Flag{
				dlPrefixFlag,
				dlSuffixFlag,
			},
			BaseDownloadFlags...,
		),
		DownloadStatus: []cli.Flag{
			idFlag,
			progressBarFlag,
		},
		DownloadCancel: []cli.Flag{
			idFlag,
		},
	}

	DownloadSingleUsage = fmt.Sprintf("aiscli download --bucket <value> [FLAGS...] %s --objname <value> --link <value>", DownloadSingle)
	DownloadRangeUsage  = fmt.Sprintf("aiscli download --bucket <value> [FLAGS...] %s --base <value> --template <value>", DownloadRange)
	DownloadCloudUsage  = fmt.Sprintf("aiscli download --bucket <value> [FLAGS...] %s --prefix <value> --suffix <value>", DownloadCloud)
	DownloadStatusUsage = fmt.Sprintf("aiscli download %s --id <value> [STATUS FLAGS...]", DownloadStatus)
	DownloadCancelUsage = fmt.Sprintf("aiscli download %s --id <value>", DownloadCancel)
)

func DownloadHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     = parseFlag(c, bucketFlag.Name)
		timeout    = parseFlag(c, timeoutFlag.Name)

		id string
	)

	if err := checkFlags(c, bucketFlag.Name); err != nil {
		return err
	}

	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}

	basePayload := cmn.DlBase{
		Bucket:      bucket,
		BckProvider: bckProvider,
		Timeout:     timeout,
	}

	commandName := c.Command.Name
	switch commandName {
	case DownloadSingle:
		if err := checkFlags(c, objNameFlag.Name, linkFlag.Name); err != nil {
			return err
		}

		objName := parseFlag(c, objNameFlag.Name)
		link := parseFlag(c, linkFlag.Name)
		payload := cmn.DlSingleBody{
			DlBase: basePayload,
			DlObj: cmn.DlObj{
				Link:    link,
				Objname: objName,
			},
		}

		id, err = api.DownloadSingleWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	case DownloadRange:
		if err := checkFlags(c, baseFlag.Name, templateFlag.Name); err != nil {
			return err
		}

		base := parseFlag(c, baseFlag.Name)
		template := parseFlag(c, templateFlag.Name)
		payload := cmn.DlRangeBody{
			DlBase:   basePayload,
			Base:     base,
			Template: template,
		}

		id, err = api.DownloadRangeWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	case DownloadCloud:
		prefix := parseFlag(c, dlPrefixFlag.Name)
		suffix := parseFlag(c, dlSuffixFlag.Name)
		payload := cmn.DlCloudBody{
			DlBase: basePayload,
			Prefix: prefix,
			Suffix: suffix,
		}
		id, err = api.DownloadCloudWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid command name '%s'", commandName)
	}
	fmt.Println(id)
	return nil
}

func DownloadAdminHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = parseFlag(c, idFlag.Name)
	)

	if err := checkFlags(c, idFlag.Name, linkFlag.Name); err != nil {
		return err
	}

	commandName := c.Command.Name
	switch commandName {
	case DownloadStatus:
		// TODO: handle ProgressBarFlag. In loop we should wait for status to
		// complete
		// https://github.com/vbauerster/mpb
		// showProgressBar := c.Bool(ProgressBarFlag.Name)

		resp, err := api.DownloadStatus(baseParams, id)
		if err != nil {
			return err
		}
		fmt.Println(resp.String())
	case DownloadCancel:
		if err := api.DownloadCancel(baseParams, id); err != nil {
			return err
		}
		fmt.Printf("download canceled: %s\n", id)
	default:
		return fmt.Errorf("invalid command name '%s'", commandName)
	}
	return nil
}

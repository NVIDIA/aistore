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

func DownloadHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     = parseFlag(c, BucketFlag.Name)
		timeout    = parseFlag(c, TimeoutFlag.Name)

		id string
	)

	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, BckProviderFlag.Name))
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
		objName := parseFlag(c, ObjNameFlag.Name)
		link := parseFlag(c, LinkFlag.Name)
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
		base := parseFlag(c, BaseFlag.Name)
		template := parseFlag(c, TemplateFlag.Name)
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
		prefix := parseFlag(c, DlPrefixFlag.Name)
		suffix := parseFlag(c, DlSuffixFlag.Name)
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
		id         = parseFlag(c, IDFlag.Name)
	)

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

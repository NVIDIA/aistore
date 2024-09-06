// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles specific bucket actions.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	storageSvcCmdsFlags = map[string][]cli.Flag{
		commandMirror: {
			copiesFlag,
			nonverboseFlag,
		},
		commandECEncode: {
			dataSlicesFlag,
			paritySlicesFlag,
			nonverboseFlag,
		},
	}

	storageSvcCmds = []cli.Command{
		{
			Name: commandMirror,
			Usage: "configure (or unconfigure) bucket as n-way mirror, and run the corresponding batch job, e.g.:\n" +
				indent1 + "\t- 'ais start mirror ais://m --copies 3'\t- configure ais://m as a 3-way mirror;\n" +
				indent1 + "\t- 'ais start mirror ais://m --copies 1'\t- configure ais://m for no redundancy (no extra copies).\n" +
				indent1 + "(see also: 'ais start ec-encode')",
			ArgsUsage:    bucketArgument,
			Flags:        storageSvcCmdsFlags[commandMirror],
			Action:       setCopiesHandler,
			BashComplete: bucketCompletions(bcmplop{}),
		},
		{
			Name: commandECEncode,
			Usage: "erasure code entire bucket, e.g.:\n" +
				indent1 + "\t- 'ais start ec-encode ais://m -d 8 -p 2'\t- erasure-code ais://m for (D=8, P=2).\n" +
				indent1 + "(see also: 'ais start mirror')",
			ArgsUsage:    bucketArgument,
			Flags:        storageSvcCmdsFlags[commandECEncode],
			Action:       ecEncodeHandler,
			BashComplete: bucketCompletions(bcmplop{}),
		},
	}
)

func setCopiesHandler(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		p   *cmn.Bprops
	)
	if bck, err = parseBckURI(c, c.Args().Get(0), false); err != nil {
		return
	}
	if p, err = headBucket(bck, false /* don't add */); err != nil {
		return
	}

	copies := c.Int(copiesFlag.Name)
	if p.Mirror.Copies == int64(copies) {
		if copies > 1 && p.Mirror.Enabled {
			fmt.Fprintf(c.App.Writer, "%s is already %d-way mirror, nothing to do\n", bck.Cname(""), copies)
			return
		}
		if copies < 2 {
			fmt.Fprintf(c.App.Writer, "%s is already configured with no redundancy, nothing to do\n", bck.Cname(""))
			return
		}
	}
	return nway(c, bck, copies)
}

func nway(c *cli.Context, bck cmn.Bck, copies int) (err error) {
	var xid string
	if xid, err = api.MakeNCopies(apiBP, bck, copies); err != nil {
		return
	}
	if flagIsSet(c, nonverboseFlag) {
		fmt.Fprintln(c.App.Writer, xid)
		return nil
	}
	var baseMsg string
	if copies > 1 {
		baseMsg = fmt.Sprintf("Configured %s as %d-way mirror. ", bck.Cname(""), copies)
	} else {
		baseMsg = fmt.Sprintf("Configured %s for single-replica (no redundancy). ", bck.Cname(""))
	}
	actionDone(c, baseMsg+toMonitorMsg(c, xid, ""))
	return nil
}

func ecEncodeHandler(c *cli.Context) error {
	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}

	var (
		bprops     *cmn.Bprops
		numd, nump int
		warned     bool
	)
	if bprops, err = headBucket(bck, false /* don't add */); err != nil {
		return err
	}
	numd = c.Int(fl1n(dataSlicesFlag.Name))
	nump = c.Int(fl1n(paritySlicesFlag.Name))

	// compare with ECConf.Validate
	if numd < cmn.MinSliceCount || numd > cmn.MaxSliceCount {
		return fmt.Errorf("invalid number %d of data slices (valid range: [%d, %d])", numd, cmn.MinSliceCount, cmn.MaxSliceCount)
	}
	if nump < cmn.MinSliceCount || nump > cmn.MaxSliceCount {
		return fmt.Errorf("invalid number %d of parity slices (valid range: [%d, %d])", nump, cmn.MinSliceCount, cmn.MaxSliceCount)
	}

	if bprops.EC.Enabled {
		if bprops.EC.DataSlices != numd || bprops.EC.ParitySlices != nump {
			// not supported yet:
			warn := fmt.Sprintf("%s is already erasure-coded, cannot change existing (D=%d, P=%d) configuration to (D=%d, P=%d)",
				bck.Cname(""), bprops.EC.DataSlices, bprops.EC.ParitySlices, numd, nump)
			actionWarn(c, warn)
			return nil
		}
		var warn string
		if bprops.EC.ObjSizeLimit == cmn.ObjSizeToAlwaysReplicate {
			warn = fmt.Sprintf("%s is already configured for (P + 1 = %d copies)", bck.Cname(""), bprops.EC.ParitySlices+1)
		} else {
			warn = fmt.Sprintf("%s is already erasure-coded for (D=%d, P=%d)", bck.Cname(""), numd, nump)
		}
		actionWarn(c, warn+" - proceeding to run anyway")
		warned = true
	}

	return ecEncode(c, bck, bprops, numd, nump, warned)
}

func ecEncode(c *cli.Context, bck cmn.Bck, bprops *cmn.Bprops, data, parity int, warned bool) error {
	xid, err := api.ECEncodeBucket(apiBP, bck, data, parity)
	if err != nil {
		return err
	}
	if flagIsSet(c, nonverboseFlag) {
		fmt.Fprintln(c.App.Writer, xid)
		return nil
	}
	if warned {
		actionDone(c, toMonitorMsg(c, xid, ""))
	} else {
		var msg string
		if bprops.EC.ObjSizeLimit == cmn.ObjSizeToAlwaysReplicate {
			msg = fmt.Sprintf("Erasure-coding %s for (P + 1 = %d copies). ", bck.Cname(""), bprops.EC.ParitySlices+1)
		} else {
			msg = fmt.Sprintf("Erasure-coding %s for (D=%d, P=%d). ", bck.Cname(""), data, parity)
		}
		actionDone(c, msg+toMonitorMsg(c, xid, ""))
	}
	return nil
}

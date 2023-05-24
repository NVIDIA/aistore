// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/urfave/cli"
)

// generic types
type (
	there struct {
		bck   cmn.Bck
		oname string
	}
	here struct {
		arg     string
		abspath string
		finfo   os.FileInfo
		lr      apc.ListRange
		isdir   bool
		recurs  bool
		stdin   bool
	}
)

// assorted specific
type (
	putargs struct {
		src here
		pt  *cos.ParsedTemplate
		dst there
	}
)

func (a *putargs) parse(c *cli.Context) (err error) {
	debug.Assert(!flagIsSet(c, createArchFlag), "TODO: '--archive'")        // TODO -- FIXME
	debug.Assert(!flagIsSet(c, archpathOptionalFlag), "TODO: '--archpath'") // TODO -- FIXME

	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if flagIsSet(c, progressFlag) || flagIsSet(c, listFileFlag) || flagIsSet(c, templateFileFlag) {
		// '--progress' steals STDOUT with multi-object producing scary looking
		// errors when there's no cluster
		if _, err = api.GetClusterMap(apiBP); err != nil {
			return
		}
	}
	switch {
	case c.NArg() == 1: // 2. BUCKET/[OBJECT_NAME] --list|--template
		uri := c.Args().Get(0) // dst
		a.dst.bck, a.dst.oname, err = parseBckObjURI(c, uri, true)
		if err != nil {
			return
		}
		// src via local lr
		if flagIsSet(c, listFileFlag) && flagIsSet(c, templateFileFlag) {
			return incorrectUsageMsg(c, errFmtExclusive, qflprn(listFileFlag), qflprn(templateFileFlag))
		}
		if !flagIsSet(c, listFileFlag) && !flagIsSet(c, templateFileFlag) {
			return missingArgSimple("FILE|DIRECTORY|DIRECTORY/PATTERN")
		}
		if flagIsSet(c, listFileFlag) {
			csv := parseStrFlag(c, listFileFlag)
			a.src.lr.ObjNames = splitCsv(csv)
			return
		}
		// must template
		var (
			pt   cos.ParsedTemplate
			tmpl = parseStrFlag(c, templateFileFlag)
		)
		pt, err = cos.NewParsedTemplate(tmpl)
		if err == nil {
			a.pt = &pt
		}
		return

	case c.NArg() == 2: // FILE|DIRECTORY|DIRECTORY/PATTERN BUCKET/[OBJECT_NAME]
		a.src.arg = c.Args().Get(0) // src
		uri := c.Args().Get(1)      // dst

		a.dst.bck, a.dst.oname, err = parseBckObjURI(c, uri, true)
		if err != nil {
			return err
		}
		// STDIN
		if a.src.arg == "-" {
			a.src.stdin = true
			if a.dst.oname == "" {
				err = fmt.Errorf("destination object name (in %s) is required when writing directly from standard input",
					c.Command.ArgsUsage)
			}
			return
		}
		// file or files
		if a.src.abspath, err = absPath(a.src.arg); err != nil {
			return
		}
		// inline "range" w/ no flag, e.g.: "/tmp/www/test{0..2}{0..2}.txt" ais://nnn/www
		pt, errV := cos.ParseBashTemplate(a.src.abspath)
		if errV == nil {
			a.pt = &pt
			return
		}
		// local file or dir?
		finfo, errV := os.Stat(a.src.abspath)
		if errV != nil {
			// must be a list of files embedded into the first arg
			a.src.lr.ObjNames = splitCsv(a.src.arg)
			return
		}

		a.src.finfo = finfo
		// reg file
		if !finfo.IsDir() {
			if a.dst.oname == "" {
				// NOTE [convention]: if objName is not provided
				// we use the filename as the destination object name
				a.dst.oname = filepath.Base(a.src.abspath)
			}
			return
		}
		// finally: a local (or client-accessible) directory
		a.src.isdir = true
		a.src.recurs = flagIsSet(c, recursFlag)
		return
	}

	const (
		efmt = "too many arguments: '%s'"
		hint = "(hint: wildcards must be in single or double quotes, see `--help` for details)"
	)
	l := c.NArg()
	if l > 4 {
		return fmt.Errorf(efmt+" ...\n%s\n", strings.Join(c.Args()[2:4], " "), hint)
	}
	return fmt.Errorf(efmt+"\n%s\n", strings.Join(c.Args()[2:], " "), hint)
}

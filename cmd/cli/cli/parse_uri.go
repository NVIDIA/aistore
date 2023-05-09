// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

// Return `bckFrom` and `bckTo` - the [shift] and the [shift+1] arguments, respectively
func parseBcks(c *cli.Context, bckFromArg, bckToArg string, shift int) (bckFrom, bckTo cmn.Bck, err error) {
	if c.NArg() == shift {
		err = missingArgumentsError(c, bckFromArg, bckToArg)
		return
	}
	if c.NArg() == shift+1 {
		err = missingArgumentsError(c, bckToArg)
		return
	}

	bckFrom, err = parseBckURI(c, c.Args().Get(shift), true /*error only*/)
	if err != nil {
		err = incorrectUsageMsg(c, "invalid %s argument '%s' - %v", bckFromArg, c.Args().Get(shift), err)
		return
	}
	bckTo, err = parseBckURI(c, c.Args().Get(shift+1), true)
	if err != nil {
		err = incorrectUsageMsg(c, "invalid %s argument '%s' - %v", bckToArg, c.Args().Get(shift+1), err)
	}
	return
}

func parseBckURI(c *cli.Context, uri string, errorOnly bool) (cmn.Bck, error) {
	const validNames = ": ais://mmm, s3://nnn or aws://nnn, gs://ppp or gcp://ppp"
	if isWebURL(uri) {
		bck := parseURLtoBck(uri)
		return bck, nil
	}

	opts := cmn.ParseURIOpts{}
	if providerRequired {
		parts := strings.Split(uri, apc.BckProviderSeparator)
		if len(parts) < 2 {
			err := fmt.Errorf("invalid %q: backend provider cannot be empty\n(e.g. valid names%s)", uri, validNames)
			return cmn.Bck{}, err
		}
		if len(parts) > 2 {
			err := fmt.Errorf("invalid bucket arg: too many parts %v\n(e.g. valid names%s)", parts, validNames)
			return cmn.Bck{}, err
		}
	} else {
		opts.DefaultProvider = cfg.DefaultProvider
	}
	bck, objName, err := cmn.ParseBckObjectURI(uri, opts)
	switch {
	case err != nil:
		return cmn.Bck{}, err
	case objName != "":
		if errorOnly {
			return cmn.Bck{}, fmt.Errorf("unexpected object name argument %q", objName)
		}
		return cmn.Bck{}, objectNameArgNotExpected(c, objName)
	case bck.Name == "":
		if errorOnly {
			return cmn.Bck{}, fmt.Errorf("missing bucket name: %q", uri)
		}
		return cmn.Bck{}, incorrectUsageMsg(c, "missing bucket name in %q", uri)
	default:
		if err = bck.Validate(); err != nil {
			if errorOnly {
				return cmn.Bck{}, err
			}
			msg := "E.g. " + bucketArgument + validNames
			return cmn.Bck{}, cannotExecuteError(c, err, msg)
		}
	}
	return bck, nil
}

// `ais ls` and friends: allow for `provider:` shortcut
func preparseBckObjURI(uri string) string {
	if uri == "" {
		return uri
	}
	p := strings.TrimSuffix(uri, ":")
	if _, err := cmn.NormalizeProvider(p); err == nil {
		return p + apc.BckProviderSeparator
	}
	return uri // unchanged
}

func parseDest(c *cli.Context, uri string) (bck cmn.Bck, pathSuffix string, err error) {
	bck, pathSuffix, err = parseBckObjURI(c, uri, true /*optional objName*/)
	if err != nil {
		return
	} else if bck.IsHTTP() {
		err = fmt.Errorf("http bucket is not supported as destination")
		return
	}
	pathSuffix = strings.Trim(pathSuffix, "/")
	return
}

func parseQueryBckURI(c *cli.Context, uri string) (cmn.QueryBcks, error) {
	uri = preparseBckObjURI(uri)
	if isWebURL(uri) {
		bck := parseURLtoBck(uri)
		return cmn.QueryBcks(bck), nil
	}
	bck, objName, err := cmn.ParseBckObjectURI(uri, cmn.ParseURIOpts{IsQuery: true})
	if err != nil {
		return cmn.QueryBcks(bck), err
	} else if objName != "" {
		return cmn.QueryBcks(bck), objectNameArgNotExpected(c, objName)
	}
	return cmn.QueryBcks(bck), nil
}

func parseBckObjURI(c *cli.Context, uri string, emptyObjnameOK bool) (bck cmn.Bck, objName string, err error) {
	if isWebURL(uri) {
		var hbo *cmn.HTTPBckObj
		hbo, err = cmn.NewHTTPObjPath(uri)
		if err != nil {
			return
		}
		bck, objName = hbo.Bck, hbo.ObjName
	} else {
		var opts cmn.ParseURIOpts
		if !providerRequired {
			opts.DefaultProvider = cfg.DefaultProvider
		}
		bck, objName, err = cmn.ParseBckObjectURI(uri, opts)
		if err != nil {
			if len(uri) > 1 && uri[:2] == "--" { // FIXME: needed smth like c.LooksLikeFlag
				return bck, objName, incorrectUsageMsg(c, "misplaced flag %q", uri)
			}
			msg := "Expecting " + objectArgument + ", e.g.: ais://mmm/obj1, s3://nnn/obj2, gs://ppp/obj3, etc."
			return bck, objName, cannotExecuteError(c, err, msg)
		}
	}

	if bck.Name == "" {
		return bck, objName, incorrectUsageMsg(c, "%q: missing bucket name", uri)
	} else if err := bck.Validate(); err != nil {
		return bck, objName, cannotExecuteError(c, err, "")
	} else if objName == "" && !emptyObjnameOK {
		return bck, objName, incorrectUsageMsg(c, "%q: missing object name", uri)
	}
	return
}

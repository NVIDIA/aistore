// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

// Creates new ais bucket
func createBucket(c *cli.Context, bck cmn.Bck, props *cmn.BpropsToSet, dontHeadRemote bool) (err error) {
	if err = api.CreateBucket(apiBP, bck, props, dontHeadRemote); err != nil {
		if herr := cmn.AsErrHTTP(err); herr != nil {
			if herr.Status == http.StatusConflict {
				desc := fmt.Sprintf("Bucket %q already exists", bck.String())
				if flagIsSet(c, ignoreErrorFlag) {
					fmt.Fprintln(c.App.Writer, desc)
					return nil
				}
				return errors.New(desc)
			}
			if cliConfVerbose() {
				herr.Message = herr.StringEx()
			}
			return fmt.Errorf("failed to create %q: %w", bck.String(), herr)
		}
		return fmt.Errorf("failed to create %q: %v", bck.String(), err)
	}
	// NOTE: see docs/bucket.md#default-bucket-properties
	fmt.Fprintf(c.App.Writer, "%q created\n", bck.Cname(""))
	return
}

// Destroy ais buckets
func destroyBuckets(c *cli.Context, buckets []cmn.Bck) (cmn.Bck, error) {
	for i := range buckets {
		bck := buckets[i]
		empty, errEmp := isBucketEmpty(bck, true /*cached*/)
		if errEmp == nil && !empty {
			if !flagIsSet(c, yesFlag) {
				if !confirm(c, fmt.Sprintf("Proceed to destroy %s?", bck.String())) {
					continue
				}
			}
		}

		if err := destroyBucket(c, bck); err != nil {
			return bck, err
		}
	}
	return cmn.Bck{}, nil
}

// Rename ais bucket
func mvBucket(c *cli.Context, bckFrom, bckTo cmn.Bck) error {
	if !flagIsSet(c, dontHeadRemoteFlag) {
		if _, err := headBucket(bckFrom, true /* don't add */); err != nil {
			return err
		}
	}
	xid, err := api.RenameBucket(apiBP, bckFrom, bckTo)
	if err != nil {
		return V(err)
	}
	_, xname := xact.GetKindName(apc.ActMoveBck)
	text := fmt.Sprintf("%s %s => %s", xact.Cname(xname, xid), bckFrom.String(), bckTo.String())
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		if flagIsSet(c, nonverboseFlag) {
			fmt.Fprintln(c.App.Writer, xid)
		} else {
			actionDone(c, text+". "+toMonitorMsg(c, xid, ""))
		}
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintln(c.App.Writer, text+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: timeout}
	if err := waitXact(&xargs); err != nil {
		fmt.Fprintf(c.App.ErrWriter, fmtXactFailed, "rename", bckFrom.String(), bckTo.String())
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return nil
}

func listOrSummBuckets(c *cli.Context, qbck cmn.QueryBcks, lsb lsbCtx) error {
	bcks, err := api.ListBuckets(apiBP, qbck, lsb.fltPresence)
	if err != nil {
		return V(err)
	}

	// NOTE:
	// typing `ls ais://@` (with an '@' symbol) to query remote ais buckets may not be
	// very obvious (albeit documented); thus, for the sake of usability making
	// an exception - extending ais queries to include remote ais

	if lsb.all && qbck.Ns.IsGlobal() && (qbck.Provider == apc.AIS || qbck.Provider == "") {
		if remais, err := api.GetRemoteAIS(apiBP); err == nil && len(remais.A) > 0 {
			qrais := qbck
			qrais.Ns = cmn.NsAnyRemote
			if brais, err := api.ListBuckets(apiBP, qrais, lsb.fltPresence); err == nil && len(brais) > 0 {
			outer:
				for i := range brais {
					bn := brais[i]
					for j := range bcks {
						bo := bcks[j]
						if bn.Equal(&bo) {
							continue outer
						}
					}
					bcks = append(bcks, bn)
				}
			}
		}
	}

	if len(bcks) == 0 {
		_lsTip(c, qbck)
		return nil
	}

	var nbcks cmn.Bcks
	if lsb.regex == nil {
		nbcks = bcks
	} else {
		for _, bck := range bcks {
			if lsb.regex.MatchString(bck.Name) {
				nbcks = append(nbcks, bck)
			}
		}
		if len(nbcks) == 0 {
			l := len(bcks)
			if l < 5 {
				fmt.Fprintf(c.App.Writer, "listed %v buckets with none matching %q regex",
					bcks, lsb.regexStr)
			} else {
				fmt.Fprintf(c.App.Writer, "listed %d buckets with none matching %q regex",
					len(bcks), lsb.regexStr)
			}
			return nil
		}
	}

	//
	// by provider
	//
	var total int
	for _, provider := range selectProvidersExclRais(bcks) {
		qbck = cmn.QueryBcks{Provider: provider}
		if provider == apc.AIS {
			qbck.Ns = cmn.NsGlobal // "local" cluster
		}
		cnt := listBckTable(c, qbck, nbcks, lsb)
		if cnt > 0 {
			fmt.Fprintln(c.App.Writer)
			total += cnt
		}
	}
	// finally, list remote ais buckets, if any
	qbck = cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}
	cnt := listBckTable(c, qbck, nbcks, lsb)
	total += cnt

	if total == 0 {
		// No buckets were actually displayed to users - show tip instead of blank line
		_lsTip(c, cmn.QueryBcks{})
	} else if cnt > 0 {
		fmt.Fprintln(c.App.Writer)
	}
	return nil
}

func _lsTip(c *cli.Context, qbck cmn.QueryBcks) {
	const (
		what1 = "No buckets in the cluster."
		what2 = "No %q buckets in the cluster."
		h1    = "Use %s option to list matching remote buckets, if any."
	)
	if qbck.IsEmpty() {
		fmt.Fprintln(c.App.Writer, what1)
		return
	}
	if flagIsSet(c, bckSummaryFlag) {
		fmt.Fprintf(c.App.Writer, what2+"\n", qbck)
	} else {
		fmt.Fprintf(c.App.Writer, what2+" "+h1+"\n", qbck, qflprn(allObjsOrBcksFlag))
	}
}

// If both backend_bck.name and backend_bck.provider are present, use them.
// Otherwise, replace as follows:
//   - e.g., `backend_bck=gcp://bucket_name` with `backend_bck.name=bucket_name` and
//     `backend_bck.provider=gcp` to match expected fields.
//   - `backend_bck=none` with `backend_bck.name=""` and `backend_bck.provider=""`.
func reformatBackendProps(c *cli.Context, nvs cos.StrKVs) error {
	var (
		originBck cmn.Bck
		v         string
		ok        bool
	)
	if v, ok = nvs[cmn.PropBackendBckName]; ok && v != "" {
		var err error
		if v, ok = nvs[cmn.PropBackendBckProvider]; ok && v != "" {
			nvs[cmn.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
			return err
		}
	}

	if v, ok = nvs[cmn.PropBackendBck]; ok {
		delete(nvs, cmn.PropBackendBck)
	} else if v, ok = nvs[cmn.PropBackendBckName]; !ok {
		goto validate
	}

	if v != apc.NilValue {
		var err error
		if originBck, err = parseBckURI(c, v, true /*error only*/); err != nil {
			return fmt.Errorf("invalid '%s=%s': expecting %q to be a valid bucket name",
				cmn.PropBackendBck, v, v)
		}
	}

	nvs[cmn.PropBackendBckName] = originBck.Name
	if v, ok = nvs[cmn.PropBackendBckProvider]; ok && v != "" {
		var err error
		nvs[cmn.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
		if err != nil {
			return err
		}
	} else {
		nvs[cmn.PropBackendBckProvider] = originBck.Provider
	}

validate:
	if nvs[cmn.PropBackendBckProvider] != "" && nvs[cmn.PropBackendBckName] == "" {
		return fmt.Errorf("invalid %q: bucket name cannot be empty when bucket provider (%q) is set",
			cmn.PropBackendBckName, cmn.PropBackendBckProvider)
	}
	return nil
}

// Get bucket props
func showBucketProps(c *cli.Context) error {
	if c.NArg() > 2 {
		return incorrectUsageMsg(c, "", c.Args()[2:])
	}

	bck, err := parseBckURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	p, err := headBucket(bck, !flagIsSet(c, addRemoteFlag) /* don't add */)
	if err != nil {
		return err
	}
	section := c.Args().Get(1)

	if bck.IsRemoteAIS() {
		if all, err := api.GetRemoteAIS(apiBP); err == nil {
			for _, remais := range all.A {
				if remais.Alias != bck.Ns.UUID && remais.UUID != bck.Ns.UUID {
					continue
				}
				altbck := bck
				if remais.Alias == bck.Ns.UUID {
					altbck.Ns.UUID = remais.UUID
				} else {
					altbck.Ns.UUID = remais.Alias
				}
				fmt.Fprintf(c.App.Writer, "remote cluster alias:\t\t%s\n", fcyan(remais.Alias))
				fmt.Fprintf(c.App.Writer, "remote cluster UUID:\t\t%s\n", fcyan(remais.UUID))
				fmt.Fprintf(c.App.Writer, "alternative bucket name:\t%s\n\n", fcyan(altbck.String()))
				break
			}
		}
	}

	if flagIsSet(c, jsonFlag) {
		if section != "" {
			if printSectionJSON(c, p, section) {
				return nil
			}
			// Section not found - show helpful error
			showSectionNotFoundError(c, section, p,
				"Try 'ais bucket props show "+bck.String()+" --json' to see all sections")
			return nil
		}
		opts := teb.Jopts(true)
		return teb.Print(p, "", opts)
	}

	defProps, errV := defaultBckProps(bck)
	if errV != nil {
		return errV
	}
	return headBckTable(c, p, defProps, section)
}

// compare w/ showClusterConfig using the same generic template
// for "flattened" cluster config
func headBckTable(c *cli.Context, props, defProps *cmn.Bprops, section string) (err error) {
	var (
		defList nvpairList
		colored = !gcfg.NoColor
		compact = flagIsSet(c, compactPropFlag)
	)
	// List instead of map to keep properties in the same order always.
	// All names are one word ones - for easier parsing.
	propList := bckPropList(props, !compact)
	if section != "" {
		tmpPropList := propList[:0]
		for _, v := range propList {
			if strings.HasPrefix(v.Name, section) {
				tmpPropList = append(tmpPropList, v)
			}
		}
		propList = tmpPropList
	}

	if colored {
		defList = bckPropList(defProps, !compact)
		for idx, p := range propList {
			for _, def := range defList {
				if def.Name != p.Name {
					continue
				}
				switch {
				case def.Name == "present":
					if p.Value == "yes" {
						p.Value = fgreen(p.Value)
					} else {
						p.Value = fcyan(p.Value)
					}
				case def.Name == cmn.PropBucketCreated:
					if p.Value != teb.NotSetVal {
						created, err := s2UnixNano(p.Value)
						if err == nil {
							p.Value = fmtBucketCreatedTime(created)
						}
						p.Value = fgreen(p.Value)
					}
				case def.Value != p.Value:
					p.Value = fcyan(p.Value)
				}

				propList[idx] = p
				break
			}
		}
	}

	if flagIsSet(c, noHeaderFlag) {
		err = teb.Print(propList, teb.PropValTmplNoHdr)
	} else {
		err = teb.Print(propList, teb.PropValTmpl)
	}
	if err != nil {
		return err
	}

	// feature flags: show all w/ descriptions
	if section == featureFlagsJname {
		err = printFeatVerbose(c, props.Features, true /*bucket scope*/)
	}
	return err
}

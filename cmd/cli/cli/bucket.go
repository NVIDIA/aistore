// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
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
func createBucket(c *cli.Context, bck cmn.Bck, props *cmn.BucketPropsToUpdate) (err error) {
	if err = api.CreateBucket(apiBP, bck, props); err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			if herr.Status == http.StatusConflict {
				desc := fmt.Sprintf("Bucket %q already exists", bck)
				if flagIsSet(c, ignoreErrorFlag) {
					fmt.Fprint(c.App.Writer, desc)
					return nil
				}
				return errors.New(desc)
			}
			if verbose() {
				herr.Message = herr.StringEx()
			}
			return fmt.Errorf("failed to create %q: %w", bck, herr)
		}
		return fmt.Errorf("failed to create %q: %v", bck, err)
	}
	// NOTE: see docs/bucket.md#default-bucket-properties
	fmt.Fprintf(c.App.Writer, "%q created\n", bck.Cname(""))
	return
}

// Destroy ais buckets
func destroyBuckets(c *cli.Context, buckets []cmn.Bck) (err error) {
	for _, bck := range buckets {
		var empty bool
		empty, err = isBucketEmpty(bck)
		if err == nil && !empty {
			if !flagIsSet(c, yesFlag) {
				if ok := confirm(c, fmt.Sprintf("Proceed to destroy %s?", bck)); !ok {
					continue
				}
			}
		}
		if err = api.DestroyBucket(apiBP, bck); err == nil {
			fmt.Fprintf(c.App.Writer, "%q destroyed\n", bck.Cname(""))
			continue
		}
		if cmn.IsStatusNotFound(err) {
			desc := fmt.Sprintf("Bucket %q does not exist", bck)
			if !flagIsSet(c, ignoreErrorFlag) {
				return errors.New(desc)
			}
			fmt.Fprint(c.App.Writer, desc)
			continue
		}
		return err
	}
	return nil
}

// Rename ais bucket
func mvBucket(c *cli.Context, bckFrom, bckTo cmn.Bck) error {
	if _, err := headBucket(bckFrom, true /* don't add */); err != nil {
		return err
	}
	xid, err := api.RenameBucket(apiBP, bckFrom, bckTo)
	if err != nil {
		return V(err)
	}
	_, xname := xact.GetKindName(apc.ActMoveBck)
	text := fmt.Sprintf("%s[%s] %s => %s", xname, xid, bckFrom, bckTo)
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		actionDone(c, text+". "+toMonitorMsg(c, xid, ""))
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintln(c.App.Writer, text+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: timeout}
	if err := waitXact(apiBP, xargs); err != nil {
		fmt.Fprintf(c.App.Writer, fmtXactFailed, "rename", bckFrom, bckTo)
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return nil
}

// Evict remote bucket
func evictBucket(c *cli.Context, bck cmn.Bck) (err error) {
	if flagIsSet(c, dryRunFlag) {
		fmt.Fprintf(c.App.Writer, "Evict: %q\n", bck.Cname(""))
		return
	}
	if err = ensureHasProvider(bck); err != nil {
		return
	}
	if err = api.EvictRemoteBucket(apiBP, bck, flagIsSet(c, keepMDFlag)); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "%q bucket evicted\n", bck.Cname(""))
	return
}

func listBuckets(c *cli.Context, qbck cmn.QueryBcks, fltPresence int, countRemoteObjs bool) (err error) {
	var (
		regex  *regexp.Regexp
		fmatch = func(_ cmn.Bck) bool { return true }
	)
	if regexStr := parseStrFlag(c, regexLsAnyFlag); regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return
		}
		fmatch = func(bck cmn.Bck) bool { return regex.MatchString(bck.Name) }
	}
	bcks, errV := api.ListBuckets(apiBP, qbck, fltPresence)
	if errV != nil {
		return V(errV)
	}

	// NOTE:
	// typing `ls ais://@` (with an '@' symbol) to query remote ais buckets may not be
	// very obvious (albeit documented); thus, for the sake of usability making
	// an exception - extending ais queries to include remote ais

	if !qbck.IsRemoteAIS() && (qbck.Provider == apc.AIS || qbck.Provider == "") {
		if _, err := api.GetRemoteAIS(apiBP); err == nil {
			qrais := qbck
			qrais.Ns = cmn.NsAnyRemote
			if brais, err := api.ListBuckets(apiBP, qrais, fltPresence); err == nil && len(brais) > 0 {
				bcks = append(bcks, brais...)
			}
		}
	}

	if len(bcks) == 0 && apc.IsFltPresent(fltPresence) && !qbck.IsAIS() {
		const hint = "Use %s option to list _all_ buckets.\n"
		if qbck.IsEmpty() {
			fmt.Fprintf(c.App.Writer, "No buckets in the cluster. "+hint, qflprn(allObjsOrBcksFlag))
		} else {
			fmt.Fprintf(c.App.Writer, "No %q matching buckets in the cluster. "+hint, qbck, qflprn(allObjsOrBcksFlag))
		}
		return
	}
	var total int
	for _, provider := range selectProvidersExclRais(bcks) {
		qbck = cmn.QueryBcks{Provider: provider}
		if provider == apc.AIS {
			qbck.Ns = cmn.NsGlobal // "local" cluster
		}
		cnt := listBckTable(c, qbck, bcks, fmatch, fltPresence, countRemoteObjs)
		if cnt > 0 {
			fmt.Fprintln(c.App.Writer)
			total += cnt
		}
	}
	// finally, list remote ais buckets, if any
	qbck = cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}
	cnt := listBckTable(c, qbck, bcks, fmatch, fltPresence, countRemoteObjs)
	if cnt > 0 || total == 0 {
		fmt.Fprintln(c.App.Writer)
	}
	return
}

// If both backend_bck.name and backend_bck.provider are present, use them.
// Otherwise, replace as follows:
//   - e.g., `backend_bck=gcp://bucket_name` with `backend_bck.name=bucket_name` and
//     `backend_bck.provider=gcp` to match expected fields.
//   - `backend_bck=none` with `backend_bck.name=""` and `backend_bck.provider=""`.
func reformatBackendProps(c *cli.Context, nvs cos.StrKVs) (err error) {
	var (
		originBck cmn.Bck
		v         string
		ok        bool
	)

	if v, ok = nvs[cmn.PropBackendBckName]; ok && v != "" {
		if v, ok = nvs[cmn.PropBackendBckProvider]; ok && v != "" {
			nvs[cmn.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
			return
		}
	}

	if v, ok = nvs[cmn.PropBackendBck]; ok {
		delete(nvs, cmn.PropBackendBck)
	} else if v, ok = nvs[cmn.PropBackendBckName]; !ok {
		goto validate
	}

	if v != NilValue {
		if originBck, err = parseBckURI(c, v, true /*error only*/); err != nil {
			return fmt.Errorf("invalid '%s=%s': expecting %q to be a valid bucket name",
				cmn.PropBackendBck, v, v)
		}
	}

	nvs[cmn.PropBackendBckName] = originBck.Name
	if v, ok = nvs[cmn.PropBackendBckProvider]; ok && v != "" {
		nvs[cmn.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
	} else {
		nvs[cmn.PropBackendBckProvider] = originBck.Provider
	}

validate:
	if nvs[cmn.PropBackendBckProvider] != "" && nvs[cmn.PropBackendBckName] == "" {
		return fmt.Errorf("invalid %q: bucket name cannot be empty when bucket provider (%q) is set",
			cmn.PropBackendBckName, cmn.PropBackendBckProvider)
	}
	return err
}

// Get bucket props
func showBucketProps(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		p   *cmn.BucketProps
	)

	if c.NArg() > 2 {
		return incorrectUsageMsg(c, "", c.Args()[2:])
	}

	section := c.Args().Get(1)

	if bck, err = parseBckURI(c, c.Args().Get(0), false); err != nil {
		return
	}
	if p, err = headBucket(bck, true /* don't add */); err != nil {
		return
	}

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
		opts := teb.Jopts(true)
		return teb.Print(p, "", opts)
	}

	defProps, err := defaultBckProps(bck)
	if err != nil {
		return err
	}
	return HeadBckTable(c, p, defProps, section)
}

func HeadBckTable(c *cli.Context, props, defProps *cmn.BucketProps, section string) error {
	var (
		defList nvpairList
		colored = !cfg.NoColor
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
				if def.Name == cmn.PropBucketCreated {
					if p.Value != teb.NotSetVal {
						created, err := cos.S2UnixNano(p.Value)
						if err == nil {
							p.Value = fmtBucketCreatedTime(created)
						}
					}
					propList[idx] = p
				}
				if def.Value != p.Value {
					p.Value = fcyan(p.Value)
					propList[idx] = p
				}
				break
			}
		}
	}

	return teb.Print(propList, teb.PropsSimpleTmpl)
}

// Configure bucket as n-way mirror
func configureNCopies(c *cli.Context, bck cmn.Bck, copies int) (err error) {
	var xid string
	if xid, err = api.MakeNCopies(apiBP, bck, copies); err != nil {
		return
	}
	var baseMsg string
	if copies > 1 {
		baseMsg = fmt.Sprintf("Configured %s as %d-way mirror. ", bck.Cname(""), copies)
	} else {
		baseMsg = fmt.Sprintf("Configured %s for single-replica (no redundancy). ", bck.Cname(""))
	}
	actionDone(c, baseMsg+toMonitorMsg(c, xid, ""))
	return
}

// erasure code the entire bucket
func ecEncode(c *cli.Context, bck cmn.Bck, data, parity int) (err error) {
	var xid string
	if xid, err = api.ECEncodeBucket(apiBP, bck, data, parity); err != nil {
		return
	}
	msg := fmt.Sprintf("Erasure-coding bucket %s. ", bck.Cname(""))
	actionDone(c, msg+toMonitorMsg(c, xid, ""))
	return
}

func printObjProps(c *cli.Context, entries cmn.LsoEntries, lstFilter *lstFilter, props string, addCachedCol bool) error {
	var (
		hideHeader     = flagIsSet(c, noHeaderFlag)
		matched, other = lstFilter.apply(entries)
		units, errU    = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}

	propsList := splitCsv(props)
	tmpl := teb.ObjPropsTemplate(propsList, hideHeader, addCachedCol)
	opts := teb.Opts{AltMap: teb.FuncMapUnits(units)}
	if err := teb.Print(matched, tmpl, opts); err != nil {
		return err
	}
	if len(matched) > 10 {
		listed := fblue("Listed:")
		fmt.Fprintln(c.App.Writer, listed, len(matched), "names")
	}
	if flagIsSet(c, showUnmatchedFlag) && len(other) > 0 {
		unmatched := fcyan("\nNames that didn't match: ") + strconv.Itoa(len(other))
		tmpl = unmatched + "\n" + tmpl
		if err := teb.Print(other, tmpl, opts); err != nil {
			return err
		}
	}
	return nil
}

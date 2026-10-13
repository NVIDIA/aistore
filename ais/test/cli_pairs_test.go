// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// exercise shared CLI assignment syntax and escaped metadata against a live cluster
func TestCLINVPairs(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 1})
	bin, err := exec.LookPath("ais")
	if err != nil {
		t.Skip("'ais' binary not found")
	}
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		bck      = cmn.Bck{Name: "cli-pairs-" + cos.GenTie(), Provider: apc.AIS}
	)
	t.Setenv(env.AisEndpoint, proxyURL)
	if bp.Token != "" {
		t.Setenv(env.AisAuthToken, bp.Token)
		t.Setenv(env.AisAuthTokenFile, "")
	}
	config, err := api.GetClusterConfig(bp)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		err := api.SetClusterConfig(bp, cos.StrKVs{"checksum.validate_warm_get": strconv.FormatBool(config.Cksum.ValidateWarmGet)}, false)
		tassert.CheckError(t, err)
	})
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	run := func(args ...string) {
		t.Helper()
		out, err := exec.Command(bin, args...).CombinedOutput()
		tassert.Fatalf(t, err == nil, "ais %v: %v\n%s", args, err, out)
	}
	forms := func(name, val string) [][]string {
		return [][]string{{name + "=" + val}, {name, val}, {name + "=", val}, {name, "=" + val}, {name, "=", val}}
	}
	for i := range 5 {
		// alternate values: a parser failure must not pass as a show/no-op command
		val := i%2 == 0
		if config.Cksum.ValidateWarmGet {
			val = !val
		}
		args := forms("checksum.validate_warm_get", strconv.FormatBool(val))[i]
		run(append([]string{"config", "cluster"}, args...)...)
		after, err := api.GetClusterConfig(bp)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, after.Cksum.ValidateWarmGet == val, "cluster config did not apply %v", args)

		kind := cos.ChecksumMD5
		if i%2 != 0 {
			kind = cos.ChecksumCesXxh
		}
		args = forms("checksum.type", kind)[i]
		run(append([]string{"bucket", "props", "set", bck.Cname("")}, args...)...)
		props, err := api.HeadBucket(bp, bck, true /*don't add*/)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, props.Cksum.Type == kind, "bucket props did not apply %v", args)
	}
	run("bucket", "props", "set", bck.Cname(""), "extra.custom", "team=alpha, two words")
	bprops, err := api.HeadBucket(bp, bck, true /*don't add*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bprops.Extra.Custom == "team=alpha, two words", "lost literal '=' or spaces in bucket value")
	run("bucket", "props", "set", bck.Cname(""), "extra.custom=", "checksum.type=md5")
	bprops, err = api.HeadBucket(bp, bck, true /*don't add*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bprops.Extra.Custom == "", "empty value did not reset extra.custom")
	run("bucket", "props", "set", bck.Cname(""), "Access=su", "Features=S3-Use-Path-Style", "Backend_Bck=none")
	bprops, err = api.HeadBucket(bp, bck, true /*don't add*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bprops.Access == apc.AccessAll && bprops.Features.IsSet(feat.S3UsePathStyle) && bprops.BackendBck.IsEmpty(),
		"mixed-case bucket properties were not converted or expanded")
	for i, value := range []string{`{"text":"a b", "nested":[1, {"text":"x=y, }"}]}`, `["a b", {"path":"a\\b", "quote":"\" }"}]`} {
		propsBck := cmn.Bck{Name: bck.Name + "-props-" + strconv.Itoa(i), Provider: apc.AIS}
		run("create", propsBck.Cname(""), "--props", "extra.custom="+value+" checksum.type=md5")
		t.Cleanup(func() { tools.DestroyBucket(t, proxyURL, propsBck) })
		props, err := api.HeadBucket(bp, propsBck, true /*don't add*/)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, props.Extra.Custom == value && props.Cksum.Type == cos.ChecksumMD5,
			"aggregate props lost JSON spaces/escapes or the following assignment: %+v", props)
	}
	const obj = "metadata"
	tassert.CheckFatal(t, tools.PutObjRR(bp, bck, obj, cos.KiB, cos.ChecksumMD5))
	// HEAD custom metadata uses unescaped key=value headers: test '=' in keys at the parser/request level.
	run("object", "set-custom", bck.Cname(obj), `name\ with\ spaces=value\=with\ spaces`, "plain name", "plain=value, with spaces",
		`json={"path":"a\\b","text":"x=y, z"}`)
	props, err := api.HeadObjectV2(bp, bck, obj, apc.GetPropsCustom, api.HeadArgs{})
	tassert.CheckFatal(t, err)
	want := cos.StrKVs{
		"name with spaces": "value=with spaces",
		"plain name":       "plain=value, with spaces",
		"json":             `{"path":"a\\b","text":"x=y, z"}`,
	}
	for name, val := range want {
		tassert.Fatalf(t, props.CustomMD[name] == val, "custom metadata %q: expected %q, got %q", name, val, props.CustomMD[name])
	}
}

// reject unknown JSON fields and warn when JSON config updates require restart
func TestCLIConfigJSON(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 1})
	bin, err := exec.LookPath("ais")
	if err != nil {
		t.Skip("'ais' binary not found")
	}
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	t.Setenv(env.AisEndpoint, proxyURL)
	if bp.Token != "" {
		t.Setenv(env.AisAuthToken, bp.Token)
		t.Setenv(env.AisAuthTokenFile, "")
	}
	config, err := api.GetClusterConfig(bp)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		err := api.SetClusterConfig(bp, cos.StrKVs{
			"net.http.idle_conns":      strconv.Itoa(config.Net.HTTP.MaxIdleConns),
			"timeout.cplane_operation": config.Timeout.CplaneOperation.String(),
		}, false)
		tassert.CheckError(t, err)
	})
	run := func(args ...string) ([]byte, error) {
		t.Helper()
		return exec.Command(bin, append([]string{"config", "cluster"}, args...)...).CombinedOutput()
	}
	for _, arg := range []string{`space={"highwn":91}`, `net.http={"idle_conn":64}`} {
		t.Run(arg, func(t *testing.T) {
			before, err := api.GetClusterConfig(bp)
			tassert.CheckFatal(t, err)
			out, err := run(arg)
			tassert.Fatalf(t, err != nil && strings.Contains(string(out), "unknown field"),
				"expected unknown field error, got %v\n%s", err, out)
			after, err := api.GetClusterConfig(bp)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, after.Version == before.Version, "invalid JSON changed cluster config version")
		})
	}
	idleConns := 64
	cplane := max(config.Timeout.CplaneOperation.D()/2, 10*time.Millisecond).String()
	if config.Net.HTTP.MaxIdleConns == idleConns {
		idleConns = 32
	}
	for _, arg := range []string{
		`net.http={"idle_conns":` + strconv.Itoa(idleConns) + `}`,
		`net={"http":{"idle_conns":` + strconv.Itoa(idleConns) + `}}`,
		`timeout={"cplane_operation":"` + cplane + `"}`,
	} {
		t.Run(arg, func(t *testing.T) {
			out, err := run(arg)
			tassert.Fatalf(t, err == nil, "JSON update failed: %v\n%s", err, out)
			tassert.Fatalf(t, strings.Contains(string(out), "cluster restart required"), "missing restart warning:\n%s", out)
			after, err := api.GetClusterConfig(bp)
			tassert.CheckFatal(t, err)
			if strings.HasPrefix(arg, "net") {
				tassert.Fatalf(t, after.Net.HTTP.MaxIdleConns == idleConns, "JSON update did not set idle_conns")
			} else {
				tassert.Fatalf(t, after.Timeout.CplaneOperation.String() == cplane, "JSON update did not set cplane_operation")
			}
		})
	}
	// a more specific path overrides its enclosing section, regardless of argument order
	for _, args := range [][]string{
		{`net={"http":{"idle_conns":32}}`, `net.http={"idle_conns":64}`},
		{`net.http={"idle_conns":64}`, `net={"http":{"idle_conns":32}}`},
	} {
		out, err := run(args...)
		tassert.Fatalf(t, err == nil, "overlapping JSON update failed: %v\n%s", err, out)
		after, err := api.GetClusterConfig(bp)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, after.Net.HTTP.MaxIdleConns == 64,
			"specific JSON path did not override its parent for %v: got %d", args, after.Net.HTTP.MaxIdleConns)
	}
	out, err := run(`space={}`)
	tassert.Fatalf(t, err == nil, "empty JSON section failed: %v\n%s", err, out)
	tassert.Fatalf(t, !strings.Contains(string(out), "cluster restart required"), "unexpected restart warning:\n%s", out)
}

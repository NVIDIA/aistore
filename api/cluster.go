// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

// to be used by external watchdogs (Kubernetes, etc.)
// (compare with api.Health below)
func GetProxyReadiness(bp BaseParams) error {
	bp.Method = http.MethodGet
	q := qalloc()
	q.Set(apc.QparamHealthReadiness, "true")
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathHealth.S
		reqParams.Query = q
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	qfree(q)
	return err
}

func Health(bp BaseParams, readyToRebalance ...bool) error {
	reqParams := mkhealth(bp, readyToRebalance...)
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func HealthUptime(bp BaseParams, readyToRebalance ...bool) (string, string, error) {
	reqParams := mkhealth(bp, readyToRebalance...)
	hdr, _, err := reqParams.doReqHdr()
	if err != nil {
		return "", "", err
	}
	clutime, nutime := hdr.Get(apc.HdrClusterUptime), hdr.Get(apc.HdrNodeUptime)
	FreeRp(reqParams)
	return clutime, nutime, err
}

func mkhealth(bp BaseParams, readyToRebalance ...bool) (reqParams *ReqParams) {
	var q url.Values
	bp.Method = http.MethodGet
	if len(readyToRebalance) > 0 && readyToRebalance[0] {
		q = url.Values{apc.QparamPrimaryReadyReb: []string{"true"}}
	}
	reqParams = AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathHealth.S
		reqParams.Query = q
	}
	return
}

// get cluster map from a BaseParams-referenced node
func GetClusterMap(bp BaseParams) (smap *meta.Smap, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatSmap)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDae.S
		reqParams.Query = q
	}
	_, err = reqParams.DoReqAny(&smap)

	FreeRp(reqParams)
	qfree(q)
	return smap, err
}

// GetNodeClusterMap retrieves cluster map from the specified node.
func GetNodeClusterMap(bp BaseParams, sid string) (smap *meta.Smap, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatSmap)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = q
		reqParams.Header = http.Header{apc.HdrNodeID: []string{sid}}
	}
	_, err = reqParams.DoReqAny(&smap)

	FreeRp(reqParams)
	qfree(q)
	return smap, err
}

// get bucket metadata (BMD) from a BaseParams-referenced node
func GetBMD(bp BaseParams) (bmd *meta.BMD, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatBMD)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDae.S
		reqParams.Query = q
	}

	bmd = &meta.BMD{}
	_, err = reqParams.DoReqAny(bmd)

	FreeRp(reqParams)
	qfree(q)
	return bmd, err
}

// - get (smap, bmd, config) *cluster-level* metadata from the spec-ed node
// - compare with GetClusterMap, GetNodeClusterMap, GetClusterConfig et al.
// - TODO: etl meta
func GetNodeMeta(bp BaseParams, sid, what string) (out any, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, what)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = q
		reqParams.Header = http.Header{apc.HdrNodeID: []string{sid}}
	}
	switch what {
	case apc.WhatSmap:
		smap := meta.Smap{}
		_, err = reqParams.DoReqAny(&smap)
		out = &smap
	case apc.WhatBMD:
		bmd := meta.BMD{}
		_, err = reqParams.DoReqAny(&bmd)
		out = &bmd
	case apc.WhatClusterConfig:
		config := cmn.ClusterConfig{}
		_, err = reqParams.DoReqAny(&config)
		out = &config
	default:
		err = fmt.Errorf("unknown or unsupported cluster-level metadata type %q", what)
	}

	FreeRp(reqParams)
	qfree(q)
	return out, err
}

// GetClusterSysInfo retrieves cluster's system information
func GetClusterSysInfo(bp BaseParams) (info apc.ClusterSysInfo, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatSysInfo)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = q
	}
	_, err = reqParams.DoReqAny(&info)

	FreeRp(reqParams)
	qfree(q)
	return info, err
}

func GetRemoteAIS(bp BaseParams) (remais meta.RemAisVec, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatRemoteAIS)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = q
	}
	_, err = reqParams.DoReqAny(&remais)

	FreeRp(reqParams)
	qfree(q)
	return remais, err
}

// (see also enable/disable backend below)
func GetConfiguredBackends(bp BaseParams) (out []string, err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatBackends)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = q
	}
	_, err = reqParams.DoReqAny(&out)

	FreeRp(reqParams)
	qfree(q)
	return out, err
}

// JoinCluster add a node to a cluster.
func JoinCluster(bp BaseParams, nodeInfo *meta.Snode, flags cos.BitFlags) (rebID, sid string, err error) {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluUserReg.S
		reqParams.Body = cos.MustMarshal(nodeInfo)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		if flags != 0 {
			reqParams.Header.Set(apc.HdrNodeFlags, strconv.FormatUint(uint64(flags), 10))
		}
	}

	var info apc.JoinNodeResult
	_, err = reqParams.DoReqAny(&info)
	FreeRp(reqParams)
	return info.RebalanceID, info.DaemonID, err
}

// SetPrimaryProxy given a daemonID sets that corresponding proxy as the
// primary proxy of the cluster.
func SetPrimary(bp BaseParams, newPrimaryID, newPrimaryURL string, force bool) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	reqParams.BaseParams = bp
	reqParams.Path = apc.URLPathCluProxy.Join(newPrimaryID)
	if force || newPrimaryURL != "" {
		q := make(url.Values, 2)
		if force {
			q.Set(apc.QparamForce, "true")
		}
		if newPrimaryURL != "" {
			q.Set(apc.QparamPrimaryCandidate, newPrimaryURL)
		}
		reqParams.Query = q
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// SetClusterConfig given key-value pairs of cluster configuration parameters,
// sets the cluster-wide configuration accordingly. Setting cluster-wide
// configuration requires sending the request to a proxy.
func SetClusterConfig(bp BaseParams, nvs cos.StrKVs, transient bool) error {
	q := qalloc()
	for key, val := range nvs {
		q.Set(key, val)
	}
	if transient {
		q.Set(apc.ActTransient, "true")
	}

	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluSetConf.S
		reqParams.Query = q
	}
	err := reqParams.DoRequest()

	FreeRp(reqParams)
	qfree(q)
	return err
}

// SetClusterConfigUsingMsg sets the cluster-wide configuration
// using the `cmn.ConfigToSet` parameter provided.
func SetClusterConfigUsingMsg(bp BaseParams, configToUpdate *cmn.ConfigToSet, transient bool) error {
	var (
		q   url.Values
		msg = apc.ActMsg{Action: apc.ActSetConfig, Value: configToUpdate}
	)
	if transient {
		q = url.Values{apc.ActTransient: []string{"true"}}
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

func setRebalance(bp BaseParams, enabled bool) error {
	configToSet := &cmn.ConfigToSet{
		Rebalance: &cmn.RebalanceConfToSet{
			Enabled: apc.Ptr(enabled),
		},
	}
	return SetClusterConfigUsingMsg(bp, configToSet, false /*transient*/)
}

func EnableRebalance(bp BaseParams) error {
	return setRebalance(bp, true)
}

func DisableRebalance(bp BaseParams) error {
	return setRebalance(bp, false)
}

// all nodes: reset configuration to cluster defaults
func ResetClusterConfig(bp BaseParams) error {
	return _putCluster(bp, apc.ActMsg{Action: apc.ActResetConfig})
}

func RotateClusterLogs(bp BaseParams) error {
	return _putCluster(bp, apc.ActMsg{Action: apc.ActRotateLogs})
}

func ReloadBackendCreds(bp BaseParams, provider string) error {
	return _putCluster(bp, apc.ActMsg{Action: apc.ActReloadBackendCreds, Name: provider})
}

func _putCluster(bp BaseParams, msg apc.ActMsg) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// GetClusterConfig returns cluster-wide configuration
// (compare with `api.GetDaemonConfig`)
func GetClusterConfig(bp BaseParams) (*cmn.ClusterConfig, error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatClusterConfig)

	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = q
	}

	cluConfig := &cmn.ClusterConfig{}
	_, err := reqParams.DoReqAny(cluConfig)

	FreeRp(reqParams)
	qfree(q)
	if err != nil {
		return nil, err
	}
	return cluConfig, nil
}

func AttachRemoteAIS(bp BaseParams, alias, u string) (err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatRemoteAIS)

	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluAttach.S
		reqParams.Query = q
		reqParams.Header = http.Header{
			apc.HdrRemAisAlias: []string{alias},
			apc.HdrRemAisURL:   []string{u},
		}
	}
	err = reqParams.DoRequest()

	FreeRp(reqParams)
	qfree(q)
	return err
}

func DetachRemoteAIS(bp BaseParams, alias string) (err error) {
	q := qalloc()
	q.Set(apc.QparamWhat, apc.WhatRemoteAIS)

	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluDetach.S
		reqParams.Query = q
		reqParams.Header = http.Header{apc.HdrRemAisAlias: []string{alias}}
	}
	err = reqParams.DoRequest()

	FreeRp(reqParams)
	qfree(q)
	return err
}

//
// Backend (enable | disable)
// see also GetConfiguredBackends above
//

func EnableBackend(bp BaseParams, provider string) error {
	np := apc.NormalizeProvider(provider)
	if !apc.IsCloudProvider(np) {
		return fmt.Errorf("can only enable cloud backend (have %q)", provider) // TODO: this check can be removed, if need be
	}
	path := apc.URLPathCluBendEnable.Join(np)
	return _backend(bp, path)
}

func DisableBackend(bp BaseParams, provider string) error {
	np := apc.NormalizeProvider(provider)
	if !apc.IsCloudProvider(np) {
		return fmt.Errorf("can only disable cloud backend (have %q)", provider)
	}
	path := apc.URLPathCluBendDisable.Join(np)
	return _backend(bp, path)
}

func _backend(bp BaseParams, path string) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

//
// Maintenance API
//

func StartMaintenance(bp BaseParams, actValue *apc.ActValRmNode) (xid string, err error) {
	msg := apc.ActMsg{
		Action: apc.ActStartMaintenance,
		Value:  actValue,
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return xid, err
}

func DecommissionNode(bp BaseParams, actValue *apc.ActValRmNode) (xid string, err error) {
	msg := apc.ActMsg{
		Action: apc.ActDecommissionNode,
		Value:  actValue,
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return xid, err
}

func StopMaintenance(bp BaseParams, actValue *apc.ActValRmNode) (xid string, err error) {
	msg := apc.ActMsg{
		Action: apc.ActStopMaintenance,
		Value:  actValue,
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	_, err = reqParams.doReqStr(&xid)
	FreeRp(reqParams)
	return xid, err
}

// ShutdownCluster shuts down the whole cluster
func ShutdownCluster(bp BaseParams) error {
	msg := apc.ActMsg{Action: apc.ActShutdownCluster}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// DecommissionCluster permanently decommissions entire cluster
func DecommissionCluster(bp BaseParams, rmUserData bool) error {
	msg := apc.ActMsg{Action: apc.ActDecommissionCluster}
	if rmUserData {
		msg.Value = &apc.ActValRmNode{RmUserData: true}
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	if cos.IsEOF(err) {
		err = nil
	}
	return err
}

// ShutdownNode shuts down a specific node
func ShutdownNode(bp BaseParams, actValue *apc.ActValRmNode) (id string, err error) {
	msg := apc.ActMsg{
		Action: apc.ActShutdownNode,
		Value:  actValue,
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	_, err = reqParams.doReqStr(&id)
	FreeRp(reqParams)
	return id, err
}

// Remove node node from the cluster immediately.
// - NOTE: potential data loss, advanced usage only!
// - NOTE: the node remains running (compare w/ shutdown) and can be re-joined at a later time
// (see api.JoinCluster).
func RemoveNodeUnsafe(bp BaseParams, sid string) error {
	msg := apc.ActMsg{
		Action: apc.ActRmNodeUnsafe,
		Value:  &apc.ActValRmNode{DaemonID: sid, SkipRebalance: true},
	}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// to be used by external watchdogs (Kubernetes, etc.)
// (compare with api.Health below)
func GetProxyReadiness(bp BaseParams) error {
	bp.Method = http.MethodGet
	q := url.Values{apc.QparamHealthReadiness: []string{"true"}}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathHealth.S
		reqParams.Query = q
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
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
	hdr, err := reqParams.doReqHdr()
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

// GetClusterMap retrieves AIStore cluster map.
func GetClusterMap(bp BaseParams) (smap *cluster.Smap, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatSmap}}
	}
	_, err = reqParams.DoReqAny(&smap)
	FreeRp(reqParams)
	return
}

// GetNodeClusterMap retrieves AIStore cluster map from a specific node.
func GetNodeClusterMap(bp BaseParams, sid string) (smap *cluster.Smap, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatSmap}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{sid}}
	}
	_, err = reqParams.DoReqAny(&smap)
	FreeRp(reqParams)
	return
}

// GetClusterSysInfo retrieves AIStore system info.
func GetClusterSysInfo(bp BaseParams) (info apc.ClusterSysInfo, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatSysInfo}}
	}
	_, err = reqParams.DoReqAny(&info)
	FreeRp(reqParams)
	return
}

// How to compute throughputs:
//
// - AIS supports several enumerated metric "kinds", including `KindThroughput`
// (for complete enumeration, see stats/api.go)
// - By convention, metrics that have `KindThroughput` kind are named with ".bps"
// ("bytes per second") suffix.
// - ".bps" metrics reported by api.GetClusterStats and api.GetDaemonStats are,
// in fact, cumulative byte numbers.
// - It is the client's responsibility to compute the actual throughputs
// as only the client knows _when_ exactly the same ".bps" metric was queried
// the previous time.
//
// - See also: `api.GetDaemonStats`, stats/api.go
func GetClusterStats(bp BaseParams) (res stats.Cluster, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatNodeStats}}
	}

	var rawStats stats.ClusterRaw
	_, err = reqParams.DoReqAny(&rawStats)
	FreeRp(reqParams)
	if err != nil {
		return
	}

	res.Proxy = rawStats.Proxy
	res.Target = make(map[string]*stats.Node)
	for tid := range rawStats.Target {
		var ts stats.Node
		if err := jsoniter.Unmarshal(rawStats.Target[tid], &ts); err == nil {
			res.Target[tid] = &ts
		}
	}
	return
}

func GetTargetDiskStats(bp BaseParams, tid string) (res ios.AllDiskStats, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatDiskStats}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{tid}}
	}
	_, err = reqParams.DoReqAny(&res)
	FreeRp(reqParams)
	return
}

func GetRemoteAIS(bp BaseParams) (remais cluster.Remotes, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatRemoteAIS}}
	}
	_, err = reqParams.DoReqAny(&remais)
	FreeRp(reqParams)
	return
}

// JoinCluster add a node to a cluster.
func JoinCluster(bp BaseParams, nodeInfo *cluster.Snode) (rebID, sid string, err error) {
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluUserReg.S
		reqParams.Body = cos.MustMarshal(nodeInfo)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}

	var info apc.JoinNodeResult
	_, err = reqParams.DoReqAny(&info)
	FreeRp(reqParams)
	return info.RebalanceID, info.DaemonID, err
}

// SetPrimaryProxy given a daemonID sets that corresponding proxy as the
// primary proxy of the cluster.
func SetPrimaryProxy(bp BaseParams, newPrimaryID string, force bool) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	reqParams.BaseParams = bp
	reqParams.Path = apc.URLPathCluProxy.Join(newPrimaryID)
	if force {
		reqParams.Query = url.Values{apc.QparamForce: []string{"true"}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// SetClusterConfig given key-value pairs of cluster configuration parameters,
// sets the cluster-wide configuration accordingly. Setting cluster-wide
// configuration requires sending the request to a proxy.
func SetClusterConfig(bp BaseParams, nvs cos.StrKVs, transient bool) error {
	q := make(url.Values, len(nvs))
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
	return err
}

// SetClusterConfigUsingMsg sets the cluster-wide configuration
// using the `cmn.ConfigToUpdate` parameter provided.
func SetClusterConfigUsingMsg(bp BaseParams, configToUpdate *cmn.ConfigToUpdate, transient bool) error {
	var (
		q   url.Values
		msg = apc.ActMsg{Action: apc.ActSetConfig, Value: configToUpdate}
	)
	if transient {
		q.Set(apc.ActTransient, "true")
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

// ResetClusterConfig resets the configuration of all nodes to the cluster configuration
func ResetClusterConfig(bp BaseParams) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActResetConfig})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// GetClusterConfig returns cluster-wide configuration
// (compare with `api.GetDaemonConfig`)
func GetClusterConfig(bp BaseParams) (*cmn.ClusterConfig, error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatClusterConfig}}
	}

	cluConfig := &cmn.ClusterConfig{}
	_, err := reqParams.DoReqAny(cluConfig)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return cluConfig, nil
}

// GetBMD returns bucket metadata
func GetBMD(bp BaseParams) (*cluster.BMD, error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatBMD}}
	}

	bmd := &cluster.BMD{}
	_, err := reqParams.DoReqAny(bmd)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return bmd, nil
}

func AttachRemoteAIS(bp BaseParams, alias, u string) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluAttach.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatRemoteAIS}}
		reqParams.Header = http.Header{
			apc.HdrRemAisAlias: []string{alias},
			apc.HdrRemAisURL:   []string{u},
		}
	}
	return reqParams.DoRequest()
}

func DetachRemoteAIS(bp BaseParams, alias string) error {
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluDetach.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.WhatRemoteAIS}}
		reqParams.Header = http.Header{apc.HdrRemAisAlias: []string{alias}}
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
	msg := apc.ActMsg{Action: apc.ActShutdown}
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
	msg := apc.ActMsg{Action: apc.ActDecommission}
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

// Remove a node from Smap immediately.
// Immediately removes a node from Smap (advanced usage - potential data loss)
func RemoveNodeFromSmap(bp BaseParams, sid string) error {
	bp.Method = http.MethodDelete
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluDaemon.Join(sid)
	}
	err := reqParams.DoRequest()
	FreeRp(reqParams)
	return err
}

// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

func Health(bp BaseParams, readyToRebalance ...bool) error {
	var q url.Values
	bp.Method = http.MethodGet
	if len(readyToRebalance) > 0 && readyToRebalance[0] {
		q = url.Values{apc.QparamPrimaryReadyReb: []string{"true"}}
	}
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathHealth.S
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// GetClusterMap retrieves AIStore cluster map.
func GetClusterMap(bp BaseParams) (smap *cluster.Smap, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatSmap}}
	}
	err = reqParams.DoHTTPReqResp(&smap)
	FreeRp(reqParams)
	return
}

// GetNodeClusterMap retrieves AIStore cluster map from specific node.
func GetNodeClusterMap(bp BaseParams, nodeID string) (smap *cluster.Smap, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatSmap}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{nodeID}}
	}
	err = reqParams.DoHTTPReqResp(&smap)
	FreeRp(reqParams)
	return
}

// GetClusterSysInfo retrieves AIStore system info.
func GetClusterSysInfo(bp BaseParams) (sysInfo apc.ClusterSysInfo, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatSysInfo}}
	}
	err = reqParams.DoHTTPReqResp(&sysInfo)
	FreeRp(reqParams)
	return
}

// GetClusterStats retrieves AIStore cluster stats (all targets and current proxy).
func GetClusterStats(bp BaseParams) (clusterStats stats.ClusterStats, err error) {
	var rawStats stats.ClusterStatsRaw
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatStats}}
	}
	err = reqParams.DoHTTPReqResp(&rawStats)
	FreeRp(reqParams)
	if err != nil {
		return
	}

	clusterStats.Proxy = rawStats.Proxy
	clusterStats.Target = make(map[string]*stats.DaemonStats)
	for tid := range rawStats.Target {
		var ts stats.DaemonStats
		if err := jsoniter.Unmarshal(rawStats.Target[tid], &ts); err == nil {
			clusterStats.Target[tid] = &ts
		}
	}
	return
}

func GetTargetDiskStats(bp BaseParams, targetID string) (diskStats ios.AllDiskStats, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathReverseDae.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatDiskStats}}
		reqParams.Header = http.Header{apc.HdrNodeID: []string{targetID}}
	}
	err = reqParams.DoHTTPReqResp(&diskStats)
	FreeRp(reqParams)
	return
}

func GetRemoteAIS(bp BaseParams) (aisInfo cmn.BackendInfoAIS, err error) {
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatRemoteAIS}}
	}
	err = reqParams.DoHTTPReqResp(&aisInfo)
	FreeRp(reqParams)
	return
}

// JoinCluster add a node to a cluster.
func JoinCluster(bp BaseParams, nodeInfo *cluster.Snode) (rebID, daemonID string, err error) {
	var info apc.JoinNodeResult
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluUserReg.S
		reqParams.Body = cos.MustMarshal(nodeInfo)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&info)
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
	err := reqParams.DoHTTPRequest()
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
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// SetClusterConfigUsingMsg sets the cluster-wide configuration
// using the `cmn.ConfigToUpdate` parameter provided.
func SetClusterConfigUsingMsg(bp BaseParams, configToUpdate *cmn.ConfigToUpdate, transient bool) error {
	var (
		q   url.Values
		msg = apc.ActionMsg{Action: apc.ActSetConfig, Value: configToUpdate}
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
	err := reqParams.DoHTTPRequest()
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
		reqParams.Body = cos.MustMarshal(apc.ActionMsg{Action: apc.ActResetConfig})
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// GetClusterConfig returns cluster-wide configuration
func GetClusterConfig(bp BaseParams) (*cmn.ClusterConfig, error) {
	cluConfig := &cmn.ClusterConfig{}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatClusterConfig}}
	}
	err := reqParams.DoHTTPReqResp(cluConfig)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return cluConfig, nil
}

// GetBMD returns bucket metadata
func GetBMD(bp BaseParams) (*cluster.BMD, error) {
	bmd := &cluster.BMD{}
	bp.Method = http.MethodGet
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Query = url.Values{apc.QparamWhat: []string{apc.GetWhatBMD}}
	}
	err := reqParams.DoHTTPReqResp(bmd)
	FreeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return bmd, nil
}

func AttachRemoteAIS(bp BaseParams, alias, u string) error {
	q := make(url.Values, 4)
	q.Set(apc.QparamWhat, apc.GetWhatRemoteAIS)
	q.Set(alias, u)
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluAttach.S
		reqParams.Query = q
	}
	return reqParams.DoHTTPRequest()
}

func DetachRemoteAIS(bp BaseParams, alias string) error {
	q := make(url.Values, 2)
	q.Set(apc.QparamWhat, apc.GetWhatRemoteAIS)
	q.Set(alias, "")
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathCluDetach.S
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

//
// Maintenance API
//

func StartMaintenance(bp BaseParams, actValue *apc.ActValRmNode) (id string, err error) {
	msg := apc.ActionMsg{
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
	err = reqParams.DoHTTPReqResp(&id)
	FreeRp(reqParams)
	return id, err
}

func DecommissionNode(bp BaseParams, actValue *apc.ActValRmNode) (id string, err error) {
	msg := apc.ActionMsg{
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
	err = reqParams.DoHTTPReqResp(&id)
	FreeRp(reqParams)
	return id, err
}

func StopMaintenance(bp BaseParams, actValue *apc.ActValRmNode) (id string, err error) {
	msg := apc.ActionMsg{
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
	err = reqParams.DoHTTPReqResp(&id)
	FreeRp(reqParams)
	return id, err
}

// ShutdownCluster shuts down the whole cluster
func ShutdownCluster(bp BaseParams) error {
	msg := apc.ActionMsg{Action: apc.ActShutdown}
	bp.Method = http.MethodPut
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

// DecommissionCluster permanently decommissions entire cluster
func DecommissionCluster(bp BaseParams, rmUserData bool) error {
	msg := apc.ActionMsg{Action: apc.ActDecommission}
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
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	if cos.IsEOF(err) {
		err = nil
	}
	return err
}

// ShutdownNode shuts down a specific node
func ShutdownNode(bp BaseParams, actValue *apc.ActValRmNode) (id string, err error) {
	msg := apc.ActionMsg{
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
	err = reqParams.DoHTTPReqResp(&id)
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
	err := reqParams.DoHTTPRequest()
	FreeRp(reqParams)
	return err
}

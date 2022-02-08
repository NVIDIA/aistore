// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// to be used by external watchdogs (Kubernetes, etc.)
// (compare with api.Health below)
func GetProxyReadiness(params BaseParams) error {
	params.Method = http.MethodGet
	q := url.Values{cmn.URLParamHealthReadiness: []string{"true"}}
	reqParams := allocRp()
	{
		reqParams.BaseParams = params
		reqParams.Path = cmn.URLPathHealth.S
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

func Health(baseParams BaseParams, readyToRebalance ...bool) error {
	var q url.Values
	baseParams.Method = http.MethodGet
	if len(readyToRebalance) > 0 && readyToRebalance[0] {
		q = url.Values{cmn.URLParamPrimaryReadyReb: []string{"true"}}
	}
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathHealth.S
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// GetClusterMap retrieves AIStore cluster map.
func GetClusterMap(baseParams BaseParams) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathDae.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}}
	}
	err = reqParams.DoHTTPReqResp(&smap)
	freeRp(reqParams)
	return
}

// GetNodeClusterMap retrieves AIStore cluster map from specific node.
func GetNodeClusterMap(baseParams BaseParams, nodeID string) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathReverseDaemon.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}}
		reqParams.Header = http.Header{cmn.HdrNodeID: []string{nodeID}}
	}
	err = reqParams.DoHTTPReqResp(&smap)
	freeRp(reqParams)
	return
}

// GetClusterSysInfo retrieves AIStore system info.
func GetClusterSysInfo(baseParams BaseParams) (sysInfo cmn.ClusterSysInfo, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}}
	}
	err = reqParams.DoHTTPReqResp(&sysInfo)
	freeRp(reqParams)
	return
}

// GetClusterStats retrieves AIStore cluster stats (all targets and current proxy).
func GetClusterStats(baseParams BaseParams) (clusterStats stats.ClusterStats, err error) {
	var rawStats stats.ClusterStatsRaw
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatStats}}
	}
	err = reqParams.DoHTTPReqResp(&rawStats)
	freeRp(reqParams)
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

func GetTargetDiskStats(baseParams BaseParams, targetID string) (diskStats ios.AllDiskStats, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathReverseDaemon.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDiskStats}}
		reqParams.Header = http.Header{cmn.HdrNodeID: []string{targetID}}
	}
	err = reqParams.DoHTTPReqResp(&diskStats)
	freeRp(reqParams)
	return
}

func GetRemoteAIS(baseParams BaseParams) (aisInfo cmn.BackendInfoAIS, err error) {
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatRemoteAIS}}
	}
	err = reqParams.DoHTTPReqResp(&aisInfo)
	freeRp(reqParams)
	return
}

// JoinCluster add a node to a cluster.
func JoinCluster(baseParams BaseParams, nodeInfo *cluster.Snode) (rebID, daemonID string, err error) {
	var info cmn.JoinNodeResult
	baseParams.Method = http.MethodPost
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathCluUserReg.S
		reqParams.Body = cos.MustMarshal(nodeInfo)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&info)
	freeRp(reqParams)
	return info.RebalanceID, info.DaemonID, err
}

// SetPrimaryProxy given a daemonID sets that corresponding proxy as the
// primary proxy of the cluster.
func SetPrimaryProxy(baseParams BaseParams, newPrimaryID string, force bool) error {
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathCluProxy.Join(newPrimaryID)
		reqParams.Query = url.Values{cmn.URLParamForce: []string{strconv.FormatBool(force)}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// SetClusterConfig given key-value pairs of cluster configuration parameters,
// sets the cluster-wide configuration accordingly. Setting cluster-wide
// configuration requires sending the request to a proxy.
func SetClusterConfig(baseParams BaseParams, nvs cos.SimpleKVs, transient ...bool) error {
	q := url.Values{}
	for key, val := range nvs {
		q.Add(key, val)
	}
	if len(transient) > 0 {
		q.Add(cmn.ActTransient, strconv.FormatBool(transient[0]))
	}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathCluSetConf.S
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// SetClusterConfigUsingMsg sets the cluster-wide configuration
// using the `cmn.ConfigToUpdate` parameter provided.
func SetClusterConfigUsingMsg(baseParams BaseParams, configToUpdate *cmn.ConfigToUpdate, transient ...bool) error {
	q := url.Values{}
	msg := cmn.ActionMsg{
		Action: cmn.ActSetConfig,
		Value:  configToUpdate,
	}
	if len(transient) > 0 {
		q.Add(cmn.ActTransient, strconv.FormatBool(transient[0]))
	}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// ResetClusterConfig resets the configuration of all nodes to the cluster configuration
func ResetClusterConfig(baseParams BaseParams) error {
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActResetConfig})
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// GetClusterConfig returns cluster-wide configuration
func GetClusterConfig(baseParams BaseParams) (*cmn.ClusterConfig, error) {
	cluConfig := &cmn.ClusterConfig{}
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatClusterConfig}}
	}
	err := reqParams.DoHTTPReqResp(cluConfig)
	freeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return cluConfig, nil
}

// GetBMD returns bucket metadata
func GetBMD(baseParams BaseParams) (*cluster.BMD, error) {
	bmd := &cluster.BMD{}
	baseParams.Method = http.MethodGet
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Query = url.Values{cmn.URLParamWhat: []string{cmn.GetWhatBMD}}
	}
	err := reqParams.DoHTTPReqResp(bmd)
	freeRp(reqParams)
	if err != nil {
		return nil, err
	}
	return bmd, nil
}

func AttachRemoteAIS(baseParams BaseParams, alias, u string) error {
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatRemoteAIS)
	q.Set(alias, u)
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathCluAttach.S
		reqParams.Query = q
	}
	return reqParams.DoHTTPRequest()
}

func DetachRemoteAIS(baseParams BaseParams, alias string) error {
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatRemoteAIS)
	q.Set(alias, "")
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathCluDetach.S
		reqParams.Query = q
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// Maintenance API
//
func StartMaintenance(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActStartMaintenance,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&id)
	freeRp(reqParams)
	return id, err
}

func DecommissionNode(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActDecommissionNode,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&id)
	freeRp(reqParams)
	return id, err
}

func StopMaintenance(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActStopMaintenance,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&id)
	freeRp(reqParams)
	return id, err
}

// ShutdownCluster shuts down the whole cluster
func ShutdownCluster(baseParams BaseParams) error {
	msg := cmn.ActionMsg{Action: cmn.ActShutdown}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

// DecommissionCluster decommissions whole cluster
func DecommissionCluster(baseParams BaseParams) error {
	msg := cmn.ActionMsg{Action: cmn.ActDecommission}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	if cos.IsEOF(err) {
		err = nil
	}
	return err
}

// ShutdownNode shuts down a specific node
func ShutdownNode(baseParams BaseParams, actValue *cmn.ActValRmNode) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: cmn.ActShutdownNode,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathClu.S
		reqParams.Body = cos.MustMarshal(msg)
		reqParams.Header = http.Header{cmn.HdrContentType: []string{cmn.ContentJSON}}
	}
	err = reqParams.DoHTTPReqResp(&id)
	freeRp(reqParams)
	return id, err
}

// Remove a node from Smap immediately.
// Immediately removes a node from Smap (advanced usage - potential data loss)
func RemoveNodeFromSmap(baseParams BaseParams, sid string) error {
	baseParams.Method = http.MethodDelete
	reqParams := allocRp()
	{
		reqParams.BaseParams = baseParams
		reqParams.Path = cmn.URLPathCluDaemon.Join(sid)
	}
	err := reqParams.DoHTTPRequest()
	freeRp(reqParams)
	return err
}

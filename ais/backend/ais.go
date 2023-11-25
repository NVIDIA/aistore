// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
)

// NOTE: some of the methods here are part of the of the *extended* native AIS API outside
// generic `BackendProvider` (see cluster/backend.go)

// TODO:
// - include `appliedCfgVer` in the GetInfo* response (to synchronize p._remais, etc.)
// - periodically refresh remote Smap
// - use m.remote[uuid].smap to load balance and retry disconnects

const ua = "aisnode/backend"

type (
	remAis struct {
		smap *meta.Smap
		m    *AISBackendProvider
		url  string
		uuid string
		bp   api.BaseParams
	}
	AISBackendProvider struct {
		t             cluster.TargetPut
		remote        map[string]*remAis // by UUID
		alias         cos.StrKVs         // alias => UUID
		mu            sync.RWMutex
		appliedCfgVer int64
	}
)

// interface guard
var _ cluster.BackendProvider = (*AISBackendProvider)(nil)

var (
	preg, treg *regexp.Regexp
)

func NewAIS(t cluster.TargetPut) *AISBackendProvider {
	suff := regexp.QuoteMeta(meta.SnameSuffix)
	preg = regexp.MustCompile(regexp.QuoteMeta(meta.PnamePrefix) + `\S*` + suff + ": ")
	treg = regexp.MustCompile(regexp.QuoteMeta(meta.TnamePrefix) + `\S*` + suff + ": ")
	return &AISBackendProvider{
		t:      t,
		remote: make(map[string]*remAis),
		alias:  make(cos.StrKVs),
	}
}

func (r *remAis) String() string {
	var alias string
	for a, uuid := range r.m.alias {
		if uuid == r.smap.UUID {
			alias = a
			break
		}
	}
	return fmt.Sprintf("remote cluster (%s, %q, %q, %s)", r.url, alias, r.smap.UUID, r.smap)
}

func unsetUUID(bck *cmn.Bck) { bck.Ns.UUID = "" }

func extractErrCode(e error, uuid string) (int, error) {
	if e == nil {
		return http.StatusOK, nil
	}
	herr := cmn.Err2HTTPErr(e)
	if herr == nil {
		return http.StatusInternalServerError, e
	}
	if uuid != "" {
		msg := herr.Message
		loc := preg.FindStringIndex(msg)
		if loc == nil {
			loc = treg.FindStringIndex(msg)
		}
		if loc != nil && loc[1] > loc[0]+2 {
			herr.Message = msg[loc[0]:loc[1]-2] + "@" + uuid + ": " + msg[loc[1]:]
		}
	}
	return herr.Status, herr
}

// apply new or updated (attach, detach) cmn.BackendConfAIS configuration
func (m *AISBackendProvider) Apply(v any, action string, cfg *cmn.ClusterConfig) (err error) {
	conf := cmn.BackendConfAIS{}
	if err = cos.MorphMarshal(v, &conf); err != nil {
		err = fmt.Errorf("%s: invalid ais backend config (%+v, %T): %v", m.t, v, v, err)
		debug.AssertNoErr(err)
		return
	}
	nlog.Infof("%s: apply %q %+v Conf v%d", m.t, action, conf, cfg.Version)
	m.mu.Lock()
	err = m._apply(cfg, conf, action)
	if err == nil {
		m.appliedCfgVer = cfg.Version
	}
	m.mu.Unlock()
	return
}

func (m *AISBackendProvider) _apply(cfg *cmn.ClusterConfig, clusterConf cmn.BackendConfAIS, action string) error {
	// detach
	if action == apc.ActDetachRemAis {
		for alias, uuid := range m.alias {
			if _, ok := clusterConf[alias]; !ok {
				if _, ok = clusterConf[uuid]; !ok {
					delete(m.alias, alias)
					delete(m.remote, uuid)
				}
			}
		}
		return nil
	}

	// validate aliases
	for alias := range clusterConf {
		if err := cmn.ValidateRemAlias(alias); err != nil {
			return err
		}
	}

	// init and attach
	for alias, clusterURLs := range clusterConf {
		remAis := &remAis{}
		if offline, err := remAis.init(alias, clusterURLs, cfg); err != nil { // and check connectivity
			if offline {
				continue
			}
			return err
		}
		if err := m.add(remAis, alias); err != nil {
			return err
		}
	}
	return nil
}

// return (m.remote + m.alias) in-memory info wo/ connecting to remote cluster(s)
// (compare with GetInfo() below)
// TODO: caller to pass its cached version to optimize-out allocations
func (m *AISBackendProvider) GetInfoInternal() (res cluster.Remotes) {
	m.mu.RLock()
	res.A = make([]*cluster.RemAis, 0, len(m.remote))
	for uuid, remAis := range m.remote {
		out := &cluster.RemAis{UUID: uuid, URL: remAis.url}
		for a, u := range m.alias {
			if uuid == u {
				out.Alias = a
				break
			}
		}
		res.A = append(res.A, out)
	}
	res.Ver = m.appliedCfgVer
	m.mu.RUnlock()
	return
}

// At the same time a cluster may have registered both types of remote AIS
// clusters(HTTP and HTTPS). So, the method must use both kinds of clients and
// select the correct one at the moment it sends a request.
// See also: GetInfoInternal()
// TODO: ditto
func (m *AISBackendProvider) GetInfo(clusterConf cmn.BackendConfAIS) (res cluster.Remotes) {
	var (
		cfg              = cmn.GCO.Get()
		cliPlain, cliTLS = remaisClients(&cfg.Client)
	)
	m.mu.RLock()
	res.A = make([]*cluster.RemAis, 0, len(m.remote))
	for uuid, remAis := range m.remote {
		var (
			out    = &cluster.RemAis{UUID: uuid, URL: remAis.url}
			client = cliPlain
		)
		if cos.IsHTTPS(remAis.url) {
			client = cliTLS
		}
		for a, u := range m.alias {
			if uuid == u {
				out.Alias = a
				break
			}
		}

		// online?
		if smap, err := api.GetClusterMap(api.BaseParams{Client: client, URL: remAis.url, UA: ua}); err == nil {
			if smap.UUID != uuid {
				nlog.Errorf("%s: UUID has changed %q", remAis, smap.UUID)
				continue
			}
			if smap.Version < remAis.smap.Version {
				nlog.Errorf("%s: detected older Smap %s - proceeding to override anyway", remAis, smap)
			}
			remAis.smap = smap
		}
		out.Smap = remAis.smap
		res.A = append(res.A, out)
	}
	// defunct (cluster config not updated yet locally?)
	for alias, clusterURLs := range clusterConf {
		if _, ok := m.alias[alias]; !ok {
			if _, ok = m.remote[alias]; !ok {
				out := &cluster.RemAis{Alias: alias, UUID: apc.RemAisDefunct}
				out.URL = fmt.Sprintf("%v", clusterURLs)
				res.A = append(res.A, out)
			}
		}
	}
	m.mu.RUnlock()
	return
}

func remaisClients(clientConf *cmn.ClientConf) (client, clientTLS *http.Client) {
	return cmn.NewDefaultClients(clientConf.Timeout.D())
}

// A list of remote AIS URLs can contains both HTTP and HTTPS links at the
// same time. So, the method must use both kind of clients and select the
// correct one at the moment it sends a request. First successful request
// saves the good client for the future usage.
func (r *remAis) init(alias string, confURLs []string, cfg *cmn.ClusterConfig) (offline bool, err error) {
	var (
		url           string
		remSmap, smap *meta.Smap
		cliH, cliTLS  = remaisClients(&cfg.Client)
	)
	for _, u := range confURLs {
		client := cliH
		if cos.IsHTTPS(u) {
			client = cliTLS
		}
		if smap, err = api.GetClusterMap(api.BaseParams{Client: client, URL: u, UA: ua}); err != nil {
			nlog.Warningf("remote cluster failing to reach %q via %s: %v", alias, u, err)
			continue
		}
		if remSmap == nil {
			remSmap, url = smap, u
			continue
		}
		if remSmap.UUID != smap.UUID {
			err = fmt.Errorf("%q(%v) references two different clusters: uuid=%q vs uuid=%q",
				alias, confURLs, remSmap.UUID, smap.UUID)
			return
		}
		if remSmap.Version < smap.Version {
			remSmap, url = smap, u
		}
	}
	if remSmap == nil {
		err = fmt.Errorf("remote cluster failed to reach %q via any/all of the configured URLs %v", alias, confURLs)
		offline = true
		return
	}
	r.smap, r.url = remSmap, url
	if cos.IsHTTPS(url) {
		r.bp = api.BaseParams{Client: cliTLS, URL: url, UA: ua}
	} else {
		r.bp = api.BaseParams{Client: cliH, URL: url, UA: ua}
	}
	r.uuid = remSmap.UUID
	return
}

// NOTE: supporting remote attachments both by alias and by UUID interchangeably,
// with mappings: 1(uuid) to 1(cluster) and 1(alias) to 1(cluster)
func (m *AISBackendProvider) add(newAis *remAis, newAlias string) (err error) {
	if remAis, ok := m.remote[newAlias]; ok {
		return fmt.Errorf("cannot attach %s: alias %q is already in use as uuid for %s",
			newAlias, newAlias, remAis)
	}
	newAis.m = m
	tag := "added"
	if newAlias == newAis.smap.UUID {
		// not an alias
		goto ad
	}
	// existing
	if remAis, ok := m.remote[newAis.smap.UUID]; ok {
		// can re-alias existing remote cluster
		for alias, uuid := range m.alias {
			if uuid == newAis.smap.UUID {
				delete(m.alias, alias)
			}
		}
		m.alias[newAlias] = newAis.smap.UUID // alias
		if newAis.url != remAis.url {
			nlog.Warningf("%s: different new URL %s - overriding", remAis, newAis)
		}
		if newAis.smap.Version < remAis.smap.Version {
			nlog.Errorf("%s: detected older Smap %s - proceeding to override anyway", remAis, newAis)
		}
		tag = "updated"
		goto ad
	}
	if uuid, ok := m.alias[newAlias]; ok {
		remAis, ok := m.remote[uuid]
		if ok {
			return fmt.Errorf("cannot attach %s: alias %q is already in use for %s",
				newAis, newAlias, remAis)
		}
		delete(m.alias, newAlias)
	}
	m.alias[newAlias] = newAis.smap.UUID
ad:
	m.remote[newAis.smap.UUID] = newAis
	nlog.Infof("%s %s", newAis, tag)
	return
}

func (m *AISBackendProvider) getRemAis(aliasOrUUID string) (remAis *remAis, err error) {
	m.mu.RLock()
	remAis, _, err = m.resolve(aliasOrUUID)
	m.mu.RUnlock()
	return
}

func (m *AISBackendProvider) headRemAis(aliasOrUUID string) (remAis *remAis, alias, uuid string, err error) {
	m.mu.RLock()
	remAis, uuid, err = m.resolve(aliasOrUUID)
	if err != nil {
		m.mu.RUnlock()
		return
	}
	for a, u := range m.alias {
		if u == uuid {
			alias = a
			break
		}
	}
	m.mu.RUnlock()
	return
}

// resolve (alias | UUID) => remAis, UUID
// is called under lock
func (m *AISBackendProvider) resolve(uuid string) (*remAis, string, error) {
	remAis, ok := m.remote[uuid]
	if ok {
		return remAis, uuid, nil
	}
	alias := uuid
	if uuid, ok = m.alias[alias]; !ok {
		return nil, "", cos.NewErrNotFound("%s: remote cluster %q", m.t, alias)
	}
	remAis, ok = m.remote[uuid]
	debug.Assertf(ok, "%q vs %q", alias, uuid)
	return remAis, uuid, nil
}

/////////////////////
// BackendProvider //
/////////////////////

func (*AISBackendProvider) Provider() string  { return apc.AIS }
func (*AISBackendProvider) MaxPageSize() uint { return apc.DefaultPageSizeAIS }

func (*AISBackendProvider) CreateBucket(_ *meta.Bck) (errCode int, err error) {
	debug.Assert(false) // Bucket creation happens only with reverse proxy to AIS cluster.
	return 0, nil
}

// TODO: remote AIS clusters provide native frontend API with additional capabilities
// that, in particular, include `dontAddRemote` = (true | false).
// Here we have to hardcode the value to keep HeadBucket() consistent across all backends.
// For similar limitations, see also ListBuckets() below.
func (m *AISBackendProvider) HeadBucket(_ ctx, remoteBck *meta.Bck) (bckProps cos.StrKVs, errCode int, err error) {
	var (
		remAis      *remAis
		p           *cmn.Bprops
		alias, uuid string
	)
	if remAis, alias, uuid, err = m.headRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	debug.Assert(uuid == remAis.uuid)
	bck := remoteBck.Clone()
	unsetUUID(&bck)
	if p, err = api.HeadBucket(remAis.bp, bck, false /*dontAddRemote*/); err != nil {
		errCode, err = extractErrCode(err, remAis.uuid)
		return
	}

	bckProps = make(cos.StrKVs, 32)
	err = cmn.IterFields(p, func(uniqueTag string, field cmn.IterField) (e error, b bool) {
		bckProps[uniqueTag] = fmt.Sprintf("%v", field.Value())
		return nil, false
	})
	// an extra
	bckProps[apc.HdrBackendProvider] = apc.AIS
	bckProps[apc.HdrRemAisUUID] = remAis.uuid
	bckProps[apc.HdrRemAisAlias] = alias
	bckProps[apc.HdrRemAisURL] = remAis.url

	return
}

func (m *AISBackendProvider) ListObjects(remoteBck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoResult) (errCode int, err error) {
	var remAis *remAis
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	remoteMsg := msg.Clone()
	remoteMsg.PageSize = calcPageSize(remoteMsg.PageSize, m.MaxPageSize())

	// TODO:
	// Currently, not encoding xaction (aka request) `UUID` from the remote cluster
	// in the `ContinuationToken` (note below).
	remoteMsg.UUID = ""

	// likewise, let remote ais gateway decide
	remoteMsg.SID = ""

	bck := remoteBck.Clone()
	unsetUUID(&bck)

	var lstRes *cmn.LsoResult
	if lstRes, err = api.ListObjectsPage(remAis.bp, bck, remoteMsg); err != nil {
		errCode, err = extractErrCode(err, remAis.uuid)
		return
	}
	*lst = *lstRes

	// Restore the original request UUID (UUID of the remote cluster is already inside `ContinuationToken`).
	lst.UUID = msg.UUID
	return
}

func (m *AISBackendProvider) ListBuckets(qbck cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	if !qbck.Ns.IsAnyRemote() {
		// caller provided uuid (or alias)
		bcks, err = m.blist(qbck.Ns.UUID, qbck)
		errCode, err = extractErrCode(err, qbck.Ns.UUID)
		return
	}

	// all attached
	m.mu.RLock()
	uuids := make([]string, 0, len(m.remote))
	for u := range m.remote {
		uuids = append(uuids, u)
	}
	m.mu.RUnlock()
	if len(uuids) == 0 {
		return
	}
	for _, uuid := range uuids {
		remoteBcks, errV := m.blist(uuid, qbck)
		bcks = append(bcks, remoteBcks...)
		if errV != nil && err == nil {
			err = errV
		}
	}
	if len(uuids) == 1 {
		errCode, err = extractErrCode(err, uuids[0])
	} else {
		errCode, err = extractErrCode(err, "")
	}
	return
}

// NOTE:
// remote AIS clusters provide native frontend with additional capabilities which
// also include apc.Flt* _location_ specifier. Here we simply hardcode the `apc.FltExists`
// to keep `ListBuckets` consistent across (aws, gcp, etc.) backends.

func (m *AISBackendProvider) blist(uuid string, qbck cmn.QueryBcks) (bcks cmn.Bcks, err error) {
	var (
		remAis      *remAis
		remoteQuery = cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.Ns{Name: qbck.Ns.Name}}
	)
	if remAis, err = m.getRemAis(uuid); err != nil {
		return
	}
	bcks, err = api.ListBuckets(remAis.bp, remoteQuery, apc.FltExists)
	if err != nil {
		_, err = extractErrCode(err, uuid)
		return nil, err
	}
	for i, bck := range bcks {
		bck.Ns.UUID = uuid // if user-provided `uuid` is in fact an alias - keep it
		bcks[i] = bck
	}
	return bcks, nil
}

// TODO: remote AIS clusters provide native frontend API with additional capabilities
// in part including apc.Flt* location specifier.
// Here, and elsewhere down below, we hardcode (the default) `apc.FltPresent` to, eesentially,
// keep HeadObj() consistent across backends.
func (m *AISBackendProvider) HeadObj(_ ctx, lom *cluster.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		remAis    *remAis
		op        *cmn.ObjectProps
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	unsetUUID(&remoteBck)
	if op, err = api.HeadObject(remAis.bp, remoteBck, lom.ObjName, apc.FltPresent, true /*silent*/); err != nil {
		errCode, err = extractErrCode(err, remAis.uuid)
		return
	}
	oa = &cmn.ObjAttrs{}
	*oa = op.ObjAttrs
	oa.SetCustomKey(cmn.SourceObjMD, apc.AIS)
	return
}

func (m *AISBackendProvider) GetObj(_ ctx, lom *cluster.LOM, owt cmn.OWT) (errCode int, err error) {
	var (
		remAis    *remAis
		r         io.ReadCloser
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	unsetUUID(&remoteBck)
	if r, err = api.GetObjectReader(remAis.bp, remoteBck, lom.ObjName, nil /*api.GetArgs*/); err != nil {
		return extractErrCode(err, remAis.uuid)
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = r
		params.OWT = owt
		params.Atime = time.Now()
	}
	err = m.t.PutObject(lom, params)
	cluster.FreePutObjParams(params)
	return extractErrCode(err, remAis.uuid)
}

func (m *AISBackendProvider) GetObjReader(_ ctx, lom *cluster.LOM) (res cluster.GetReaderResult) {
	var (
		remAis    *remAis
		op        *cmn.ObjectProps
		remoteBck = lom.Bck().Clone()
	)
	if remAis, res.Err = m.getRemAis(remoteBck.Ns.UUID); res.Err != nil {
		return
	}
	unsetUUID(&remoteBck)
	if op, res.Err = api.HeadObject(remAis.bp, remoteBck, lom.ObjName, apc.FltPresent, true /*silent*/); res.Err != nil {
		res.ErrCode, res.Err = extractErrCode(res.Err, remAis.uuid)
		return
	}
	oa := lom.ObjAttrs()
	*oa = op.ObjAttrs
	res.Size = oa.Size
	oa.SetCustomKey(cmn.SourceObjMD, apc.AIS)
	res.ExpCksum = oa.Cksum
	lom.SetCksum(nil)
	// reader
	res.R, res.Err = api.GetObjectReader(remAis.bp, remoteBck, lom.ObjName, nil /*api.GetArgs*/)
	res.ErrCode, res.Err = extractErrCode(res.Err, remAis.uuid)
	return
}

func (m *AISBackendProvider) PutObj(r io.ReadCloser, lom *cluster.LOM) (errCode int, err error) {
	var (
		oah       api.ObjAttrs
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		cos.Close(r)
		return
	}
	unsetUUID(&remoteBck)
	size := lom.SizeBytes(true) // _special_ as it's still a workfile at this point
	args := api.PutArgs{
		BaseParams: remAis.bp,
		Bck:        remoteBck,
		ObjName:    lom.ObjName,
		Cksum:      lom.Checksum(),
		Reader:     r.(cos.ReadOpenCloser),
		Size:       uint64(size),
	}
	if oah, err = api.PutObject(&args); err != nil {
		errCode, err = extractErrCode(err, remAis.uuid)
		return
	}
	// compare w/ lom.CopyAttrs
	oa := lom.ObjAttrs()
	*oa = oah.Attrs()

	// NOTE: restore back into the lom as PUT response header does not contain "Content-Length" (cos.HdrContentLength)
	oa.Size = size

	oa.SetCustomKey(cmn.SourceObjMD, apc.AIS)
	return
}

func (m *AISBackendProvider) DeleteObj(lom *cluster.LOM) (errCode int, err error) {
	var (
		remAis    *remAis
		remoteBck = lom.Bck().Clone()
	)
	if remAis, err = m.getRemAis(remoteBck.Ns.UUID); err != nil {
		return
	}
	unsetUUID(&remoteBck)
	err = api.DeleteObject(remAis.bp, remoteBck, lom.ObjName)
	return extractErrCode(err, remAis.uuid)
}

// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type Bck cmn.Bck

func NewBck(name, provider string, ns cmn.Ns, optProps ...*cmn.Bprops) *Bck {
	var err error
	provider, err = cmn.NormalizeProvider(provider)
	debug.AssertNoErr(err)
	bck := &Bck{Name: name, Provider: provider, Ns: ns}
	if len(optProps) > 0 {
		bck.Props = optProps[0]
		debug.Assert(bck.Props != nil)
	}
	return bck
}

// clone (*meta.Bck | *cmn.Bck) <=> (cmn.Bck | meta.Bck) respectively
func (b *Bck) Clone() cmn.Bck { return cmn.Bck(*b) }

func CloneBck(bck *cmn.Bck) *Bck {
	b := *bck
	normp, err := cmn.NormalizeProvider(bck.Provider)
	debug.Assert(err == nil, bck.Provider)
	b.Provider = normp
	return (*Bck)(&b)
}

// cast *meta.Bck => *cmn.Bck
func (b *Bck) Bucket() *cmn.Bck { return (*cmn.Bck)(b) }

//
// inline delegations => cmn.Bck
//

func (b *Bck) IsAIS() bool                  { return (*cmn.Bck)(b).IsAIS() }
func (b *Bck) HasProvider() bool            { return (*cmn.Bck)(b).HasProvider() }
func (b *Bck) IsHT() bool                   { return (*cmn.Bck)(b).IsHT() }
func (b *Bck) IsCloud() bool                { return (*cmn.Bck)(b).IsCloud() }
func (b *Bck) IsRemote() bool               { return (*cmn.Bck)(b).IsRemote() }
func (b *Bck) IsRemoteAIS() bool            { return (*cmn.Bck)(b).IsRemoteAIS() }
func (b *Bck) IsQuery() bool                { return (*cmn.Bck)(b).IsQuery() }
func (b *Bck) RemoteBck() *cmn.Bck          { return (*cmn.Bck)(b).RemoteBck() }
func (b *Bck) Validate() error              { return (*cmn.Bck)(b).Validate() }
func (b *Bck) MakeUname(name string) []byte { return (*cmn.Bck)(b).MakeUname(name) }
func (b *Bck) HashUname(s string) uint64    { return (*cmn.Bck)(b).HashUname(s) }
func (b *Bck) Cname(name string) string     { return (*cmn.Bck)(b).Cname(name) }
func (b *Bck) IsEmpty() bool                { return (*cmn.Bck)(b).IsEmpty() }
func (b *Bck) HasVersioningMD() bool        { return (*cmn.Bck)(b).HasVersioningMD() }

func (b *Bck) IsRemoteS3() bool {
	if b.Provider == apc.AWS {
		return true
	}
	backend := b.Backend()
	return backend != nil && backend.Provider == apc.AWS
}

func (b *Bck) IsRemoteOCI() bool {
	if b.Provider == apc.OCI {
		return true
	}
	backend := b.Backend()
	return backend != nil && backend.Provider == apc.OCI
}

func (b *Bck) IsRemoteGCP() bool {
	if b.Provider == apc.GCP {
		return true
	}
	backend := b.Backend()
	return backend != nil && backend.Provider == apc.GCP
}

func (b *Bck) IsRemoteAzure() bool {
	if b.Provider == apc.Azure {
		return true
	}
	backend := b.Backend()
	return backend != nil && backend.Provider == apc.Azure
}

// TODO: mem-pool
func (b *Bck) NewQuery() (q url.Values) {
	q = make(url.Values, 4)
	(*cmn.Bck)(b).SetQuery(q)
	return q
}
func (b *Bck) AddToQuery(q url.Values) url.Values { return (*cmn.Bck)(b).AddToQuery(q) }

func (b *Bck) Backend() *Bck { backend := (*cmn.Bck)(b).Backend(); return (*Bck)(backend) }

func (b *Bck) AddUnameToQuery(q url.Values, uparam string) url.Values {
	bck := (*cmn.Bck)(b)
	return bck.AddUnameToQuery(q, uparam, "")
}

func (b *Bck) String() string {
	if b.Props == nil {
		return b.Bucket().String()
	}

	// [NOTE]
	// add BID
	// for the mask to clear "ais" bit and/or other high bits reserved for LOM flags, see core/lombid
	const (
		aisBID = cos.MSB64
	)
	var sb strings.Builder
	sb.Grow(64)
	b.Bucket().Str(&sb)
	sb.WriteString("(0x")
	sb.WriteString(strconv.FormatUint((b.Props.BID &^ aisBID), 16))
	sb.WriteByte(')')
	return sb.String()
}

func (b *Bck) Equal(other *Bck, sameID, sameBackend bool) bool {
	if b == nil || other == nil {
		return false
	}
	if b == other {
		return true
	}

	left, right := (*cmn.Bck)(b), (*cmn.Bck)(other)
	if left.IsEmpty() || right.IsEmpty() {
		return false
	}
	if !left.Equal(right) {
		return false
	}

	// only enforce BID when both are initialized
	if sameID && b.Props != nil && other.Props != nil && b.Props.BID != other.Props.BID {
		return false
	}
	if !sameBackend {
		return true
	}

	// for backends: (nil, nil) is fine; otherwise by (name, provider, namespace) only
	bl, br := left.Backend(), right.Backend()
	if bl == nil || br == nil {
		return bl == br
	}
	return bl.Equal(br)
}

func (b *Bck) Eq(other *cmn.Bck) bool { return other.Equal(b.Bucket()) }

// when the bucket is not present in the BMD:
// - always returns the corresponding *DoesNotExist error
// - Cloud bucket: fills in the props with defaults from config
// - AIS bucket: sets the props to nil
// - Remote (Cloud or Remote AIS) bucket: caller can type-cast err.(*cmn.ErrRemoteBckNotFound) and proceed
func (b *Bck) Init(bowner Bowner) (err error) {
	if err = b.Validate(); err != nil {
		return
	}
	bmd := bowner.Get()
	if err = b.init(bmd); err != nil {
		return
	}
	if backend := b.Backend(); backend != nil {
		if backend.Props == nil {
			err = backend.init(bmd)
		} else {
			p, exists := bmd.Get(backend)
			if exists {
				exists = p.BID == backend.Props.BID
			}
			if !exists {
				err = cmn.NewErrRemBckNotFound(backend.Bucket())
			} else if backend.Props != p {
				backend.Props = p
			}
		}
	}
	return
}

// part of lom.init (compare with the above)
func (b *Bck) InitFast(bowner Bowner) (err error) {
	bmd := bowner.Get()
	if err = b.init(bmd); err != nil {
		return
	}
	if backend := b.Backend(); backend != nil && backend.Props == nil {
		debug.Assert(apc.IsRemoteProvider(backend.Provider))
		err = backend.init(bmd)
	}
	return
}

func (b *Bck) InitNoBackend(bowner Bowner) error { return b.init(bowner.Get()) }

func (b *Bck) init(bmd *BMD) error {
	switch {
	case b.Provider == "": // ais: is the default
		b.Provider = apc.AIS
		bmd.initBckGlobalNs(b)
	case apc.IsRemoteProvider(b.Provider):
		bmd.initBck(b)
	default:
		b.Props, _ = bmd.Get(b)
	}
	if b.Props != nil {
		return nil // ok
	}
	return cmn.NewErrBckNotFound(b.Bucket())
}

// to support s3 clients:
// find an already existing bucket by name (and nothing else)
// returns an error when name cannot be unambiguously resolved to a single bucket
func InitByNameOnly(bckName string, bowner Bowner) (bck *Bck, ecode int, err error) {
	bmd := bowner.Get()
	all := bmd.getAllByName(bckName)
	switch {
	case all == nil:
		err = cmn.NewErrAisBckNotFound(&cmn.Bck{Name: bckName})
		ecode = http.StatusNotFound
	case len(all) == 1:
		bck = &all[0]
		if bck.Props == nil {
			err = cmn.NewErrAisBckNotFound(bck.Bucket())
			ecode = http.StatusNotFound
		} else if backend := bck.Backend(); backend != nil && backend.Props == nil {
			debug.Assert(apc.IsRemoteProvider(backend.Provider))
			err = backend.init(bmd)
		}
	default:
		err = fmt.Errorf("cannot unambiguously resolve bucket name %q to a single bucket (%v)", bckName, all)
		ecode = http.StatusUnprocessableEntity
	}
	return bck, ecode, err
}

func (b *Bck) CksumConf() (conf *cmn.CksumConf) { return &b.Props.Cksum }

func (b *Bck) VersionConf() cmn.VersionConf {
	if backend := b.Backend(); backend != nil && backend.Props != nil {
		conf := backend.Props.Versioning
		conf.ValidateWarmGet = b.Props.Versioning.ValidateWarmGet
		return conf
	}
	return b.Props.Versioning
}

//
// access perms
//

func (b *Bck) Allow(bit apc.AccessAttrs) error { return b.checkAccess(bit) }

func (b *Bck) checkAccess(bit apc.AccessAttrs) (err error) {
	if b.Props.Access.Has(bit) {
		return
	}
	op := apc.AccessOp(bit)
	err = cmn.NewBucketAccessDenied(b.String(), op, b.Props.Access)
	return
}

func (b *Bck) MaxPageSize() int64 {
	switch b.Provider {
	case apc.AIS:
		return apc.MaxPageSizeAIS
	case apc.AWS:
		// ref:
		// - https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination.html#cli-usage-pagination-serverside
		// - https://docs.openstack.org/swift/latest/api/pagination.html
		if b == nil || b.Props == nil || b.Props.Extra.AWS.MaxPageSize == 0 {
			return apc.MaxPageSizeAWS
		}
		return b.Props.Extra.AWS.MaxPageSize
	case apc.GCP:
		// ref: https://cloud.google.com/storage/docs/json_api/v1/objects/list#parameters
		return apc.MaxPageSizeGCP
	case apc.Azure:
		// ref: https://docs.microsoft.com/en-us/connectors/azureblob/#general-limits
		return apc.MaxPageSizeAzure
	case apc.OCI:
		// ref: https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/20160918/Object/ListObjects
		return apc.MaxPageSizeOCI
	default:
		return 1000
	}
}

//
// rate limits: frontend, backend with respect to `nat` (number active targets)
//

func (b *Bck) NewFrontendRateLim(na int) (*cos.BurstRateLim, error) {
	conf := b.Props.RateLimit.Frontend
	if !conf.Enabled {
		return nil, nil
	}
	if na <= 0 {
		err := cmn.NewErrNoNodes(apc.Proxy, 0)
		debug.Assert(false, err, " ", na)
		return nil, err
	}
	maxTokens := cos.DivRound(conf.MaxTokens, na)
	brl, err := cos.NewBurstRateLim(b.Cname(""), maxTokens, conf.Size, conf.Interval.D())
	if err != nil {
		nlog.ErrorDepth(1, err, "[num active nodes:", na, "]")
		return nil, err
	}
	return brl, nil
}

func (b *Bck) NewBackendRateLim(nat int) *cos.AdaptRateLim {
	conf := b.Props.RateLimit.Backend
	if !conf.Enabled {
		return nil
	}
	if nat <= 0 {
		err := cmn.NewErrNoNodes(apc.Target, 0)
		debug.Assert(false, err, " ", nat)
		return nil
	}
	if b.IsCloud() && conf.NumRetries < 3 {
		nlog.Warningf("%s: rate_limit.backend.num_retries set to %d is, which is dangerously low", b.Cname(""), conf.NumRetries)
	}
	// slightly increase, to compensate for potential intra-cluster imbalance
	maxTokens := cos.DivRound(conf.MaxTokens, nat)
	maxTokens = max(maxTokens+maxTokens>>2, 2) // slightly increase, to compensate for potential intra-cluster imbalance

	arl, err := cos.NewAdaptRateLim(maxTokens, conf.NumRetries, conf.Interval.D())
	if err != nil {
		nlog.Errorln(err)
		debug.AssertNoErr(err)
	}
	return arl
}

// parse and validate
func ParseUname(uname string, withObjname bool) (*Bck, string, error) {
	bck, objName := cmn.ParseUname(uname)
	if err := bck.Validate(); err != nil {
		return nil, "", err
	}

	withoutObjname := !withObjname
	switch {
	case objName != "" && withoutObjname:
		return nil, "", fmt.Errorf("parse-uname %q: not expecting object name (got %q)", uname, bck.Cname(objName))
	case objName == "" && withObjname:
		return nil, "", fmt.Errorf("parse-uname %q: missing object name in %q", uname, bck.Cname(""))
	default:
		return CloneBck(&bck), objName, nil
	}
}

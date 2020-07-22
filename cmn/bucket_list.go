// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// BEWARE: change in this source MAY require re-running go generate ..

//go:generate msgp -tests=false

// BucketEntry corresponds to a single entry in the BucketList and
// contains file and directory metadata as per the SelectMsg
// Flags is a bit field:
// 0-2: objects status, all statuses are mutually exclusive, so it can hold up
//      to 8 different statuses. Now only OK=0, Moved=1, Deleted=2 are supported
// 3:   CheckExists (for cloud bucket it shows if the object in local cache)
type BucketEntry struct {
	Name      string `json:"name"`                  // name of the object - note: does not include the bucket name
	Size      int64  `json:"size,string,omitempty"` // size in bytes
	Checksum  string `json:"checksum,omitempty"`    // checksum
	Atime     string `json:"atime,omitempty"`       // formatted as per SelectMsg.TimeFormat
	Version   string `json:"version,omitempty"`     // version/generation ID. In GCP it is int64, in AWS it is a string
	TargetURL string `json:"target_url,omitempty"`  // URL of target which has the entry
	Copies    int16  `json:"copies,omitempty"`      // ## copies (non-replicated = 1)
	Flags     uint16 `json:"flags,omitempty"`       // object flags, like CheckExists, IsMoved etc
}

func (be *BucketEntry) CheckExists() bool {
	return be.Flags&EntryIsCached != 0
}
func (be *BucketEntry) SetExists() {
	be.Flags |= EntryIsCached
}

func (be *BucketEntry) IsStatusOK() bool {
	return be.Flags&EntryStatusMask == 0
}

func (be *BucketEntry) String() string { return "{" + be.Name + "}" }

func (be *BucketEntry) SetProps(propsSet StringSet) {
	if !propsSet.Contains(GetPropsChecksum) {
		be.Checksum = ""
	}
	if !propsSet.Contains(GetPropsSize) {
		be.Size = 0
	}
	if !propsSet.Contains(GetPropsAtime) {
		be.Atime = ""
	}
	if !propsSet.Contains(GetPropsVersion) {
		be.Version = ""
	}
	if !propsSet.Contains(GetTargetURL) {
		be.TargetURL = ""
	}
	if !propsSet.Contains(GetPropsCopies) {
		be.Copies = 0
	}
}

// BucketList represents the contents of a given bucket - somewhat analogous to the 'ls <bucket-name>'
type BucketList struct {
	Entries    []*BucketEntry `json:"entries"`
	PageMarker string         `json:"pagemarker"`
	UUID       string         `json:"uuid"`
	// TODO: alias for page marker. Eventually it should replace `PageMarker` and
	//  maybe `UUID` as well.
	ContinuationToken string `json:"continuation_token"`
}

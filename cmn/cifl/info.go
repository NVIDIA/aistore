// Package cifl: cluster information and flags
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cifl

import "fmt"

type (
	Info struct {
		Smap struct {
			Primary struct {
				PubURL  string `json:"pub_url"`
				CtrlURL string `json:"control_url"`
				ID      string `json:"id"`
			}
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
		} `json:"smap"`
		BMD struct {
			UUID    string `json:"uuid"`
			Version int64  `json:"version,string"`
		} `json:"bmd"`
		RMD struct {
			Version int64 `json:"version,string"`
		} `json:"rmd"`
		Config struct {
			Version int64 `json:"version,string"`
		} `json:"config"`
		EtlMD struct {
			Version int64 `json:"version,string"`
		} `json:"etlmd"`
		Flags Flags `json:"flags"`
	}
)

func (cii *Info) String() string { return fmt.Sprintf("%+v", *cii) }

func (cii *Info) SmapEqual(other *Info) (ok bool) {
	if cii == nil || other == nil {
		return false
	}
	return cii.Smap.Version == other.Smap.Version && cii.Smap.Primary.ID == other.Smap.Primary.ID
}

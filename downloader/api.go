// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

// Download POST result returned to the user
type DlPostResp struct {
	ID string `json:"id"`
}

type DlStatusResp struct {
	Finished  int `json:"finished"`
	Scheduled int `json:"scheduled"` // tasks being processed or already processed by dispatched
	Total     int `json:"total"`     // total number of tasks, negative if unknown

	Aborted       bool `json:"aborted"`
	AllDispatched bool `json:"all_dispatched"` // if true, dispatcher has already scheduled all tasks for given job
	Pending       int  `json:"num_pending"`    // tasks currently in download queue

	CurrentTasks  []TaskDlInfo  `json:"current_tasks,omitempty"`
	FinishedTasks []TaskDlInfo  `json:"finished_tasks,omitempty"`
	Errs          []TaskErrInfo `json:"download_errors,omitempty"`
}

func (d *DlStatusResp) JobFinished() bool {
	return d.Aborted || (d.AllDispatched && d.Scheduled == d.Finished+len(d.Errs))
}

func (d *DlStatusResp) TotalCnt() int {
	if d.Total > 0 {
		return d.Total
	}
	return d.Scheduled
}

// Summary info of the download job
// FIXME: add more stats
type DlJobInfo struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	NumErrors   int    `json:"num_errors"`
	NumPending  int    `json:"num_pending"`
	Aborted     bool   `json:"aborted"`
}

func (j *DlJobInfo) Aggregate(rhs DlJobInfo) {
	j.NumErrors += rhs.NumErrors
	j.NumPending += rhs.NumPending
}

func (j *DlJobInfo) IsRunning() bool {
	return !j.Aborted && j.NumPending > 0
}

func (j *DlJobInfo) IsFinished() bool {
	return !j.IsRunning()
}

func (j *DlJobInfo) String() string {
	var sb strings.Builder

	sb.WriteString(j.ID)

	if j.Description != "" {
		sb.WriteString(fmt.Sprintf(" (%s)", j.Description))
	}

	sb.WriteString(": ")

	if j.NumPending == 0 {
		sb.WriteString("finished")
	} else {
		sb.WriteString(fmt.Sprintf("%d files still being downloaded", j.NumPending))
	}

	return sb.String()
}

type DlBase struct {
	Description string  `json:"description"`
	Bck         cmn.Bck `json:"bck"`
	Timeout     string  `json:"timeout"`
}

func (b *DlBase) InitWithQuery(query url.Values) {
	if b.Bck.Name == "" {
		b.Bck.Name = query.Get(cmn.URLParamBucket)
	}
	b.Bck.Provider = query.Get(cmn.URLParamProvider)
	b.Bck.Ns = cmn.ParseNsUname(query.Get(cmn.URLParamNamespace))
	b.Timeout = query.Get(cmn.URLParamTimeout)
	b.Description = query.Get(cmn.URLParamDescription)
}

func (b *DlBase) AsQuery() url.Values {
	query := url.Values{}
	if b.Bck.Name != "" {
		query.Add(cmn.URLParamBucket, b.Bck.Name)
	}
	if b.Bck.Provider != "" {
		query.Add(cmn.URLParamProvider, b.Bck.Provider)
	}
	if !b.Bck.Ns.IsGlobal() {
		query.Add(cmn.URLParamNamespace, b.Bck.Ns.Uname())
	}
	if b.Timeout != "" {
		query.Add(cmn.URLParamTimeout, b.Timeout)
	}
	if b.Description != "" {
		query.Add(cmn.URLParamDescription, b.Description)
	}
	return query
}

func (b *DlBase) Validate() error {
	if b.Bck.Name == "" {
		return fmt.Errorf("missing the %q", cmn.URLParamBucket)
	}
	if b.Timeout != "" {
		if _, err := time.ParseDuration(b.Timeout); err != nil {
			return fmt.Errorf("failed to parse timeout field: %v", err)
		}
	}
	return nil
}

type DlSingleObj struct {
	ObjName   string `json:"object_name"`
	Link      string `json:"link"`
	FromCloud bool   `json:"from_cloud"`
}

func (b *DlSingleObj) Validate() error {
	if b.ObjName == "" {
		objName := path.Base(b.Link)
		if objName == "." || objName == "/" {
			return fmt.Errorf("can not extract a valid %q from the provided download link", cmn.URLParamObjName)
		}
		b.ObjName = objName
	}
	if b.Link == "" && !b.FromCloud {
		return fmt.Errorf("missing the %q from the request body", cmn.URLParamLink)
	}
	if b.ObjName == "" {
		return fmt.Errorf("missing the %q from the request body", cmn.URLParamObjName)
	}
	return nil
}

// Internal status/delete request body
type DlAdminBody struct {
	ID    string `json:"id"`
	Regex string `json:"regex"`
}

func (b *DlAdminBody) InitWithQuery(query url.Values) {
	b.ID = query.Get(cmn.URLParamID)
	b.Regex = query.Get(cmn.URLParamRegex)
}

func (b *DlAdminBody) AsQuery() url.Values {
	query := url.Values{}
	if b.ID != "" {
		query.Add(cmn.URLParamID, b.ID)
	}
	if b.Regex != "" {
		query.Add(cmn.URLParamRegex, b.Regex)
	}
	return query
}

func (b *DlAdminBody) Validate(requireID bool) error {
	if b.ID != "" && b.Regex != "" {
		return fmt.Errorf("regex %q defined at the same time as id %q", cmn.URLParamRegex, cmn.URLParamID)
	} else if b.Regex != "" {
		if _, err := regexp.CompilePOSIX(b.Regex); err != nil {
			return err
		}
	} else if b.ID == "" && requireID {
		return fmt.Errorf("ID not specified")
	}
	return nil
}

type TaskInfoByName []TaskDlInfo

func (t TaskInfoByName) Len() int           { return len(t) }
func (t TaskInfoByName) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t TaskInfoByName) Less(i, j int) bool { return t[i].Name < t[j].Name }

// Info about a task that is currently or has been downloaded by one of the joggers
type TaskDlInfo struct {
	Name       string    `json:"name"`
	Downloaded int64     `json:"downloaded,string"`
	Total      int64     `json:"total,string,omitempty"`
	StartTime  time.Time `json:"start_time,omitempty"`
	EndTime    time.Time `json:"end_time,omitempty"`
	Running    bool      `json:"running"`
}

type TaskErrByName []TaskErrInfo

func (t TaskErrByName) Len() int           { return len(t) }
func (t TaskErrByName) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t TaskErrByName) Less(i, j int) bool { return t[i].Name < t[j].Name }

type TaskErrInfo struct {
	Name string `json:"name"`
	Err  string `json:"error"`
}

// Single request
type DlSingleBody struct {
	DlBase
	DlSingleObj
}

func (b *DlSingleBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Link = query.Get(cmn.URLParamLink)
	b.ObjName = query.Get(cmn.URLParamObjName)
}

func (b *DlSingleBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(cmn.URLParamLink, b.Link)
	query.Add(cmn.URLParamObjName, b.ObjName)
	return query
}

func (b *DlSingleBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if err := b.DlSingleObj.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlSingleBody) ExtractPayload() (cmn.SimpleKVs, error) {
	objects := make(cmn.SimpleKVs, 1)
	objects[b.ObjName] = b.Link
	return objects, nil
}

func (b *DlSingleBody) Describe() string {
	return fmt.Sprintf("%s -> %s/%s", b.Link, b.Bck, b.ObjName)
}

func (b *DlSingleBody) String() string {
	return fmt.Sprintf("Link: %q, Bucket: %q, ObjName: %q.", b.Link, b.Bck, b.ObjName)
}

// Range request
type DlRangeBody struct {
	DlBase
	Template string `json:"template"`
	Subdir   string `json:"subdir"`
}

func (b *DlRangeBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Template = query.Get(cmn.URLParamTemplate)
	b.Subdir = query.Get(cmn.URLParamSubdir)
}

func (b *DlRangeBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(cmn.URLParamTemplate, b.Template)
	if b.Subdir != "" {
		query.Add(cmn.URLParamSubdir, b.Subdir)
	}
	return query
}

func (b *DlRangeBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if b.Template == "" {
		return fmt.Errorf("no %q for range found, %q is required", cmn.URLParamTemplate, cmn.URLParamTemplate)
	}
	return nil
}

func (b *DlRangeBody) Describe() string {
	if b.Description != "" {
		return b.Description
	}
	return fmt.Sprintf("%s -> %s", b.Template, b.Bck)
}

func (b *DlRangeBody) String() string {
	return fmt.Sprintf("bucket: %q, template: %q", b.Bck, b.Template)
}

// Multi request
type DlMultiBody struct {
	DlBase
}

func (b *DlMultiBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
}

func (b *DlMultiBody) Validate(body []byte) error {
	if len(body) == 0 {
		return errors.New("body should not be empty")
	}

	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlMultiBody) ExtractPayload(objectsPayload interface{}) (cmn.SimpleKVs, error) {
	objects := make(cmn.SimpleKVs, 10)
	switch ty := objectsPayload.(type) {
	case map[string]interface{}:
		for key, val := range ty {
			switch v := val.(type) {
			case string:
				objects[key] = v
			default:
				return nil, fmt.Errorf("values in map should be strings, found: %T", v)
			}
		}
	case []interface{}:
		// process list of links
		for _, val := range ty {
			switch link := val.(type) {
			case string:
				objName := path.Base(link)
				if objName == "." || objName == "/" {
					// should we continue and let the use worry about this after?
					return nil, fmt.Errorf("can not extract a valid `object name` from the provided download link: %q", link)
				}
				objects[objName] = link
			default:
				return nil, fmt.Errorf("values in array should be strings, found: %T", link)
			}
		}
	default:
		return nil, fmt.Errorf("JSON body should be map (string -> string) or array of strings, found: %T", ty)
	}
	return objects, nil
}

func (b *DlMultiBody) Describe() string {
	return fmt.Sprintf("multi-download -> %s", b.Bck)
}

func (b *DlMultiBody) String() string {
	return fmt.Sprintf("bucket: %q", b.Bck)
}

// Cloud request
type DlCloudBody struct {
	DlBase
	Prefix string `json:"prefix"`
	Suffix string `json:"suffix"`
}

func (b *DlCloudBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Prefix = query.Get(cmn.URLParamPrefix)
	b.Suffix = query.Get(cmn.URLParamSuffix)
}

func (b *DlCloudBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlCloudBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(cmn.URLParamPrefix, b.Prefix)
	query.Add(cmn.URLParamSuffix, b.Suffix)
	return query
}

func (b *DlCloudBody) Describe() string {
	return fmt.Sprintf("cloud prefetch -> %s", b.Bck)
}

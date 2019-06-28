// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
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

func (d *DlStatusResp) Print(verbose bool) string {
	if d.Aborted {
		return "Download was aborted."
	}

	var sb strings.Builder
	errCount := len(d.Errs)
	if d.JobFinished() {
		sb.WriteString(fmt.Sprintf("Done: %d file%s downloaded, %d error%s\n",
			d.Finished, NounEnding(d.Finished), errCount, NounEnding(errCount)))

		if verbose {
			for _, e := range d.Errs {
				sb.WriteString(fmt.Sprintf("%s: %s\n", e.Name, e.Err))
			}
		}

		return sb.String()
	}

	realFinished := d.Finished + errCount
	sb.WriteString(fmt.Sprintf("Download progress: %d/%d (%.2f%%)", realFinished, d.TotalCnt(), 100*float64(realFinished)/float64(d.TotalCnt())))
	if !verbose {
		return sb.String()
	}

	if len(d.CurrentTasks) != 0 {
		sb.WriteString("\n")
		sb.WriteString("Progress of files that are currently being downloaded:\n")
		for _, task := range d.CurrentTasks {
			sb.WriteString(fmt.Sprintf("\t%s: %s\n", task.Name, B2S(task.Downloaded, 2))) // TODO: print total too
		}
	}

	return sb.String()
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
	NumPending  int    `json:"num_pending"`
	Aborted     bool   `json:"aborted"`
}

func (j *DlJobInfo) Aggregate(rhs DlJobInfo) {
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
	Description string `json:"description"`
	Bucket      string `json:"bucket"`
	BckProvider string `json:"bprovider"`
	Timeout     string `json:"timeout"`
}

func (b *DlBase) InitWithQuery(query url.Values) {
	if b.Bucket == "" {
		b.Bucket = query.Get(URLParamBucket)
	}
	b.BckProvider = query.Get(URLParamBckProvider)
	b.Timeout = query.Get(URLParamTimeout)
	b.Description = query.Get(URLParamDescription)
}

func (b *DlBase) AsQuery() url.Values {
	query := url.Values{}
	if b.Bucket != "" {
		query.Add(URLParamBucket, b.Bucket)
	}
	if b.BckProvider != "" {
		query.Add(URLParamBckProvider, b.BckProvider)
	}
	if b.Timeout != "" {
		query.Add(URLParamTimeout, b.Timeout)
	}
	if b.Description != "" {
		query.Add(URLParamDescription, b.Description)
	}
	return query
}

func (b *DlBase) Validate() error {
	if b.Bucket == "" {
		return fmt.Errorf("missing the %q which is required", URLParamBucket)
	}
	if b.Timeout != "" {
		if _, err := time.ParseDuration(b.Timeout); err != nil {
			return fmt.Errorf("failed to parse timeout field: %v", err)
		}
	}
	return nil
}

type DlObj struct {
	Objname   string `json:"objname"`
	Link      string `json:"link"`
	FromCloud bool   `json:"from_cloud"`
}

func (b *DlObj) Validate() error {
	if b.Objname == "" {
		objName := path.Base(b.Link)
		if objName == "." || objName == "/" {
			return fmt.Errorf("can not extract a valid %q from the provided download link", URLParamObjName)
		}
		b.Objname = objName
	}
	if b.Link == "" && !b.FromCloud {
		return fmt.Errorf("missing the %q from the request body", URLParamLink)
	}
	if b.Objname == "" {
		return fmt.Errorf("missing the %q from the request body", URLParamObjName)
	}
	return nil
}

// Internal status/delete request body
type DlAdminBody struct {
	ID    string `json:"id"`
	Regex string `json:"regex"`
}

func (b *DlAdminBody) InitWithQuery(query url.Values) {
	b.ID = query.Get(URLParamID)
	b.Regex = query.Get(URLParamRegex)
}

func (b *DlAdminBody) AsQuery() url.Values {
	query := url.Values{}
	if b.ID != "" {
		query.Add(URLParamID, b.ID)
	}
	if b.Regex != "" {
		query.Add(URLParamRegex, b.Regex)
	}
	return query
}

func (b *DlAdminBody) Validate(requireID bool) error {
	if b.ID != "" && b.Regex != "" {
		return fmt.Errorf("regex %q defined at the same time as id %q", URLParamRegex, URLParamID)
	} else if b.Regex != "" {
		if _, err := regexp.CompilePOSIX(b.Regex); err != nil {
			return err
		}
	} else if b.ID == "" && requireID {
		return fmt.Errorf("ID not specified")
	}
	return nil
}

// Internal download request body
type DlBody struct {
	DlBase
	ID   string  `json:"id"`
	Objs []DlObj `json:"objs"`

	Aborted bool `json:"aborted"`
}

func (b *DlBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if b.ID == "" {
		return fmt.Errorf("missing %q, something went wrong", URLParamID)
	}
	if b.Aborted {
		// used to store abort status in db, should be unset
		return fmt.Errorf("invalid flag 'aborteded'")
	}
	for _, obj := range b.Objs {
		if err := obj.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (b *DlBody) String() string {
	return fmt.Sprintf("%s, id=%q", b.DlBase, b.ID)
}

type TaskInfoByName []TaskDlInfo

func (t TaskInfoByName) Len() int           { return len(t) }
func (t TaskInfoByName) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t TaskInfoByName) Less(i, j int) bool { return t[i].Name < t[j].Name }

// Info about a task that is currently or has been downloaded by one of the joggers
type TaskDlInfo struct {
	Name       string `json:"name"`
	Downloaded int64  `json:"downloaded"`
	Total      int64  `json:"total,omitempty"`

	StartTime time.Time `json:"start_time,omitempty"`
	EndTime   time.Time `json:"end_time,omitempty"`

	Running bool `json:"running"`
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
	DlObj
}

func (b *DlSingleBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Link = query.Get(URLParamLink)
	b.Objname = query.Get(URLParamObjName)
}

func (b *DlSingleBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(URLParamLink, b.Link)
	query.Add(URLParamObjName, b.Objname)
	return query
}

func (b *DlSingleBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if err := b.DlObj.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlSingleBody) ExtractPayload() (SimpleKVs, error) {
	objects := make(SimpleKVs, 1)
	objects[b.Objname] = b.Link
	return objects, nil
}

func (b *DlSingleBody) Describe() string {
	return fmt.Sprintf("%s -> %s/%s", b.Link, b.Bucket, b.Objname)
}

func (b *DlSingleBody) String() string {
	return fmt.Sprintf("Link: %q, Bucket: %q, Objname: %q.", b.Link, b.Bucket, b.Objname)
}

// Range request
type DlRangeBody struct {
	DlBase
	Template string `json:"template"`
	Subdir   string `json:"subdir"`
}

func (b *DlRangeBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Template = query.Get(URLParamTemplate)
	b.Subdir = query.Get(URLParamSubdir)
}

func (b *DlRangeBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(URLParamTemplate, b.Template)
	if b.Subdir != "" {
		query.Add(URLParamSubdir, b.Subdir)
	}
	return query
}

func (b *DlRangeBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if b.Template == "" {
		return fmt.Errorf("no %q for range found, %q is required", URLParamTemplate, URLParamTemplate)
	}
	return nil
}

func (b *DlRangeBody) ExtractPayload() (SimpleKVs, error) {
	pt, err := ParseBashTemplate(b.Template)
	if err != nil {
		return nil, err
	}

	objects := make(SimpleKVs, pt.Count())
	linksIt := pt.Iter()
	for link, hasNext := linksIt(); hasNext; link, hasNext = linksIt() {
		objName := path.Join(b.Subdir, path.Base(link))
		objects[objName] = link
	}
	return objects, nil
}

func (b *DlRangeBody) Describe() string {
	return fmt.Sprintf("%s -> %s", b.Template, b.Bucket)
}

func (b *DlRangeBody) String() string {
	return fmt.Sprintf("bucket: %q, template: %q", b.Bucket, b.Template)
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

func (b *DlMultiBody) ExtractPayload(objectsPayload interface{}) (SimpleKVs, error) {
	objects := make(SimpleKVs, 10)
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
	return fmt.Sprintf("multi-download -> %s", b.Bucket)
}

func (b *DlMultiBody) String() string {
	return fmt.Sprintf("bucket: %q", b.Bucket)
}

// Cloud request
type DlCloudBody struct {
	DlBase
	Prefix string `json:"prefix"`
	Suffix string `json:"suffix"`
}

func (b *DlCloudBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Prefix = query.Get(URLParamPrefix)
	b.Suffix = query.Get(URLParamSuffix)
}

func (b *DlCloudBody) Validate(bckIsLocal bool) error {
	if bckIsLocal {
		return errors.New("bucket download requires cloud bucket")
	}
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlCloudBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(URLParamPrefix, b.Prefix)
	query.Add(URLParamSuffix, b.Suffix)
	return query
}

func (b *DlCloudBody) Describe() string {
	return fmt.Sprintf("cloud prefetch -> %s", b.Bucket)
}

// Removes everything that goes after '?', eg. "?query=key..." so it will not
// be part of final object name.
func NormalizeObjName(objName string) (string, error) {
	u, err := url.Parse(objName)
	if err != nil {
		return "", nil
	}

	if u.Path == "" {
		return objName, nil
	}

	return url.PathUnescape(u.Path)
}

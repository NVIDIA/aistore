// Package dloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package dloader

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

const (
	TypeSingle  Type = "single"
	TypeRange   Type = "range"
	TypeMulti   Type = "multi"
	TypeBackend Type = "backend"

	DownloadProgressInterval = 10 * time.Second
)

type (
	Type string

	// NOTE: Changing this structure requires changes in `MarshalJSON` and `UnmarshalJSON` methods.
	Body struct {
		Type Type `json:"type"`
		json.RawMessage
	}

	// Download POST result returned to the user
	DlPostResp struct {
		ID string `json:"id"`
	}

	// Summary info of the download job
	Job struct {
		ID            string    `json:"id"`
		Description   string    `json:"description"`
		FinishedCnt   int       `json:"finished_cnt"`
		ScheduledCnt  int       `json:"scheduled_cnt"` // tasks being processed or already processed by dispatched
		SkippedCnt    int       `json:"skipped_cnt"`   // number of tasks skipped
		ErrorCnt      int       `json:"error_cnt"`
		Total         int       `json:"total"`          // total number of tasks, negative if unknown
		AllDispatched bool      `json:"all_dispatched"` // if true, dispatcher has already scheduled all tasks for given job
		Aborted       bool      `json:"aborted"`
		StartedTime   time.Time `json:"started_time"`
		FinishedTime  time.Time `json:"finished_time"`
	}

	JobInfos []*Job

	StatusResp struct {
		Job
		CurrentTasks  []TaskDlInfo  `json:"current_tasks,omitempty"`
		FinishedTasks []TaskDlInfo  `json:"finished_tasks,omitempty"`
		Errs          []TaskErrInfo `json:"download_errors,omitempty"`
	}
)

func IsType(a string) bool {
	b := Type(a)
	return b == TypeMulti || b == TypeBackend || b == TypeSingle || b == TypeRange
}

func (j *Job) Aggregate(rhs *Job) {
	j.FinishedCnt += rhs.FinishedCnt
	j.ScheduledCnt += rhs.ScheduledCnt
	j.SkippedCnt += rhs.SkippedCnt
	j.ErrorCnt += rhs.ErrorCnt
	j.Total += rhs.Total
	j.AllDispatched = j.AllDispatched && rhs.AllDispatched
	j.Aborted = j.Aborted || rhs.Aborted
	if j.StartedTime.After(rhs.StartedTime) {
		j.StartedTime = rhs.StartedTime
	}
	// Compute max out of `FinishedTime` only when both are non-zero.
	if !cos.IsTimeZero(j.FinishedTime) {
		if cos.IsTimeZero(rhs.FinishedTime) {
			j.FinishedTime = rhs.FinishedTime
		} else if j.FinishedTime.Before(rhs.FinishedTime) {
			j.FinishedTime = rhs.FinishedTime
		}
	}
}

func (db Body) MarshalJSON() ([]byte, error) {
	b, err := db.RawMessage.MarshalJSON()
	if err != nil {
		return nil, err
	}
	debug.Assert(b[0] == '{' && b[len(b)-1] == '}')
	s := fmt.Sprintf(`{"type": %q, %s}`, db.Type, string(b[1:len(b)-1]))
	return []byte(s), nil
}

func (db *Body) UnmarshalJSON(b []byte) error {
	db.Type = Type(jsoniter.Get(b, "type").ToString())
	if db.Type == "" {
		return errors.New("'type' field is empty")
	}
	return db.RawMessage.UnmarshalJSON(b)
}

func (j Job) JobFinished() bool {
	if cos.IsTimeZero(j.FinishedTime) {
		return false
	}
	cos.Assert(j.Aborted || (j.AllDispatched && j.ScheduledCnt == j.DoneCnt()))
	return true
}

func (j Job) JobRunning() bool {
	return !j.JobFinished()
}

func (j Job) TotalCnt() int {
	if j.Total > 0 {
		return j.Total
	}
	return j.ScheduledCnt
}

// DoneCnt returns number of tasks that have finished (either successfully or with an error).
func (j Job) DoneCnt() int { return j.FinishedCnt + j.ErrorCnt }

// PendingCnt returns number of tasks which are currently being processed.
func (j Job) PendingCnt() int {
	pending := j.TotalCnt() - j.DoneCnt()
	cos.Assert(pending >= 0)
	return pending
}

func (j Job) String() string {
	var sb strings.Builder

	sb.WriteString(j.ID)
	if j.Description != "" {
		sb.WriteString(" (")
		sb.WriteString(j.Description)
		sb.WriteString(")")
	}
	sb.WriteString(": ")

	if j.JobFinished() {
		sb.WriteString("finished")
	} else {
		sb.WriteString(fmt.Sprintf("%d files still being downloaded", j.PendingCnt()))
	}
	return sb.String()
}

func (d JobInfos) Len() int {
	return len(d)
}

func (d JobInfos) Less(i, j int) bool {
	di, dj := d[i], d[j]
	if di.JobRunning() && dj.JobFinished() {
		return true
	} else if di.JobFinished() && dj.JobRunning() {
		return false
	} else if di.JobFinished() && dj.JobFinished() {
		return di.FinishedTime.Before(dj.FinishedTime)
	}
	return di.StartedTime.Before(dj.StartedTime)
}

func (d JobInfos) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d *StatusResp) Aggregate(rhs StatusResp) *StatusResp {
	if d == nil {
		r := StatusResp{}
		err := cos.MorphMarshal(rhs, &r)
		cos.AssertNoErr(err)
		return &r
	}

	d.Job.Aggregate(&rhs.Job)
	d.CurrentTasks = append(d.CurrentTasks, rhs.CurrentTasks...)
	d.FinishedTasks = append(d.FinishedTasks, rhs.FinishedTasks...)
	d.Errs = append(d.Errs, rhs.Errs...)
	return d
}

type Limits struct {
	Connections  int `json:"connections"`
	BytesPerHour int `json:"bytes_per_hour"`
}

type Base struct {
	Description      string  `json:"description"`
	Bck              cmn.Bck `json:"bucket"`
	Timeout          string  `json:"timeout"`
	ProgressInterval string  `json:"progress_interval"`
	Limits           Limits  `json:"limits"`
}

func (b *Base) Validate() error {
	if b.Bck.Name == "" {
		return errors.New("missing 'bucket.name'")
	}
	if b.Timeout != "" {
		if _, err := time.ParseDuration(b.Timeout); err != nil {
			return fmt.Errorf("failed to parse timeout field: %v", err)
		}
	}
	if b.Limits.Connections < 0 {
		return fmt.Errorf("'limit.connections' must be non-negative (got: %d)", b.Limits.Connections)
	}
	if b.Limits.BytesPerHour < 0 {
		return fmt.Errorf("'limit.bytes_per_hour' must be non-negative (got: %d)", b.Limits.BytesPerHour)
	}
	return nil
}

type SingleObj struct {
	ObjName    string `json:"object_name"`
	Link       string `json:"link"`
	FromRemote bool   `json:"from_remote"`
}

func (b *SingleObj) Validate() error {
	if b.ObjName == "" {
		objName := path.Base(b.Link)
		if objName == "." || objName == "/" {
			return errors.New("can not extract a valid 'object_name' from the provided download 'link'")
		}
		b.ObjName = objName
	}
	if b.Link == "" && !b.FromRemote {
		return errors.New("missing 'link' in the request body")
	}
	if b.ObjName == "" {
		return fmt.Errorf("missing 'object_name' in the request body")
	}
	return nil
}

// Internal status/delete request body
type AdminBody struct {
	ID         string `json:"id"`
	Regex      string `json:"regex"`
	OnlyActive bool   `json:"only_active_tasks"` // Skips detailed info about tasks finished/errored
}

func (b *AdminBody) Validate(requireID bool) error {
	if b.ID != "" && b.Regex != "" {
		return fmt.Errorf("regex %q and UUID %q cannot be defined together (choose one or the other)",
			apc.QparamRegex, apc.QparamUUID)
	} else if b.Regex != "" {
		if _, err := regexp.CompilePOSIX(b.Regex); err != nil {
			return err
		}
	} else if b.ID == "" && requireID {
		return errors.New("UUID not specified")
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
type SingleBody struct {
	Base
	SingleObj
}

func (b *SingleBody) Validate() error {
	if err := b.Base.Validate(); err != nil {
		return err
	}
	return b.SingleObj.Validate()
}

func (b *SingleBody) ExtractPayload() (cos.StrKVs, error) {
	objects := make(cos.StrKVs, 1)
	objects[b.ObjName] = b.Link
	return objects, nil
}

func (b *SingleBody) Describe() string {
	if b.Description != "" {
		return b.Description
	}
	return fmt.Sprintf("%s -> %s/%s", b.Link, b.Bck, b.ObjName)
}

func (b *SingleBody) String() string {
	return fmt.Sprintf("Link: %q, Bucket: %q, ObjName: %q.", b.Link, b.Bck, b.ObjName)
}

// Range request
type RangeBody struct {
	Base
	Template string `json:"template"`
	Subdir   string `json:"subdir"`
}

func (b *RangeBody) Validate() error {
	if err := b.Base.Validate(); err != nil {
		return err
	}
	if b.Template == "" {
		return errors.New("missing 'template' in the request body")
	}
	return nil
}

func (b *RangeBody) Describe() string {
	if b.Description != "" {
		return b.Description
	}
	return fmt.Sprintf("%s -> %s", b.Template, b.Bck)
}

func (b *RangeBody) String() string {
	return fmt.Sprintf("bucket: %q, template: %q", b.Bck, b.Template)
}

// Multi request
type MultiBody struct {
	Base
	ObjectsPayload any `json:"objects"`
}

func (b *MultiBody) Validate() error {
	if b.ObjectsPayload == nil {
		return errors.New("body should not be empty")
	}
	return b.Base.Validate()
}

func (b *MultiBody) ExtractPayload() (cos.StrKVs, error) {
	objects := make(cos.StrKVs, 10)
	switch ty := b.ObjectsPayload.(type) {
	case map[string]any:
		for key, val := range ty {
			switch v := val.(type) {
			case string:
				objects[key] = v
			default:
				return nil, fmt.Errorf("values in map should be strings, found: %T", v)
			}
		}
	case []any:
		// process all links
		for _, val := range ty {
			switch link := val.(type) {
			case string:
				objName := path.Base(link)
				if objName == "." || objName == "/" {
					err := fmt.Errorf("failed to extract object name from the download %q", link)
					// TODO: ignore and continue?
					return nil, err
				}
				objects[objName] = link
			default:
				return nil, fmt.Errorf("expected download link to be a string, got: %T", link)
			}
		}
	default:
		return nil, fmt.Errorf("JSON body should be map (string -> string) or array of strings, found: %T", ty)
	}
	return objects, nil
}

func (b *MultiBody) Describe() string {
	if b.Description != "" {
		return b.Description
	}
	return fmt.Sprintf("multi-download -> %s", b.Bck)
}

func (b *MultiBody) String() string {
	return fmt.Sprintf("bucket: %q", b.Bck)
}

// Backend download request
type BackendBody struct {
	Base
	Sync   bool   `json:"sync"`
	Prefix string `json:"prefix"`
	Suffix string `json:"suffix"`
}

func (b *BackendBody) Validate() error { return b.Base.Validate() }

func (b *BackendBody) Describe() string {
	if b.Description != "" {
		return b.Description
	}
	return fmt.Sprintf("remote bucket prefetch -> %s", b.Bck)
}

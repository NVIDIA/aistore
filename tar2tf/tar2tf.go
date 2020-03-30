// Package ta2tf provides core functionality for integrating with TensorFlow tools
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */

package tar2tf

import (
	"bytes"
	b64 "encoding/base64"
	"fmt"
	"image"
	"image/color"
	"math/rand"
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/disintegration/imaging"
)

const (
	TfOpDecode = "Decode"
	TfOpRotate = "Rotate"
	TfOpResize = "Resize"
	TfOpRename = "Rename"

	TfSelect     = "Select"
	TfSelectJSON = "SelectJSON"
	TfSelectList = "SelectList"
	TfSelectDict = "SelectDict"
)

type (
	TarRecord      map[string]*TarRecordEntry
	TarRecordEntry struct {
		Value interface{}
	}

	TarRecordSelection interface {
		Select(_ TarRecord) string
		String() string
	}

	TarRecordConversion interface {
		Do(_ TarRecord) TarRecord
	}

	TarConversionMsg struct {
		MsgType string              `json:"type"`
		Key     string              `json:"ext_name"`
		DstSize []int               `json:"dst_size"`
		Renames map[string][]string `json:"renames"`
		Angle   float64             `json:"angle"`
	}

	TarSelectionMsg struct {
		MsgType string `json:"type"`
		Key     string `json:"ext_name"`
		Path    []string
		List    []*TarSelectionMsg
		Dict    map[string]*TarSelectionMsg
	}

	// Conversions
	DecodeConv struct {
		key string
	}

	RotateConv struct {
		key   string
		angle float64
	}

	ResizeConv struct {
		dstSize []int
		key     string
	}

	RenameConv struct {
		renames map[string][]string
	}

	// Selections
	Select struct {
		key string
	}

	SelectJSON struct {
		key  string
		path []string
	}

	SelectList struct {
		list []TarRecordSelection
	}

	SelectDict struct {
		dict map[string]TarRecordSelection
	}

	SamplesStreamJobMsg struct {
		Conversions []TarConversionMsg `json:"conversions"`
		Selections  []TarSelectionMsg  `json:"selections"`
		Template    string             `json:"template"`
		ShuffleTar  bool               `json:"shuffle_tar"`
	}

	SamplesStreamJob struct {
		Conversions []TarRecordConversion
		Selections  []TarRecordSelection
		Template    cmn.ParsedTemplate
		Writer      http.ResponseWriter
		Request     *http.Request
		Wg          sync.WaitGroup
		ShuffleTar  bool
	}
)

func NewTarRecord(size int) TarRecord {
	return make(map[string]*TarRecordEntry, size)
}

func (j *SamplesStreamJobMsg) ToSamplesStreamJob(req *http.Request, w http.ResponseWriter) (*SamplesStreamJob, error) {
	var (
		r = &SamplesStreamJob{
			Request: req,
			Writer:  w,
		}
		err error
	)

	r.ShuffleTar = j.ShuffleTar
	r.Template, err = cmn.ParseBashTemplate(j.Template)
	if err != nil {
		return r, err
	}

	for _, c := range j.Conversions {
		conversion, err := c.ToTarRecordConversion()
		if err != nil {
			return r, err
		}
		r.Conversions = append(r.Conversions, conversion)
	}
	for _, s := range j.Selections {
		selection, err := s.ToTarRecordSelection()
		if err != nil {
			return r, err
		}
		r.Selections = append(r.Selections, selection)
	}
	return r, nil
}

func (msg *TarConversionMsg) ToTarRecordConversion() (TarRecordConversion, error) {
	switch msg.MsgType {
	case TfOpDecode:
		return &DecodeConv{key: msg.Key}, nil
	case TfOpResize:
		return &ResizeConv{key: msg.Key, dstSize: msg.DstSize}, nil
	case TfOpRename:
		return &RenameConv{renames: msg.Renames}, nil
	case TfOpRotate:
		return &RotateConv{key: msg.Key, angle: msg.Angle}, nil
	default:
		return nil, fmt.Errorf("unknown conversion type %q", msg.MsgType)
	}
}

func (msg *TarSelectionMsg) ToTarRecordSelection() (TarRecordSelection, error) {
	switch msg.MsgType {
	case TfSelect:
		return &Select{key: msg.Key}, nil
	case TfSelectJSON:
		return &SelectJSON{key: msg.Key, path: msg.Path}, nil
	case TfSelectDict:
		m := make(map[string]TarRecordSelection, len(msg.Dict))
		for k := range msg.Dict {
			val, err := msg.Dict[k].ToTarRecordSelection()
			if err != nil {
				return nil, err
			}
			m[k] = val
		}
		return &SelectDict{dict: m}, nil
	case TfSelectList:
		m := make([]TarRecordSelection, 0, len(msg.Dict))
		for _, k := range msg.List {
			val, err := k.ToTarRecordSelection()
			if err != nil {
				return nil, err
			}
			m = append(m, val)
		}
		return &SelectList{list: m}, nil
	default:
		return nil, fmt.Errorf("unknown selection type %q", msg.Key)
	}
}

func (c *DecodeConv) Do(record TarRecord) TarRecord {
	img, _, err := image.Decode(cmn.NewByteHandle(record[c.key].Value.([]byte)))
	cmn.AssertNoErr(err)
	entry := record[c.key]
	entry.Value = img
	return record
}

func (c *DecodeConv) String() string { return fmt.Sprintf("decode;%s", c.key) }

func (c *RotateConv) String() string { return fmt.Sprintf("convert;%s;%f", c.key, c.angle) }

func (c *RotateConv) Do(record TarRecord) TarRecord {
	entry := record[c.key]
	img := entry.Value.(image.Image)
	angle := c.angle
	if angle == 0 {
		angle = rand.Float64() * 100
	}
	entry.Value = imaging.Rotate(img, angle, color.Black)
	return record
}

func (c *ResizeConv) Do(record TarRecord) TarRecord {
	entry := record[c.key]
	entry.Value = imaging.Resize(entry.Value.(image.Image), c.dstSize[0], c.dstSize[1], imaging.Linear)
	return record
}

func (c *ResizeConv) String() string { return fmt.Sprintf("resize;%s;%v", c.key, c.dstSize) }

func (c *RenameConv) Do(record TarRecord) TarRecord {
	for dstName, srcNames := range c.renames {
		for _, srcName := range srcNames {
			if _, ok := record[srcName]; ok {
				record[dstName] = record[srcName]
				delete(record, srcName)
			}
		}
	}
	return record
}

func (c *RenameConv) String() string { return fmt.Sprintf("rename;%v", c.renames) }

func (s *Select) Select(record TarRecord) string {
	entry := record[s.key]
	if img, ok := entry.Value.(image.Image); ok {
		var b bytes.Buffer
		cmn.AssertNoErr(imaging.Encode(&b, img, imaging.JPEG))
		return b64.StdEncoding.EncodeToString(b.Bytes())
	}
	return b64.StdEncoding.EncodeToString(record[s.key].Value.([]byte))
}

func (s *Select) String() string { return fmt.Sprintf("select;%s", s.key) }

func (s *SelectJSON) Select(record TarRecord) string {
	// TODO implement
	return string(cmn.MustMarshal(record))
}

func (s *SelectJSON) String() string { return fmt.Sprintf("selectjson;%s;%v", s.key, s.path) }

func (s *SelectList) Select(record TarRecord) string {
	r := make([]string, 0, len(s.list))

	for _, elem := range s.list {
		r = append(r, elem.Select(record))
	}

	return b64.StdEncoding.EncodeToString(cmn.MustMarshal(r))
}

func (s *SelectList) String() string { return fmt.Sprintf("selectlist;%v", s.list) }

func (s *SelectDict) Select(record TarRecord) string {
	r := make(map[string]string, len(s.dict))

	for key, val := range s.dict {
		r[key] = val.Select(record)
	}

	return b64.StdEncoding.EncodeToString(cmn.MustMarshal(r))
}

func (s *SelectDict) String() string { return fmt.Sprintf("selectdict;%v", s.dict) }

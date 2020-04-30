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
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
	"github.com/NVIDIA/go-tfdata/tfdata/transform/selection"
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
	TarRecordSelection interface {
		SelectSample(_ *core.Sample) []string // select key from samples
		SelectValue(_ *core.Sample) string    // encode value into string
		String() string
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
		Conversions []transform.SampleTransformation
		Selections  []TarRecordSelection
		Template    cmn.ParsedTemplate
		Writer      http.ResponseWriter
		Request     *http.Request
		Wg          sync.WaitGroup
		ShuffleTar  bool
	}
)

var (
	_ selection.Sample = &Select{}
	_ selection.Sample = &SelectJSON{}
	_ selection.Sample = &SelectDict{}
	_ selection.Sample = &SelectList{}

	_ transform.SampleTransformation = &RenameConv{}
	_ transform.SampleTransformation = &DecodeConv{}
	_ transform.SampleTransformation = &ResizeConv{}
	_ transform.SampleTransformation = &RotateConv{}
)

func sampleKeys(sample *core.Sample) []string {
	res := make([]string, len(sample.Entries))
	for k := range sample.Entries {
		res = append(res, k)
	}
	return res
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

func (msg *TarConversionMsg) ToTarRecordConversion() (transform.SampleTransformation, error) {
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

func (c *DecodeConv) TransformSample(sample *core.Sample) *core.Sample {
	img, _, err := image.Decode(cmn.NewByteHandle(sample.Entries[c.key].([]byte)))
	cmn.AssertNoErr(err)
	sample.Entries[c.key] = img
	return sample
}

func (c *DecodeConv) String() string { return fmt.Sprintf("decode;%s", c.key) }

func (c *RotateConv) String() string { return fmt.Sprintf("convert;%s;%f", c.key, c.angle) }

func (c *RotateConv) TransformSample(sample *core.Sample) *core.Sample {
	img := sample.Entries[c.key].(image.Image)
	angle := c.angle
	if angle == 0 {
		angle = rand.Float64() * 100
	}
	sample.Entries[c.key] = imaging.Rotate(img, angle, color.Black)
	return sample
}

func (c *ResizeConv) TransformSample(sample *core.Sample) *core.Sample {
	sample.Entries[c.key] = imaging.Resize(sample.Entries[c.key].(image.Image), c.dstSize[0], c.dstSize[1], imaging.Linear)
	return sample
}

func (c *ResizeConv) String() string { return fmt.Sprintf("resize;%s;%v", c.key, c.dstSize) }

func (c *RenameConv) TransformSample(sample *core.Sample) *core.Sample {
	for dstName, srcNames := range c.renames {
		for _, srcName := range srcNames {
			if _, ok := sample.Entries[srcName]; ok {
				sample.Entries[dstName] = sample.Entries[srcName]
				delete(sample.Entries, srcName)
			}
		}
	}
	return sample
}

func (c *RenameConv) String() string { return fmt.Sprintf("rename;%v", c.renames) }

func (s *Select) SelectValue(sample *core.Sample) string {
	if img, ok := sample.Entries[s.key].(image.Image); ok {
		var b bytes.Buffer
		cmn.AssertNoErr(imaging.Encode(&b, img, imaging.JPEG))
		return b64.StdEncoding.EncodeToString(b.Bytes())
	}
	return b64.StdEncoding.EncodeToString(sample.Entries[s.key].([]byte))
}

func (s *Select) SelectSample(sample *core.Sample) []string {
	return []string{s.key}
}

func (s *Select) String() string { return fmt.Sprintf("select;%s", s.key) }

func (s *SelectJSON) SelectValue(sample *core.Sample) string {
	// TODO implement
	return string(cmn.MustMarshal(sample))
}

func (s *SelectJSON) SelectSample(sample *core.Sample) []string {
	return sampleKeys(sample)
}

func (s *SelectJSON) String() string { return fmt.Sprintf("selectjson;%s;%v", s.key, s.path) }

func (s *SelectList) SelectValue(sample *core.Sample) string {
	r := make([]string, 0, len(s.list))

	for _, elem := range s.list {
		r = append(r, elem.SelectValue(sample))
	}

	return b64.StdEncoding.EncodeToString(cmn.MustMarshal(r))
}

func (s *SelectList) SelectSample(sample *core.Sample) []string {
	return sampleKeys(sample)
}

func (s *SelectList) String() string { return fmt.Sprintf("selectlist;%v", s.list) }

func (s *SelectDict) SelectValue(sample *core.Sample) string {
	r := make(map[string]string, len(s.dict))

	for key, val := range s.dict {
		r[key] = val.SelectValue(sample)
	}

	return b64.StdEncoding.EncodeToString(cmn.MustMarshal(r))
}

func (s *SelectDict) SelectSample(sample *core.Sample) []string {
	return sampleKeys(sample)
}

func (s *SelectDict) String() string { return fmt.Sprintf("selectdict;%v", s.dict) }

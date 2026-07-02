// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

const (
	// Keep in sync with apc.ActSelectObject. The CLI module can be built against
	// the previous root-module version before the next AIS module bump.
	objectSelectAction    = "select-obj"
	objectSelectFormatCSV = "csv"
)

type objectSelectMsg struct {
	Query        string `json:"query"`
	InputFormat  string `json:"input_format"`
	OutputFormat string `json:"output_format"`
}

var (
	objectSelectQueryFlag = cli.StringFlag{
		Name:  "query,q",
		Usage: "SQL-like projection and filter expression, e.g. \"SELECT col1,col2 WHERE col3 > 10\"",
	}
	objectSelectInputFormatFlag = cli.StringFlag{
		Name:  "input-format",
		Value: objectSelectFormatCSV,
		Usage: "Input object format; currently supported: csv",
	}
	objectSelectOutputFormatFlag = cli.StringFlag{
		Name:  "output-format",
		Value: objectSelectFormatCSV,
		Usage: "Output format; currently supported: csv",
	}
)

func objectSelectHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	msg := &objectSelectMsg{
		Query:        parseStrFlag(c, objectSelectQueryFlag),
		InputFormat:  parseStrFlag(c, objectSelectInputFormatFlag),
		OutputFormat: parseStrFlag(c, objectSelectOutputFormatFlag),
	}
	if msg.Query == "" {
		return missingArgumentsError(c, flprn(objectSelectQueryFlag))
	}
	if err := msg.validate(); err != nil {
		return err
	}

	bck, objName, err := parseBckObjURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	if objName == "" {
		return missingArgumentsError(c, "object name in the form "+objectArgument)
	}

	var warned bool
	encObjName := warnEscapeObjName(c, objName, &warned)
	reader, err := selectObjectReader(bck, encObjName, msg)
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.Copy(c.App.Writer, reader)
	return err
}

func (msg *objectSelectMsg) validate() error {
	msg.InputFormat = strings.ToLower(msg.InputFormat)
	msg.OutputFormat = strings.ToLower(msg.OutputFormat)
	if msg.InputFormat == "" {
		msg.InputFormat = objectSelectFormatCSV
	}
	if msg.OutputFormat == "" {
		msg.OutputFormat = objectSelectFormatCSV
	}
	if msg.InputFormat != objectSelectFormatCSV {
		return fmt.Errorf("unsupported %s=%q: currently supported format is %q",
			flprn(objectSelectInputFormatFlag), msg.InputFormat, objectSelectFormatCSV)
	}
	if msg.OutputFormat != objectSelectFormatCSV {
		return fmt.Errorf("unsupported %s=%q: currently supported format is %q",
			flprn(objectSelectOutputFormatFlag), msg.OutputFormat, objectSelectFormatCSV)
	}
	return nil
}

func selectObjectReader(bck cmn.Bck, objName string, msg *objectSelectMsg) (io.ReadCloser, error) {
	q := make(url.Values, 2)
	bck.SetQuery(q)
	reqArgs := cmn.HreqArgs{
		Method: http.MethodPost,
		Base:   apiBP.URL,
		Path:   apc.URLPathObjects.Join(bck.Name, objName),
		Query:  q,
		Body:   cos.MustMarshal(apc.ActMsg{Action: objectSelectAction, Value: msg}),
		Header: http.Header{cos.HdrContentType: []string{cos.ContentJSON}},
	}
	req, err := reqArgs.ReqDeprecated()
	if err != nil {
		return nil, err
	}
	api.SetAuxHeaders(req, &apiBP)

	resp, err := apiBP.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if err := cmn.CheckResp(resp, http.MethodPost, req.URL.Path); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

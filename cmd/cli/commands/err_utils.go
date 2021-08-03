// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains error handlers and utilities.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

type (
	errUsage struct {
		context       *cli.Context
		message       string
		bottomMessage string
		helpData      interface{}
		helpTemplate  string
	}
	errAdditionalInfo struct {
		baseErr        error
		additionalInfo string
	}
)

//////////////
// errUsage //
//////////////

func (e *errUsage) Error() string {
	msg := helpMessage(e.helpTemplate, e.helpData)
	if e.bottomMessage != "" {
		msg += fmt.Sprintf("\n%s\n", e.bottomMessage)
	}
	if e.context.Command.Name != "" {
		return fmt.Sprintf("Incorrect usage of \"%s %s\": %s.\n\n%s",
			e.context.App.Name, e.context.Command.Name, e.message, msg)
	}
	return fmt.Sprintf("Incorrect usage of \"%s\": %s.\n\n%s", e.context.App.Name, e.message, msg)
}

///////////////////////
// errAdditionalInfo //
///////////////////////

func newAdditionalInfoError(err error, info string) error {
	cos.Assert(err != nil)
	return &errAdditionalInfo{
		baseErr:        err,
		additionalInfo: info,
	}
}

func (e *errAdditionalInfo) Error() string {
	return fmt.Sprintf("%s. %s", e.baseErr.Error(), cos.StrToSentence(e.additionalInfo))
}

/////////////////
// error utils //
/////////////////

func commandNotFoundError(c *cli.Context, cmd string) error {
	return &errUsage{
		context:       c,
		message:       fmt.Sprintf("unknown command %q", cmd),
		helpData:      c.App,
		helpTemplate:  templates.ShortUsageTmpl,
		bottomMessage: didYouMeanMessage(c, cmd),
	}
}

func incorrectUsageHandler(c *cli.Context, err error, _ bool) error {
	if c == nil {
		return err
	}
	return cannotExecuteError(c, err)
}

func cannotExecuteError(c *cli.Context, err error) error {
	return &errUsage{
		context:      c,
		message:      err.Error(),
		helpData:     c.Command,
		helpTemplate: templates.ShortUsageTmpl,
	}
}

func incorrectUsageMsg(c *cli.Context, fmtString string, args ...interface{}) error {
	msg := fmt.Sprintf(fmtString, args...)
	return _errUsage(c, msg)
}

func missingArgumentsError(c *cli.Context, missingArgs ...string) error {
	msg := fmt.Sprintf("missing arguments %q", strings.Join(missingArgs, ", "))
	return _errUsage(c, msg)
}

func objectNameArgumentNotSupported(c *cli.Context, objectName string) error {
	msg := fmt.Sprintf("object name %q argument not supported", objectName)
	return _errUsage(c, msg)
}

func _errUsage(c *cli.Context, msg string) error {
	return &errUsage{
		context:      c,
		message:      msg,
		helpData:     c.Command,
		helpTemplate: cli.CommandHelpTemplate,
	}
}

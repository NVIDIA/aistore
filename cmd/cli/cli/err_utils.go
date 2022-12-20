// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains error handlers and utilities.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

type (
	errUsage struct {
		context       *cli.Context
		message       string
		bottomMessage string
		helpData      any
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
		return fmt.Sprintf("Incorrect '%s %s' usage: %s.\n\n%s",
			e.context.App.Name, e.context.Command.Name, e.message, msg)
	}
	return fmt.Sprintf("Incorrect usage: %s.\n\n%s", e.message, msg)
}

///////////////////////
// errAdditionalInfo //
///////////////////////

func newAdditionalInfoError(err error, info string) error {
	return &errAdditionalInfo{baseErr: err, additionalInfo: info}
}

func (e *errAdditionalInfo) Error() string {
	return fmt.Sprintf("%s.\n%s\n", e.baseErr.Error(), cos.StrToSentence(e.additionalInfo))
}

/////////////////
// error utils //
/////////////////

func commandNotFoundError(c *cli.Context, cmd string) *errUsage {
	msg := "unknown subcommand \"" + cmd + "\""
	if !isAlphaLc(msg) {
		msg = "unknown or misplaced \"" + cmd + "\""
	}
	return &errUsage{
		context:       c,
		message:       msg,
		helpData:      c.App,
		helpTemplate:  tmpls.ShortUsageTmpl,
		bottomMessage: didYouMeanMessage(c, cmd),
	}
}

func isAlphaLc(s string) bool {
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || c == '-' {
			continue
		}
		return false
	}
	return true
}

func incorrectUsageHandler(c *cli.Context, err error, _ bool) error {
	if c == nil {
		return err
	}
	return cannotExecuteError(c, err)
}

func cannotExecuteError(c *cli.Context, err error) *errUsage {
	return &errUsage{
		context:      c,
		message:      err.Error(),
		helpData:     c.Command,
		helpTemplate: tmpls.ShortUsageTmpl,
	}
}

func incorrectUsageMsg(c *cli.Context, fmtString string, args ...any) *errUsage {
	const incorrectUsageFmt = "too many arguments or unrecognized (or misplaced) option '%+v'"

	if fmtString == "" {
		fmtString = incorrectUsageFmt
	}
	msg := fmt.Sprintf(fmtString, args...)
	return _errUsage(c, msg)
}

func missingArgumentsError(c *cli.Context, missingArgs ...string) *errUsage {
	msg := fmt.Sprintf("missing arguments %q", strings.Join(missingArgs, ", "))
	return _errUsage(c, msg)
}

func missingKeyValueError(c *cli.Context) *errUsage {
	return missingArgumentsError(c, "attribute key=value pairs")
}

func objectNameArgumentNotSupported(c *cli.Context, objectName string) *errUsage {
	msg := fmt.Sprintf("object name %q argument not supported", objectName)
	return _errUsage(c, msg)
}

func _errUsage(c *cli.Context, msg string) *errUsage {
	return &errUsage{
		context:      c,
		message:      msg,
		helpData:     c.Command,
		helpTemplate: cli.CommandHelpTemplate,
	}
}

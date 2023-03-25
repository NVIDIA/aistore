// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains error handlers and utilities.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

type (
	errUsage struct {
		helpData      any
		context       *cli.Context
		message       string
		bottomMessage string
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

	// remove "alias for" (simplify)
	reg := regexp.MustCompile(`\s+\(alias for ".+"\)`)
	if loc := reg.FindStringIndex(msg); loc != nil {
		msg = msg[:loc[0]] + msg[loc[1]:]
	}

	// format
	if e.bottomMessage != "" {
		msg = strings.TrimSuffix(msg, "\n")
		msg = strings.TrimSuffix(msg, "\n")
		msg += "\n\n" + e.bottomMessage + "\n"
	}
	if e.context.Command.Name != "" {
		return fmt.Sprintf("Incorrect '%s %s' usage: %s.\n\n%s",
			e.context.App.Name, e.context.Command.Name, e.message, msg)
	}
	return fmt.Sprintf("Incorrect usage: %s.\n%s", e.message, msg)
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
	var (
		// via `similarWords` map
		similar = findCmdByKey(cmd).ToSlice()

		// alternatively, using https://en.wikipedia.org/wiki/Damerau–Levenshtein_distance
		closestCommand, distance = findClosestCommand(cmd, c.App.VisibleCommands())

		// finally, the case of trailing `show` (`ais object ... show` == `ais show object`)
		trailingShow = argLast(c) == commandShow
	)
	return &errUsage{
		context:       c,
		message:       msg,
		helpData:      c.App,
		helpTemplate:  teb.ShortUsageTmpl,
		bottomMessage: didYouMeanMessage(c, cmd, similar, closestCommand, distance, trailingShow),
	}
}

func didYouMeanMessage(c *cli.Context, cmd string, similar []string, closestCommand string, distance int, trailingShow bool) string {
	const prefix = "Did you mean: '"
	sb := &strings.Builder{}

	switch {
	case trailingShow:
		sb.WriteString(prefix)
		sb.WriteString(c.App.Name) // NOTE: the entire command-line (vs cliName)
		sb.WriteString(" " + commandShow)
		sb.WriteString(" " + c.Args()[0])
		sbWriteFlags(c, sb)
		sb.WriteString("'?")
		sbWriteSearch(sb, cmd, true)
	case len(similar) == 1:
		sb.WriteString(prefix)
		msg := fmt.Sprintf("%v", similar)
		sb.WriteString(msg)
		sbWriteTail(c, sb)
		sbWriteFlags(c, sb)
		sb.WriteString("'?")
		sbWriteSearch(sb, cmd, true)
	case distance < cos.Max(incorrectCmdDistance, len(cmd)/2):
		sb.WriteString(prefix)
		sb.WriteString(c.App.Name) // ditto
		sb.WriteString(" " + closestCommand)
		sbWriteTail(c, sb)
		sbWriteFlags(c, sb)
		sb.WriteString("'?")
		sbWriteSearch(sb, cmd, true)
	default:
		sbWriteSearch(sb, cmd, false)
	}

	return sb.String()
}

func sbWriteTail(c *cli.Context, sb *strings.Builder) {
	if c.NArg() > 1 {
		for _, a := range c.Args()[1:] { // skip the wrong one
			sb.WriteString(" " + a)
		}
	}
}

func sbWriteFlags(c *cli.Context, sb *strings.Builder) {
	for _, f := range c.Command.Flags {
		n := flprn(f)
		if !strings.Contains(n, "help") {
			sb.WriteString(" " + n)
		}
	}
}

func sbWriteSearch(sb *strings.Builder, cmd string, found bool) {
	searchCmd := cliName + " " + commandSearch + " " + cmd
	if found {
		sb.WriteString("\n")
		sb.WriteString("If not, try search: '" + searchCmd + "'")
	} else {
		sb.WriteString("Try search: '" + searchCmd + "'")
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

func cannotExecuteError(c *cli.Context, err error, bottomMessage string) *errUsage {
	return &errUsage{
		context:       c,
		message:       err.Error(),
		helpData:      c.Command,
		helpTemplate:  teb.ShortUsageTmpl,
		bottomMessage: bottomMessage,
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

func missingArgSimple(missingArgs ...string) error {
	if len(missingArgs) == 1 {
		return fmt.Errorf("missing %q argument", missingArgs[0])
	}
	return fmt.Errorf("missing %q arguments", missingArgs)
}

func missingArgumentsError(c *cli.Context, missingArgs ...string) *errUsage {
	var msg string
	if len(missingArgs) == 1 && !strings.Contains(missingArgs[0], " ") {
		arg := missingArgs[0]
		if len(arg) > 0 && arg[0] == '[' {
			arg = arg[1 : len(arg)-1]
		}
		msg = fmt.Sprintf("missing %q argument", arg)
	} else {
		msg = fmt.Sprintf("missing arguments %q", strings.Join(missingArgs, ", "))
	}
	return _errUsage(c, msg)
}

func missingKeyValueError(c *cli.Context) *errUsage {
	return missingArgumentsError(c, "attribute key=value pairs")
}

func objectNameArgNotExpected(c *cli.Context, objectName string) *errUsage {
	msg := fmt.Sprintf("unexpected object name argument %q", objectName)
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

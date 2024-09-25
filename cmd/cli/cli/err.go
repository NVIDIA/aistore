// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains error handlers and utilities.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/urfave/cli"
)

const (
	errFmtSameBucket = "cannot %s bucket %q onto itself"
	errFmtExclusive  = "flags %s and %s are mutually exclusive"
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
	errDoesNotExist struct {
		what   string
		name   string
		suffix string
	}
)

//////////////
// errUsage //
//////////////

func (e *errUsage) Error() string {
	msg := helpMessage(e.helpTemplate, e.helpData)

	// remove "alias for" (simplify)
	reg := regexp.MustCompile(aliasForRegex)
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
	return fmt.Sprintf("%s.\n%s\n", e.baseErr.Error(), strToSentence(e.additionalInfo))
}

/////////////////////
// errDoesNotExist //
/////////////////////

func (e *errDoesNotExist) Error() string {
	return fmt.Sprintf("%s %q does not exist%s", e.what, e.name, e.suffix)
}

func isErrDoesNotExist(err error) bool {
	_, ok := err.(*errDoesNotExist)
	return ok
}

//
// misc. utils, helpers
//

func isCertificateVerificationError(err error) bool {
	return strings.HasPrefix(err.Error(), "CertificateVerificationError")
}

func isUnreachableError(err error) (msg string, unreachable bool) {
	switch err := err.(type) {
	case *cmn.ErrHTTP:
		herr := cmn.Err2HTTPErr(err)
		if cliConfVerbose() {
			herr.Message = herr.StringEx()
		}
		msg = herr.Message
		unreachable = cos.IsUnreachable(err, err.Status)
	case *errUsage, *errAdditionalInfo:
		return "", false
	default:
		msg = err.Error()
		regx := regexp.MustCompile("dial.*(timeout|refused)")
		if unreachable = regx.MatchString(msg); unreachable {
			i := strings.Index(msg, "dial")
			debug.Assert(i >= 0)
			msg = msg[i:]
		}
	}
	return
}

func redErr(err error) error {
	msg := strings.TrimRight(err.Error(), "\n")
	if i := strings.Index(msg, "Error:"); i >= 0 && len(msg) > i+10 {
		// e.g. "CertificateVerificationError: ..."
		if cos.IsAlphaNice(msg[:i]) {
			typeCode := msg[:i+6]

			// NOTE (usability vs hardcoded check)
			// quoting Go source, "OpError is the error type usually returned by functions in the net package."
			// the "Op" part in it is likely from "operation" - tells nothing...
			if typeCode == "OpError:" {
				typeCode = "NetworkError:"
			}

			return errors.New(fred(typeCode) + msg[i+6:])
		}
	}
	return errors.New(fred("Error: ") + msg)
}

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
		helpTemplate:  teb.ExtendedUsageTmpl,
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
		sb.WriteByte(' ')
		sb.WriteString(commandShow)
		sb.WriteByte(' ')
		sb.WriteString(c.Args()[0])
		sbWriteFlags(c, sb)
		sb.WriteString("'?")
		sbWriteSearch(sb, cmd, true)
	case strings.Contains(cmd, apc.BckProviderSeparator):
		_, objName, err := cmn.ParseBckObjectURI(cmd, cmn.ParseURIOpts{})
		if err != nil {
			return ""
		}
		sb.WriteString(prefix)
		sb.WriteString(c.App.Name) // ditto
		sbWriteTail(c, sb)
		sb.WriteByte(' ')
		if objName == "" {
			sb.WriteString(commandBucket)
		} else {
			sb.WriteString(commandObject)
		}
		sb.WriteByte(' ')
		sb.WriteString(cmd)
		sbWriteFlags(c, sb)
		sb.WriteString("'?")
	case len(similar) == 1:
		sb.WriteString(prefix)
		msg := fmt.Sprintf("%v", similar)
		sb.WriteString(msg)
		sbWriteTail(c, sb)
		sbWriteFlags(c, sb)
		sb.WriteString("'?")
		sbWriteSearch(sb, cmd, true)
	case distance < max(incorrectCmdDistance, len(cmd)/2):
		sb.WriteString(prefix)
		sb.WriteString(c.App.Name) // ditto
		sb.WriteByte(' ')
		sb.WriteString(closestCommand)
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
			sb.WriteByte(' ')
			sb.WriteString(a)
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

func incorrectUsageMsg(c *cli.Context, fmtMsg string, args ...any) *errUsage {
	const dfltMsg = "too many arguments or unrecognized (misplaced?) option '%+v'"
	if fmtMsg == "" {
		fmtMsg = dfltMsg
	}
	if len(args) == 0 {
		debug.Assert(!strings.Contains(fmtMsg, "%"))
		return _errUsage(c, fmtMsg)
	}
	return _errUsage(c, fmt.Sprintf(fmtMsg, args...))
}

func missingArgumentsError(c *cli.Context, missingArgs ...string) *errUsage {
	var msg string
	if len(missingArgs) == 1 && !strings.Contains(missingArgs[0], " ") {
		arg := missingArgs[0]
		if arg != "" && arg[0] == '[' {
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
		helpTemplate: teb.ShortUsageTmpl,
	}
}

// prints completion (TAB-TAB) error when calling AIS APIs
func completionErr(c *cli.Context, err error) {
	fmt.Fprintln(c.App.ErrWriter)
	fmt.Fprintln(c.App.ErrWriter, formatErr(err))
}

//
// ais errors -- formatting
//

func notV(err error) error {
	if err != nil && !cliConfVerbose() {
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			return errors.New(herr.Message)
		}
	}
	return err
}
func V(err error) error {
	if err != nil && cliConfVerbose() {
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			herr.Message = herr.StringEx()
			return herr
		}
	}
	return err
}

// with hints and tips (compare with `stripErr` below)
func formatErr(err error) error {
	if err == nil {
		return nil
	}
	if _, unreachable := isUnreachableError(err); unreachable {
		errmsg := fmt.Sprintf("AIStore cannot be reached at %s\n", clusterURL)
		errmsg += fmt.Sprintf("Make sure that environment '%s' has the address of any AIS gateway (proxy).\n"+
			"For defaults, see CLI config at %s or run `ais show config cli`.",
			env.AIS.Endpoint, config.Path())
		return redErr(errors.New(errmsg))
	}
	switch err := err.(type) {
	case *cmn.ErrHTTP:
		herr := err
		return redErr(herr)
	case *errUsage:
		return err
	case *errAdditionalInfo:
		err.baseErr = formatErr(err.baseErr)
		return err
	default:
		// unwrap and check assorted system error types
		// (currently, only tls cert validation)
		const tip = "\n(Tip: review CLI configuration ('ais config cli'), in particular tls and 'skip_verify' settings)"
		if uerr := errors.Unwrap(err); uerr != nil {
			if isCertificateVerificationError(uerr) {
				err = errors.New(uerr.Error() + tip)
			}
		}
		return redErr(err)
	}
}

// remove "verb URL" from the error (compare with `formatErr`)
// TODO: add more apc.URLPath* paths
func stripErr(err error) error {
	var (
		s = err.Error()
		l int
	)
	i := strings.Index(s, apc.URLPathObjects.S)
	if i < 0 {
		i = strings.Index(s, apc.URLPathBuckets.S)
	}
	if i < 0 {
		return err
	}

	k := strings.Index(s[i+l:], " ")
	if k < 0 || len(s) < i+l+k+5 {
		return err
	}
	return errors.New(s[i+l+k+1:])
}

func isTimeout(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "timeout") || strings.Contains(s, "deadline")
}

func isStartingUp(err error) bool {
	if herr, ok := err.(*cmn.ErrHTTP); ok {
		return herr.Status == http.StatusServiceUnavailable
	}
	// TODO: look for fmt.Errorf("... %v", api-error) - must use %w instead
	if uerr := errors.Unwrap(err); uerr != nil {
		if herr, ok := uerr.(*cmn.ErrHTTP); ok {
			return herr.Status == http.StatusServiceUnavailable
		}
	}
	return false
}

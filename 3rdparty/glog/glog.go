// Go support for leveled logs, analogous to https://code.google.com/p/google-glog/
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package glog

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type severity int32

const (
	infoLog severity = iota
	warningLog
	errorLog
	numSeverity = 3
)

const (
	freeListBufMaxSize = 1024
	severityChar       = "IWE"
	digits             = "0123456789"
)

type (
	flushSyncWriter interface {
		flush(waitPrev bool)
		flushExit()
		io.Writer
	}
	loggingT struct {
		freeList     *buffer
		file         [numSeverity]flushSyncWriter
		toStderr     bool
		alsoToStderr bool
		mu           sync.Mutex
	}
	buffer struct {
		bytes.Buffer
		tmp  [64]byte // for headers
		next *buffer
	}
)

var (
	MaxSize int64 = 4 * 1024 * 1024

	severityName = []string{
		infoLog:  "INFO",
		errorLog: "ERROR",
	}
	// assorted filenames (and their line numbers) that we don't want to clutter the logs
	redactFnames = map[string]int{
		"target_stats.go": 0,
		"proxy_stats.go":  0,
		"common_stats.go": 0,
	}
	logging loggingT

	logDirs  []string
	logDir   string
	pid      int
	program  string
	aisrole  string
	host     = "unknownhost"
	userName = "unknownuser"

	FileHeaderCB func() string

	onceInitFiles sync.Once
)

func init() {
	pid = os.Getpid()
	program = filepath.Base(os.Args[0])

	h, err := os.Hostname()
	if err == nil {
		host = shortHostname(h)
	}

	current, err := user.Current()
	if err == nil {
		userName = current.Username
	}

	// Sanitize userName since it may contain filepath separators on Windows.
	userName = strings.ReplaceAll(userName, `\`, "_")
}

func InitFlags(flset *flag.FlagSet) {
	flset.BoolVar(&logging.toStderr, "logtostderr", false, "log to standard error instead of files")
	flset.BoolVar(&logging.alsoToStderr, "alsologtostderr", false, "log to standard error as well as files")
}

func initFiles() {
	if logDir != "" {
		logDirs = append(logDirs, logDir)
	}
	logDirs = append(logDirs, filepath.Join(os.TempDir(), "aislogs"))
	if err := logging.fcreateAll(errorLog); err != nil {
		panic(err)
	}
}

func Info(args ...any)                    { logging.print(infoLog, args...) }
func InfoDepth(depth int, args ...any)    { logging.printDepth(infoLog, depth, args...) }
func Infoln(args ...any)                  { logging.println(infoLog, args...) }
func Infof(format string, args ...any)    { logging.printf(infoLog, format, args...) }
func Warning(args ...any)                 { logging.print(warningLog, args...) }
func WarningDepth(depth int, args ...any) { logging.printDepth(warningLog, depth, args...) }
func Warningln(args ...any)               { logging.println(warningLog, args...) }
func Warningf(format string, args ...any) { logging.printf(warningLog, format, args...) }
func Error(args ...any)                   { logging.print(errorLog, args...) }
func ErrorDepth(depth int, args ...any)   { logging.printDepth(errorLog, depth, args...) }
func Errorln(args ...any)                 { logging.println(errorLog, args...) }
func Errorf(format string, args ...any)   { logging.printf(errorLog, format, args...) }

func SetLogDirRole(dir, role string) { logDir, aisrole = dir, role }

func shortProgram() (prog string) {
	prog = program
	if prog == "aisnode" && aisrole != "" {
		prog = "ais" + aisrole
	}
	return
}

func InfoLogName() string { return shortProgram() + ".INFO" }
func WarnLogName() string { return shortProgram() + ".WARNING" }
func ErrLogName() string  { return shortProgram() + ".ERROR" }

func shortHostname(hostname string) string {
	if i := strings.Index(hostname, "."); i >= 0 {
		return hostname[:i]
	}
	if len(hostname) < 16 || strings.IndexByte(hostname, '-') < 0 {
		return hostname
	}
	// shorten even more (e.g. "runner-r9rhlq8--project-4149-concurrent-0")
	parts := strings.Split(hostname, "-")
	if len(parts) < 2 {
		return hostname
	}
	if parts[1] != "" || len(parts) == 2 {
		return parts[0] + "-" + parts[1]
	}
	return parts[0] + "-" + parts[2]
}

func logName(tag string, t time.Time) (name, link string) {
	prog := shortProgram()
	name = fmt.Sprintf("%s.%s.%s.%02d%02d-%02d%02d%02d.%d",
		prog,
		host,
		tag,
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		pid)
	return name, prog + "." + tag
}

func fcreate(tag string, t time.Time) (f *os.File, filename string, err error) {
	if len(logDirs) == 0 {
		return nil, "", errors.New("no log dirs")
	}
	name, link := logName(tag, t)
	var lastErr error
	for _, dir := range logDirs {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			lastErr = nil
			continue
		}

		fname := filepath.Join(dir, name)
		f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0o640)
		if err != nil {
			lastErr = err
			continue
		}
		symlink := filepath.Join(dir, link)
		os.Remove(symlink)        // ignore err
		os.Symlink(name, symlink) // ignore err
		return f, fname, nil
	}
	return nil, "", fmt.Errorf("cannot create log: %v", lastErr)
}

func Flush() {
	l := &logging
	l.file[errorLog].flush(false)
	l.file[infoLog].flush(false)
}

func FlushExit() {
	l := &logging
	l.file[errorLog].flushExit()
	l.file[infoLog].flushExit()
}

//////////////
// loggingT //
//////////////

func (l *loggingT) fcreateAll(sev severity) error {
	now := time.Now()
	for s := sev; s >= infoLog && l.file[s] == nil; s-- {
		if s == warningLog {
			continue
		}
		nlog := newNlog(s)
		if err := nlog.rotate(now, 0); err != nil {
			return err
		}
		l.file[s] = nlog
	}
	return nil
}
func (l *loggingT) getBuffer() *buffer {
	l.mu.Lock()
	b := l.freeList
	if b != nil {
		l.freeList = b.next
	}
	l.mu.Unlock()
	if b == nil {
		b = new(buffer)
	} else {
		b.next = nil
		b.Reset()
	}
	return b
}

func (l *loggingT) putBuffer(b *buffer) {
	if b.Len() > freeListBufMaxSize {
		return
	}
	l.mu.Lock()
	b.next = l.freeList
	l.freeList = b
	l.mu.Unlock()
}

func (l *loggingT) header(s severity, depth int) *buffer {
	var redact bool
	_, file, line, ok := runtime.Caller(3 + depth)
	if !ok {
		file = "???"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		if slash >= 0 {
			file = file[slash+1:]
		}
		_, redact = redactFnames[file]
	}
	return l.formatHeader(s, file, line, redact)
}

func (l *loggingT) formatHeader(s severity, file string, line int, redact bool) *buffer {
	now := time.Now()
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
	}
	if s > errorLog {
		s = infoLog // for safety.
	}
	buf := l.getBuffer()

	// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
	// It's worth about 3X. Fprintf is hard.
	hour, minute, second := now.Clock()
	// L hh:mm:ss.uuuuu file:line]
	buf.tmp[0] = severityChar[s]
	buf.tmp[1] = ' '
	buf.twoDigits(2, hour)
	buf.tmp[4] = ':'
	buf.twoDigits(5, minute)
	buf.tmp[7] = ':'
	buf.twoDigits(8, second)
	buf.tmp[10] = '.'
	buf.nDigits(6, 11, now.Nanosecond()/1000, '0')
	buf.tmp[17] = ' '
	buf.Write(buf.tmp[:18])
	if redact {
		buf.WriteString("")
	} else {
		buf.WriteString(file)
		buf.tmp[0] = ':'
		n := buf.someDigits(1, line)
		buf.tmp[n+1] = ' '
		buf.Write(buf.tmp[:n+2])
	}
	return buf
}

func (buf *buffer) twoDigits(i, d int) {
	buf.tmp[i+1] = digits[d%10]
	d /= 10
	buf.tmp[i] = digits[d%10]
}

func (buf *buffer) nDigits(n, i, d int, pad byte) {
	j := n - 1
	for ; j >= 0 && d > 0; j-- {
		buf.tmp[i+j] = digits[d%10]
		d /= 10
	}
	for ; j >= 0; j-- {
		buf.tmp[i+j] = pad
	}
}

func (buf *buffer) someDigits(i, d int) int {
	j := len(buf.tmp)
	for {
		j--
		buf.tmp[j] = digits[d%10]
		d /= 10
		if d == 0 {
			break
		}
	}
	return copy(buf.tmp[i:], buf.tmp[j:])
}

func (l *loggingT) println(s severity, args ...any) {
	buf := l.header(s, 0)
	fmt.Fprintln(buf, args...)
	l.output(s, buf, false)
}

func (l *loggingT) print(s severity, args ...any) {
	l.printDepth(s, 1, args...)
}

func (l *loggingT) printDepth(s severity, depth int, args ...any) {
	buf := l.header(s, depth)
	fmt.Fprint(buf, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf, false)
}

func (l *loggingT) printf(s severity, format string, args ...any) {
	buf := l.header(s, 0)
	fmt.Fprintf(buf, format, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf, false)
}

// main method
func (l *loggingT) output(s severity, buf *buffer, alsoToStderr bool) {
	onceInitFiles.Do(initFiles)

	data := buf.Bytes()
	if len(data) == 0 {
		return
	}
	switch {
	case !flag.Parsed():
		os.Stderr.WriteString("ERROR: logging before flag.Parse: ")
		os.Stderr.Write(data)
	case l.toStderr:
		os.Stderr.Write(data)
	default:
		if alsoToStderr || l.alsoToStderr || s >= errorLog {
			os.Stderr.Write(data)
		}
		switch s {
		case errorLog, warningLog:
			l.file[errorLog].Write(data)
			l.file[infoLog].Write(data)
		case infoLog:
			l.file[infoLog].Write(data)
		}
	}
	// free buf
	l.putBuffer(buf)
}

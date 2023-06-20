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
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// These constants identify the log levels in order of increasing severity.
// A message written to a high-severity log file is also written to each
// lower-severity log file.
const (
	infoLog severity = iota
	warningLog
	errorLog
	numSeverity = 3
)

const recycleBufMaxSize = 1024

const severityChar = "IWE"

// severity identifies the sort of log: info, warning etc. It also implements
// the flag.Value interface. The -stderrthreshold flag is of type severity and
// should be modified only through the flag.Value interface. The values match
// the corresponding constants in C++.
type (
	severity int32 // sync/atomic int32

	// flushSyncWriter is the interface satisfied by logging destinations.
	flushSyncWriter interface {
		Flush() error
		Sync() error
		io.Writer
	}
)

// loggingT collects all the global state of the logging setup.
type loggingT struct {
	// freeList is a list of byte buffers, maintained under freeListMu.
	freeList *buffer

	// file holds writer for each of the log types.
	file [numSeverity]flushSyncWriter

	// Boolean flags.
	toStderr     bool // The -logtostderr flag.
	alsoToStderr bool // The -alsologtostderr flag.

	// freeListMu maintains the free list. It is separate from the main mutex
	// so buffers can be grabbed and printed to without holding the main lock,
	// for better parallelization.
	freeListMu sync.Mutex

	// mu protects the remaining elements of this structure and is
	// used to synchronize logging.
	mu sync.Mutex
}

type (
	// buffer holds a byte Buffer for reuse. The zero value is ready for use.
	buffer struct {
		bytes.Buffer
		tmp  [64]byte // temporary byte array for creating headers.
		next *buffer
	}

	// syncBuffer joins a bufio.Writer to its underlying file, providing access to the
	// file's Sync method and providing a wrapper for the Write method that provides log
	// file rotation. There are conflicting methods, so the file cannot be embedded.
	// l.mu is held for all its methods.
	syncBuffer struct {
		*bufio.Writer
		logger *loggingT
		file   *os.File
		nbytes uint64 // The number of bytes written to this file
		sev    severity
	}

	// Verbose is a boolean type that implements Infof (like Printf) etc.
	// See the documentation of V for more information.
	Verbose bool
)

var (
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

	timeNow = time.Now // Stubbed out for testing.
)

// interface guard
var _ flushSyncWriter = (*syncBuffer)(nil)

// Flush flushes all pending log I/O.
func Flush() {
	logging.lockAndFlushAll()
}

// getBuffer returns a new, ready-to-use buffer.
func (l *loggingT) getBuffer() *buffer {
	l.freeListMu.Lock()
	b := l.freeList
	if b != nil {
		l.freeList = b.next
	}
	l.freeListMu.Unlock()
	if b == nil {
		b = new(buffer)
	} else {
		b.next = nil
		b.Reset()
	}
	return b
}

// putBuffer returns a buffer to the free list.
func (l *loggingT) putBuffer(b *buffer) {
	if b.Len() > recycleBufMaxSize {
		// Let big buffers die a natural death.
		return
	}
	l.freeListMu.Lock()
	b.next = l.freeList
	l.freeList = b
	l.freeListMu.Unlock()
}

/*
header formats a log header as defined by the C++ implementation.
It returns a buffer containing the formatted header and the user's file and line number.
The depth specifies how many stack frames above lives the source line to be identified in the log message.

Log lines have this form:
	L hh:mm:ss.uuuuuu file:line] msg...
where the fields are defined as follows:
	L                A single character, representing the log level (eg 'I' for INFO)
	hh:mm:ss.uuuuuu  Time in hours, minutes and fractional seconds
	file             The file name
	line             The line number
	msg              The user-supplied message
*/

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

// formatHeader formats a log header using the provided file name and line number.
func (l *loggingT) formatHeader(s severity, file string, line int, redact bool) *buffer {
	now := timeNow()
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

// Some custom tiny helper functions to print the log header efficiently.

const digits = "0123456789"

// twoDigits formats a zero-prefixed two-digit integer at buf.tmp[i].
func (buf *buffer) twoDigits(i, d int) {
	buf.tmp[i+1] = digits[d%10]
	d /= 10
	buf.tmp[i] = digits[d%10]
}

// nDigits formats an n-digit integer at buf.tmp[i],
// padding with pad on the left.
// It assumes d >= 0.
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

// someDigits formats a zero-prefixed variable-width integer at buf.tmp[i].
func (buf *buffer) someDigits(i, d int) int {
	// Print into the top, then copy down. We know there's space for at least
	// a 10-digit number.
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

// output writes the data to the log files and releases the buffer.
func (l *loggingT) output(s severity, buf *buffer, alsoToStderr bool) {
	onceInitFiles.Do(initFiles)

	data := buf.Bytes()
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
		l.mu.Lock()
		switch s {
		case errorLog, warningLog:
			l.file[errorLog].Write(data)
			l.file[infoLog].Write(data)
		case infoLog:
			l.file[infoLog].Write(data)
		}
		l.mu.Unlock()
	}
	// free buf
	l.putBuffer(buf)
}

func (sb *syncBuffer) Sync() error {
	return sb.file.Sync()
}

func (sb *syncBuffer) Write(p []byte) (n int, err error) {
	if sb.nbytes+uint64(len(p)) >= MaxSize {
		if err := sb.rotateFile(time.Now()); err != nil {
			os.Stderr.WriteString(err.Error())
		}
	}
	n, err = sb.Writer.Write(p)
	sb.nbytes += uint64(n)
	if err != nil {
		os.Stderr.WriteString(err.Error())
	}
	return
}

// rotateFile closes the syncBuffer's file and starts a new one.
func (sb *syncBuffer) rotateFile(now time.Time) (err error) {
	if sb.file != nil {
		sb.Flush()
		sb.file.Close()
	}
	if sb.file, _, err = create(severityName[sb.sev], now); err != nil {
		return
	}
	sb.nbytes = 0
	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	// Write header.
	var (
		buf  bytes.Buffer
		n    int
		x    = fmt.Sprintf("host %s, %s for %s/%s\n", host, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		snow = now.Format("2006/01/02 15:04:05")
	)
	if FileHeaderCB == nil {
		fmt.Fprintf(&buf, "Started up at %s, %s", snow, x)
	} else {
		fmt.Fprintf(&buf, "Created at %s, %s", snow, x)
		fmt.Fprint(&buf, FileHeaderCB())
	}
	n, err = sb.file.Write(buf.Bytes())
	sb.nbytes += uint64(n)
	return
}

// bufferSize sizes the buffer associated with each log file. It's large
// so that log records can accumulate without the logging thread blocking
// on disk I/O. The flushDaemon will block instead.
const bufferSize = 256 * 1024

// createFiles creates all the log files for severity from sev down to infoLog.
// l.mu is held.
func (l *loggingT) createFiles(sev severity) error {
	now := time.Now()
	// Files are created in decreasing severity order, so as soon as we find one
	// has already been created, we can stop.
	for s := sev; s >= infoLog && l.file[s] == nil; s-- {
		if s == warningLog {
			continue
		}
		sb := &syncBuffer{logger: l, sev: s}
		if err := sb.rotateFile(now); err != nil {
			return err
		}
		l.file[s] = sb
	}
	return nil
}

// lockAndFlushAll is like flushAll but locks l.mu first.
func (l *loggingT) lockAndFlushAll() {
	l.mu.Lock()
	l.flushAll()
	l.mu.Unlock()
}

// flushAll flushes all the logs and attempts to "sync" their data to disk.
// l.mu is held.
func (l *loggingT) flushAll() {
	// Flush from fatal down, in case there's trouble flushing.
	for s := errorLog; s >= infoLog; s-- {
		sb := l.file[s]
		if sb != nil {
			sb.Flush() // ignore error
			sb.Sync()  // ignore error
		}
	}
}

// Info logs to the INFO log.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
func Info(args ...any) {
	logging.print(infoLog, args...)
}

// InfoDepth acts as Info but uses depth to determine which call frame to log.
// InfoDepth(0, "msg") is the same as Info("msg").
func InfoDepth(depth int, args ...any) {
	logging.printDepth(infoLog, depth, args...)
}

// Infoln logs to the INFO log.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
func Infoln(args ...any) {
	logging.println(infoLog, args...)
}

// Infof logs to the INFO log.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Infof(format string, args ...any) {
	logging.printf(infoLog, format, args...)
}

// Warning logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
func Warning(args ...any) {
	logging.print(warningLog, args...)
}

// WarningDepth acts as Warning but uses depth to determine which call frame to log.
// WarningDepth(0, "msg") is the same as Warning("msg").
func WarningDepth(depth int, args ...any) {
	logging.printDepth(warningLog, depth, args...)
}

// Warningln logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
func Warningln(args ...any) {
	logging.println(warningLog, args...)
}

// Warningf logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Warningf(format string, args ...any) {
	logging.printf(warningLog, format, args...)
}

// Error logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
func Error(args ...any) {
	logging.print(errorLog, args...)
}

// ErrorDepth acts as Error but uses depth to determine which call frame to log.
// ErrorDepth(0, "msg") is the same as Error("msg").
func ErrorDepth(depth int, args ...any) {
	logging.printDepth(errorLog, depth, args...)
}

// Errorln logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Println; a newline is appended if missing.
func Errorln(args ...any) {
	logging.println(errorLog, args...)
}

// Errorf logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Errorf(format string, args ...any) {
	logging.printf(errorLog, format, args...)
}

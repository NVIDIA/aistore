package lumber

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// mode constants
	APPEND = iota
	TRUNC
	BACKUP
	ROTATE
)

const (
	BUFSIZE = 100
)

type FileLogger struct {
	queue                                         chan *Message
	done                                          chan bool
	out                                           *os.File
	timeFormat, prefix                            string
	outLevel, maxLines, curLines, maxRotate, mode int
	closed, errored                               bool
	levels                                        []string
}

// Convenience function to create a new append-only logger
func NewAppendLogger(f string) (*FileLogger, error) {
	return NewFileLogger(f, INFO, APPEND, 0, 0, BUFSIZE)
}

// Convenience function to create a new truncating logger
func NewTruncateLogger(f string) (*FileLogger, error) {
	return NewFileLogger(f, INFO, TRUNC, 0, 0, BUFSIZE)
}

// Convenience function to create a new backup logger
func NewBackupLogger(f string, maxBackup int) (*FileLogger, error) {
	return NewFileLogger(f, INFO, BACKUP, 0, maxBackup, BUFSIZE)
}

// Convenience function to create a new rotating logger
func NewRotateLogger(f string, maxLines, maxRotate int) (*FileLogger, error) {
	return NewFileLogger(f, INFO, ROTATE, maxLines, maxRotate, BUFSIZE)
}

// Creates a new FileLogger with filename f, output level o, and an empty prefix.
// Modes are described in the documentation; maxLines and maxRotate are only significant
// for some modes.
func NewFileLogger(f string, o, mode, maxLines, maxRotate, bufsize int) (*FileLogger, error) {
	var file *os.File
	var err error

	switch mode {
	case APPEND:
		// open log file, append if it already exists
		file, err = os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	case TRUNC:
		// just truncate file and start logging
		file, err = os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	case BACKUP:
		// rotate every time a new logger is created
		file, err = openBackup(f, 0, maxRotate)
	case ROTATE:
		// "normal" rotation, when file reaches line limit
		file, err = openBackup(f, maxLines, maxRotate)
	default:
		return nil, fmt.Errorf("Invalid mode parameter: %d", mode)
	}
	if err != nil {
		return nil, fmt.Errorf("Error creating logger: %s", err)
	}

	return newFileLogger(file, o, mode, maxLines, maxRotate, bufsize), nil
}

func NewBasicFileLogger(f *os.File, level int) (l *FileLogger) {
	return newFileLogger(f, level, 0, 0, 0, BUFSIZE)
}

func newFileLogger(f *os.File, o, mode, maxLines, maxRotate, bufsize int) (l *FileLogger) {
	l = &FileLogger{
		queue:      make(chan *Message, bufsize),
		done:       make(chan bool),
		out:        f,
		outLevel:   o,
		timeFormat: TIMEFORMAT,
		prefix:     "",
		maxLines:   maxLines,
		maxRotate:  maxRotate,
		mode:       mode,
		levels:     levels,
	}

	if mode == ROTATE {
		// get the current line count if relevant
		l.curLines = countLines(l.out)
	}

	go l.startOutput()
	return
}

func (l *FileLogger) startOutput() {
	for {
		m, ok := <-l.queue
		if !ok {
			// the channel is closed and empty
			l.printLog(&Message{len(l.levels) - 1, fmt.Sprintf("Closing log now"), time.Now()})
			l.out.Sync()
			if err := l.out.Close(); err != nil {
				l.printLog(&Message{len(l.levels) - 1, fmt.Sprintf("Error closing log file: %s", err), time.Now()})
			}
			l.done <- true
			return
		}
		l.output(m)
	}
}

// Attempt to create new log. Specific behavior depends on the maxLines setting
func openBackup(f string, maxLines, maxRotate int) (*os.File, error) {
	// first try to open the file with O_EXCL (file must not already exist)
	file, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	// if there are no errors (it's a new file), we can just use this file
	if err == nil {
		return file, nil
	}
	// if the error wasn't an 'Exist' error, we've got a problem
	if !os.IsExist(err) {
		return nil, fmt.Errorf("Error opening file for logging: %s", err)
	}

	if maxLines == 0 {
		// we're in backup mode, rotate and return the new file
		return doRotate(f, maxRotate)
	}

	// the file already exists, open it
	return os.OpenFile(f, os.O_RDWR|os.O_APPEND, 0644)
}

// Rotate the logs
func (l *FileLogger) rotate() error {
	oldFile := l.out
	file, err := doRotate(l.out.Name(), l.maxRotate)
	if err != nil {
		return fmt.Errorf("Error rotating logs: %s", err)
	}
	l.curLines = 0
	l.out = file
	oldFile.Close()
	return nil
}

// Rotate all the logs and return a file with newly vacated filename
// Rename 'log.name' to 'log.name.1' and 'log.name.1' to 'log.name.2' etc
func doRotate(f string, limit int) (*os.File, error) {
	// create a format string with the correct amount of zero-padding for the limit
	numFmt := fmt.Sprintf(".%%0%dd", len(fmt.Sprintf("%d", limit)))
	// get all rotated files and sort them in reverse order
	list, err := filepath.Glob(fmt.Sprintf("%s.*", f))
	if err != nil {
		return nil, fmt.Errorf("Error rotating logs: %s", err)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(list)))
	for _, file := range list {
		parts := strings.Split(file, ".")
		numPart := parts[len(parts)-1]
		num, err := strconv.Atoi(numPart)
		if err != nil {
			// not a number, don't rotate it
			continue
		}
		if num >= limit {
			// we're at the limit, don't rotate it
			continue
		}
		newName := fmt.Sprintf(strings.Join(parts[:len(parts)-1], ".")+numFmt, num+1)
		// don't check error because there's nothing we can do
		os.Rename(file, newName)
	}
	if err = os.Rename(f, fmt.Sprintf(f+numFmt, 1)); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("Error rotating logs: %s", err)
		}
	}
	return os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
}

// Generic output function. Outputs messages if they are higher level than outLevel for this
// specific logger. If msg does not end with a newline, one will be appended.
func (l *FileLogger) output(msg *Message) {
	if l.mode == ROTATE && l.curLines >= l.maxLines && !l.errored {
		err := l.rotate()
		if err != nil {
			// if we can't rotate the logs, we should stop logging to prevent the log file from growing
			// past the limit and continuously retrying the rotate operation (but log current msg first)
			l.printLog(msg)
			l.printLog(&Message{len(l.levels) - 1, fmt.Sprintf("Error rotating logs: %s. Closing log."), time.Now()})
			l.errored = true
			l.close()
		}
	}
	l.printLog(msg)
}

func (l *FileLogger) printLog(msg *Message) {
	buf := []byte{}
	buf = append(buf, msg.time.Format(l.timeFormat)...)
	if l.prefix != "" {
		buf = append(buf, ' ')
		buf = append(buf, l.prefix...)
	}
	buf = append(buf, ' ')
	buf = append(buf, l.levels[msg.level]...)
	buf = append(buf, ' ')
	buf = append(buf, msg.m...)
	if len(msg.m) > 0 && msg.m[len(msg.m)-1] != '\n' {
		buf = append(buf, '\n')
	}
	l.curLines += 1
	l.out.Write(buf)
}

// Sets the available levels for this logger
// TODO: append a *LOG* level
func (l *FileLogger) SetLevels(lvls []string) {
	if lvls[len(lvls)-1] != "*LOG*" {
		lvls = append(lvls, "*LOG*")
	}
	l.levels = lvls
}

// Sets the output level for this logger
func (l *FileLogger) Level(o int) {
	if o >= 0 && o <= len(l.levels)-1 {
		l.outLevel = o
	}
}

// Sets the prefix for this logger
func (l *FileLogger) Prefix(p string) {
	l.prefix = p
}

// Sets the time format for this logger
func (l *FileLogger) TimeFormat(f string) {
	l.timeFormat = f
}

// Flush the messages in the queue and shut down the logger.
func (l *FileLogger) close() {
	l.closed = true
	// closing the channel will signal the goroutine to finish writing messages in the queue
	// and then shut down by sync'ing and close'ing the file.
	close(l.queue)
}

// Flush the messages in the queue and shut down the logger.
func (l *FileLogger) Close() {
	l.close()
	<-l.done
}

// return the number of lines in the given file
func countLines(f *os.File) int {
	r := bufio.NewReader(f)
	count := 0
	var err error = nil
	for err == nil {
		prefix := true
		_, prefix, err = r.ReadLine()
		if err != nil {
		}
		// sometimes we don't get the whole line at once
		if !prefix && err == nil {
			count++
		}
	}
	return count
}

func (l *FileLogger) log(lvl int, format string, v ...interface{}) {
	if lvl < l.outLevel || l.closed {
		return
	}
	// recover in case the channel has already been closed (unlikely race condition)
	// this could also be solved with a lock, but would cause a performance hit
	defer recover()
	l.queue <- &Message{lvl, fmt.Sprintf(format, v...), time.Now()}
}

// Logging functions
func (l *FileLogger) Fatal(format string, v ...interface{}) {
	l.log(FATAL, format, v...)
}

func (l *FileLogger) Error(format string, v ...interface{}) {
	l.log(ERROR, format, v...)
}

func (l *FileLogger) Warn(format string, v ...interface{}) {
	l.log(WARN, format, v...)
}

func (l *FileLogger) Info(format string, v ...interface{}) {
	l.log(INFO, format, v...)
}

func (l *FileLogger) Debug(format string, v ...interface{}) {
	l.log(DEBUG, format, v...)
}

func (l *FileLogger) Trace(format string, v ...interface{}) {
	l.log(TRACE, format, v...)
}

func (l *FileLogger) Print(lvl int, v ...interface{}) {
	l.output(&Message{lvl, fmt.Sprint(v...), time.Now()})
}

func (l *FileLogger) Printf(lvl int, format string, v ...interface{}) {
	l.output(&Message{lvl, fmt.Sprintf(format, v...), time.Now()})
}

func (l *FileLogger) GetLevel() int {
	return l.outLevel
}

func (l *FileLogger) IsFatal() bool {
	return l.outLevel <= FATAL
}

func (l *FileLogger) IsError() bool {
	return l.outLevel <= ERROR
}

func (l *FileLogger) IsWarn() bool {
	return l.outLevel <= WARN
}

func (l *FileLogger) IsInfo() bool {
	return l.outLevel <= INFO
}

func (l *FileLogger) IsDebug() bool {
	return l.outLevel <= DEBUG
}

func (l *FileLogger) IsTrace() bool {
	return l.outLevel <= TRACE
}

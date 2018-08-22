/*
Package lumber implements a simple logger that supports log levels and rotation.
*/
package lumber

import (
	"strings"
	"time"
)

const (
	TRACE = iota
	DEBUG
	INFO
	WARN
	ERROR
	FATAL

	TIMEFORMAT = "2006-01-02 15:04:05"
)

var (
	stdLog     Logger = NewConsoleLogger(INFO)
	levels            = []string{"TRACE", "DEBUG", "INFO ", "WARN ", "ERROR", "FATAL", "*LOG*"}
	timeFormat        = TIMEFORMAT
)

type Logger interface {
	Fatal(string, ...interface{})
	Error(string, ...interface{})
	Warn(string, ...interface{})
	Info(string, ...interface{})
	Debug(string, ...interface{})
	Trace(string, ...interface{})

	IsFatal() bool
	IsError() bool
	IsWarn() bool
	IsInfo() bool
	IsDebug() bool
	IsTrace() bool
	GetLevel() int

	Print(int, ...interface{})
	Printf(int, string, ...interface{})
	Level(int)
	Prefix(string)
	TimeFormat(string)
	Close()
	output(msg *Message)
}

type Message struct {
	level int
	m     string
	time  time.Time
}

// SetLogger sets a new default logger
func SetLogger(l Logger) {
	stdLog = l
}

// Returns the string representation of the level
func LvlStr(l int) string {
	if l >= 0 && l <= len(levels)-1 {
		return levels[l]
	}
	return ""
}

// Returns the int value of the level
func LvlInt(s string) int {
	for i, str := range levels {
		if strings.TrimSpace(str) == strings.ToUpper(s) {
			return i
		}
	}
	return 0
}

// Sets the output level for the default logger
func Level(o int) {
	stdLog.Level(o)
}

// Sets the time format for the default logger
func TimeFormat(f string) {
	stdLog.TimeFormat(f)
}

// Close the default logger
func Close() {
	stdLog.Close()
}

// Prefix sets a prefix for the default logger
func Prefix(p string) {
	stdLog.Prefix(p)
}

// Logging functions
func Fatal(format string, v ...interface{}) {
	stdLog.Fatal(format, v...)
}

func Error(format string, v ...interface{}) {
	stdLog.Error(format, v...)
}

func Warn(format string, v ...interface{}) {
	stdLog.Warn(format, v...)
}

func Info(format string, v ...interface{}) {
	stdLog.Info(format, v...)
}

func Debug(format string, v ...interface{}) {
	stdLog.Debug(format, v...)
}

func Trace(format string, v ...interface{}) {
	stdLog.Trace(format, v...)
}

func Print(lvl int, v ...interface{}) {
	stdLog.Print(lvl, v...)
}

func Printf(lvl int, format string, v ...interface{}) {
	stdLog.Printf(lvl, format, v...)
}

func GetLevel() int {
	return stdLog.GetLevel()
}

func IsFatal() bool {
	return stdLog.IsFatal()
}

func IsError() bool {
	return stdLog.IsError()
}

func IsWarn() bool {
	return stdLog.IsWarn()
}

func IsInfo() bool {
	return stdLog.IsInfo()
}

func IsDebug() bool {
	return stdLog.IsDebug()
}

func IsTrace() bool {
	return stdLog.IsTrace()
}

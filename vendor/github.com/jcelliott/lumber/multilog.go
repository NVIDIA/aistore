package lumber

import (
	"fmt"
	"time"
)

type MultiLogger struct {
	loggers []Logger
}

func NewMultiLogger() (l *MultiLogger) {
	return &MultiLogger{}
}

func (p *MultiLogger) AddLoggers(newLogs ...Logger) {
	for _, l := range newLogs {
		p.loggers = append(p.loggers, l)
	}
}

func (p *MultiLogger) ClearLoggers() {
	p.loggers = make([]Logger, 0)
}

// All of these implement the Logger interface and distribute calls to it over
// all of the member Logger objects.
func (p *MultiLogger) Fatal(s string, v ...interface{}) {
	for _, logger := range p.loggers {
		logger.Fatal(s, v...)
	}
}

func (p *MultiLogger) Error(s string, v ...interface{}) {
	for _, logger := range p.loggers {
		logger.Error(s, v...)
	}
}

func (p *MultiLogger) Warn(s string, v ...interface{}) {
	for _, logger := range p.loggers {
		logger.Warn(s, v...)
	}
}

func (p *MultiLogger) Info(s string, v ...interface{}) {
	for _, logger := range p.loggers {
		logger.Info(s, v...)
	}
}

func (p *MultiLogger) Debug(s string, v ...interface{}) {
	for _, logger := range p.loggers {
		logger.Debug(s, v...)
	}
}

func (p *MultiLogger) Trace(s string, v ...interface{}) {
	for _, logger := range p.loggers {
		logger.Trace(s, v...)
	}
}

func (p *MultiLogger) Level(i int) {
	for _, logger := range p.loggers {
		logger.Level(i)
	}
}

func (p *MultiLogger) Prefix(s string) {
	for _, logger := range p.loggers {
		logger.Prefix(s)
	}
}

func (p *MultiLogger) TimeFormat(s string) {
	for _, logger := range p.loggers {
		logger.TimeFormat(s)
	}
}

func (p *MultiLogger) Close() {
	for _, logger := range p.loggers {
		logger.Close()
	}
}

func (p *MultiLogger) output(m *Message) {
	for _, logger := range p.loggers {
		logger.output(m)
	}
}

func (p *MultiLogger) Print(lvl int, v ...interface{}) {
	p.output(&Message{lvl, fmt.Sprint(v...), time.Now()})
}

func (p *MultiLogger) Printf(lvl int, format string, v ...interface{}) {
	p.output(&Message{lvl, fmt.Sprintf(format, v...), time.Now()})
}

func (p *MultiLogger) GetLevel() int {
	level := FATAL
	for _, logger := range p.loggers {
		if logger.GetLevel() <= level {
			level = logger.GetLevel()
		}
	}
	return level
}

func (p *MultiLogger) IsFatal() bool {
	return p.GetLevel() <= FATAL
}

func (p *MultiLogger) IsError() bool {
	return p.GetLevel() <= ERROR
}

func (p *MultiLogger) IsWarn() bool {
	return p.GetLevel() <= WARN
}

func (p *MultiLogger) IsInfo() bool {
	return p.GetLevel() <= INFO
}

func (p *MultiLogger) IsDebug() bool {
	return p.GetLevel() <= DEBUG
}

func (p *MultiLogger) IsTrace() bool {
	return p.GetLevel() <= TRACE
}

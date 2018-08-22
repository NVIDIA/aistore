lumber
======

A simple logger for Go.

Provides console and file loggers that support 6 log levels. The file logger supports log backup and
rotation.

### Usage: ###
Log to the default (console) logger

```go
lumber.Error("An error message")
```

Create a new console logger that only logs messages of level WARN or higher

```go
log := lumber.NewConsoleLogger(lumber.WARN)
```

Change the log level for a logger

```go
log.Level(lumber.INFO)
```  
  
Create a new file logger that rotates at 5000 lines (up to 9 backups) with a 100 message buffer

```go
log := lumber.NewFileLogger("filename.log", lumber.INFO, lumber.ROTATE, 5000, 9, 100)
// or
log := lumber.NewRotateLogger("filename.log", 5000, 9)
```

Send messages to the log

```go
// the log methods use fmt.Printf() syntax
log.Trace("the %s log level", "lowest")
log.Debug("")
log.Info("the default log level")
log.Warn("")
log.Error("")
log.Fatal("the %s log level", "highest")
```

Add a prefix to label different logs

```go
log.Prefix("MYAPP")
```

Use a MultiLogger

```go
mlog := NewMultiLogger()
mlog.AddLoggers(log1, log2)
mlog.Warn("This message goes to multiple loggers")
mlog.Close() // closes all loggers
```

### Modes: ###

APPEND: Append if the file exists, otherwise create a new file

TRUNC: Open and truncate the file, regardless of whether it already exists

BACKUP: Rotate the log every time a new logger is created

ROTATE: Append if the file exists, when the log reaches maxLines rotate files

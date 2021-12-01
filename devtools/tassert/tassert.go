// Package tassert provides common asserts for tests
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package tassert

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	fatalities = make(map[string]struct{})
	mu         sync.Mutex
)

func CheckFatal(tb testing.TB, err error) {
	if err == nil {
		return
	}
	mu.Lock()
	if _, ok := fatalities[tb.Name()]; ok {
		mu.Unlock()
		fmt.Printf("--- %s: duplicate CheckFatal\n", tb.Name()) // see #1057
		runtime.Goexit()
	} else {
		fatalities[tb.Name()] = struct{}{}
		mu.Unlock()
		printStack()
		now := fmt.Sprintf("[%s]", time.Now().Format("15:04:05.000000"))
		tb.Fatal(now, err)
	}
}

func CheckError(tb testing.TB, err error) {
	if err != nil {
		printStack()
		now := fmt.Sprintf("[%s]", time.Now().Format("15:04:05.000000"))
		tb.Error(now, err)
	}
}

func CheckResp(tb testing.TB, client *http.Client, req *http.Request, statusCode ...int) {
	resp, err := client.Do(req)
	CheckFatal(tb, err)
	resp.Body.Close()
	for _, code := range statusCode {
		if resp.StatusCode == code {
			return
		}
	}
	Errorf(tb, false, "expected %v status code, got %d", statusCode, resp.StatusCode)
}

func Fatalf(tb testing.TB, cond bool, msg string, args ...interface{}) {
	if !cond {
		printStack()
		tb.Fatalf(msg, args...)
	}
}

func Errorf(tb testing.TB, cond bool, msg string, args ...interface{}) {
	if !cond {
		printStack()
		tb.Errorf(msg, args...)
	}
}

// TODO: Make this a range over `errCh` post closing it ?
func SelectErr(tb testing.TB, errCh chan error, verb string, errIsFatal bool) {
	if num := len(errCh); num > 0 {
		err := <-errCh
		f := tb.Errorf
		if errIsFatal {
			f = tb.Fatalf
		}
		if num > 1 {
			f("Failed to %s %d objects, e.g. error:\n%v", verb, num, err)
		} else {
			f("Failed to %s object: %v", verb, err)
		}
	}
}

func printStack() {
	var buffer bytes.Buffer
	fmt.Fprintln(os.Stderr, "    tassert.printStack:")
	for i := 1; i < 9; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		i := strings.Index(file, "aistore")
		if i < 0 {
			break
		}
		if strings.Contains(file, "tassert") {
			continue
		}
		fmt.Fprintf(&buffer, "\t%s:%d\n", file[i+8:], line)
	}
	os.Stderr.Write(buffer.Bytes())
}

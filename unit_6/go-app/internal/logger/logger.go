package logger

import (
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Logger struct {
	prefix  string
	info    *log.Logger
	error   *log.Logger
	success *log.Logger
}

func New(prefix string) *Logger {

	if prefix != "" {
		prefix += ": "
	}

	return &Logger{
		prefix:  prefix,
		info:    log.New(os.Stdout, "INFO: ", log.LstdFlags),
		error:   log.New(os.Stderr, "ERROR: ", log.LstdFlags),
		success: log.New(os.Stdout, "SUCCESS: ", log.LstdFlags),
	}
}

func (l *Logger) Info(msg string, args ...any) {
	l.info.Printf(l.getMsgWithFileLine(msg), args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.error.Printf(l.getMsgWithFileLine(msg), args...)
}
func (l *Logger) Success(msg string, args ...any) {
	l.success.Printf(l.getMsgWithFileLine(msg), args...)
}

func (l *Logger) getMsgWithFileLine(msg string) string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "unknown_file"
		line = 0
	} else {
		parts := strings.Split(file, "/")
		file = parts[len(parts)-1]
	}

	return l.prefix + "in file: " + file + ":" + strconv.Itoa(line) + ": " + msg + "\n\n"
}

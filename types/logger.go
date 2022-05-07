package types

import "log"

type Logger interface {
	Debug(format string, a ...interface{})
	Info(format string, a ...interface{})
	Warn(format string, a ...interface{})
	Error(format string, a ...interface{})
}

var StdLogger = &stdLogger{}
var NoopLogger = &noopLogger{}

type stdLogger struct {
}

func (l *stdLogger) Debug(format string, a ...interface{}) {
	log.Printf("DEBUG "+format, a...)
}

func (l *stdLogger) Info(format string, a ...interface{}) {
	log.Printf("INFO "+format, a...)
}

func (l *stdLogger) Warn(format string, a ...interface{}) {
	log.Printf("WARN "+format, a...)
}

func (l *stdLogger) Error(format string, a ...interface{}) {
	log.Printf("ERROR "+format, a...)
}

type noopLogger struct {
}

func (l *noopLogger) Debug(format string, a ...interface{}) {
}

func (l *noopLogger) Info(format string, a ...interface{}) {
}

func (l *noopLogger) Warn(format string, a ...interface{}) {
}

func (l *noopLogger) Error(format string, a ...interface{}) {
}

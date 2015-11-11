package resv

import "log"

type Logger interface {
	Printf(string, ...interface{})
}

type discardLog struct{}

func (discardLog) Printf(string, ...interface{}) {}

type defaultLog struct{}

func (defaultLog) Printf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

var NullLogger = discardLog{}
var BaseLogger = defaultLog{}

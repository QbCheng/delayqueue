package logger

import (
	"fmt"
	"log"
)

type Logger interface {
	Print(string, ...interface{})
}

type DefaultLog struct {
	Logger *log.Logger
}

func NewDefaultLog() Logger {
	l := &DefaultLog{
		Logger: log.Default(),
	}
	l.Logger.SetFlags(log.LstdFlags | log.Llongfile)
	return l
}

func (d DefaultLog) Print(layout string, data ...interface{}) {
	_ = d.Logger.Output(3, fmt.Sprintf(layout, data...))
}

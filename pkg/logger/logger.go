package logger

import (
	"log"
	"os"
)

type Logger struct {
	*log.Logger
}

func NewLogger() *Logger {
	return &Logger{
		Logger: log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func (l *Logger) Info(message string) {
	l.Println(message)
}

func (l *Logger) Error(message string) {
	l.SetPrefix("ERROR: ")
	l.Println(message)
	l.SetPrefix("INFO: ")
}

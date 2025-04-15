package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

type Logger struct {
	*logrus.Logger
}

func NewLogger(logLevel string) *Logger {
	logger := logrus.New()
	logger.Out = os.Stdout

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logger.Warnf("Invalid log level: %s. Defaulting to info.", logLevel)
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	return &Logger{Logger: logger}
}

func (l *Logger) Info(message string) {
	l.Logger.Info(message)
}

func (l *Logger) Error(message string) {
	l.Logger.Error(message)
}

func (l *Logger) Debug(message string) {
	l.Logger.Debug(message)
}

func (l *Logger) Warn(message string) {
	l.Logger.Warn(message)
}

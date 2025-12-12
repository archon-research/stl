package common

import "log"

// Logger is a placeholder for a structured logger
type Logger struct{}

func NewLogger() *Logger {
	return &Logger{}
}

func (l *Logger) Info(msg string) {
	log.Println("INFO:", msg)
}

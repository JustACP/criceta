package logs

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/JustACP/criceta/pkg/common"
)

const (
	MaxFileSize = 1 << 20 // 1MB
)

type LogLevel int

const (
	DEBUG LogLevel = 1
	INFO  LogLevel = 2
	WARN  LogLevel = 3
	ERROR LogLevel = 4
	FATAL LogLevel = 5
)

var levelNames = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

func (l LogLevel) String() string {
	idx := int(l) - 1
	if len(levelNames) <= idx || idx < 0 {
		return "UNKNOWN"
	}
	return levelNames[idx]
}

func (l LogLevel) FromString(s string) (common.Enum, error) {
	loglevelName := strings.ToUpper(s)
	for idx, currLogLevelName := range levelNames {
		if strings.Compare(currLogLevelName, loglevelName) == 0 {
			return LogLevel(idx + 1), nil
		}
	}

	return LogLevel(0), errors.New("invalid log mode")
}

type Logger struct {
	mu         sync.Mutex
	logger     *log.Logger
	file       *os.File
	filePath   string
	fileSize   int64
	lastRotate time.Time
	config     *Config
}

type Config struct {
	LogLevel  LogLevel
	LogToFile bool
	FilePath  string
}

var defaultLogger = &Logger{
	logger: log.New(os.Stdout, "", 0),
	config: &Config{LogLevel: INFO},
}

func NewLogger(config *Config) (*Logger, error) {
	l := &Logger{
		config: config,
		logger: log.New(os.Stdout, "", 0),
	}

	if config.LogToFile {
		if err := l.setupFileOutput(); err != nil {
			return nil, err
		}
	}

	return l, nil
}

func (l *Logger) setupFileOutput() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		l.file.Close()
	}

	if err := os.MkdirAll(filepath.Dir(l.config.FilePath), 0755); err != nil {
		return err
	}

	file, err := os.OpenFile(l.getCurrentLogFileName(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	l.file = file
	l.filePath = l.config.FilePath
	l.fileSize = 0
	l.lastRotate = time.Now()

	if l.config.LogToFile {
		l.logger.SetOutput(io.MultiWriter(os.Stdout, l.file))
	} else {
		l.logger.SetOutput(l.file)
	}

	return nil
}

func (l *Logger) getCurrentLogFileName() string {
	now := time.Now()
	return fmt.Sprintf("%s.%s.log", l.config.FilePath, now.Format("2006-01-02"))
}

func (l *Logger) rotateIfNeeded() error {
	if l.file == nil {
		return nil
	}

	now := time.Now()
	if now.Day() != l.lastRotate.Day() || l.fileSize > MaxFileSize {
		if err := l.setupFileOutput(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.config.LogLevel {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Rotate log file if needed
	if err := l.rotateIfNeeded(); err != nil {
		log.Printf("Failed to rotate log file: %v\n", err)
		return
	}

	// Format log message
	msg := fmt.Sprintf(format, args...)
	_, file, line, _ := runtime.Caller(3)
	logMsg := fmt.Sprintf("[%s][%s][%s:%d]: %s",
		time.Now().Format("2006:01:02 15:04:05"),
		level.String(),
		filepath.Base(file),
		line,
		msg)

	// Write log using standard logger
	l.logger.Println(logMsg)

	// Update file size
	if l.file != nil {
		l.fileSize += int64(len(logMsg))
	}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WARN, format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, format, args...)
}

func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
	os.Exit(1)
}

// Default logger functions
func Debug(format string, args ...interface{}) {
	defaultLogger.Debug(format, args...)
}

func Info(format string, args ...interface{}) {
	defaultLogger.Info(format, args...)
}

func Warn(format string, args ...interface{}) {
	defaultLogger.Warn(format, args...)
}

func Error(format string, args ...interface{}) {
	defaultLogger.Error(format, args...)
}

func Fatal(format string, args ...interface{}) {
	defaultLogger.Fatal(format, args...)
}

func SetConfig(config *Config) error {
	newLogger, err := NewLogger(config)
	if err != nil {
		return err
	}
	defaultLogger = newLogger
	return nil
}

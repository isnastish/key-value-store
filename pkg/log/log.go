package log

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/mattn/go-colorable"
	"github.com/rs/zerolog"
)

type logger struct {
	zerolog zerolog.Logger
}

var Logger = newLogger()

var logLevelsMap map[string]zerolog.Level

func init() {
	logLevelsMap = make(map[string]zerolog.Level)
	logLevelsMap["debug"] = zerolog.DebugLevel
	logLevelsMap["info"] = zerolog.InfoLevel
	logLevelsMap["warn"] = zerolog.WarnLevel
	logLevelsMap["error"] = zerolog.ErrorLevel
	logLevelsMap["fatal"] = zerolog.FatalLevel
	logLevelsMap["panic"] = zerolog.PanicLevel
	logLevelsMap["disabled"] = zerolog.Disabled
}

func SetupGlobalLogLevel(level string) {
	if zerologLevel, exists := logLevelsMap[level]; exists {
		zerolog.SetGlobalLevel(zerologLevel)
	} else {
		fmt.Printf("Failed to set log level to %s. Backing up to 'debug'\n", level)
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	zerolog.TimeFieldFormat = time.UnixDate
	zerolog.TimestampFieldName = "timestamp"
}

func newLogger() *logger {
	l := &logger{}
	if runtime.GOOS == "windows" {
		outputWriter := zerolog.ConsoleWriter{Out: colorable.NewColorableStdout()}
		l.zerolog = zerolog.New(outputWriter).With().Timestamp().Logger()
		return l
	}
	l.zerolog = zerolog.New(os.Stdout).With().Timestamp().Logger()
	return l
}

func (l *logger) Debug(fmt string, args ...interface{}) {
	l.zerolog.Debug().Msgf(fmt, args...)
}

func (l *logger) Info(fmt string, args ...interface{}) {
	l.zerolog.Info().Msgf(fmt, args...)
}

func (l *logger) Warn(fmt string, args ...interface{}) {
	l.zerolog.Warn().Msgf(fmt, args...)
}

func (l *logger) Error(fmt string, args ...interface{}) {
	l.zerolog.Error().Msgf(fmt, args...)
}

func (l *logger) Fatal(fmt string, args ...interface{}) {
	l.zerolog.Fatal().Msgf(fmt, args...)
}

func (l *logger) Panic(fmt string, args ...interface{}) {
	l.zerolog.Panic().Msgf(fmt, args...)
}

package controller

import (
	"io"
	"log/slog"
	"os"
)

var (
	gLogger *slog.Logger
	writer  swappableWriter = swappableWriter{os.Stderr}
)

type swappableWriter struct {
	io.Writer
}

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	gLogger = slog.New(slog.NewJSONHandler(&writer, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Key = "logged_at"
			case slog.LevelKey:
				a.Key = "severity"
			case slog.MessageKey:
				a.Key = "message"
			}
			return a
		},
	})).With(slog.String("utsname", hostname))
}

func setLoggerWriter(w io.Writer) {
	writer.Writer = w
}

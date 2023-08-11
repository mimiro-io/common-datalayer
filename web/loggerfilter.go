package web

import (
	"fmt"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/mimiro-io/common-datalayer/core"
)

type LoggerConfig struct {
	// Skipper defines a function to skip middleware.
	Skipper middleware.Skipper

	// BeforeFunc defines a function which is executed just before the middleware.
	BeforeFunc middleware.BeforeFunc

	core *core.Service
}

func LoggerFilter(config LoggerConfig) echo.MiddlewareFunc {
	service := config.core.Config.SystemConfig().ServiceName()

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if config.Skipper(c) {
				return next(c)
			}

			if config.BeforeFunc != nil {
				config.BeforeFunc(c)
			}

			start := time.Now()
			req := c.Request()
			res := c.Response()

			tags := []string{
				fmt.Sprintf("application:%s", service),
				fmt.Sprintf("method:%s", strings.ToLower(req.Method)),
				fmt.Sprintf("url:%s", strings.ToLower(req.RequestURI)),
				fmt.Sprintf("status:%d", res.Status),
			}

			err := next(c)
			if err != nil {
				c.Error(err)
			}

			timed := time.Since(start)

			err = config.core.Metrics.Incr("http.count", tags, 1)
			err = config.core.Metrics.Timing("http.time", timed, tags, 1)
			err = config.core.Metrics.Gauge("http.size", float64(res.Size), tags, 1)
			if err != nil {
				config.core.Logger.Warn("Error with metrics", "error", err.Error())
			}

			msg := fmt.Sprintf("%d - %s %s (time: %s, size: %d, user_agent: %s)", res.Status, req.Method, req.RequestURI, timed.String(), res.Size, req.UserAgent())

			args := []any{
				"time", timed.String(),
				"request", fmt.Sprintf("%s %s", req.Method, req.RequestURI),
				"status", res.Status,
				"size", res.Size,
				"user_agent", req.UserAgent(),
			}

			id := req.Header.Get(echo.HeaderXRequestID)
			if id == "" {
				id = res.Header().Get(echo.HeaderXRequestID)
				args = append(args, "request_id", id)
			}

			config.core.Logger.Info(msg, args...)

			return nil
		}
	}
}

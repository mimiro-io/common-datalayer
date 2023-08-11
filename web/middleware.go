package web

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/mimiro-io/common-datalayer/core"
)

type Middleware []echo.MiddlewareFunc

func NewMiddleware(core *core.Service) Middleware {
	skipper := func(c echo.Context) bool {
		// skip health check
		return strings.HasPrefix(c.Request().URL.Path, "/health")
	}

	m := Middleware{LoggerFilter(LoggerConfig{Skipper: skipper, core: core})}

	m = append(m, setupCors(core.Config.SystemConfig()))
	//m = append(m, setupJWT(core.Config.SystemConfig().AuthConfig(), skipper))
	m = append(m, setupRecovery(skipper, core.Logger))
	return m
}

func setupCors(conf *core.SystemConfig) echo.MiddlewareFunc {
	return middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: conf.CorsOrigins(),
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept},
	})
}

func setupRecovery(skipper func(c echo.Context) bool, logger core.Logger) echo.MiddlewareFunc {
	config := middleware.DefaultRecoverConfig
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if skipper(c) {
				return next(c)
			}

			defer func() {
				if r := recover(); r != nil {
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}
					stack := make([]byte, config.StackSize)
					length := runtime.Stack(stack, !config.DisableStackAll)
					if !config.DisablePrintStack {
						msg := fmt.Sprintf("[PANIC RECOVER] %v %s\n", err, stack[:length])
						logger.Warn(msg)
					}
					c.Error(err)
				}
			}()
			return next(c)
		}
	}
}

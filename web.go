package common_datalayer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type dataLayerWebService struct {
	// service specific service core
	datalayerService DataLayerService

	core *CoreService
	e    *echo.Echo
}

func newDataLayerWebService(core *CoreService, dataLayerService DataLayerService) (*dataLayerWebService, error) {

	e := echo.New()
	e.HideBanner = true

	mw(core, e)

	s := &dataLayerWebService{core: core, datalayerService: dataLayerService, e: e}

	e.GET("/health", s.health)
	e.POST("/datasets/:dataset/entities", s.postEntities)
	e.GET("/datasets/:dataset/entities", s.getEntities)
	e.GET("/datasets/:dataset/changes", s.getChanges)
	e.GET("/datasets", s.listDatasets)

	return s, nil
}

// wrap all handlers with middleware
func mw(core *CoreService, e *echo.Echo) {
	skipper := func(c echo.Context) bool {
		// skip health check
		return strings.HasPrefix(c.Request().URL.Path, "/health")
	}
	e.Use(
		// Request logging and HTTP metrics
		func(next echo.HandlerFunc) echo.HandlerFunc {
			//service := core.Config.SystemConfig.ServiceName()
			return func(c echo.Context) error {
				if skipper(c) {
					return next(c)
				}

				start := time.Now()
				tags := []string{
					//fmt.Sprintf("application:%s", service),
					fmt.Sprintf("method:%s", strings.ToLower(c.Request().Method)),
					fmt.Sprintf("url:%s", strings.ToLower(c.Request().RequestURI)),
					fmt.Sprintf("status:%d", c.Response().Status),
				}

				// Recover from panic
				defer func() {
					if r := recover(); r != nil {
						err, ok := r.(error)
						if !ok {
							err = fmt.Errorf("%v", r)
						}
						stack := make([]byte, middleware.DefaultRecoverConfig.StackSize)
						length := runtime.Stack(stack, !middleware.DefaultRecoverConfig.DisableStackAll)
						if !middleware.DefaultRecoverConfig.DisablePrintStack {
							msg := fmt.Sprintf("[PANIC RECOVER] %v %s\n", err, stack[:length])
							core.Logger.Warn(msg)
						}
						c.Error(err)
					}
				}()

				// next middleware/handler
				err := next(c)
				if err != nil {
					c.Error(err)
				}

				timed := time.Since(start)

				err = core.Metrics.Incr("http.count", tags, 1)
				err = core.Metrics.Timing("http.time", timed, tags, 1)
				err = core.Metrics.Gauge("http.size", float64(c.Response().Size), tags, 1)
				if err != nil {
					core.Logger.Warn("Error with metrics", "error", err.Error())
				}

				msg := fmt.Sprintf("%d - %s %s (time: %s, size: %d, user_agent: %s)",
					c.Response().Status, c.Request().Method, c.Request().RequestURI, timed.String(),
					c.Response().Size, c.Request().UserAgent())

				args := []any{
					"time", timed.String(),
					"request", fmt.Sprintf("%s %s", c.Request().Method, c.Request().RequestURI),
					"status", c.Response().Status,
					"size", c.Response().Size,
					"user_agent", c.Request().UserAgent(),
				}

				id := c.Request().Header.Get(echo.HeaderXRequestID)
				if id == "" {
					id = c.Response().Header().Get(echo.HeaderXRequestID)
					args = append(args, "request_id", id)
				}

				core.Logger.Info(msg, args...)

				return nil
			}
		})
}

func (ws *dataLayerWebService) Start() error {
	port := ws.core.config.SystemConfig.HttpPort()
	ws.core.Logger.Info(fmt.Sprintf("Starting Http server on :%s", port))
	go func() {
		_ = ws.e.Start(":" + port)
	}()

	return nil
}

func (ws *dataLayerWebService) Stop(ctx context.Context) error {
	return ws.e.Shutdown(ctx)
}

// TODO mechanism to add health checks from layer code
func (ws *dataLayerWebService) health(c echo.Context) error {
	return c.String(http.StatusOK, "UP")
}

func (ws *dataLayerWebService) postEntities(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.core.Logger.Info(fmt.Sprintf("POST to dataset %s", datasetName))
	ds := ws.datalayerService.GetDataset(datasetName)
	if ds == nil {
		ws.core.Logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return echo.NewHTTPError(http.StatusNotFound, "dataset not found")
	}
	mappings := ws.core.config.GetDatasetDefinition(datasetName).Mappings
	mapper := NewDefaultItemMapper(mappings)
	parser := egdm.NewEntityParser(egdm.NewNamespaceContext())
	// if stripProps is enabled, the producers service will strip all namespace prefixes from the properties
	if !ws.core.config.GetDatasetDefinition(datasetName).StripProps() {
		// if it is NOT enabled, we will expand all namespace prefixes in the entity parser
		parser = parser.WithExpandURIs()
	}
	err := parser.Parse(c.Request().Body, func(entity *egdm.Entity) error {
		item := mapper.EntityToItem(entity)
		err2 := ds.WriteItem(item)
		if err2 != nil {
			return err2
		}

		return nil
	}, nil)
	if err != nil {
		ws.core.Logger.Warn(err.Error())
		return echo.NewHTTPError(http.StatusBadRequest, "could not parse the json payload")
	}

	return c.NoContent(http.StatusOK)
}

func (ws *dataLayerWebService) getEntities(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.core.Logger.Info(fmt.Sprintf("GET entities for dataset %s", datasetName))
	ds := ws.datalayerService.GetDataset(datasetName)
	if ds == nil {
		ws.core.Logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return echo.NewHTTPError(http.StatusNotFound, "dataset not found")
	}
	entityIterator, err := ws.datalayerService.GetDataset(datasetName).GetEntities("", 10000)
	err = ws.responseOut(c, entityIterator)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return nil
}

func (ws *dataLayerWebService) getChanges(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.core.Logger.Info(fmt.Sprintf("GET changes for dataset %s", datasetName))
	ds := ws.datalayerService.GetDataset(datasetName)
	if ds == nil {
		ws.core.Logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return echo.NewHTTPError(http.StatusNotFound, "dataset not found")
	}

	entityIterator, err := ws.datalayerService.GetDataset(datasetName).GetChanges("", 10000, false)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	err = ws.responseOut(c, entityIterator)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return nil
}

func (ws *dataLayerWebService) responseOut(c echo.Context, entityIterator EntityIterator) error {
	for {
		entity := entityIterator.Next()
		//fmt.Println(entity, entity == nil)
		if entity == nil {
			break
		}
		b, err := json.Marshal(entity)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		_, err = c.Response().Write(b)
		_, err = c.Response().Write([]byte(",\n"))

		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}
	return nil
}

func (ws *dataLayerWebService) listDatasets(c echo.Context) error {
	ws.core.Logger.Info("listing datasets")
	b, err := json.Marshal(ws.datalayerService.ListDatasetNames())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	_, err = c.Response().Write(b)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return nil
}
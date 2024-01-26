package common_datalayer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type dataLayerWebService struct {
	// service specific service core
	datalayerService DataLayerService
	e                *echo.Echo
	metrics          Metrics
	logger           Logger
	config           *Config
}

func newDataLayerWebService(config *Config, logger Logger, metrics Metrics, dataLayerService DataLayerService) (*dataLayerWebService, error) {
	e := echo.New()
	e.HideBanner = true

	mw(logger, metrics, e)

	s := &dataLayerWebService{config: config, logger: logger, metrics: metrics, datalayerService: dataLayerService, e: e}

	e.GET("/health", s.health)
	e.POST("/datasets/:dataset/entities", s.postEntities)
	e.GET("/datasets/:dataset/entities", s.getEntities)
	e.GET("/datasets/:dataset/changes", s.getChanges)
	e.GET("/datasets", s.listDatasets)

	return s, nil
}

// wrap all handlers with middleware
func mw(logger Logger, metrics Metrics, e *echo.Echo) {
	skipper := func(c echo.Context) bool {
		// skip health check
		return strings.HasPrefix(c.Request().URL.Path, "/health")
	}
	e.Use(
		// Request logging and HTTP metrics
		func(next echo.HandlerFunc) echo.HandlerFunc {
			// service := core.Config.SystemConfig.ServiceName()
			return func(c echo.Context) error {
				if skipper(c) {
					return next(c)
				}

				start := time.Now()
				tags := []string{
					// fmt.Sprintf("application:%s", service),
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
							logger.Warn(msg)
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

				err = metrics.Incr("http.count", tags, 1)
				err = metrics.Timing("http.time", timed, tags, 1)
				err = metrics.Gauge("http.size", float64(c.Response().Size), tags, 1)
				if err != nil {
					logger.Warn("Error with metrics", "error", err.Error())
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

				logger.Info(msg, args...)

				return nil
			}
		})
}

func (ws *dataLayerWebService) Start() error {
	port := ws.config.LayerServiceConfig.Port
	ws.logger.Info(fmt.Sprintf("Starting Http server on :%s", port))
	go func() {
		_ = ws.e.Start(":" + port.String())
	}()

	return nil
}

func (ws *dataLayerWebService) Stop(ctx context.Context) error {
	return ws.e.Shutdown(ctx)
}

// TODO mechanism to add health checks from layer code
func (ws *dataLayerWebService) health(c echo.Context) error {
	return c.String(http.StatusOK, "running")
}

func getBoolFromString(s string) bool {
	return strings.ToLower(s) == "true"
}

func (ws *dataLayerWebService) postEntities(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.logger.Info(fmt.Sprintf("POST to dataset %s", datasetName))
	ds, err := ws.datalayerService.Dataset(datasetName)
	if err != nil {
		ws.logger.Warn(err.Error())
		return echo.NewHTTPError(http.StatusBadRequest, "could not find dataset "+datasetName)
	}
	if ds == nil {
		ws.logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return echo.NewHTTPError(http.StatusNotFound, "dataset not found")
	}

	// get UDA full sync headers
	udaFullSyncStart := c.Request().Header.Get("universal-data-api-full-sync-start")
	udaFullSyncEnd := c.Request().Header.Get("universal-data-api-full-sync-end")
	udaFullSyncId := c.Request().Header.Get("universal-data-api-full-sync-id")

	// make batch info
	var batchInfo BatchInfo

	if udaFullSyncId != "" {
		batchInfo = BatchInfo{
			IsStartBatch: getBoolFromString(udaFullSyncStart),
			IsLastBatch:  getBoolFromString(udaFullSyncEnd),
			SyncId:       udaFullSyncId,
		}
	}

	var writer DatasetWriter

	if udaFullSyncId != "" {
		writer, err = ds.FullSync(context.Background(), batchInfo)
	} else {
		writer, err = ds.Incremental(context.Background())
	}
	if err != nil {
		ws.logger.Warn(err.Error())
		return echo.NewHTTPError(http.StatusInternalServerError, "could not create dataset writer")
	}

	parser := egdm.NewEntityParser(egdm.NewNamespaceContext())
	parser.WithExpandURIs()

	err2 := parser.Parse(c.Request().Body, func(entity *egdm.Entity) error {
		err3 := writer.Write(entity)
		if err3 != nil {
			return err3.Underlying()
		}

		return nil
	}, nil)

	if err2 != nil {
		ws.logger.Warn(err2.Error())
		return echo.NewHTTPError(http.StatusBadRequest, "could not parse the json payload")
	}

	err = writer.Close()
	if err != nil {
		ws.logger.Warn(err.Error())
		return echo.NewHTTPError(http.StatusInternalServerError, "could not close the dataset writer")
	}

	return c.NoContent(http.StatusOK)
}

func (ws *dataLayerWebService) getEntities(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.logger.Info(fmt.Sprintf("GET entities for dataset %s", datasetName))
	ds, err := ws.datalayerService.Dataset(datasetName)
	if err != nil {
		ws.logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return err.toHTTPError()
	}

	// get the from query param
	from := c.QueryParam("from")

	entityIterator, err := ds.Entities(from, 10000)
	err2 := ws.writeEntities(c, entityIterator)
	if err2 != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return nil
}

func (ws *dataLayerWebService) getChanges(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.logger.Info(fmt.Sprintf("GET changes for dataset %s", datasetName))
	ds, err := ws.datalayerService.Dataset(datasetName)
	if err != nil {
		ws.logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return err.toHTTPError()
	}

	// get since query param
	since := c.QueryParam("since")

	// get the take param
	takeParam := c.QueryParam("take")
	take := 0
	if takeParam != "" {
		var perr error
		take, perr = strconv.Atoi(takeParam)
		if perr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "could not parse the take parameter")
		}
	}

	// get the latestOnly param
	latestOnly := getBoolFromString(c.QueryParam("latestOnly"))

	entityIterator, err := ds.Changes(since, take, latestOnly)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	err2 := ws.writeEntities(c, entityIterator)
	if err2 != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err2.Error())
	}

	return nil
}

func (ws *dataLayerWebService) writeEntities(c echo.Context, entityIterator EntityIterator) error {
	// write context
	_, err := c.Response().Write([]byte("[\n"))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	ctx := entityIterator.Context()
	if ctx == nil {
		// create empty context
		ctx = &egdm.Context{ID: "@context", Namespaces: make(map[string]string)}
	}

	// write out context
	b, err := json.Marshal(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	_, err = c.Response().Write(b)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	_, err = c.Response().Write([]byte(",\n"))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	// write out entities
	for {
		entity, lerr := entityIterator.Next()
		if lerr != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, lerr.Error())
		}

		if entity == nil {
			break
		}
		b, err2 := json.Marshal(entity)
		if err2 != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err2.Error())
		}
		_, err2 = c.Response().Write(b)
		if err2 != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err2.Error())
		}
		_, err2 = c.Response().Write([]byte(",\n"))
		if err2 != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err2.Error())
		}
	}

	// write out token
	token, lerr := entityIterator.Token()
	if lerr != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, lerr.Error())
	}
	if token != nil {
		b, err2 := json.Marshal(token)
		if err2 != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err2.Error())
		}
		_, err2 = c.Response().Write(b)
		if err2 != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err2.Error())
		}
	}

	// close array response
	_, err = c.Response().Write([]byte("]\n"))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return nil
}

func (ws *dataLayerWebService) listDatasets(c echo.Context) error {
	ws.logger.Info("listing datasets")
	b, err := json.Marshal(ws.datalayerService.DatasetDescriptions())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	_, err = c.Response().Write(b)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return nil
}

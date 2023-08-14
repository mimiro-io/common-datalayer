package common_datalayer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"
	egdm "github.com/mimiro-io/entity-graph-data-model"

	"github.com/mimiro-io/common-datalayer/core"
	"github.com/mimiro-io/common-datalayer/layer"
	"github.com/mimiro-io/common-datalayer/web"
)

type DataLayerWebService struct {
	// service specific service core
	datalayerService layer.DataLayerService

	core *core.Service
	e    *echo.Echo
}

func NewDataLayerWebService(core *core.Service, dataLayerService layer.DataLayerService) (*DataLayerWebService, error) {

	e := echo.New()
	e.HideBanner = true

	mw := web.NewMiddleware(core)
	e.Use(mw...)

	s := &DataLayerWebService{core: core, datalayerService: dataLayerService, e: e}

	e.GET("/health", s.health)
	e.POST("/datasets/:dataset/entities", s.postEntities)
	e.GET("/datasets/:dataset/entities", s.GetEntities)
	e.GET("/datasets/:dataset/changes", s.GetChanges)
	e.GET("/datasets", s.ListDatasets)

	return s, nil
}

func (ws *DataLayerWebService) Start() error {
	port := ws.core.Config.SystemConfig.HttpPort()
	ws.core.Logger.Info(fmt.Sprintf("Starting Http server on :%s", port))
	go func() {
		_ = ws.e.Start(":" + port)
	}()

	return nil
}

func (ws *DataLayerWebService) Stop(ctx context.Context) error {
	return ws.e.Shutdown(ctx)
}

func (ws *DataLayerWebService) Restart() error {
	return nil
}

// TODO mechanism to add health checks from layer code
func (ws *DataLayerWebService) health(c echo.Context) error {
	return c.String(http.StatusOK, "UP")
}

func (ws *DataLayerWebService) postEntities(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.core.Logger.Info(fmt.Sprintf("POST to dataset %s", datasetName))
	ds := ws.datalayerService.GetDataset(datasetName)
	if ds == nil {
		ws.core.Logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return echo.NewHTTPError(http.StatusNotFound, "dataset not found")
	}
	mappings := ws.core.Config.GetDatasetDefinition(datasetName).Mappings
	mapper := layer.NewDefaultItemMapper(mappings)
	parser := egdm.NewEntityParser(egdm.NewNamespaceContext())
	// if stripProps is enabled, the producers service will strip all namespace prefixes from the properties
	if !ws.core.Config.GetDatasetDefinition(datasetName).StripProps() {
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

func (ws *DataLayerWebService) GetEntities(c echo.Context) error {
	datasetName, _ := url.QueryUnescape(c.Param("dataset"))
	ws.core.Logger.Info(fmt.Sprintf("GET entities for dataset %s", datasetName))
	ds := ws.datalayerService.GetDataset(datasetName)
	if ds == nil {
		ws.core.Logger.Error(fmt.Sprintf("dataset not found: %s", datasetName))
		return echo.NewHTTPError(http.StatusNotFound, "dataset not found")
	}
	entityIterator, err := ws.datalayerService.GetDataset(datasetName).GetEntities("", 10000)
	err = ws.writeEntities(c, entityIterator)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return nil
}

func (ws *DataLayerWebService) GetChanges(c echo.Context) error {
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
	err = ws.writeEntities(c, entityIterator)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return nil
}

func (ws *DataLayerWebService) writeEntities(c echo.Context, entityIterator layer.EntityIterator) error {
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

func (ws *DataLayerWebService) ListDatasets(c echo.Context) error {
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
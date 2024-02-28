package main

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-uuid"
	layer "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/common-datalayer/encoder"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type FileSystemDataLayer struct {
	config   *layer.Config
	logger   layer.Logger
	metrics  layer.Metrics
	datasets map[string]*FileSystemDataset
}

func NewFileSystemDataLayer(conf *layer.Config, logger layer.Logger, metrics layer.Metrics) (layer.DataLayerService, error) {
	datalayer := &FileSystemDataLayer{config: conf, logger: logger, metrics: metrics}

	// initialize the datasets
	datalayer.datasets = make(map[string]*FileSystemDataset)

	// iterate dataset definitions in the config and create a dataset for each
	for _, dataset := range conf.DatasetDefinitions {
		datalayer.datasets[dataset.DatasetName] = &FileSystemDataset{name: dataset.DatasetName, datasetDefinition: dataset}
	}

	return datalayer, nil
}

func (dl *FileSystemDataLayer) Stop(ctx context.Context) error {
	// noop
	return nil
}

func (dl *FileSystemDataLayer) UpdateConfiguration(config *layer.Config) layer.LayerError {
	dl.config = config
	dl.datasets = make(map[string]*FileSystemDataset)

	// iterate dataset definitions in the config and create a dataset for each
	for _, dataset := range config.DatasetDefinitions {
		dl.datasets[dataset.DatasetName] = &FileSystemDataset{name: dataset.DatasetName, datasetDefinition: dataset}
	}

	return nil
}

func (dl *FileSystemDataLayer) Dataset(dataset string) (layer.Dataset, layer.LayerError) {
	ds := &FileSystemDataset{name: dataset}

	// TODO: load other config

	return ds, nil
}

func (dl *FileSystemDataLayer) DatasetDescriptions() []*layer.DatasetDescription {
	var datasetDescriptions []*layer.DatasetDescription

	// iterate over the datasest config and create one for each
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &layer.DatasetDescription{Name: key})
	}

	return datasetDescriptions
}

type FileSystemDataset struct {
	logger                      layer.Logger
	name                        string
	datasetDefinition           *layer.DatasetDefinition
	path                        string
	filePattern                 string
	supportSinceByFileTimestamp bool
	fullSyncFileName            string
	incrementalFileName         string
}

func (f FileSystemDataset) MetaData() map[string]any {
	return make(map[string]any)
}

func (f FileSystemDataset) Name() string {
	return f.name
}

func (f FileSystemDataset) FullSync(ctx context.Context, batchInfo layer.BatchInfo) (layer.DatasetWriter, layer.LayerError) {
	var file *os.File
	var err error
	filePath := filepath.Join(f.path, f.fullSyncFileName)
	if batchInfo.IsStartBatch {
		file, err = os.Create(filePath)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not create file %s", filePath), layer.LayerErrorInternal)
		}
	} else {
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not open file %s", filePath), layer.LayerErrorInternal)
		}
	}

	enc, err := encoder.NewItemWriter(f.datasetDefinition.SourceConfig, file, &batchInfo)
	factory, err := encoder.NewItemFactory(f.datasetDefinition.SourceConfig)
	mapper := layer.NewMapper(f.logger, f.datasetDefinition.IncomingMappingConfig, f.datasetDefinition.OutgoingMappingConfig)
	datasetWriter := &FileSystemDatasetWriter{logger: f.logger, enc: enc, mapper: mapper, factory: factory}

	return datasetWriter, nil
}

func (f FileSystemDataset) Incremental(ctx context.Context) (layer.DatasetWriter, layer.LayerError) {
	id, _ := uuid.GenerateUUID()
	partfileName := fmt.Sprintf("part-%s-%s", id, f.incrementalFileName)
	filePath := filepath.Join(f.path, partfileName)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not create file %s", filePath), layer.LayerErrorInternal)
	}

	enc, err := encoder.NewItemWriter(f.datasetDefinition.SourceConfig, file, nil)
	factory, err := encoder.NewItemFactory(f.datasetDefinition.SourceConfig)
	mapper := layer.NewMapper(f.logger, f.datasetDefinition.IncomingMappingConfig, f.datasetDefinition.OutgoingMappingConfig)
	datasetWriter := &FileSystemDatasetWriter{logger: f.logger, enc: enc, mapper: mapper, factory: factory}

	return datasetWriter, nil
}

type FileSystemDatasetWriter struct {
	logger  layer.Logger
	enc     encoder.ItemWriter
	factory encoder.ItemFactory
	mapper  *layer.Mapper
}

func (f FileSystemDatasetWriter) Write(entity *egdm.Entity) layer.LayerError {
	item := f.factory.NewItem()
	err := f.mapper.MapEntityToItem(entity, item)
	if err != nil {
		return layer.Err(fmt.Errorf("could not map entity to item because %s", err.Error()), layer.LayerErrorInternal)
	}

	err = f.enc.Write(item)
	if err != nil {
		return layer.Err(fmt.Errorf("could not write item to file because %s", err.Error()), layer.LayerErrorInternal)
	}

	return nil
}

func (f FileSystemDatasetWriter) Close() layer.LayerError {
	err := f.enc.Close()
	if err != nil {
		return layer.Err(fmt.Errorf("could not close file because %s", err.Error()), layer.LayerErrorInternal)
	}
	return nil
}

func (f FileSystemDataset) Changes(since string, limit int, latestOnly bool) (layer.EntityIterator, layer.LayerError) {
	// get root folder
	if _, err := os.Stat(f.path); os.IsNotExist(err) {
		return nil, layer.Err(fmt.Errorf("path %s does not exist", f.path), layer.LayerErrorBadParameter)
	}

	// get all files in the folder that match the file pattern
	files, err := os.ReadDir(f.path)
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not read directory %s", f.path), layer.LayerErrorBadParameter)
	}

	dataFiles := make([]os.DirEntry, 0)
	for _, file := range files {
		fileName := file.Name()
		isMatch, err := filepath.Match(f.filePattern, fileName)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not match file pattern %s", f.filePattern), layer.LayerErrorInternal)
		}

		if isMatch {
			if f.supportSinceByFileTimestamp && since != "" {
				layout := "2006-01-02T15:04:05Z07:00"
				sinceTime, err := time.Parse(layout, since)
				finfo, err := file.Info()
				if err != nil {
					return nil, layer.Err(fmt.Errorf("could not get file info for %s", fileName), layer.LayerErrorInternal)
				}
				fileModTime := finfo.ModTime()
				if fileModTime.After(sinceTime) {
					dataFiles = append(dataFiles, file)
				}
			} else {
				dataFiles = append(dataFiles, file)
			}
		}
	}

	// if since defined using file timestamp, order files based on date, remove files older than since
	if len(dataFiles) > 0 {
		sort.Slice(dataFiles, func(i, j int) bool {
			f1, _ := files[i].Info()
			f2, _ := files[j].Info()
			return f1.ModTime().Before(f2.ModTime())
		})
	}

	mapper := layer.NewMapper(f.logger, nil, f.datasetDefinition.OutgoingMappingConfig)
	iterator := NewFileCollectionEntityIterator(f.datasetDefinition.SourceConfig, f.path, dataFiles, mapper, "")
	return iterator, nil
}

func (f FileSystemDataset) Entities(from string, limit int) (layer.EntityIterator, layer.LayerError) {
	// get root folder
	if _, err := os.Stat(f.path); os.IsNotExist(err) {
		return nil, layer.Err(fmt.Errorf("path %s does not exist", f.path), layer.LayerErrorBadParameter)
	}

	// get all files in the folder that match the file pattern
	files, err := os.ReadDir(f.path)
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not read directory %s", f.path), layer.LayerErrorBadParameter)
	}

	dataFiles := make([]os.DirEntry, 0)
	for _, file := range files {
		fileName := file.Name()
		isMatch, err := filepath.Match(f.filePattern, fileName)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not match file pattern %s", f.filePattern), layer.LayerErrorInternal)
		}
		if isMatch {
			dataFiles = append(dataFiles, file)
		}
	}

	mapper := layer.NewMapper(f.logger, nil, f.datasetDefinition.OutgoingMappingConfig)
	iterator := NewFileCollectionEntityIterator(f.datasetDefinition.SourceConfig, f.path, dataFiles, mapper, "")
	return iterator, nil
}

func NewFileCollectionEntityIterator(sourceConfig map[string]any, path string, files []os.DirEntry, mapper *layer.Mapper, token string) *FileCollectionEntityIterator {
	return &FileCollectionEntityIterator{sourceConfig: sourceConfig, mapper: mapper, token: token, path: path, files: files, filesIndex: 0}
}

type FileCollectionEntityIterator struct {
	mapper            *layer.Mapper
	token             string
	path              string
	files             []os.DirEntry
	filesIndex        int
	currentItemReader encoder.ItemIterator
	sourceConfig      map[string]any
}

func (f *FileCollectionEntityIterator) Context() *egdm.Context {
	ctx := egdm.NewNamespaceContext()
	return ctx.AsContext()
}

func (f *FileCollectionEntityIterator) Next() (*egdm.Entity, layer.LayerError) {
	if f.currentItemReader == nil {
		if f.filesIndex < len(f.files) {
			// initialize the current file entity iterator
			file := filepath.Join(f.path, f.files[f.filesIndex].Name())
			itemReader, err := f.NewItemReadCloser(file, f.sourceConfig)
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not create item reader for file %s becuase %s", file, err.Error()), layer.LayerErrorInternal)
			}

			f.currentItemReader = itemReader
		}
	}

	// read the next entity from the current file
	item, err := f.currentItemReader.Read()
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not read item from file because %s", err.Error()), layer.LayerErrorInternal)
	}

	if item == nil {
		// close the current file and move to the next
		err := f.currentItemReader.Close()
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not close item reader for file because %s", err.Error()), layer.LayerErrorInternal)
		}
		f.filesIndex++
		if f.filesIndex < len(f.files) {
			file := filepath.Join(f.path, f.files[f.filesIndex].Name())
			itemReader, err := f.NewItemReadCloser(file, f.sourceConfig)
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not create item reader for file %s becuase %s", file, err.Error()), layer.LayerErrorInternal)
			}

			f.currentItemReader = itemReader
			item, err = f.currentItemReader.Read()
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not read item from file because %s", err.Error()), layer.LayerErrorInternal)
			}
		}
	}

	if item == nil {
		return nil, nil
	} else {
		entity := &egdm.Entity{Properties: make(map[string]any)}
		err := f.mapper.MapItemToEntity(item, entity)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not map item to entity because %s", err.Error()), layer.LayerErrorInternal)
		}
		return entity, nil
	}
}

func (f *FileCollectionEntityIterator) NewItemReadCloser(filePath string, sourceConfig map[string]any) (encoder.ItemIterator, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not open file %s", filePath), layer.LayerErrorInternal)
	}

	// get encoder for the file
	itemReader, err := encoder.NewItemIterator(sourceConfig, file)
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not create encoder specified in dataset source config"), layer.LayerErrorBadParameter)
	}

	return itemReader, nil
}

func (f *FileCollectionEntityIterator) Token() (*egdm.Continuation, layer.LayerError) {
	cont := egdm.NewContinuation()
	cont.Token = f.token
	return cont, nil
}

func (f *FileCollectionEntityIterator) Close() layer.LayerError {
	err := f.currentItemReader.Close()
	if err != nil {
		return layer.Err(fmt.Errorf("could not close item reader because %s", err.Error()), layer.LayerErrorInternal)
	}
	return nil
}

package importer

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

// Handler is the interface importers use to handle processing the event.
type Handler interface {
	// CreateDatasource allows the handler to create the datasource before importing data.
	CreateDatasource(schema internal.SchemaMap) error

	// ImportEvent allows the handler to process the event.
	ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error

	// ImportCompleted is called when all events have been processed.
	ImportCompleted() error
}

// Run will import data from the importer configuration and call the handler to handle the event.
func Run(logger logger.Logger, config internal.ImporterConfig, handler Handler) error {
	files, err := util.ListDir(config.DataDir)
	if err != nil {
		return fmt.Errorf("unable to list files in directory: %w", err)
	}
	schema, err := config.SchemaRegistry.GetLatestSchema()
	if err != nil {
		return fmt.Errorf("unable to get schema: %w", err)
	}
	started := time.Now()
	if err := handler.CreateDatasource(schema); err != nil {
		return err
	}
	var total int
	for _, file := range files {
		table, tv, ok := util.ParseCRDBExportFile(file)
		if !ok {
			logger.Debug("skipping file: %s", file)
			continue
		}
		if !util.SliceContains(config.Tables, table) {
			continue
		}
		data := schema[table]
		if data == nil {
			return fmt.Errorf("unexpected table (%s) not found in schema but in import directory: %s", table, file)
		}
		logger.Debug("processing file: %s, table: %s", file, table)
		dec, err := util.NewNDJSONDecoder(file)
		if err != nil {
			return fmt.Errorf("unable to create JSON decoder for %s: %w", file, err)
		}
		defer dec.Close()
		var count int
		tstarted := time.Now()
		for dec.More() {
			var event internal.DBChangeEvent
			event.Operation = "INSERT"
			event.Table = table
			event.Timestamp = tv.UnixMilli()
			event.MVCCTimestamp = fmt.Sprintf("%v", tv.UnixNano())
			event.ID = util.Hash(filepath.Base(file))
			event.ModelVersion = schema[table].ModelVersion
			if err := dec.Decode(&event.After); err != nil {
				return fmt.Errorf("unable to decode JSON: %w", err)
			}
			event.Key = []string{event.GetPrimaryKey()}
			event.Imported = true
			count++
			if err := handler.ImportEvent(event, data); err != nil {
				return err
			}
		}
		if err := dec.Close(); err != nil {
			return err
		}
		total += count
		logger.Debug("imported %d %s records in %s", count, table, time.Since(tstarted))
	}

	if err := handler.ImportCompleted(); err != nil {
		return err
	}

	logger.Info("imported %d records from %d files in %s", total, len(files), time.Since(started))
	return nil
}

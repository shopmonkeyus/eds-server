package file

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/importer"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type fileDriver struct {
	config       internal.DriverConfig
	logger       logger.Logger
	dir          string
	importConfig internal.ImporterConfig
}

var _ internal.Driver = (*fileDriver)(nil)
var _ internal.DriverLifecycle = (*fileDriver)(nil)
var _ internal.DriverHelp = (*fileDriver)(nil)
var _ internal.Importer = (*fileDriver)(nil)
var _ internal.ImporterHelp = (*fileDriver)(nil)
var _ importer.Handler = (*fileDriver)(nil)

func (p *fileDriver) GetPathFromURL(urlString string) (string, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return "", fmt.Errorf("unable to parse url: %w", err)
	}

	if u.Path == "" {
		return "", fmt.Errorf("path is required in url which should be the directory to store files")
	} else {
		if u.Path[0:1] == "/" {
			p.dir = u.Path
		} else {
			p.dir, err = filepath.Abs(p.dir)
			if err != nil {
				return "", fmt.Errorf("unable to get absolute path for %s: %w", p.dir, err)
			}
		}
		if !util.Exists(p.dir) {
			if err := os.MkdirAll(p.dir, 0755); err != nil {
				return "", fmt.Errorf("unable to create directory: %w", err)
			}
		}
	}
	return p.dir, nil
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *fileDriver) Start(pc internal.DriverConfig) error {
	p.config = pc
	p.logger = pc.Logger.WithPrefix("[file]")
	if _, err := p.GetPathFromURL(pc.URL); err != nil {
		return err
	}
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *fileDriver) Stop() error {
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *fileDriver) MaxBatchSize() int {
	return -1
}

func (p *fileDriver) getFileName(table string, ts time.Time, id string) string {
	return fmt.Sprintf("%s/%d-%s.json", table, ts.Unix(), id)
}

func (p *fileDriver) writeEvent(logger logger.Logger, event internal.DBChangeEvent, dryRun bool) error {
	key := p.getFileName(event.Table, time.UnixMilli(event.Timestamp), event.GetPrimaryKey())
	buf := []byte(util.JSONStringify(event))
	fp := filepath.Join(p.dir, key)
	if !dryRun {
		dir := filepath.Dir(fp)
		if !util.Exists(dir) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("unable to create directory: %w", err)
			}
		}
		if err := os.WriteFile(fp, buf, 0644); err != nil {
			return fmt.Errorf("unable to write file: %w", err)
		}
		logger.Trace("stored %s", fp)
	} else {
		logger.Trace("would have stored %s", fp)
	}
	return nil
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *fileDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	if err := p.writeEvent(logger, event, false); err != nil {
		return false, err
	}
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *fileDriver) Flush(logger logger.Logger) error {
	return nil
}

// Name is a unique name for the driver.
func (p *fileDriver) Name() string {
	return "File"
}

// Description is the description of the driver.
func (p *fileDriver) Description() string {
	return "Supports streaming EDS messages to local filesystem directory."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *fileDriver) ExampleURL() string {
	return "file://folder"
}

// Help should return a detailed help documentation for the driver.
func (p *fileDriver) Help() string {
	var help strings.Builder
	help.WriteString("Provide a directory in the URL path to store events into this folder.\n")
	return help.String()
}

// CreateDatasource allows the handler to create the datasource before importing data.
func (p *fileDriver) CreateDatasource(schema internal.SchemaMap) error {
	return nil
}

// ImportEvent allows the handler to process the event.
func (p *fileDriver) ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error {
	return p.writeEvent(p.logger, event, p.importConfig.DryRun)
}

// ImportCompleted is called when all events have been processed.
func (p *fileDriver) ImportCompleted() error {
	return nil
}

func (p *fileDriver) Import(config internal.ImporterConfig) error {
	if config.SchemaOnly {
		return nil
	}
	p.logger = config.Logger.WithPrefix("[file]")
	if _, err := p.GetPathFromURL(config.URL); err != nil {
		return err
	}
	p.importConfig = config
	return importer.Run(p.logger, config, p)
}

// SupportsDelete returns true if the importer supports deleting data.
func (p *fileDriver) SupportsDelete() bool {
	return false
}

// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
func (p *fileDriver) Test(ctx context.Context, logger logger.Logger, url string) error {
	_, err := p.GetPathFromURL(url)
	return err
}

// Configuration returns the configuration fields for the driver.
func (p *fileDriver) Configuration() []internal.DriverField {
	return []internal.DriverField{
		internal.RequiredStringField("Directory", "The directory on the server to store files", nil),
	}
}

// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
func (p *fileDriver) Validate(values map[string]any) (string, []internal.FieldError) {
	dir := internal.GetRequiredStringValue("Directory", values)
	if dir == "/" {
		return "", []internal.FieldError{internal.NewFieldError("Directory", "cannot be the root directory")}
	}
	absdir, err := filepath.Abs(dir)
	if err != nil {
		return "", []internal.FieldError{internal.NewFieldError("Directory", err.Error())}
	}
	if !util.Exists(absdir) {
		parent := filepath.Dir(absdir)
		ok, err := util.IsDirWritable(parent)
		if err != nil {
			return "", []internal.FieldError{internal.NewFieldError("Directory", fmt.Sprintf("error checking for parent %s directory permission: %s", parent, err))}
		}
		if !ok {
			return "", []internal.FieldError{internal.NewFieldError("Directory", fmt.Sprintf("%s directory isn't writable and directory currently does not exist: %s", parent, err))}
		}
	} else {
		ok, err := util.IsDirWritable(absdir)
		if err != nil {
			return "", []internal.FieldError{internal.NewFieldError("Directory", fmt.Sprintf("error checking directory permission: %s", err))}
		}
		if !ok {
			return "", []internal.FieldError{internal.NewFieldError("Directory", fmt.Sprintf("%s directory isn't writable", absdir))}
		}
	}
	return "file://" + filepath.ToSlash(absdir), nil
}

func init() {
	internal.RegisterDriver("file", &fileDriver{})
	internal.RegisterImporter("file", &fileDriver{})
}

package provider

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
)

type FileProvider struct {
	logger internal.Logger
	dir    string
}

var _ internal.Provider = (*FileProvider)(nil)

// NewFileProvider returns a provider that will stream files to a folder provided in the url
func NewFileProvider(logger internal.Logger, urlstring string) (internal.Provider, error) {
	u, err := url.Parse(urlstring)
	if err != nil {
		return nil, err
	}
	dir := u.Path
	if dir == "/" {
		return nil, fmt.Errorf("refusing to save files in the root directory. please choose a path")
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("couldn't create directory: %s", dir)
		}
	}
	logger.Info("file provider will save files to: %s", dir)
	return &FileProvider{
		logger,
		dir,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *FileProvider) Start() error {
	return nil
}

// Stop the provider and return an error or nil if ok
func (p *FileProvider) Stop() error {
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *FileProvider) Process(data datatypes.ChangeEventPayload) error {
	fn := path.Join(p.dir, data.GetTable()+"_"+data.GetMvccTimestamp()+"_"+data.GetID()+".json.gz")
	f, err := os.Open(fn)
	if err != nil {
		return fmt.Errorf("error opening file: %s. %s", fn, err)
	}
	defer f.Close()
	w := gzip.NewWriter(f)
	buf, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return fmt.Errorf("error converting to json: %s", err)
	}
	w.Write(buf)
	w.Flush()
	return nil
}

// Migrate will tell the provider to do any migration work and return an error or nil if ok
func (p *FileProvider) Migrate() error {
	return nil
}
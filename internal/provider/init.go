package provider

import (
	"fmt"
	"net/url"

	"github.com/shopmonkeyus/eds-server/internal"
)

func parseURLForProvider(urlstring string) (string, error) {
	u, err := url.Parse(urlstring)
	if err != nil {
		return "", err
	}
	return u.Scheme, nil
}

// NewProviderForURL will return a new internal.Provider for the driver based on the url
func NewProviderForURL(logger internal.Logger, url string) (internal.Provider, error) {
	driver, err := parseURLForProvider(url)
	if err != nil {
		return nil, err
	}
	switch driver {
	case "postgresql", "mysql", "sqlserver", "sqlite", "clickhouse":
		return NewGormProvider(logger, url)
	case "file":
		return NewFileProvider(logger, url)
	default:
		return nil, fmt.Errorf("no suitable provider found for url: %s", url)
	}
}
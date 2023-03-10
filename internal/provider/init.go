package provider

import (
	"fmt"
	"net/url"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

func parseURLForProvider(urlstring string) (string, error) {
	u, err := url.Parse(urlstring)
	if err != nil {
		return "", err
	}
	return u.Scheme, nil
}

type ProviderOpts struct {
	DryRun bool
}

// NewProviderForURL will return a new internal.Provider for the driver based on the url
func NewProviderForURL(logger logger.Logger, url string, opts *ProviderOpts) (internal.Provider, error) {
	driver, err := parseURLForProvider(url)
	if err != nil {
		return nil, err
	}
	switch driver {
	case "postgresql", "mysql", "sqlserver", "sqlite", "clickhouse":
		return NewGormProvider(logger, url, opts)
	case "file":
		return NewFileProvider(logger, url, opts)
	case "nats":
		return NewNatsProvider(logger, url, opts)
	default:
		return nil, fmt.Errorf("no suitable provider found for url: %s", url)
	}
}

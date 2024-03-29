package provider

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/shopmonkeyus/eds-server/internal"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
)

func parseURLForProvider(urlstring string) (string, *url.URL, error) {
	u, err := url.Parse(urlstring)
	if err != nil {
		return "", nil, err
	}
	return u.Scheme, u, nil
}

func convertSnowflakeConnectionString(urlString string) (string, error) {
	if strings.HasPrefix(urlString, "snowflake://") {
		return strings.Replace(urlString, "snowflake://", "", 1), nil
	}
	return "", fmt.Errorf("invalid snowflake connection string: %s", urlString)
}

type ProviderOpts struct {
	DryRun   bool
	Verbose  bool
	Importer string
}

// NewProviderForURL will return a new internal.Provider for the driver based on the url
func NewProviderForURL(logger logger.Logger, urlstr string, schemaModelVersionCache *map[string]dm.Model, opts *ProviderOpts) (internal.Provider, error) {

	driver, u, err := parseURLForProvider(urlstr)
	if err != nil {
		return nil, err
	}
	switch driver {
	case "file":
		qs := u.Query()
		args := []string{u.Path}
		for k, v := range qs {
			args = append(args, k)
			if len(v) > 0 {
				args = append(args, v...)
			}
		}
		return NewFileProvider(logger, args, schemaModelVersionCache, opts)
	case "postgresql":
		return NewPostgresProvider(logger, urlstr, schemaModelVersionCache, opts)
	case "sqlserver":
		return NewSqlServerProvider(logger, urlstr, schemaModelVersionCache, opts)
	case "snowflake":
		urlstr, err := convertSnowflakeConnectionString(urlstr)
		if err != nil {
			return nil, err
		}
		return NewSnowflakeProvider(logger, urlstr, schemaModelVersionCache, opts)

	default:
		return nil, fmt.Errorf("no suitable provider found for url: %s", urlstr)
	}
}

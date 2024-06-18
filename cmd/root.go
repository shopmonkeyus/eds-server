package cmd

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/provider"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
)

func mustFlagBool(cmd *cobra.Command, name string, required bool) bool {
	val, err := cmd.Flags().GetBool(name)
	if required && err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(1)
	}
	return val
}

func mustFlagString(cmd *cobra.Command, name string, required bool) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(1)
	}
	if required && val == "" {
		fmt.Printf("error: required flag --%s missing\n", name)
		os.Exit(1)
	}
	return val
}

type ProviderFunc func(p []internal.Provider) error

func runLocalProvider(logger logger.Logger, natsProvider internal.Provider, fn ProviderFunc, nc *nats.Conn) {
	providers := []internal.Provider{natsProvider}
	ferr := fn(providers)
	if ferr != nil {
		for _, provider := range providers {
			provider.Stop()
			logger.Error("error: %s", ferr)
			os.Exit(1)
		}
	}

	for _, provider := range providers {
		if err := provider.Stop(); err != nil {
			logger.Error("error stopping provider: %s", err)
			os.Exit(1)
		}
	}

}

func runProviders(logger logger.Logger, urls []string, schemaModelCache *map[string]dm.Model, dryRun bool, verbose bool, importer string, fn ProviderFunc, nc *nats.Conn) {
	opts := &provider.ProviderOpts{
		DryRun:   dryRun,
		Verbose:  verbose,
		Importer: importer,
	}
	providers := []internal.Provider{}
	for _, url := range urls {
		provider, err := provider.NewProviderForURL(logger, url, schemaModelCache, opts)
		if err != nil {
			logger.Error("error creating provider: %s", err)
			os.Exit(1)
		}
		if err := provider.Start(); err != nil {
			logger.Error("error starting provider: %s", err)
			os.Exit(1)
		}

		providers = append(providers, provider)
	}
	if importer != "" {
		//opts.Importer should be the location of a file directory, get all the files from this directory
		files, err := util.ListDir(opts.Importer)
		//Can potentially run on goroutines to process multiple files at once, but performance seems ok right now
		for _, file := range files {
			err = processFile(logger, file, providers, nc)
			if err != nil {
				logger.Error("error processing file: %s", err)
				os.Exit(1)
			}
		}

		if err != nil {
			logger.Error("error reading directory: %s", err)
			os.Exit(1)
		}
		logger.Info("Imported file data instead of streaming")
		os.Exit(0)
	}

	ferr := fn(providers)
	if ferr != nil {
		for _, provider := range providers {
			provider.Stop()
			logger.Error("error: %s", ferr)
			os.Exit(1)
		}
	}
	for _, provider := range providers {
		if err := provider.Stop(); err != nil {
			logger.Error("error stopping provider: %s", err)
			os.Exit(1)
		}
	}
}

func processFile(logger logger.Logger, fileName string, providers []internal.Provider, nc *nats.Conn) error {
	logger.Info("processing file: %s", fileName)
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	tableName, err := util.GetTableNameFromPath(fileName)
	if err != nil {
		return err
	}
	logger.Info("importing table: %s", tableName)
	scanner := bufio.NewScanner(gzipReader)
	for scanner.Scan() {
		data := scanner.Bytes()
		var dataMap map[string]interface{}
		if err := json.Unmarshal(data, &dataMap); err != nil {
			err = fmt.Errorf("error unmarshalling data: %s", err)
			return err

		}

		for _, provider := range providers {

			err = provider.Import(dataMap, tableName, nc)
			if err != nil {
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func newLogger(cmd *cobra.Command) logger.Logger {
	return logger.NewConsoleLogger()
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:  "eds-server",
	Long: "Shopmonkey Enterprise Data Streaming server",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("url", "", "the connection string")
	rootCmd.PersistentFlags().Bool("verbose", false, "turn on verbose logging")
	rootCmd.PersistentFlags().Bool("silent", false, "turn off all logging")
	rootCmd.PersistentFlags().Bool("nats-provider", false, "the connection string")
}

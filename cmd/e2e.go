//go:build e2e

package cmd

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/shopmonkeyus/eds-server/e2e"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
	v3 "github.com/shopmonkeyus/go-datamodel/v3"
	"github.com/spf13/cobra"
)

var allDrivers = []string{"postgresql", "cockroach", "sqlserver", "file"}

var e2eCmd = &cobra.Command{
	Use:   "e2e [driver]",
	Short: "Run the end-to-end test suite",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		started := time.Now()
		cwd, _ := os.Getwd()
		testdata := path.Join(cwd, "/testdata/")
		orders := []string{
			"order_1672694210689374000_8c373a977b7d6117.json",
			"order_1672694241263716000_d8d72eab8c688320.json",
		}
		drivers := allDrivers
		if len(args) > 0 {
			drivers = []string{args[0]}
		}
		for _, driver := range drivers {
			logger.Info("%s test starting", driver)
			lstarted := time.Now()
			runner, err := e2e.NewTestProviderRunner(logger, driver)
			if err != nil {
				logger.Error("error creating %s test provider: %s", driver, err)
				os.Exit(1)
			}
			if err := runner.Start(); err != nil {
				logger.Error("error starting %s test provider: %s", driver, err)
				os.Exit(1)
			}
			url := runner.URL()
			defer runner.Stop()

			runProvider(logger, url, false, func(provider internal.Provider) error {
				if err := provider.Migrate(); err != nil {
					return err
				}
				for _, relfn := range orders {
					fn := path.Join(testdata, relfn)
					buf, err := os.ReadFile(fn)
					if err != nil {
						runner.Stop()
						return fmt.Errorf("error reading test file: %s. %s", fn, err)
					}
					object, err := v3.NewFromChangeEvent("order", buf, path.Ext(fn) == ".gz")
					if err != nil {
						runner.Stop()
						return fmt.Errorf("error deserializing file: %s. %s", fn, err)
					}
					data := object.(datatypes.ChangeEventPayload)
					if err := provider.Process(data); err != nil {
						runner.Stop()
						return fmt.Errorf("error processing file: %s. %s", fn, err)
					}
					if err := runner.Validate(data); err != nil {
						runner.Stop()
						return err
					}
				}
				return nil
			})
			if err := runner.Stop(); err != nil {
				logger.Error("error stopping %s test provider: %s", driver, err)
				os.Exit(1)
			}
			logger.Info("%s test completed, took %v", driver, time.Since(lstarted))
		}
		logger.Info("e2e completed, took %v", time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(e2eCmd)
}

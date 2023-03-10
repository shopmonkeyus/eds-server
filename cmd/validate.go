package cmd

import (
	"io"
	"os"
	"path"
	"regexp"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
	v3 "github.com/shopmonkeyus/go-datamodel/v3"
	"github.com/spf13/cobra"
)

var filePattern = regexp.MustCompile(`^(\w+)_\d+_\w+\.json(\.gz)?$`)

func processFile(logger logger.Logger, cmd *cobra.Command, name string, url string) {
	fn := path.Base(name)
	if !filePattern.MatchString(fn) {
		return
	}
	tok := filePattern.FindStringSubmatch(fn)
	model := tok[1]
	logger.Info("file: %s, model: %s", name, model)
	f, err := os.Open(name)
	if err != nil {
		logger.Error("error opening file: %s. %s", name, err)
		os.Exit(1)
	}
	buf, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		logger.Error("error reading file: %s. %s", name, err)
		os.Exit(1)
	}
	object, err := v3.NewFromChangeEvent(model, buf, path.Ext(fn) == ".gz")
	if err != nil {
		logger.Error("error deserializing file: %s. %s", name, err)
		os.Exit(1)
	}
	data := object.(datatypes.ChangeEventPayload)
	logger.Info("loaded: %s with object: %v", fn, object)

	if url != "" {
		dryRun := mustFlagBool(cmd, "dry-run", false)
		runProvider(logger, url, dryRun, func(provider internal.Provider) error {
			return provider.Process(data)
		})
	}
}

var validateCmd = &cobra.Command{
	Use:   "validate [file_or_dir]",
	Short: "Validate dump files and optionally load them into the db",
	Long: `Validate dump files and optionally load them into the db

You can validate files from a dump directory:

	go run . validate ./dump

You can validate a specific file by path:

	go run . validate ./dump/customer_location_connection_1672613089535087000_9c53baa609e55158.json.gz

You can validate and also load into the db:

	go run . validate ./dump/customer_location_connection_1672613089535087000_9c53baa609e55158.json.gz --url 'postgresql://root@localhost:26257/test?sslmode=disable'
`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		dir := args[0]
		url := mustFlagString(cmd, "url", false)
		ls, _ := os.Lstat(dir)
		if ls.IsDir() {
			files, err := os.ReadDir(dir)
			if err != nil {
				logger.Error("error reading files from: %s. %s", dir, err)
				os.Exit(1)
			}
			for _, file := range files {
				name := path.Join(dir, file.Name())
				processFile(logger, cmd, name, url)
			}
		} else {
			processFile(logger, cmd, dir, url)
		}
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
	validateCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
}

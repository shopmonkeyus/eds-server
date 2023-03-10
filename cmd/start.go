package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	snats "github.com/shopmonkeyus/go-common/nats"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the server",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger.Trace("connecting to nats server")

		hosts, _ := cmd.Flags().GetStringSlice("server")
		creds, _ := cmd.Flags().GetString("creds")

		if len(hosts) > 0 && creds == "" && strings.Contains(hosts[0], "nats.shopmonkey.cloud") {
			logger.Error("error: missing required credentials file. use --creds and specify the location of your credentials file")
			os.Exit(1)
		}

		natsurl := strings.Join(hosts, ",")

		var natsCredentials nats.Option
		companyID, _ := cmd.Flags().GetString("company-id")

		if creds != "" {
			if _, err := os.Stat(creds); os.IsNotExist(err) {
				logger.Error("error: invalid credential file: %s", creds)
				os.Exit(1)
			}
			buf, err := os.ReadFile(creds)
			if err != nil {
				logger.Error("error: reading credentials file: %s", err)
				os.Exit(1)
			}
			natsCredentials = nats.UserCredentials(creds)

			natsJWT, err := jwt.ParseDecoratedJWT(buf)
			if err != nil {
				logger.Error("error: parsing valid JWT: %s", err)
				os.Exit(1)
			}
			claim, err := jwt.DecodeUserClaims(natsJWT)
			if err != nil {
				logger.Error("error: decoding JWT claims: %s", err)
				os.Exit(1)
			}
			companyID = claim.Audience
			if companyID == "" {
				logger.Error("error: invalid JWT claim. missing audience")
				os.Exit(1)
			}
		}

		nc, err := snats.NewNats(logger, "eds-server-"+companyID, natsurl, natsCredentials)
		if err != nil {
			logger.Error("nats: %s", err)
			os.Exit(1)
		}
		defer nc.Close()
		url := mustFlagString(cmd, "url", false)
		dryRun := mustFlagBool(cmd, "dry-run", false)
		embedNats := mustFlagBool(cmd, "embed-nats", false)

		if url == "" && !embedNats {
			fmt.Printf("error: EDS Server requires either --url or --embed-nats missing\n")
			os.Exit(1)
		}
		var runProviderCallback func(internal.Provider) error = func(provider internal.Provider) error {
			logger.Trace("creating message processor")
			processor, err := internal.NewMessageProcessor(internal.MessageProcessorOpts{
				Logger:          logger,
				CompanyID:       companyID,
				Provider:        provider,
				NatsConnection:  nc,
				TraceNats:       mustFlagBool(cmd, "trace-nats", false),
				DumpMessagesDir: mustFlagString(cmd, "dump-dir", false),
				ConsumerPrefix:  mustFlagString(cmd, "consumer-prefix", false),
			})
			if err != nil {
				return err
			}
			defer processor.Stop()

			logger.Trace("starting message processor")

			if err := processor.Start(); err != nil {
				return fmt.Errorf("processor start: %s", err)
			}
			logger.Info("started message processor")
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
			<-c
			logger.Info("stopped message processor")
			return nil
		}

		var wg sync.WaitGroup

		if embedNats {
			opts := &server.Options{JetStream: true}
			ns, err := server.NewServer(opts)

			if err != nil {
				panic(err)
			}

			go ns.Start()

			for !ns.ReadyForConnections(4 * time.Second) {
				logger.Info("Waiting for nats server to start...")
			}

			logger.Info("Nats server started at url: %s", ns.ClientURL())

			wg.Add(1)
			go func() {
				defer wg.Done()
				runProvider(logger, ns.ClientURL(), dryRun, runProviderCallback)
			}()
		}
		if url != "" {
			wg.Add(1)
			go func() {
				defer wg.Done()
				runProvider(logger, url, dryRun, runProviderCallback)
			}()
		}
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
	startCmd.Flags().String("company-id", "", "the customer id")
	startCmd.Flags().MarkHidden("company-id") // only for testing, otherwise should come from the credentials file
	startCmd.Flags().Bool("trace-nats", false, "turn on lower level nats tracing")
	startCmd.Flags().String("dump-dir", "", "write each incoming message to this directory")
	startCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
	startCmd.Flags().StringSlice("server", []string{"nats://nats.shopmonkey.cloud"}, "the nats server url")
	startCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	startCmd.Flags().String("consumer-prefix", "", "a consumer group prefix to add to the name")
	startCmd.Flags().Bool("embed-nats", false, "Run a local nats instance as a sink")
}

package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
	snats "github.com/shopmonkeyus/go-common/nats"
)

var emptyJSON = []byte("{}")

const modelRequestTimeout = time.Duration(time.Second * 30)

type MessageProcessor struct {
	logger                  logger.Logger
	companyID               []string
	providers               []Provider
	conn                    *nats.Conn
	mainNATSConn            *nats.Conn
	js                      nats.JetStreamContext
	subscriber              []snats.Subscriber
	dumpMessagesDir         string
	consumerPrefix          string
	context                 context.Context
	cancel                  context.CancelFunc
	schemaModelVersionCache *map[string]dm.Model
	// consumerStartTime is the time to start the consumer
	// this is different from the duration because it depends on when we run the process.
	consumerStartTime time.Time
}

// MessageProcessorOpts is the options for the message processor
type MessageProcessorOpts struct {
	Logger                   logger.Logger
	CompanyID                []string
	Providers                []Provider
	NatsConnection           *nats.Conn
	MainNatsConnection       *nats.Conn
	DumpMessagesDir          string
	TraceNats                bool
	ConsumerPrefix           string
	SchemaModelVersionCache  *map[string]dm.Model
	ConsumerLookbackDuration time.Duration // ConsumerLookbackDuration is the duration to look back for messages
}

// NewMessageProcessor will create a new processor for a given customer id
func NewMessageProcessor(opts MessageProcessorOpts) (*MessageProcessor, error) {
	js, err := opts.NatsConnection.JetStream(&nats.ClientTrace{
		RequestSent: func(subj string, payload []byte) {
			if opts.TraceNats {
				opts.Logger.Trace("nats tx: %s: %s", subj, string(payload))
			}
		},
		ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
			if opts.TraceNats {
				opts.Logger.Trace("nats rx: %s: %s", subj, string(payload))
			}
		},
	})
	if err != nil {
		return nil, err
	}
	if opts.DumpMessagesDir != "" {
		if _, err := os.Stat(opts.DumpMessagesDir); os.IsNotExist(err) {
			if err := os.MkdirAll(opts.DumpMessagesDir, 0755); err != nil {
				return nil, fmt.Errorf("couldn't create directory: %s", opts.DumpMessagesDir)
			}
		}
	}

	consumerPrefix := opts.ConsumerPrefix
	startTime := time.Now().Add(-opts.ConsumerLookbackDuration)
	if opts.ConsumerLookbackDuration != 0 {
		consumerPrefix = fmt.Sprintf("%s-%d", opts.ConsumerPrefix, startTime.UnixMilli())
	}

	context, cancel := context.WithCancel(context.Background())
	processor := &MessageProcessor{
		logger:                  opts.Logger.WithPrefix("[nats]"),
		companyID:               opts.CompanyID,
		providers:               opts.Providers,
		conn:                    opts.NatsConnection,
		mainNATSConn:            opts.MainNatsConnection,
		dumpMessagesDir:         opts.DumpMessagesDir,
		consumerPrefix:          consumerPrefix,
		js:                      js,
		context:                 context,
		cancel:                  cancel,
		schemaModelVersionCache: opts.SchemaModelVersionCache,
	}
	if opts.ConsumerLookbackDuration != 0 {
		processor.consumerStartTime = startTime
	}

	return processor, nil
}

// callback processes db change events
func (p *MessageProcessor) callback(ctx context.Context, payload []byte, msg *nats.Msg) error {
	tok := strings.Split(msg.Subject, ".")
	model := tok[1]

	msgid := msg.Header.Get("Nats-Msg-Id")
	encoding := msg.Header.Get("content-encoding")
	gzipped := encoding == "gzip/json"
	p.logger.Trace("received msgid: %s, subject: %s", msgid, msg.Subject)

	// unpack as json based change  event
	data, err := datatypes.FromChangeEvent(msg.Data, gzipped)
	if err != nil {
		if gzipped {
			dec, _ := datatypes.Gunzip(msg.Data)
			p.logger.Error("decode error for change event: %s. %s", string(dec), err)
		} else {
			p.logger.Error("decode error for change event: %s. %s", string(msg.Data), err)
		}
		msg.AckSync()
		return nil
	}
	data.MsgId = msgid
	dumpFiles := p.dumpMessagesDir != ""
	if dumpFiles {
		ext := ".json"
		if gzipped {
			ext += ".gz"
		}
		fn := path.Join(p.dumpMessagesDir, fmt.Sprintf("%s_%v_%s%s", model, time.Now().UnixNano(), msgid, ext))
		os.WriteFile(fn, msg.Data, 0644)
	}

	var schema dm.Model

	modelVersion := data.GetModelVersion()
	modelVersionId := fmt.Sprintf("%s-%s", model, modelVersion)
	p.logger.Trace("got modelVersionId for: %s %s %s for msgid: %s", modelVersionId, model, modelVersion, msgid)

	currentModelVersion, found := (*p.schemaModelVersionCache)[modelVersionId]

	if found {
		schema = currentModelVersion
		p.logger.Trace("found cached modelVersion for: %s for msgid: %s", modelVersionId, msgid)

	} else {
		// lookup version in nats kv
		p.logger.Trace("looking up modelVersion for: %s for msgid: %s", modelVersionId, msgid)

		entry, err := p.mainNATSConn.Request(fmt.Sprintf("schema.%s.%s", model, modelVersion), emptyJSON, modelRequestTimeout)

		if err != nil {
			p.logger.Trace("error getting schema for: %s for msgid: %s", modelVersionId, msgid)
			return err
		}
		var foundSchema datatypes.SchemaResponse
		err = json.Unmarshal(entry.Data, &foundSchema)
		if err != nil {
			return fmt.Errorf("error unmarshalling change event schema: %s. %s", string(entry.Data), err)
		}
		schema = foundSchema.Data
		if foundSchema.Success {
			p.logger.Trace("got schema for: %s %v for msgid: %s", modelVersionId, foundSchema.Data, msgid)
			(*p.schemaModelVersionCache)[modelVersionId] = schema
		} else {
			return fmt.Errorf("no schema found for: %s %v for msgid: %s", modelVersionId, foundSchema.Data, msgid)
		}
	}
	var wg sync.WaitGroup
	var wgErrorChan = make(chan error, len(p.providers))

	for _, provider := range p.providers {
		wg.Add(1)
		go func(provider Provider) {
			defer wg.Done()
			if err := provider.Process(data, schema); err != nil {
				p.logger.Error("error processing change event: %s. %s", data, err)
				wgErrorChan <- err
			}
		}(provider)
	}
	wg.Wait()
	close(wgErrorChan)
	// Only trigger in the case of multiple providers being used and less than all of them experience an error from the same change event
	if len(wgErrorChan) > 0 && len(wgErrorChan) < len(p.providers) {
		p.logger.Error("Change event processing failed for some providers, ending EDS to prevent providers from going out of sync")
		os.Exit(1)

	}

	if err := msg.AckSync(); err != nil {
		//if we already acked this message, continue
		if err == nats.ErrMsgAlreadyAckd {
			p.logger.Trace("message already acked: %s", msgid)
			return nil
		}
		p.logger.Error("error calling ack for message: %s. %s", data, err)
		return err
	}
	return nil
}

// Start will start processing messages
func (p *MessageProcessor) Start() error {
	p.logger.Trace("message processor starting")
	p.logger.Trace("starting message processor for company ids: %s", p.companyID)
	for _, companyID := range p.companyID {
		name := fmt.Sprintf("%seds-server-%s", p.consumerPrefix, companyID)
		p.logger.Trace("starting message processor for consumer: %s and company id: %s", name, companyID)

		if companyID == "" {
			companyID = "*"
		}

		var (
			c   snats.Subscriber
			err error
		)

		if p.consumerStartTime.IsZero() {
			p.logger.Trace("creating consumer with New delivery policy")
			c, err = snats.NewExactlyOnceConsumer(p.logger, p.js, "dbchange", name, "dbchange.*.*."+companyID+".*.PUBLIC.>", p.callback,
				snats.WithExactlyOnceContext(p.context),
				snats.WithExactlyOnceReplicas(1), // TODO: make configurable for testing
			)
			if err != nil {
				return err
			}
			if len(p.companyID) > 5 {
				//Add some delay to allow the consumer to start up
				//Just a band-aid fix but this seems to allow start-up of around 30+ companies
				time.Sleep(10 * time.Second)
			}
		} else {
			p.logger.Trace("creating consumer with StartTime delivery policy")
			p.logger.Trace("consumerStartTime: %v", p.consumerStartTime)
			c, err = snats.NewExactlyOnceConsumer(p.logger, p.js, "dbchange", name, "dbchange.*.*."+companyID+".*.PUBLIC.>", p.callback,
				snats.WithExactlyOnceContext(p.context),
				snats.WithExactlyOnceReplicas(1), // TODO: make configurable for testing
				snats.WithExactlyOnceByStartTimePolicy(p.consumerStartTime),
			)
			if err != nil {
				return err
			}
		}

		p.subscriber = append(p.subscriber, c)
		p.logger.Trace("message processor started for consumer: %s and company id: %s", name, companyID)
	}
	return nil
}

// Stop will stop processing messages
func (p *MessageProcessor) Stop() error {
	p.logger.Trace("message processor stopping")
	p.cancel()
	if p.subscriber != nil && len(p.subscriber) > 0 {
		for _, subscriber := range p.subscriber {
			if err := subscriber.Close(); err != nil {
				return err
			}
		}
	}
	p.logger.Trace("message processor stopped")
	return nil
}

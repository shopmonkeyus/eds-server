package provider

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
)

type NatsProvider struct {
	logger logger.Logger
	nc	   *nats.Conn
	js     nats.JetStreamContext
	opts   *ProviderOpts
}

var _ internal.Provider = (*NatsProvider)(nil)

// NewFileProvider returns a provider that will stream files to a folder provided in the url
func NewNatsProvider(logger logger.Logger, urlstring string, opts *ProviderOpts) (internal.Provider, error) {
	nc, err := nats.Connect(urlstring)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to nats server: %s with error: %s", urlstring, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("unable to configure Jetstream: %s", err)
	}

	duration, _ := time.ParseDuration("1d")
	cfg := nats.StreamConfig{
		Name: "dbchange",
		Subjects: []string{"dbchange.>"},
		Storage: nats.MemoryStorage,
		Description: "dbchange stream for eds",
	    Retention: nats.LimitsPolicy,
		MaxConsumers: -1,
		MaxMsgs: -1,
		MaxBytes: -1,
		Discard: nats.DiscardOld,
		DiscardNewPerSubject: false,
		MaxAge: duration,
		MaxMsgsPerSubject: -1,
		MaxMsgSize: -1,
		Replicas: 0,
		NoAck: false,
		Duplicates: duration,
		Sealed: false,
		DenyDelete: false,
		DenyPurge: false,
		AllowRollup: false,
	}
	cfg.Storage = nats.FileStorage
	_, err = js.AddStream(&cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to configure Jetstream: %s", err)
	}

	logger.Info("nats provider will publish to: %s", urlstring)
	return &NatsProvider{
		logger,
		nc,
		js,
		opts,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *NatsProvider) Start() error {
	return nil
}

// Stop the provider and return an error or nil if ok
func (p *NatsProvider) Stop() error {
	p.nc.Close()
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *NatsProvider) Process(data datatypes.ChangeEventPayload) error {
	p.logger.Info("Republish Message to: dbchange.%s.%s.%s",  data.GetTable(), data.GetOperation(), *data.GetLocationID())
	subject := fmt.Sprintf("dbchange.%s.%s.%s", data.GetTable(), data.GetOperation(), *data.GetLocationID())

	buf, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return err
	}
	
	p.logger.Info("Republish Message with Payload", string(buf))
	if p.opts != nil && p.opts.DryRun {
		p.logger.Info("[dry-run] would publish to: %s", subject)
		return nil
	}

	_, err = p.js.Publish(subject, buf)
	if err != nil {
		return err
	}

	return nil
}

// Migrate will tell the provider to do any migration work and return an error or nil if ok
func (p *NatsProvider) Migrate() error {
	return nil
}

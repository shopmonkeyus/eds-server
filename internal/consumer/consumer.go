package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	cnats "github.com/shopmonkeyus/go-common/nats"
)

const (
	emptyBufferPauseTime = time.Millisecond * 50 // time to wait when the buffer is empty to prevent CPU spinning
	minPendingLatency    = time.Second           // minimum accumulation period before flushing
	maxPendingLatency    = time.Second * 30      // maximum accumulation period before flushing
)

// ConsumerConfig is the configuration for the consumer.
type ConsumerConfig struct {

	// Context is the context for the consumer.
	Context context.Context

	// Logger is the logger for the consumer.
	Logger logger.Logger

	// URL to the nats server
	URL string

	// Credentials for the nats server
	Credentials string

	// Suffix for the consumer name
	Suffix string

	// MaxAckPending is the maximum number of messages that can be in-flight at once.
	MaxAckPending int

	// MaxPendingBuffer is the maximum number of messages that can be buffered before the consumer starts dropping messages.
	MaxPendingBuffer int

	// Replicas are the number of replicas for the consumer.
	Replicas int

	// Processor is the processor for the consumer.
	Processor internal.Processor
}

type Consumer struct {
	ctx            context.Context
	cancel         context.CancelFunc
	max            int
	processor      internal.Processor
	conn           *nats.Conn
	jsconn         jetstream.Consumer
	logger         logger.Logger
	subscriber     jetstream.ConsumeContext
	buffer         chan jetstream.Msg
	pending        []jetstream.Msg
	pendingStarted *time.Time
	waitGroup      sync.WaitGroup
	once           sync.Once
	lock           sync.Mutex
	stopping       bool
}

// Stop the consumer and close the connection to the NATS server.
func (c *Consumer) Stop() error {
	c.logger.Debug("stopping consumer")
	c.once.Do(func() {
		c.logger.Debug("stopping bufferer")
		// set the consumer to stopping in a safe way since we have the goroutine running
		c.lock.Lock()
		c.stopping = true
		c.lock.Unlock()
		c.flush()
		c.cancel()
		c.logger.Debug("waiting on bufferer")
		c.waitGroup.Wait()
		c.logger.Debug("stopped bufferer")

		// once we get here, the bufferer should be done and its safe to start shutting down

		c.nackEverything() // just be safe

		if c.subscriber != nil {
			c.logger.Debug("stopping subscriber")
			c.subscriber.Stop()
			c.logger.Debug("stopped subscriber")
		}
		if c.conn != nil {
			c.logger.Debug("stopping nats connection")
			c.conn.Close()
			c.logger.Debug("stopped nats connection")
		}
		c.subscriber = nil
		c.conn = nil
	})
	c.logger.Debug("stopped consumer")
	return nil
}

func (c *Consumer) nackEverything() {
	c.logger.Debug("nack everything")
	for _, m := range c.pending {
		if err := m.Nak(); err != nil {
			c.logger.Error("error nacking msg %s: %s", m.Headers().Get(nats.MsgIdHdr), err)
		}
	}
	c.pending = nil
	c.pendingStarted = nil
}

func (c *Consumer) handleError(err error) {
	c.logger.Error("error: %s", err)
	c.nackEverything()
}

func (c *Consumer) flush() bool {
	c.logger.Trace("flush")
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := c.processor.Flush(); err != nil {
		c.handleError(err)
		return true
	}
	for _, m := range c.pending {
		if err := m.Ack(); err != nil {
			c.logger.Error("error acking msg %s: %s", m.Headers().Get(nats.MsgIdHdr), err)
			c.nackEverything()
			return true
		}
	}
	c.pending = nil
	c.pendingStarted = nil
	return c.stopping
}

func (c *Consumer) bufferer() {
	c.logger.Trace("starting bufferer")
	c.waitGroup.Add(1)
	defer func() {
		c.waitGroup.Done()
		c.logger.Trace("stopped bufferer")
	}()
	for {
		select {
		case <-c.ctx.Done():
			c.nackEverything()
			return
		case msg := <-c.buffer:
			log := c.logger.With(map[string]any{
				"msgId":   msg.Headers().Get(nats.MsgIdHdr),
				"subject": msg.Subject(),
			})
			if m, err := msg.Metadata(); err == nil {
				log.Trace("msg received (deliveries %d)", m.NumDelivered)
			}
			c.pending = append(c.pending, msg)
			buf := msg.Data()
			md, _ := msg.Metadata()
			var evt internal.DBChangeEvent
			if err := json.Unmarshal(buf, &evt); err != nil {
				log.Error("error unmarshalling: %s (seq:%d): %s", string(buf), md.Sequence.Consumer, err)
				c.handleError(err)
				return
			}
			flush, err := c.processor.Process(evt)
			if err != nil {
				c.handleError(err)
				return
			}
			if flush || len(c.pending) >= c.processor.MaxBatchSize() {
				if c.flush() {
					return
				}
				continue
			}
			if c.pendingStarted == nil {
				ts := time.Now()
				c.pendingStarted = &ts
			}
			if md.NumPending > uint64(c.max) && time.Since(*c.pendingStarted) < maxPendingLatency*2 {
				continue // if we have a large number, just keep going to try and catchup
			}
			if len(c.pending) >= c.max || time.Since(*c.pendingStarted) >= maxPendingLatency {
				if c.flush() {
					return
				}
				continue
			}
		default:
			count := len(c.pending)
			if count > 0 && count < c.max && time.Since(*c.pendingStarted) >= minPendingLatency {
				if c.flush() {
					return
				}
				continue
			}
			if count > 0 {
				continue
			}
			select {
			case <-c.ctx.Done():
				c.logger.Debug("context done")
				c.nackEverything()
				return
			default:
				time.Sleep(emptyBufferPauseTime)
			}
		}
	}
}

func (c *Consumer) process(msg jetstream.Msg) {
	c.buffer <- msg
}

// NewConsumer creates a new nats consumer
func NewConsumer(config ConsumerConfig) (*Consumer, error) {

	var natsCredentials nats.Option
	var companyName string
	var companyIDs []string
	var err error

	if util.IsLocalhost(config.URL) {
		companyName = "dev"
		companyIDs = []string{"*"}
	} else {
		natsCredentials, companyIDs, companyName, err = getNatsCreds(config.Credentials)
		if err != nil {
			return nil, err
		}
		// normalize the company name so we can use it in the nats client name and in the consumer name
		companyName = strings.ToLower(strings.ReplaceAll(companyName, " ", "_"))
	}

	// Nats connection to main NATS server
	nc, err := cnats.NewNats(config.Logger, "eds-server-"+companyName, config.URL, natsCredentials)
	if err != nil {
		return nil, fmt.Errorf("error creating nats connection: %w", err)
	}

	ctx, cancel := context.WithCancel(config.Context)

	var consumer Consumer
	consumer.max = config.MaxAckPending
	consumer.ctx = ctx
	consumer.cancel = cancel
	consumer.conn = nc
	consumer.processor = config.Processor
	consumer.buffer = make(chan jetstream.Msg, config.MaxAckPending)
	consumer.pending = make([]jetstream.Msg, 0)

	consumer.logger = config.Logger.WithPrefix("[nats]")
	js, err := jetstream.New(nc, jetstream.WithClientTrace(&jetstream.ClientTrace{
		RequestSent: func(subj string, payload []byte) {
			consumer.logger.Trace("nats tx: %s: %s", subj, string(payload))
		},
		ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
			consumer.logger.Trace("nats rx: %s: %s", subj, string(payload))
		},
	}))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("error creating jetstream connection: %w", err)
	}

	var prefix string
	if config.Suffix != "" {
		prefix = "-" + config.Suffix
	}
	name := fmt.Sprintf("eds-server-%s%s", companyName, prefix)
	var subjects []string
	for _, companyID := range companyIDs {
		subject := "dbchange.*.*." + companyID + ".*.PUBLIC.>"
		subjects = append(subjects, subject)
	}
	replicas := 1
	if config.Replicas > 1 {
		replicas = config.Replicas
	}

	jsConfig := jetstream.ConsumerConfig{
		Durable:         name,
		MaxAckPending:   config.MaxAckPending,
		MaxDeliver:      1_000,
		AckWait:         time.Minute * 5,
		Replicas:        replicas,
		DeliverPolicy:   jetstream.DeliverNewPolicy,
		MaxRequestBatch: config.MaxPendingBuffer,
		FilterSubjects:  subjects,
		AckPolicy:       jetstream.AckExplicitPolicy,
	}
	createConsumerContext, cancelCreate := context.WithDeadline(config.Context, time.Now().Add(time.Minute*10))
	defer cancelCreate()
	c, err := js.CreateOrUpdateConsumer(createConsumerContext, "dbchange", jsConfig)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("error creating jetstream consumer: %w", err)
	}
	cancelCreate()

	consumer.jsconn = c

	// start consuming messages
	sub, err := c.Consume(consumer.process)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("error creating jetstream consumer: %w", err)
	}
	consumer.subscriber = sub

	// start the background processor
	go consumer.bufferer()

	consumer.logger.Debug("started")
	return &consumer, nil
}
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tycheg/reasonr-engine/port"
)

// Requests is the kafka transport of external requests (RFC §18): a
// request is a record in the system's topic, the integration service reads
// it and retries the system call itself. The engine retries only a write
// fault, so a write error is always retried (except a request that cannot
// be encoded): with no topic the request waits until it is created and is
// not dropped.
//
// The format is CloudEvents, binary mode: ce_id is the idempotency key,
// ce_type is {System}.{request}, ce_source is reasonr/{tenant}/{System},
// ce_time is the time of the unit of work, the extensions are tenant,
// attempt and deadline (the timer deadline, if any); the record key is the
// idempotency key; the value is the arguments without the timeout
// parameter, as the body of an HTTP request.
type Requests struct {
	Client *kgo.Client
	// Topic is the topic name template: {tenant}, {system}; "" —
	// port.DefaultRequestTopic. The engine resolves it into
	// LeasedRequest.Topic.
	Topic string
	// Settings are the topic creation settings (create_topics); nil — not
	// set.
	Settings *port.TopicSettings
}

var (
	_ port.IntegrationTransport = (*Requests)(nil)
	_ port.TopicBroker          = (*Requests)(nil)
)

// Record is the Kafka record for a request the engine made ready (body,
// key, attributes, topic).
func (q *Requests) Record(lr *port.LeasedRequest) (*kgo.Record, error) {
	if lr.Topic == "" || lr.Key == "" {
		return nil, fmt.Errorf("%w: the request is not ready (topic, key)", port.ErrPermanent)
	}
	return &kgo.Record{Topic: lr.Topic, Key: []byte(lr.Key), Value: lr.Body, Headers: headers(lr.Headers("ce_"))}, nil
}

// Send writes a request to the topic and waits for the acknowledgement (port.IntegrationTransport).
func (q *Requests) Send(ctx context.Context, lr *port.LeasedRequest) error {
	r, err := q.Record(lr)
	if err != nil {
		return err
	}
	if err := q.Client.ProduceSync(ctx, r).FirstErr(); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	return nil
}

// TopicTemplate is the topic name template (port.TopicBroker).
func (q *Requests) TopicTemplate() string {
	if q.Topic == "" {
		return port.DefaultRequestTopic
	}
	return q.Topic
}

// TopicSettings are the create_topics settings (port.TopicBroker).
func (q *Requests) TopicSettings() *port.TopicSettings { return q.Settings }

// DescribeTopics gives the partitions of existing topics (port.TopicBroker).
func (q *Requests) DescribeTopics(ctx context.Context, names []string) (map[string]int, error) {
	return describeTopics(ctx, q.Client, names)
}

// CreateTopics creates topics (port.TopicBroker).
func (q *Requests) CreateTopics(ctx context.Context, names []string, s port.TopicSettings) error {
	return newTopics(ctx, q.Client, names, s)
}

// Close closes the client.
func (q *Requests) Close() error {
	q.Client.Close()
	return nil
}

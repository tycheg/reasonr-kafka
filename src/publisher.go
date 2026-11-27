package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tycheg/reasonr-engine/port"
)

// Publisher is the Kafka bus (port.Publisher): a message is a record in
// the topic by the template Topic with the key Message.Subject, the
// CloudEvents attributes are ce_* headers, the arguments are the value (the
// binary mode of the Kafka binding).
type Publisher struct {
	Client *kgo.Client
	// Topic is the topic name template: {tenant}, {kind}, {type}; "" —
	// port.DefaultBusTopic. The engine resolves it into Message.Topic.
	Topic string
	// Settings are the topic creation settings (create_topics); nil — not
	// set.
	Settings *port.TopicSettings
}

var (
	_ port.Publisher   = (*Publisher)(nil)
	_ port.TopicBroker = (*Publisher)(nil)
)

func (p *Publisher) topic() string {
	if p.Topic == "" {
		return port.DefaultBusTopic
	}
	return p.Topic
}

// Record is the Kafka record for a message the engine made ready (topic
// and attributes).
func (p *Publisher) Record(m *port.Message) *kgo.Record {
	r := &kgo.Record{Topic: m.Topic, Value: m.Data, Headers: headers(m.Headers("ce_"))}
	if m.Subject != "" {
		r.Key = []byte(m.Subject)
	}
	return r
}

// Publish writes messages and waits until all are acknowledged
// (port.Publisher). Records with one key go to one partition in order: the
// idempotent producer keeps their order even on retries.
func (p *Publisher) Publish(ctx context.Context, msgs []port.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	records := make([]*kgo.Record, len(msgs))
	for i := range msgs {
		records[i] = p.Record(&msgs[i])
	}
	return produce(ctx, p.Client, records)
}

// Close closes the client: unfinished records are written out.
func (p *Publisher) Close() error {
	p.Client.Close()
	return nil
}

// TopicTemplate is the topic name template (port.TopicBroker).
func (p *Publisher) TopicTemplate() string { return p.topic() }

// TopicSettings are the create_topics settings (port.TopicBroker).
func (p *Publisher) TopicSettings() *port.TopicSettings { return p.Settings }

// DescribeTopics gives the partitions of existing topics (port.TopicBroker).
func (p *Publisher) DescribeTopics(ctx context.Context, names []string) (map[string]int, error) {
	return describeTopics(ctx, p.Client, names)
}

// CreateTopics creates topics (port.TopicBroker).
func (p *Publisher) CreateTopics(ctx context.Context, names []string, s port.TopicSettings) error {
	return newTopics(ctx, p.Client, names, s)
}

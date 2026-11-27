package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/porttest"
	"github.com/tycheg/reasonr-engine/port/registry"
)

// Tests on Kafka: REASONR_TEST_KAFKA is the broker address (the Apache
// Kafka KRaft image in Docker, see the README); without it the tests are
// skipped.

func broker(t *testing.T) string {
	addr := os.Getenv("REASONR_TEST_KAFKA")
	if addr == "" {
		t.Skip("REASONR_TEST_KAFKA is not set")
	}
	return addr
}

func node(t *testing.T, yaml string) *port.Node {
	t.Helper()
	n, err := port.ParseNode([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// fastMetadata refreshes metadata more often: topics are created during the checks.
var fastMetadata = kgo.MetadataMinAge(100 * time.Millisecond)

// prefix is a unique topic prefix of a check.
func prefix() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "reasonr-test-" + hex.EncodeToString(b[:])
}

// createTopics creates topics with partitions partitions and deletes them at
// the end of the check.
func createTopics(t *testing.T, addr string, partitions int32, topics ...string) {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10000
	for _, name := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, partitions, 1
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	for _, rt := range resp.Topics {
		if err := kerr.ErrorForCode(rt.ErrorCode); err != nil {
			t.Fatalf("topic %s: %v", rt.Topic, err)
		}
	}
	deleteTopicsLater(t, addr, topics...)
}

// deleteTopicsLater deletes topics at the end of the check.
func deleteTopicsLater(t *testing.T, addr string, topics ...string) {
	t.Cleanup(func() {
		c, err := kgo.NewClient(kgo.SeedBrokers(addr))
		if err != nil {
			return
		}
		defer c.Close()
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = topics
		for _, name := range topics {
			dt := kmsg.NewDeleteTopicsRequestTopic()
			dt.Topic = kmsg.StringPtr(name)
			del.Topics = append(del.Topics, dt)
		}
		_, _ = del.RequestWith(context.Background(), c)
	})
}

// reader reads topics from the start.
type reader struct {
	c *kgo.Client
}

func newReader(t *testing.T, addr string, topics ...string) *reader {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), fastMetadata, kgo.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return &reader{c: c}
}

// records waits for n records.
func (r *reader) records(t *testing.T, n int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := r.c.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d records of %d", len(out), n)
		}
		fs.EachError(func(topic string, p int32, err error) { t.Fatalf("reading %s/%d: %v", topic, p, err) })
		out = append(out, fs.Records()...)
	}
	if len(out) > n {
		t.Fatalf("read %d records, want %d", len(out), n)
	}
	return out
}

// received is a record as a CloudEvents message in the Kafka binding.
func received(r *kgo.Record) porttest.Received {
	out := porttest.Received{Attributes: map[string]string{}, Key: string(r.Key), Data: r.Value}
	for _, h := range r.Headers {
		if name, ok := strings.CutPrefix(h.Key, "ce_"); ok {
			out.Attributes[name] = string(h.Value)
		} else if h.Key == "content-type" {
			out.ContentType = string(h.Value)
		}
	}
	return out
}

func TestPublisher(t *testing.T) {
	addr := broker(t)
	porttest.RunPublisher(t, func(t *testing.T) (port.Publisher, func(*testing.T, int) []porttest.Received) {
		template := prefix() + ".{kind}.{type}"
		topics := porttest.BusTopics(template)
		createTopics(t, addr, 3, topics...)
		c, err := newClient(node(t, fmt.Sprintf("brokers: [%s]\ntimeout: 10s\n", addr)), fastMetadata)
		if err != nil {
			t.Fatal(err)
		}
		p := &Publisher{Client: c, Topic: template}
		t.Cleanup(func() { _ = p.Close() })
		return p, readerOf(newReader(t, addr, topics...))
	})
}

// readerOf reads what was published as the suites see it.
func readerOf(r *reader) func(t *testing.T, n int) []porttest.Received {
	return func(t *testing.T, n int) []porttest.Received {
		var out []porttest.Received
		for _, rec := range r.records(t, n) {
			out = append(out, received(rec))
		}
		return out
	}
}

func TestTopics(t *testing.T) {
	addr := broker(t)
	pre := prefix()
	deps := registry.Deps{}
	f := newPublisher
	cfg := fmt.Sprintf("type: kafka\nbrokers: [%s]\ntopic: %q\nnotifications: true\n"+
		"create_topics: { partitions: 2, replication: 1, config: { retention.ms: 3600000, cleanup.policy: delete } }\n", addr, pre+".{kind}.{type}")
	pub, err := f(node(t, cfg), deps)
	if err != nil {
		t.Fatalf("the factory without topics: %v", err)
	}
	defer func() { _ = pub.(*Publisher).Close() }()
	b := pub.(port.TopicBroker)
	want := &port.TopicSettings{Partitions: 2, Replication: 1, Config: map[string]string{"retention.ms": "3600000", "cleanup.policy": "delete"}}
	if b.TopicTemplate() != pre+".{kind}.{type}" || !reflect.DeepEqual(b.TopicSettings(), want) {
		t.Fatalf("template %q, settings %+v", b.TopicTemplate(), b.TopicSettings())
	}
	ctx := context.Background()
	names := []string{pre + ".event.Box", pre + ".notification.Keeper"}
	deleteTopicsLater(t, addr, names...)
	if have, err := b.DescribeTopics(ctx, names); err != nil || len(have) != 0 {
		t.Fatalf("before creation: %v, %v", have, err)
	}
	for range 2 { // the second time the topics already exist
		if err := b.CreateTopics(ctx, names, *b.TopicSettings()); err != nil {
			t.Fatal(err)
		}
	}
	if have, err := b.DescribeTopics(ctx, names); err != nil || !maps.Equal(have, map[string]int{names[0]: 2, names[1]: 2}) {
		t.Fatalf("after creation: %v, %v", have, err)
	}
	if got := topicConfig(t, addr, names[0], "retention.ms"); got != "3600000" {
		t.Errorf("retention.ms = %q", got)
	}
	// The replication factor goes to the broker: the test broker has one node.
	if err := b.CreateTopics(ctx, []string{pre + ".replication"}, port.TopicSettings{Replication: 2}); err == nil || !strings.Contains(err.Error(), pre+".replication") {
		t.Errorf("replication 2 on one broker: %v", err)
	}

	tr, err := newRequests(node(t, fmt.Sprintf("type: kafka\nbrokers: [%s]\n", addr)), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.(*Requests).Close() }()
	if rb := tr.(port.TopicBroker); rb.TopicTemplate() != port.DefaultRequestTopic || rb.TopicSettings() != nil {
		t.Errorf("transport: template %q, settings %+v", rb.TopicTemplate(), rb.TopicSettings())
	}
	for _, bad := range []string{"publish: [Box.stored]", "create_topics: { partitions: -1 }", "create_topics: { shards: 1 }",
		"create_topics: { config: [retention.ms] }", "create_topics: { config: { retention.ms: [1] } }"} {
		if _, err := f(node(t, fmt.Sprintf("type: kafka\nbrokers: [%s]\n%s\n", addr, bad)), deps); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
}

// topicConfig returns the setting key of a topic.
func topicConfig(t *testing.T, addr, topic, key string) string {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	req := kmsg.NewPtrDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType, res.ResourceName, res.ConfigNames = kmsg.ConfigResourceTypeTopic, topic, []string{key}
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp.Resources {
		for _, cfg := range r.Configs {
			if cfg.Name == key && cfg.Value != nil {
				return *cfg.Value
			}
		}
	}
	return ""
}

// TestUnavailable: with an unavailable broker publishing returns an error
// to retry, without a topic ErrPermanent; after recovery the messages are
// all published, in order within a key.
func TestUnavailable(t *testing.T) {
	addr := broker(t)
	template := prefix() + ".{kind}.{type}"
	topic := porttest.BusTopics(template)[0]
	var msgs []port.Message
	for pos := range 6 {
		msgs = append(msgs, port.Message{Kind: port.MessageEvent, Tenant: porttest.T1, Position: port.Position(pos + 1), Type: "Shop.Order",
			Name: "changed", Subject: []string{"order-a", "order-b"}[pos%2], Data: []byte(fmt.Sprintf(`{"n":%d}`, pos)), Topic: topic,
			Attributes: []port.Attribute{{Name: "id", Value: fmt.Sprint(pos)}}})
	}
	down, err := newClient(node(t, "brokers: [127.0.0.1:1]\ntimeout: 1s\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	ctx := context.Background()
	if err := (&Publisher{Client: down, Topic: template}).Publish(ctx, msgs); err == nil || errors.Is(err, port.ErrPermanent) {
		t.Fatalf("an unavailable broker: %v", err)
	}
	up, err := newClient(node(t, fmt.Sprintf("brokers: [%s]\ntimeout: 10s\n", addr)), fastMetadata)
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	pub := &Publisher{Client: up, Topic: template}
	if err := pub.Publish(ctx, msgs); !errors.Is(err, port.ErrPermanent) {
		t.Fatalf("no topic: %v", err)
	}
	createTopics(t, addr, 2, topic)
	if err := pub.Publish(ctx, msgs); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	byKey := map[string][]string{}
	for _, r := range newReader(t, addr, topic).records(t, 6) {
		byKey[string(r.Key)] = append(byKey[string(r.Key)], string(r.Value))
	}
	if a, b := byKey["order-a"], byKey["order-b"]; !slices.Equal(a, []string{`{"n":0}`, `{"n":2}`, `{"n":4}`}) || !slices.Equal(b, []string{`{"n":1}`, `{"n":3}`, `{"n":5}`}) {
		t.Errorf("the order by key: %v", byKey)
	}
}

// TestRequests: the kafka transport writes a ready request to its topic
// (porttest.RunTransport). Without a topic it is a write error, but not
// ErrPermanent: the request waits for the topic.
func TestRequests(t *testing.T) {
	addr := broker(t)
	porttest.RunTransport(t, func(t *testing.T) (port.IntegrationTransport, func(*testing.T, int) []porttest.Received) {
		template := prefix() + ".{tenant}.{system}"
		tr, err := newRequests(node(t, fmt.Sprintf("type: kafka\nbrokers: [%s]\ntopic: %q\ntimeout: 3s\n", addr, template)), registry.Deps{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tr.(*Requests).Close() })
		topic := porttest.RequestTopic(template)
		lr := &port.LeasedRequest{Tenant: porttest.T1, Key: "k1", Body: []byte(`{}`), Topic: topic}
		if err := tr.Send(context.Background(), lr); err == nil || errors.Is(err, port.ErrPermanent) {
			t.Fatalf("no topic: %v", err)
		}
		if err := tr.Send(context.Background(), &port.LeasedRequest{Tenant: porttest.T1}); !errors.Is(err, port.ErrPermanent) {
			t.Fatalf("a request that is not ready: %v", err)
		}
		createTopics(t, addr, 1, topic)
		return tr, readerOf(newReader(t, addr, topic))
	})
}

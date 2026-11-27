package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/tycheg/reasonr-engine/port"
)

// clientKeys are the connection and topic creation keys shared by the bus
// and the request transport.
var clientKeys = []string{"brokers", "client_id", "timeout", "tls", "sasl", "create_topics"}

// newClient creates a client from the connection keys of the node n:
//
//	brokers: [kafka:9092]                 # required
//	client_id: reasonr                    # reasonr by default
//	timeout: 30s                          # the record delivery deadline
//	tls: { ca_file, cert_file, key_file } # or tls: {} — the system CAs
//	sasl: { mechanism: plain | scram-sha-256 | scram-sha-512, user, password_env }
//
// The producer is idempotent (the franz-go default), acks=all; the key is
// partitioned with murmur2, as in the Java client, so the order by key is
// shared with the topic's other producers. A missing topic is an error
// after one metadata retry, not after timeout.
func newClient(n *port.Node, extra ...kgo.Opt) (*kgo.Client, error) {
	brokers, err := n.Strings("brokers")
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: set brokers — the broker addresses")
	}
	id, err := n.String("client_id", "reasonr")
	if err != nil {
		return nil, err
	}
	timeout, err := n.Duration("timeout", 30*time.Second)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ClientID(id), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.RecordDeliveryTimeout(timeout),
		kgo.ProduceRequestTimeout(timeout), kgo.UnknownTopicRetries(1)}
	if t := n.Get("tls"); t != nil {
		cfg, err := tlsConfig(t)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	if s := n.Get("sasl"); s != nil {
		m, err := mechanism(s)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(m))
	}
	return kgo.NewClient(append(opts, extra...)...)
}

func tlsConfig(n *port.Node) (*tls.Config, error) {
	if err := n.Only("ca_file", "cert_file", "key_file"); err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	ca, _ := n.String("ca_file", "")
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("kafka tls: %w", err)
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka tls: %s has no PEM certificates", ca)
		}
	}
	cert, _ := n.String("cert_file", "")
	key, _ := n.String("key_file", "")
	if (cert == "") != (key == "") {
		return nil, fmt.Errorf("kafka tls: cert_file and key_file are set together")
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("kafka tls: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

func mechanism(n *port.Node) (sasl.Mechanism, error) {
	if err := n.Only("mechanism", "user", "password_env"); err != nil {
		return nil, err
	}
	mech, _ := n.String("mechanism", "")
	user, _ := n.String("user", "")
	env, _ := n.String("password_env", "")
	if user == "" || env == "" {
		return nil, fmt.Errorf("kafka sasl: set user and password_env")
	}
	pass := os.Getenv(env)
	if pass == "" {
		return nil, fmt.Errorf("kafka sasl: the environment variable %s is empty", env)
	}
	switch mech {
	case "plain":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	}
	return nil, fmt.Errorf("kafka sasl: the mechanism %q is not supported (plain, scram-sha-256, scram-sha-512)", mech)
}

// permanent reports that retrying a write without a configuration change
// will not help: no topic, no permissions, the record is too large.
func permanent(err error) bool {
	for _, e := range []error{kerr.UnknownTopicOrPartition, kerr.TopicAuthorizationFailed, kerr.ClusterAuthorizationFailed,
		kerr.SaslAuthenticationFailed, kerr.MessageTooLarge, kerr.RecordListTooLarge} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// produce writes records and waits until all are acknowledged; errors a
// retry will not fix carry port.ErrPermanent.
func produce(ctx context.Context, c *kgo.Client, records []*kgo.Record) error {
	err := c.ProduceSync(ctx, records...).FirstErr()
	switch {
	case err == nil:
		return nil
	case permanent(err):
		return fmt.Errorf("%w: kafka: %v", port.ErrPermanent, err)
	}
	return fmt.Errorf("kafka: %w", err)
}

// describeTopics returns the number of partitions of those topics from
// names that exist in the cluster.
func describeTopics(ctx context.Context, c *kgo.Client, names []string) (map[string]int, error) {
	req := kmsg.NewPtrMetadataRequest() // AllowAutoTopicCreation is false: the request does not create a topic
	for _, t := range names {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, c)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if !errors.Is(err, kerr.UnknownTopicOrPartition) {
				return nil, fmt.Errorf("topic %s: %w", *t.Topic, err)
			}
			continue
		}
		out[*t.Topic] = len(t.Partitions)
	}
	return out, nil
}

// newTopics creates topics with the settings s (0 — the broker default); a
// topic that already exists (another instance created it) is not an error.
func newTopics(ctx context.Context, c *kgo.Client, names []string, s port.TopicSettings) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30000
	keys := make([]string, 0, len(s.Config))
	for k := range s.Config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range names {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, -1, -1
		if s.Partitions > 0 {
			rt.NumPartitions = int32(s.Partitions)
		}
		if s.Replication > 0 {
			rt.ReplicationFactor = int16(s.Replication)
		}
		for _, k := range keys {
			tc := kmsg.NewCreateTopicsRequestTopicConfig()
			tc.Name, tc.Value = k, kmsg.StringPtr(s.Config[k])
			rt.Configs = append(rt.Configs, tc)
		}
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, c)
	if err != nil {
		return err
	}
	var errs []error
	for _, rt := range resp.Topics {
		if err := kerr.ErrorForCode(rt.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			if rt.ErrorMessage != nil {
				err = fmt.Errorf("%w: %s", err, *rt.ErrorMessage)
			}
			errs = append(errs, fmt.Errorf("topic %s: %w", rt.Topic, err))
		}
	}
	return errors.Join(errs...)
}

// topicSettings reads create_topics: { partitions, replication, config } of
// the node n; nil — no key.
func topicSettings(n *port.Node) (*port.TopicSettings, error) {
	ct := n.Get("create_topics")
	if ct == nil {
		return nil, nil
	}
	if err := ct.Only("partitions", "replication", "config"); err != nil {
		return nil, fmt.Errorf("create_topics: %w", err)
	}
	s := &port.TopicSettings{}
	var err error
	if s.Partitions, err = ct.Int("partitions", 0); err != nil || s.Partitions < 0 {
		return nil, fmt.Errorf("create_topics.partitions: expected a number of partitions greater than 0 (%v)", err)
	}
	if s.Replication, err = ct.Int("replication", 0); err != nil || s.Replication < 0 || s.Replication > 32767 {
		return nil, fmt.Errorf("create_topics.replication: expected a replication factor greater than 0 (%v)", err)
	}
	if cfg := ct.Get("config"); cfg != nil {
		if cfg.Kind != port.NodeMap {
			return nil, fmt.Errorf("create_topics.config: expected a map \"setting: value\"")
		}
		s.Config = map[string]string{}
		for i, k := range cfg.Keys {
			if v := cfg.Values[i]; v.Kind != port.NodeScalar {
				return nil, fmt.Errorf("create_topics.config.%s: expected a scalar value", k)
			}
			s.Config[k] = cfg.Values[i].Value
		}
	}
	return s, nil
}

// headers are the record headers: CloudEvents attributes in the Kafka binding.
func headers(attrs []port.Attribute) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, len(attrs))
	for i, a := range attrs {
		out[i] = kgo.RecordHeader{Key: a.Name, Value: []byte(a.Value)}
	}
	return out
}

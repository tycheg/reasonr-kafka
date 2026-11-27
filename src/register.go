package kafka

import (
	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/registry"
)

func init() {
	registry.RegisterPublisher("kafka", newPublisher)
	registry.RegisterIntegration("kafka", newRequests)
}

// newPublisher is the bus bus: { type: kafka, brokers, topic,
// notifications, client_id, timeout, tls, sasl, create_topics }. The
// factory does not check the topics: reasonrd does that at startup
// (port.TopicBroker).
func newPublisher(n *port.Node, d registry.Deps) (port.Publisher, error) {
	if err := n.Only(append([]string{"type", "topic", "notifications"}, clientKeys...)...); err != nil {
		return nil, err
	}
	topic, err := n.String("topic", "")
	if err != nil {
		return nil, err
	}
	if _, err := n.Bool("notifications", false); err != nil {
		return nil, err
	}
	settings, err := topicSettings(n)
	if err != nil {
		return nil, err
	}
	c, err := newClient(n)
	if err != nil {
		return nil, err
	}
	return &Publisher{Client: c, Topic: topic, Settings: settings}, nil
}

// newRequests is the transport integrations.<system>: { type: kafka,
// brokers, topic, client_id, timeout, tls, sasl, create_topics }.
func newRequests(n *port.Node, d registry.Deps) (port.IntegrationTransport, error) {
	if err := n.Only(append([]string{"type", "topic"}, clientKeys...)...); err != nil {
		return nil, err
	}
	topic, err := n.String("topic", "")
	if err != nil {
		return nil, err
	}
	settings, err := topicSettings(n)
	if err != nil {
		return nil, err
	}
	c, err := newClient(n)
	if err != nil {
		return nil, err
	}
	return &Requests{Client: c, Topic: topic, Settings: settings}, nil
}

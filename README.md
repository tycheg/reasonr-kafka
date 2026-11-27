# ReasonR Kafka adapters

The Kafka bus of the ReasonR engine and the `kafka` transport of external requests, on franz-go: an idempotent producer, `acks=all`, partitioning by key compatible with the Java client (murmur2).

The standard `reasonrd` distribution ([reasonr-engine](https://github.com/tycheg/reasonr-engine)) includes the adapters. A server of your own imports the package:

```go
import _ "github.com/tycheg/reasonr-kafka/src" // bus: { type: kafka }, integrations.<System>: { type: kafka }
```

## Contracts

It requires the contract of plugins alone (`reasonr-engine/port`): imports `port` and `port/registry`; tests: `port/porttest` (the bus and transport suites). The bus gets ready messages (topic, CloudEvents attributes, body) and the request transport ready requests (topic, key, attributes, body).

## Layout

| Path | What is in it |
|---|---|
| `src/publisher.go` | The bus: a ready message is a record with ce_* headers |
| `src/requests.go` | The request transport: a ready request is a record in the system's topic |
| `src/client.go` | Connections, TLS, SASL, topic creation |
| `src/register.go` | The `bus` and `integrations` factories |
| `docs` | The documentation |

## Documentation

- [Configuration](docs/configuration.md)
- [Messages](docs/messages.md)
- [Development and tests](docs/development.md)

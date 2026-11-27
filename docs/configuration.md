# Configuration

```yaml
events: main                        # the event store: the bus reads it
bus:
  type: kafka
  brokers: [kafka:9092]
  topic: "reasonr.{kind}.{type}"    # {tenant}, {kind}: event | notification, {type}: Order
  notifications: true
  create_topics: { partitions: 12, replication: 3, config: { retention.ms: "604800000" } }

integrations:
  Payments: { type: kafka, brokers: [kafka:9092], topic: "reasonr.request.{system}" }   # {tenant}, {system}
```

| Key | What it sets |
|---|---|
| `brokers` | The broker addresses (required) |
| `topic` | The topic name template |
| `notifications` | The bus also publishes notifications (right after the commit, without delivery guarantees) |
| `client_id`, `timeout` | The client id (`reasonr` by default) and the record delivery deadline (`30s`) |
| `tls` | `{ ca_file, cert_file, key_file }`; `tls: {}` — the system CAs |
| `sasl` | `{ mechanism: plain \| scram-sha-256 \| scram-sha-512, user, password_env }` |
| `create_topics` | `{ partitions, replication, config }`: `reasonrd migrate` creates the missing topics with these settings |

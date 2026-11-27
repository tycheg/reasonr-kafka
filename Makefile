GO ?= go
# The tests run on a live Kafka (KRaft, one node) when REASONR_TEST_KAFKA is
# set, otherwise they are skipped: make kafka-up, then
# REASONR_TEST_KAFKA=localhost:19092 make test. Topics are not created
# automatically: the tests create their own.

.PHONY: all check fmt-check vet lint test test-race kafka-up kafka-down

all: check

check: fmt-check vet lint test

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...

lint:
	staticcheck ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

kafka-up:
	docker run -d --name reasonr-kafka -p 19092:9092 \
		-e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,INTERNAL://:29092 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092,INTERNAL://localhost:29092 \
		-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
		-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
		apache/kafka:3.9.1
	# The readiness probe runs inside the container: the host listener
	# advertises localhost:19092, which exists only outside, so it uses INTERNAL.
	until docker exec reasonr-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list >/dev/null 2>&1; do sleep 1; done

kafka-down:
	docker rm -f reasonr-kafka

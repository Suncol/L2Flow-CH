# Repository-local Kafka

This directory manages an unprivileged, single-node Apache Kafka 4.3.1
instance for local development and throughput testing. It uses the system Java
runtime, writes only below this directory, and does not install packages,
create a system service, or write to `/etc`, `/usr`, or `/var`.

The broker and KRaft controller bind only to loopback:

```text
Broker:     127.0.0.1:9092
Controller: 127.0.0.1:9093
```

The lifecycle scripts also suppress Kafka's default local JMX connector so the
JVM does not open an additional random port. An explicitly supplied non-empty
`KAFKA_JMX_OPTS` value is preserved when JMX monitoring is needed.

## Install and run

Kafka 4.3.1 requires Java 17 or newer. Check it, install the verified Apache
binary distribution, start the broker, and run the round-trip test:

```bash
java -version
./kafka-test/install.sh
./kafka-test/start.sh
./kafka-test/smoke-test.sh
```

`install.sh` downloads `kafka_2.13-4.3.1.tgz` from Apache and verifies its
published SHA-512 before extracting it. `start.sh` initializes KRaft storage
only when `data/meta.properties` does not yet exist. Repeated starts and
restarts never reformat existing data.

Lifecycle commands work from any current working directory:

```bash
./kafka-test/status.sh
./kafka-test/restart.sh
./kafka-test/stop.sh
```

## Runtime layout

The downloaded archive, extracted distribution, symlink, and all runtime
state are ignored by Git:

```text
downloads/  downloaded Apache archive
dist/       extracted Kafka distribution
kafka       stable symlink to the selected version
data/       Kafka records and KRaft metadata
logs/       Kafka and launcher logs
run/        managed PID and smoke-test scratch files
```

The tracked broker configuration is `config/server.properties`. Because the
lifecycle scripts set their working directory to `kafka-test/`, its
`log.dirs=./data` setting resolves to this directory regardless of where the
scripts are invoked.

## Topics and clients

Create an eight-partition topic for the market-data path:

```bash
./kafka-test/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --create --topic market-data \
  --partitions 8 --replication-factor 1
```

List or describe topics:

```bash
./kafka-test/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 --list

./kafka-test/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --describe --topic market-data
```

Use `bootstrap.servers=127.0.0.1:9092` in C/C++ clients and
`bootstrap_servers="127.0.0.1:9092"` in Python clients.

## Operations and scope

Follow startup output with:

```bash
tail -f kafka-test/logs/server-console.log
```

This configuration intentionally uses one combined broker/controller and
replication factor 1. It is suitable for local integration and benchmark work,
but it is not a highly available production deployment.

Do not expose the listener beyond loopback without first adding authentication
and encryption and reviewing the host firewall. For a remote client,
`listeners` and `advertised.listeners` must both be configured deliberately;
changing only the bind address is insufficient.

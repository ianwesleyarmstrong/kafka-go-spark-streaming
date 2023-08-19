# kafka-go-spark-streaming

This project serves as a self-contained example of an event driven system using

- go
- Sarama (kafka library)
- Redpanda
- Spark Streaming

# Getting Started

1. Spin up infrastructure/microservices

```bash
docker compose up -d
```

2. Navigate to the [Redpanda Console](http://localhost:8080/topics/kraken-trades#messages) to see the trade topic

3. Watch the spark streaming logs

```bash
docker logs --follow $(docker ps -aqf "name=^spark_streaming$")
```

## Improvements

- Add structured logging/metrics/traces to kafka producers & consumers
- Use protobufs instead of JSON to pass messages
- Include more currency pairs
- Add an HTTP/gRPC API to dynamically subsribe/unsubscribe from a given currency pair
- Materialize the results of the spark streaming job in a cache/DB (redis, Apache Cassandra, ScyllaDB, etc)
- Spin up multiple Redpanda nodes
- Host on Minikube/k3s

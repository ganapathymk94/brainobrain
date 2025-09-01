#!/bin/bash

# Configurable parameters
TOPIC="quota-test-topic"
BROKER_LIST="localhost:9092"
NUM_RECORDS=1000000
RECORD_SIZE=100000  # 100 KB
THROUGHPUT=-1
BATCH_SIZE=1048576  # 1 MB

echo "Running Kafka producer performance test..."

kafka-producer-perf-test.sh \
  --topic $TOPIC \
  --num-records $NUM_RECORDS \
  --record-size $RECORD_SIZE \
  --throughput $THROUGHPUT \
  --producer-props bootstrap.servers=$BROKER_LIST \
                   batch.size=$BATCH_SIZE \
                   client.id=quota-test-client \
                   acks=1 \
                   compression.type=none \
                   linger.ms=0 \
                   max.in.flight.requests.per.connection=1 \
                   retries=0 \
  --producer.config client.properties \
  --print-metrics

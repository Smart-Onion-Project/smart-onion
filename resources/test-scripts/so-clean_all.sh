#!/bin/bash

echo "Stopping services..."
systemctl daemon-reload
/opt/smart-onion/resources/test-scripts/test_services_status.sh stop --quiet
echo "Starting Zookeeper..."
systemctl start zookeeper
sleep 2
echo "Starting Kafka..."
systemctl start kafka-server
sleep 2
echo "Starting StatsD..."
systemctl start statsd
sleep 2
echo "Deleting all metrics in StatsD..."
for g in `echo "gauges" | nc 127.0.0.1 8126 | sed 's/END//g' | sed "s/'/\"/g" | jq -r "keys[]"`; do echo "delgauges $g" | nc 127.0.0.1 8126; done
systemctl restart statsd
echo "Deleting all relevant topics in Kafka..."
/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --topic metrics --delete
/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --topic metric-collection-tasks --delete
/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --topic metric-analyzer-detected-anomalies --delete
sleep 5
echo "Removing all Nupic models..."
rm -rf /data/models/models/*
echo "Removing all Nupic anomaly likelihood calculators..."
rm -rf /data/models/anomaly_likelihood_calcs/*
echo "Removing all whisper files..."
rm -rf /data/metrics/whisper/*

# TODO: Regenerate all passwords (for DBs, users, etc)

echo =============================================
/opt/smart-onion/resources/test-scripts/so-stat.sh
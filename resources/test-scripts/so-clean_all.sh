#!/bin/bash

systemctl daemon-reload
/opt/smart-onion/resources/test-scripts/test_services_status.sh stop
systemctl start zookeeper
sleep 2
systemctl start kafka-server
sleep 2
/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --topic metrics --delete
/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --topic metric-collection-tasks --delete
/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --topic metric-analyzer-detected-anomalies --delete
sleep 5
rm -rf /data/models/models/*
rm -rf /data/models/anomaly_likelihood_calcs/*
rm -rf /data/metrics/whisper/*

# TODO: Regenerate all passwords (for DBs, users, etc)
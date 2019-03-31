#!/bin/bash

systemctl daemon-reload
/opt/smart-onion/resources/test-scripts/test_services_status.sh stop
systemctl start zookeeper
systemctl start kafka-server
sleep 2
/opt/kafka_2.11-2.1.0/bin/kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --entity-name metrics --add-config retention.ms=1000
/opt/kafka_2.11-2.1.0/bin/kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --entity-name metric-collection-tasks --add-config retention.ms=1000
/opt/kafka_2.11-2.1.0/bin/kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --entity-name metric-analyzer-detected-anomalies --add-config retention.ms=1000
sleep 5
rm -rf /data/models/models/*
rm -rf /data/models/anomaly_likelihood_calcs/*
rm -rf /data/metrics/whisper/*

# TODO: Regenerate all passwords (for DBs, users, etc)
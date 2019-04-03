#!/bin/bash

systemctl daemon-reload
/opt/smart-onion/resources/test-scripts/test_services_status.sh stop --quiet
echo "Deleting all metrics in StatsD..."
systemctl start statsd
for g in `echo "gauges" | nc 127.0.0.1 8126 | sed 's/END//g' | sed "s/'/\"/g" | jq -r "keys[]"`; do echo "delgauges $g" | nc 127.0.0.1 8126; done
systemctl stop statsd
cd /opt/smart-onion
echo "Pulling new version from GitHub..."
git stash
git pull
echo "Updating systemctl with the new Unit files and starting services..."
systemctl daemon-reload
systemctl start smart-onion-configurator
sleep 2s
systemctl start kafka-server
sleep 2s
/opt/smart-onion/resources/test-scripts/test_services_status.sh start --quiet
echo -=-=-=-=-=-=-=-=-=-=-=-=-=-=
/opt/smart-onion/resources/test-scripts/so-stat.sh
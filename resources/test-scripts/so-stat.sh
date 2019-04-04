#!/bin/bash

INFRA_SERVICES=`echo -e "zookeeper\nkafka-server\nlogstash_statsd2kafka\nlogstash_kafka2graphite\ngraphite-carbon\ngraphite-api\nstatsd\n"`
ALL_UNIT_FILES=`ls -1 /etc/systemd/system/smart-onion*.service`
ALL_SERVICES="$INFRA_SERVICES $ALL_UNIT_FILES"

for svc_unit in $ALL_SERVICES; do
  SERVICE_NAME=`/usr/bin/basename $svc_unit | sed -s 's/\.service//g'`
  STATUS=`systemctl --no-pager status $SERVICE_NAME | grep 'Active: active (running) since' | wc -l | sed 's/1/UP/g' | sed 's/0/DOWN/g'`
  printf "%-33s %s\n" "$SERVICE_NAME:" [$STATUS]
done

echo
echo "Non Anomaly Metric (*.wsp) Files: "`find /data/metrics/whisper/ -name '*.wsp' | grep -v anomaly | wc -l`
echo "Anomaly Metric (*.wsp) Files    : "`find /data/metrics/whisper/ -name '*.wsp' | grep anomaly | wc -l`
echo "Metrics in StatsD               : "`echo "gauges" | nc 127.0.0.1 8126 | sed 's/END//g' | sed "s/'/\"/g" | jq -r "keys[]" | wc -l`
#!/bin/bash

INFRA_SERVICES=`echo -e "zookeeper\nkafka-server\nlogstash_statsd2kafka\nlogstash_kafka2graphite\n"`
ALL_UNIT_FILES=`ls -1 /etc/systemd/system/smart-onion*.service`
ALL_SERVICES="$INFRA_SERVICES $ALL_UNIT_FILES"

for svc_unit in $ALL_SERVICES; do
  SERVICE_NAME=`/usr/bin/basename $svc_unit | sed -s 's/\.service//g'`
  STATUS=`systemctl --no-pager status $SERVICE_NAME | grep 'Active: active (running) since' | wc -l | sed 's/1/UP/g' | sed 's/0/DOWN/g'`
  echo "$SERVICE_NAME: $STATUS"
done

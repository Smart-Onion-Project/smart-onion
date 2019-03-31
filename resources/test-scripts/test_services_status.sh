#!/bin/bash

INFRA_SERVICES=`echo -e "zookeeper\nkafka-server\nlogstash_statsd2kafka\nlogstash_kafka2graphite\n"`
ALL_UNIT_FILES=`ls -1 /etc/systemd/system/smart-onion*.service`
ALL_SERVICES="$INFRA_SERVICES $ALL_UNIT_FILES"
COMMAND='status'

if [ "$#" -gt 0 ]; then
  COMMAND=$1
fi

for svc_unit in $ALL_SERVICES; do
  SERVICE_NAME=`/usr/bin/basename $svc_unit | sed -s 's/\.service//g'`
  echo "Getting/running $COMMAND of $SERVICE_NAME..."
  systemctl --no-pager $COMMAND $SERVICE_NAME | grep Active
  systemctl --no-pager status $SERVICE_NAME | tail -n1
  echo
done

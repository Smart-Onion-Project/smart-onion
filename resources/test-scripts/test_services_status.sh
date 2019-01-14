#!/bin/bash

ALL_UNIT_FILES=`ls -1 /etc/systemd/system/smart-onion*.service`

for svc_unit in $ALL_UNIT_FILES; do
  SERVICE_NAME=`/usr/bin/basename $svc_unit | sed -s 's/\.service//g'`
  echo "Getting status of $SERVICE_NAME..."
  systemctl --no-pager status $SERVICE_NAME | grep Active
  systemctl --no-pager status $SERVICE_NAME | tail -n1
  echo
done
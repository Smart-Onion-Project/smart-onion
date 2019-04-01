#!/bin/bash

systemctl daemon-reload
/opt/smart-onion/resources/test-scripts/test_services_status.sh stop --quiet
cd /opt/smart-onion
git stash
git pull
systemctl daemon-reload
systemctl start smart-onion-configurator
systemctl status smart-onion-configurator | tail -n1
sleep 2s
/opt/smart-onion/resources/test-scripts/test_services_status.sh start --quiet
echo -=-=-=-=-=-=-=-=-=-=-=-=-=-=
/opt/smart-onion/resources/test-scripts/so-stat.sh
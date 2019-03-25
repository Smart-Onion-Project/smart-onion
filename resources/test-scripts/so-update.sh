#!/bin/bash

systemctl daemon-reload
/opt/smart-onion/resources/test-scripts/test_services_status.sh stop
cd /opt/smart-onion
git pull
systemctl daemon-reload
systemctl start smart-onion-configurator
sleep 2s
/opt/smart-onion/resources/test-scripts/test_services_status.sh start
echo -=-=-=-=-=-=-=-=-=-=-=-=-=-=
/opt/smart-onion/resources/test-scripts/test_services_status.sh
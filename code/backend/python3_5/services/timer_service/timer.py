#!/usr/bin/python3.5
##########################################################################
# Timer service                                                          #
# -------------                                                          #
#                                                                        #
# This service is part of the Smart-Onion package. This micro-service is #
# responsible for periodically calling discover methods on the metrics   #
# collector and sending metrics collection tasks to kafka for every      #
# object detected. The metric collector will listen on the relevant      #
# kafka topic and will carry out the monitoring tasks                    #
#                                                                        #
##########################################################################

import json
import time
import sys
import os
import re
import kafka
from urllib import request as urllib_req


class TimerService:

    def __init__(self, queries, listen_ip, listen_port, config_copy, interval=3600):
        self._interval = interval
        self._queries = queries
        self._listen_ip = listen_ip
        self._listen_port = listen_port
        self._config_copy = config_copy

    def pack_and_resend_tasks(self, tasks_list):
        max_items_in_batch = self._config_copy["smart-onion.config.architecture.internal_services.backend.timer.max_items_in_batch"]
        kafka_sender = kafka.producer.KafkaProducer(bootstrap_servers=self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"])
        batch = []
        for item in tasks_list:
            batch.append(item)
            if len(batch) >= max_items_in_batch:
                kafka_sender.send(topic="metric_collection_tasks", value=json.dumps(batch).encode())
                batch = []
        
        if len(batch) > 0:
            kafka_sender.send(topic="metric_collection_tasks", value=json.dumps(batch).encode())
        
    def run(self):
        base_url = ""
        if self._config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol" in self._config_copy:
            base_url = base_url + self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol"] + "://"
        if self._config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host" in self._config_copy:
            base_url = base_url + self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host"] + ":"
        if self._config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port" in self._config_copy:
            base_url = base_url + str(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port"])
        base_url = base_url + self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.lld"]

        cur_tasks_list = []
        print("INFO: Loaded with the following settings: DiscoveryInterval=" + str(self._interval) + ",MaxBatchSize:" + str(self._config_copy["smart-onion.config.architecture.internal_services.backend.timer.max_items_in_batch"]) + ",BaseURL=" + base_url + ",KafkaBootstrapServers=" + self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"])
        while True:
            lld_queries = [q for q in self._queries if self._queries[q]["type"]=="LLD"]
            for query in lld_queries:
                cur_url = base_url + query
                print("Calling " + cur_url)
                discover_raw_res = str(urllib_req.urlopen(cur_url).read().decode('utf-8')).replace("@@RES: ", '', 1)
                if "@@EXCEPTION:" in discover_raw_res:
                    print("WARN: Discovery call returned an exception. See the metrics_collector logs for more info. Re-Sending last tasks for this discovery ('" + query + "') to the Kafka service...")
                else:
                    discover_res = json.loads(discover_raw_res)
                    for task in discover_res["data"]:
                        cur_tasks_list.append({
                            "URL": task["{#URL}"]
                        })

            self.pack_and_resend_tasks(cur_tasks_list)
            cur_tasks_list = []
            time.sleep(self._interval)

config_copy = {}
configurator_base_url = ""
script_path = os.path.dirname(os.path.realpath(__file__))
config_file_name = "queries.json"
config_file_default_path = "/etc/smart-onion/"
config_file = os.path.join(config_file_default_path, config_file_name)
settings_file_name = "metrics_collector_settings.json"
settings_file = os.path.join(config_file_default_path, settings_file_name)
settings = None
try:
    with open(settings_file, "r") as settings_file_obj:
        settings = json.loads(settings_file_obj.read())

    configurator_host = settings["smart-onion.config.architecture.configurator.listening-host"]
    configurator_port = int(settings["smart-onion.config.architecture.configurator.listening-port"])
    configurator_proto = settings["smart-onion.config.architecture.configurator.protocol"]
except:
    configurator_host = "127.0.0.1"
    configurator_port = 9003
    configurator_proto = "http"

queries_conf = None
while queries_conf is None:
    try:
        # Contact configurator to fetch all of our config and configure listen-ip and port
        configurator_base_url = str(configurator_proto).strip() + "://" + str(configurator_host).strip() + ":" + str(configurator_port).strip() + "/smart-onion/configurator/"
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.architecture.internal_services.backend.*"
        configurator_response = urllib_req.urlopen(configurator_final_url).read().decode('utf-8')
        config_copy = json.loads(configurator_response)
        listen_ip = config_copy["smart-onion.config.architecture.internal_services.backend.timer.listening-host"]
        listen_port = config_copy["smart-onion.config.architecture.internal_services.backend.timer.listening-port"]
        discover_interval = config_copy["smart-onion.config.architecture.internal_services.backend.timer.discover-interval"]
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.queries"
        configurator_response = urllib_req.urlopen(configurator_final_url).read().decode('utf-8')
        queries_conf = json.loads(configurator_response)
    except:
        print(
            "WARN: Waiting (indefinetly in 10 sec intervals) for the Configurator service to become available (waiting for queries config)...")
        time.sleep(10)

config_file_specified_on_cmd = False
if len(sys.argv) > 1:
    for arg in sys.argv:
        if "=" in arg:
            arg_name = arg.split("=")[0]
            arg_value = arg.split("=")[1]

            if arg_name == "--listen-ip":
                if not re.match("[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+", arg_value):
                    print("EEROR: The --listen-ip must be a valid IPv4 address.  Using default of " + str(listen_ip) + " (hardcoded)")
                else:
                    listen_ip = arg_value

            if arg_name == "--listen-port":
                try:
                    listen_port = int(arg_value)
                except:
                    print("ERROR: The --listen-port argument must be numeric. Using default of " + str(listen_port) + " (hardcoded)")

            if arg_name == "--config-file":
                config_file = arg_value
                config_file_specified_on_cmd = True
        else:
            if arg == "--help" or arg == "-h" or arg == "/h" or arg == "/?":
                print("USAGE: " + os.path.basename(os.path.realpath(__file__)) + " [--listen-ip=127.0.0.1 --listen-port=8080 --config-file=/etc/smart-onion/queries.json]")
                print("")
                print("-h, --help, /h and /q will print this help screen.")
                print("")
                quit(1)

TimerService(queries=queries_conf, interval=discover_interval, listen_ip=listen_ip, listen_port=listen_port, config_copy=config_copy).run()
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
import uuid
import hashlib
import threading
from bottle import Bottle
from urllib import request as urllib_req
from multiprocessing import Value

DEBUG = False

class TimerService:

    def __init__(self, queries, listen_ip, listen_port, config_copy, interval=3600):
        self._time_loaded = time.time()
        self._interval = interval
        self._queries = queries
        self._listen_ip = listen_ip
        self._listen_port = listen_port
        self._config_copy = config_copy
        self._kafka_client_id = "SmartOnionTimerService_" + str(uuid.uuid4()) + "_" + str(int(time.time()))
        self._kafka_server = self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"]
        self._kafka_producer = None
        self._discovery_requests_ran = Value('i', 0)
        self._discovery_requests_completed_successfully = Value('i', 0)
        while self._kafka_producer is None:
            try:
                self._kafka_producer = kafka.producer.KafkaProducer(bootstrap_servers=self._kafka_server, client_id=self._kafka_client_id)
            except Exception as ex:
                print("WARN: Waiting (indefinetly in 10 sec intervals) for the Kafka service to become available...  (" + str(ex) + " (" + type(ex).__name__ + "))")
                time.sleep(10)
        self._app = Bottle()
        self._route()

    def _route(self):
        self._app.route('/ping', method="GET", callback=self._ping)

    def pack_and_resend_tasks(self, tasks_list):
        max_items_in_batch = self._config_copy["smart-onion.config.architecture.internal_services.backend.timer.max_items_in_batch"]
        kafka_topic = self._config_copy["smart-onion.config.architecture.internal_services.backend.timer.metrics_collection_tasks_topic"]
        batch = []
        for item in tasks_list:
            batch.append(item)
            timestamp = time.time()
            if len(batch) >= max_items_in_batch:
                # print("DEBUG: [" + str(timestamp) + "]Sending a batch of " + str(len(batch)) + " to Kafka in topic '" + kafka_topic + "' on server " + self._kafka_server)
                self._kafka_producer.send(topic=kafka_topic, value=json.dumps({"batch": batch, "timestamp": timestamp}).encode('utf-8'))
                batch = []

        if len(batch) > 0:
            timestamp = time.time()
            # print("DEBUG: [" + str(timestamp) + "]Sending a batch of " + str(len(batch)) + " to Kafka in topic '" + kafka_topic + "' on server " + self._kafka_server)
            self._kafka_producer.send(topic=kafka_topic, value=json.dumps({"batch": batch, "timestamp": timestamp}).encode('utf-8'))

        timestamp = time.time()
        print("INFO[" + str(timestamp) + "]: Sent " + str(len(tasks_list)) + " tasks to Kafka in topic '" + kafka_topic + "' on server " + self._kafka_server)

    def _file_as_bytes(self, filename):
        with open(filename, 'rb') as file:
            return file.read()

    def _ping(self):
        return {
            "response": "PONG",
            "file": __file__,
            "hash": hashlib.md5(self._file_as_bytes(__file__)).hexdigest(),
            "uptime": time.time() - self._time_loaded,
            "service_specific_info": {
                "discovery_requests_ran": self._discovery_requests_ran,
                "discovery_requests_completed_successfully": self._discovery_requests_completed_successfully
            }
        }

    def run(self):
        print("DEBUG: Launching the timer thread...")
        self._timer_thread = threading.Thread(target=self.run_timer)
        self._timer_thread.start()

        if DEBUG:
            self._app.run(host=self._listen_ip, port=self._listen_port)
        else:
            self._app.run(host=self._listen_ip, port=self._listen_port, server="gunicorn", workers=32, timeout=120)

    def run_timer(self):
        while True:
            try:
                base_url = ""
                if self._config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol" in self._config_copy:
                    base_url = base_url + self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol"] + "://"
                if self._config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host" in self._config_copy:
                    base_url = base_url + self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host"] + ":"
                if self._config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port" in self._config_copy:
                    base_url = base_url + str(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port"])
                base_url = base_url + self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.lld"]

                cur_tasks_list = []
                print("INFO: Loaded with the following settings: DiscoveryInterval=" + str(self._interval) + ",MaxBatchSize:" + str(self._config_copy["smart-onion.config.architecture.internal_services.backend.timer.max_items_in_batch"]) + ",BaseURL=" + base_url + ",KafkaBootstrapServers=" + self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"] + ",KafkaClientID:" + self._kafka_client_id)
                while True:
                    try:
                        lld_queries = [q for q in self._queries if self._queries[q]["type"]=="LLD"]
                        for query in lld_queries:
                            self._discovery_requests_ran.value += 1
                            try:
                                cur_url = base_url + query
                                print("Calling " + cur_url)
                                discover_raw_res = str(urllib_req.urlopen(cur_url).read().decode('utf-8')).replace("@@RES: ", '', 1)
                                if "@@EXCEPTION:" in discover_raw_res:
                                    print("WARN: Discovery call ('" + cur_url + "') returned an exception. See the metrics_collector logs for more info. Skipping these URLs. The metric collector should still collect those metrics according to their TTL...")
                                else:
                                    discover_res = json.loads(discover_raw_res)
                                    for task in discover_res["data"]:
                                        cur_tasks_list.append({
                                            "URL": task["{#URL}"]
                                        })
                                    self._discovery_requests_completed_successfully.value += 1
                            except Exception as ex:
                                print("WARN: Failed to query or parse the discovery results (" + str(ex) + " (" + type(ex).__name__ + ")). TRYING OTHER DISCOVERIES. SOME OR ALL METRICS MIGHT NOT BE CREATED OR UPDATED.")

                    except Exception as ex:
                        print("WARN: Failed to query or parse the discovery results (" + str(ex) + " (" + type(ex).__name__ + ")). CANNOT REPORT RESULTS TO KAFKA. SOME OR ALL METRICS MIGHT NOT BE CREATED OR UPDATED.")

                    if len(cur_tasks_list) > 0:
                        try:
                            self.pack_and_resend_tasks(cur_tasks_list)
                        except Exception as ex:
                            print("WARN: Failed to report the list of tasks to the Kafka server. Will try again in the next discovery cycle. (" + str(ex) + " (" + type(ex).__name__ + "))")

                    cur_tasks_list = []
                    print("INFO: Going to sleep for " + str(self._interval) + " seconds...")
                    time.sleep(self._interval)
            except KeyboardInterrupt:
                print("INFO: Shutting down... (KeyboardInterrupt)")
            except Exception as ex:
                print("ERROR: An '" + str(ex) + "' (" + type(ex).__name__ + ") exception has been thrown in the timer thread. Waiting 10 seconds and retrying...")
                time.sleep(10)

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
    except Exception as ex:
        print("WARN: Waiting (indefinetly in 10 sec intervals) for the Configurator service to become available (waiting for queries config)... (" + str(ex) + " (" + type(ex).__name__ + ")")
        time.sleep(10)

metric_collector_ping_response = None
while metric_collector_ping_response is None:
    try:
        base_url = ""
        if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol" in config_copy:
            base_url = base_url + config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol"] + "://"
        if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host" in config_copy:
            base_url = base_url + config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host"] + ":"
        if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port" in config_copy:
            base_url = base_url + str(config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port"])

        metric_collector_ping_response = json.loads(urllib_req.urlopen(base_url + "/ping").read().decode('utf-8'))
        if metric_collector_ping_response["response"] != "PONG":
            metric_collector_ping_response = None
    except:
        print("WARN: Waiting (indefinetly in 10 sec intervals) for the Metrics-Collector service to become available...")
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

sys.argv = [sys.argv[0]]
TimerService(queries=queries_conf, interval=discover_interval, listen_ip=listen_ip, listen_port=listen_port, config_copy=config_copy).run()
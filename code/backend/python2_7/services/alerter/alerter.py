#!/usr/bin/python2.7
from __future__ import print_function
import sys
import os
import re
import urllib
from bottle import route, run, template, get, post, request
from bottle import Bottle
import datetime
import json
import base64
import syslog
import hashlib
import time
from multiprocessing import Value
import kafka
import uuid
import threading

# This service should hold the following models for detecting important anomalies:
# - A model that will encode a pattern of anomalies reported within a 10 mins time period along with the current date and detect anomalies
# - A model that will encode a pattern of anomalies reported within a 10 mins time period along with the current jewish date and detect anomalies
# - A model that will encode a pattern of anomalies reported within a 1 hour time period along with the current date and detect anomalies
# - A model that will encode a pattern of anomalies reported within a 1 hour time period along with the current jewish date and detect anomalies
# - A model that will encode a pattern of anomalies reported within a 1 day time period along with the current date and detect anomalies
# - A model that will encode a pattern of anomalies reported within a 1 day time period along with the current jewish date and detect anomalies
# - A model that will encode a pattern of anomalies reported within a 1 week time period along with the current date and detect anomalies
# - A model that will encode a pattern of anomalies reported within a 1 week time period along with the current jewish date and detect anomalies

# The pattern of anomalies should encode a number per metric category (lateral movement, penetration, privilege escalation, etc.) that number would be
# the sum of all the anomalies reported in that specific category.


DEBUG = True
SINGLE_THREADED = True


class SmartOnionAlerter:

    def __init__(self, listen_ip, listen_port, config_copy):
        self._config_copy = config_copy
        self._logging_format = self._config_copy["smart-onion.config.common.logging_format"]
        self._time_loaded = time.time()
        self._unique_metrics_lock = threading.Lock()
        self._unique_metrics = {

        }
        self._unique_metrics_length = {}
        self._host = listen_ip
        self._port = listen_port
        self._app = Bottle()
        self._route()
        self._kafka_client_id = "SmartOnionAlerterService_" + str(uuid.uuid4()) + "_" + str(int(time.time()))
        self._kafka_server = self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"]
        self._metrics_kafka_topic = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.metrics_topic_name"]
        self._reported_anomalies_kafka_topics = [
            self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.reported_anomalies_topic"],
            self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.reported_anomalies_topic"]
        ]
        self._metrics_to_work_on_pattern = re.compile(self._config_copy["smart-onion.config.architecture.internal_services.backend.alerter.metrics_to_work_on_pattern"])
        self._metrics_for_anomaly_detectives_to_work_on_pattern = re.compile(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.metrics_to_work_on_pattern"])
        self._kafka_metrics_consumer = None
        self._kafka_alerts_consumer = None
        while self._kafka_metrics_consumer is None:
            try:
                self._kafka_metrics_consumer = kafka.KafkaConsumer(self._metrics_kafka_topic,
                                                     bootstrap_servers=self._kafka_server,
                                                     client_id=self._kafka_client_id)
                if self._reported_anomalies_kafka_topics[0] == self._reported_anomalies_kafka_topics[1]:
                    self._kafka_alerts_consumer = kafka.KafkaConsumer(topics=self._reported_anomalies_kafka_topics[0],
                                                     bootstrap_servers=self._kafka_server,
                                                     client_id=self._kafka_client_id)
                else:
                    self._kafka_alerts_consumer = kafka.KafkaConsumer(topics=self._reported_anomalies_kafka_topics,
                                                     bootstrap_servers=self._kafka_server,
                                                     client_id=self._kafka_client_id)

                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "__init__", "INFO", str(None), str(None), str(None), str(None), "Loaded Kafka consumers successfully. (self._metrics_kafka_topic=" + str(self._metrics_kafka_topic) + ";self._reported_anomalies_kafka_topics=" + json.dumps(self._reported_anomalies_kafka_topics) + ";bootstrap_servers=" + str(self._kafka_server) + ";client_id=" + str(self._kafka_client_id) + ")"))
            except Exception as ex:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "__init__", "INFO", str(None), str(ex), str(type(ex).__name__), str(None), "Waiting on a dedicated thread for the Kafka server to be available... Going to sleep for 10 seconds"))
                time.sleep(10)
        self._metrics_poller_thread = threading.Thread(target=self._pull_metrics)
        self._anomaly_reports_poller_thread = threading.Thread(target=self._pull_anomaly_reports)

    def _route(self):
        # self._app.route('/smart-onion/alerter/report_alert', method="POST", callback=self.report_alert)
        self._app.route('/ping', method="GET", callback=self._ping)
        self._app.route('/ping_details', method="GET", callback=self._ping_details)

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
            }
        }

    def _ping_details(self):
        with self._unique_metrics_lock:
            return {
                "response": "PONG",
                "file": __file__,
                "hash": hashlib.md5(self._file_as_bytes(__file__)).hexdigest(),
                "uptime": time.time() - self._time_loaded,
                "service_specific_info": {
                    "unique_metrics_length": self._unique_metrics_length,
                    "unique_metrics": self._unique_metrics
                }
        }

    def run(self):
        try:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "run", "INFO", str(None), str(None), str(None), str(None), "Starting the metrics poller thread"))
            self._metrics_poller_thread.start()
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "run", "INFO", str(None), str(None), str(None), str(None), "Starting the anomalies poller thread"))
            self._anomaly_reports_poller_thread.start()
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "run", "INFO", str(None), str(None), str(None), str(None), "Starting an HTTP listener on " + str(self._host) + ":" + str(self._port)))
            if SINGLE_THREADED:
                self._app.run(host=self._host, port=self._port)
            else:
                self._app.run(host=self._host, port=self._port, server="gunicorn", workers=32)
        except Exception as ex:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "run", "ERROR", str(None), str(None), str(ex), str(type(ex).__name__), str(None), "Failed to start an HTTP listener on " + str(self._host) + ":" + str(self._port)))
            raise ex

    def _pull_anomaly_reports(self):
        for anomaly_report_record in self._kafka_alerts_consumer:
            try:
                report_obj = json.loads(anomaly_report_record.value.decode())
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "pull_anomaly_reports", "INFO", str(None), str(None), str(None), str(None), "Received anomaly report from " + report_obj["reporter"] + ". Report contents is " + json.dumps(report_obj)))
            except Exception as ex:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "pull_anomaly_reports", "WARN", str(None), str(None), str(ex), str(type(ex).__name__), str(None), "Received an anomaly report that was not structured properly. Cannot process it. DISCARDING. Raw content is: " + base64.b64encode(str(anomaly_report_record.value).encode('utf-8')).decode('utf-8')))

    def _pull_metrics(self):
        for metric in self._kafka_metrics_consumer:
            metric_name = None
            try:
                metric_name = metric.value
                if metric.value is None or metric.value.strip() == "" or len(metric.value.split(" ")) != 3:
                    if DEBUG:
                        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "pull_metrics", "DEBUG", str(None), str(metric_name), str(None), str(None), "Received this malformed metric. Ignoring"))
                    continue

                metric_name = metric.value.split(" ")[0]
                if re.match(self._metrics_to_work_on_pattern, str(metric_name)):
                    # if DEBUG:
                    #     syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "pull_metrics", "DEBUG", str(None), str(metric_name), str(None), str(None), "Handling this metric since it matches the regex " + self._metrics_to_work_on_pattern.pattern))
                    pass

                elif re.match(self._metrics_for_anomaly_detectives_to_work_on_pattern, str(metric_name)):
                    metric_name = "stats.gauges.smart-onion.anomaly_score.anomaly_detector." + metric_name

                else:
                    # if DEBUG:
                    #     syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "pull_metrics", "DEBUG", str(None), str(metric_name), str(None), str(None), "Ignoring this metric since it DOES NOT match the regex " + self._metrics_to_work_on_pattern.pattern))
                    continue

                metric_name_parts = metric_name.split(".")
                with self._unique_metrics_lock:
                    metric_name_including_path_sender_agnostic = ".".join(metric_name_parts[0:10])
                    metric_name_including_path_sender_agnostic = metric_name_including_path_sender_agnostic.replace(".anomaly_detector.", ".__detecting_service__.")
                    metric_name_including_path_sender_agnostic = metric_name_including_path_sender_agnostic.replace(".metrics_analyzer.", ".__detecting_service__.")
                    for detecting_service in ["anomaly_detector", "metrics_analyzer"]:
                        metric_name_to_use = metric_name.replace(".anomaly_detector.", "." + detecting_service + ".")
                        metric_name_to_use = metric_name_to_use.replace(".metrics_analyzer.", "." + detecting_service + ".")
                        if metric_name_including_path_sender_agnostic not in self._unique_metrics.keys():
                            self._unique_metrics[metric_name_including_path_sender_agnostic] = [metric_name_to_use]
                            self._unique_metrics_length[metric_name_including_path_sender_agnostic] = 1
                        else:
                            if metric_name_to_use not in self._unique_metrics[metric_name_including_path_sender_agnostic]:
                                self._unique_metrics[metric_name_including_path_sender_agnostic].append(metric_name_to_use)
                                self._unique_metrics_length[metric_name_including_path_sender_agnostic] += 1

            except Exception as ex:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "alerter", "run", "ERROR", str(None), str(metric_name), str(ex), str(type(ex).__name__), str(None), "Failed to handle a metric from Kafka."))

    def report_alert(self):
        #This method should expect to receive a JSON (as an argument in the POST vars) with all the details of the metrics that triggered this alert
        #This method should then search for relevant details for the specific alert that has been triggered and decide whether or not to create a case for
        #the relevant event.
        #
        #Score = highest_base_family_score + pattern_predictability_as_true_positive - pattern_predictability_by_htm - pattern_predictability_as_false_positive
        #Algorithm for detecting priority for each event - Should run after each event is added to the list:
        # - Divide the highest score on the list by 5 => x
        # - Divide each event's score by x => event_priority
        try:
            report_obj = request.json
            syslog.syslog("SmartOnionAlerter: INFO: Received anomaly report from " + report_obj["reporter"] + ". Report contents is " + json.dumps(report_obj))
        except:
            syslog.syslog("SmartOnionAlerter: WARN: Received an anomaly report that was not structured properly. Cannot process it. DISCARDING. Raw content is: " + base64.b64encode(str(request.body).encode('utf-8')).decode('utf-8'))


script_path = os.path.dirname(os.path.realpath(__file__))
config_file_default_path = "/etc/smart-onion/"
settings_file_name = "alerter_settings.json"
settings_file = os.path.join(config_file_default_path, settings_file_name)
settings = None
config_copy = None
listen_ip = None
listen_port = 0
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

while config_copy is None:
    try:
        # Contact configurator to fetch all of our config and configure listen-ip and port
        configurator_base_url = str(configurator_proto).strip() + "://" + str(configurator_host).strip() + ":" + str(configurator_port).strip() + "/smart-onion/configurator/"
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.architecture.internal_services.backend.*"
        configurator_response = urllib.urlopen(configurator_final_url).read().decode('utf-8')
        config_copy = json.loads(configurator_response)
        generic_config_url = configurator_base_url + "get_config/" + "smart-onion.config.common.*"
        configurator_response = urllib.urlopen(generic_config_url).read().decode('utf-8')
        config_copy = dict(config_copy, **json.loads(configurator_response))
        logging_format = config_copy["smart-onion.config.common.logging_format"]
        listen_ip = config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-host"]
        listen_port = config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-port"]
    except Exception as ex:
        print("WARN: Waiting (indefinetly in 10 sec intervals) for the Configurator service to become available... (Exception: " + str(ex) + ")")
        time.sleep(10)

if len(sys.argv) > 1:
    for arg in sys.argv:
        if "=" in arg:
            arg_name = arg.split("=")[0]
            arg_value = arg.split("=")[1]

            if arg_name == "--listen-ip":
                if not re.match("[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+", arg_value):
                    print("ERROR: The --listen-ip must be a valid IPv4 address.  Using default of " + str(listen_ip) + " (hardcoded)")
                else:
                    listen_ip = arg_value

            if arg_name == "--listen-port":
                try:
                    listen_port = int(arg_value)
                except:
                    print("ERROR: The --listen-port argument must be numeric. Using default of " + str(listen_port) + " (hardcoded)")
        else:
            if arg == "--help" or arg == "-h" or arg == "/h" or arg == "/?":
                print("USAGE: " + os.path.basename(os.path.realpath(__file__)) + " [--listen-ip=127.0.0.1 --listen-port=8080]")
                print("")
                print("-h, --help, /h and /q will print this help screen.")
                print("")
                quit(1)

sys.argv = [sys.argv[0]]
SmartOnionAlerter(listen_ip=listen_ip, listen_port=listen_port, config_copy=config_copy).run()


#!/usr/bin/python2.7
##########################################################################
# Metrics Analyzer                                                       #
# ----------------                                                       #
#                                                                        #
# This service is part of the Smart-Onion package. This micro-service is #
# responsible for creating models and detecting anomalies for metrics    #
# that are sent to it (it uses the graphite format for sending in        #
# metrics (e.g. metric.name.hierarchy value timestamp_in_unix_ms)        #
#                                                                        #
# it uses the HTM algorithm to detect temporal anomalies in the metric   #
# values and make predictions.                                           #
##########################################################################

import sys
import yaml
import time
import os
# import socket
import importlib
import json
import urllib
import statsd
from threading import Lock
from os.path import dirname
from datetime import datetime
from nupic.frameworks.opf.model_factory import ModelFactory
from nupic.algorithms.anomaly_likelihood import AnomalyLikelihood
import hashlib
import threading
from bottle import Bottle
import base64
import uuid
import kafka
from multiprocessing import Value
import re
from __future__ import print_function


DEBUG = False
create_model_thread_lock = Lock()
create_anomaly_likelihood_calc_thread_lock = Lock()

class Utils:

    def __init__(self):
        pass

    @staticmethod
    def extract_args(arg_prefix, error_if_not_found=True):
        """
        This method extracts command line arguments to help get configuration from the command line.
        The command line arguments should be in this format:
        python metrics_analyzer.py --conn-backlog=20 --listen-port=3000 ...
        :param arg_prefix: Indicates the argument prefix that this method should look for in the command line (i.e.
        conn-backlog)
        :param error_if_not_found: Whether or not to raise an exception if the requested argument was not found in the
        command line
        :return: The value of the requested command line argument (if found) and None (or an exception) otherwise.
        """
        for arg in sys.argv:
            if arg.startswith("--" + arg_prefix + "="):
                return arg.split("=")[1]
        if error_if_not_found:
            raise Exception("Argument not found.")
        else:
            return None


class MetricsRealtimeAnalyzer:
    EXIT_ALL_THREADS_FLAG = False
    models = {}
    anomaly_likelihood_detectors = {}
    anomaly_likelihood_detectors_save_base_path = None
    models_save_base_path = None
    models_params_base_path = None
    anomaly_likelihood_calculator_filename = "anomaly_likelihood_calculator"
    metrics_prefix = "smart-onion.anomaly_score.metrics_analyzer"
    statsd_client = statsd.StatsClient(prefix=metrics_prefix)

    def __init__(self, config_copy):
        self._metrics_received = Value('i', 0)
        self._metrics_successfully_processed = Value('i', 0)
        self._raw_metrics_downloaded_from_kafka = Value('i', 0)
        self._time_loaded = time.time()
        self._app = Bottle()
        self._route()
        self._config_copy = config_copy
        self._last_logged_message_about_too_many_models = 0
        self._kafka_client_id = "SmartOnionMetricsAnalyzerService_" + str(uuid.uuid4()) + "_" + str(int(time.time()))
        self._kafka_server = self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"]
        self._reported_anomalies_kafka_topic = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.reported_anomalies_topic"]
        self._allowed_to_work_on_metrics_pattern = re.compile(self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.metrics_to_work_on_pattern"])
        self._kafka_producer = None
        while self._kafka_producer is None:
            try:
                self._kafka_producer = kafka.producer.KafkaProducer(bootstrap_servers=self._kafka_server, client_id=self._kafka_client_id)
            except Exception as ex:
                print("WARN: Waiting (indefinetly in 10 sec intervals) for the Kafka service to become available... (" + str(ex) + " (" + type(ex).__name__ + ")")
                time.sleep(10)

    def _route(self):
        self._app.route('/ping', method="GET", callback=self._ping)

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
                "metrics_received": self._metrics_received.value,
                "metrics_successfully_processed": self._metrics_successfully_processed.value,
                "raw_metrics_downloaded_from_kafka": self._raw_metrics_downloaded_from_kafka.value,
                "models_loaded": len(self.models),
                "anomaly_likelihood_calculators_loaded": len(self.anomaly_likelihood_detectors)
            }
        }

    def report_anomaly(self, metric, anomaly_info):
        anomaly_report = {
            "report_id": str(uuid.uuid4()),
            "metric": metric,
            "reporter": "metrics_analyzer",
            "meta_data": anomaly_info
        }
        try:
            res = urllib.urlopen(url=self.alerter_url, data=anomaly_report, context={'Content-Type': 'application/json'}).read().decode('utf-8')
            if res is None:
                res = "None"
        except Exception as ex:
            print("WARN: Failed to report the following anomaly to the alerter service directly due to the follwing exception: " + str(ex) + ". Will still try to report the following anomaly to Kafka: " + json.dumps(anomaly_report))

        try:
            self._kafka_producer.send(topic=self._reported_anomalies_kafka_topic, value=json.dumps(anomaly_report).encode('utf-8'))
            print("INFO: Reported the following anomaly to the alerter service and to kafka: " + json.dumps(anomaly_report))
        except Exception as ex:
            print("WARN: Failed to report the following anomaly to Kafka due to the follwing exception: " + str(ex) + ". These are the anomaly details: " + json.dumps(anomaly_report))

    def get_save_path(self, metric, path_element="model"):
        """
        This method returns the save path of the module or anomaly likelihood detector. (based on the current
        configuration and the metric in question)
        :param metric: The metric name (metric.entire.hierarchy) of the metric to return the save path for.
        :param path_element: Whether to return the model save path or the anomaly likelihood detector's path
        :return: String with the save path of the requested metric's model/anomaly detector
        """

        if path_element == "model":
            save_path = self.models_save_base_path
            save_path = os.path.join(save_path, metric.replace(".", "/"))
            return save_path
        elif path_element == "anomaly_likelihood_calculator":
            save_path = self.anomaly_likelihood_detectors_save_base_path
            save_path = os.path.join(save_path, metric.replace(".", "/"))
            return save_path
        else:
            raise Exception("Unrecognized path element code")

    def auto_save_models(self, interval):
        """
        This method runs on a dedicated thread for automatically saving the currently used models and anomaly likelihood
        detectors to files.
        :param interval: The number of seconds between each save attempt.
        :return: None
        """

        for i in range(0, interval):
            if self.EXIT_ALL_THREADS_FLAG:
                return

            time.sleep(1)

        while True:
            if self.EXIT_ALL_THREADS_FLAG:
                return

            create_model_thread_lock.acquire()
            models_count = len(self.models)
            for model_idx in range(0, models_count):
                model_obj = self.models.items()[model_idx]
                metric = model_obj[0]
                model = model_obj[1]
                model_save_path = self.get_save_path(metric=metric, path_element="model")
                try:
                    model.save(model_save_path)
                except Exception as ex:
                    print("WARN: Could NOT auto save module no." + str(model_idx) + " at " + model_save_path + " due to the following exception: " + str(ex))

                if self.EXIT_ALL_THREADS_FLAG:
                    create_model_thread_lock.release()
                    return
            create_model_thread_lock.release()

            create_anomaly_likelihood_calc_thread_lock.acquire()
            for metric, anomaly_likelihood_calculator in self.anomaly_likelihood_detectors.iteritems():
                anomaly_likelihood_calculators_path = self.get_save_path(metric=metric,
                                                                         path_element="anomaly_likelihood_calculator")

                try:
                    if not os.path.exists(anomaly_likelihood_calculators_path):
                        os.makedirs(anomaly_likelihood_calculators_path)
                    with open(os.path.join(anomaly_likelihood_calculators_path, self.anomaly_likelihood_calculator_filename)
                            , "w") as anomaly_likelihood_calc_file:
                        anomaly_likelihood_calculator.writeToFile(anomaly_likelihood_calc_file)
                except OSError as ex:
                    print("WARN: Could NOT auto save anomaly likelihood calc for metric " + str(metric) + " at " + anomaly_likelihood_calculators_path + " due to the following exception: " + str(ex))

                if self.EXIT_ALL_THREADS_FLAG:
                    create_anomaly_likelihood_calc_thread_lock.release()
                    return
            create_anomaly_likelihood_calc_thread_lock.release()

            for i in range(0, interval):
                if self.EXIT_ALL_THREADS_FLAG:
                    return

                time.sleep(1)

    def create_model(self, modelParams):
        """
        Given a model params dictionary, create a CLA Model. Automatically enables
        inference for kw_energy_consumption.
        :param modelParams: Model params dict
        :return: OPF Model object
        """
        if modelParams:
            model = ModelFactory.create(modelParams)
            model.enableInference({"predictedField": "value"})
            return model
        return None

    def get_model_params_from_metric_name(self, metric_family, useYaml=False):
        """
        Given a gym name, assumes a matching model params python module exists within
        the model_params directory and attempts to import it.
        :param metric_family: Gym name, used to guess the model params module name.
        :return: OPF Model params dictionary
        """

        if useYaml:
            yaml_filename = self.models_params_base_path + "/%s.yaml" % (
                metric_family.replace(" ", "_").replace("-", "_").replace(".", "/")
            )
            if DEBUG:
                print("Importing model params from %s" % yaml_filename)

            with open(yaml_filename, "r") as yaml_file:
                model_params = yaml.safe_load(yaml_file)
            return model_params
        else:
            importName = "data_model_params.%s" % (
                metric_family.replace(" ", "_").replace("-", "_")
            )
            if DEBUG:
                print("Importing model params from %s" % importName)
            try:
                importedModelParams = importlib.import_module(importName).MODEL_PARAMS
            except ImportError:
                #Using default model params
                importName = "data_model_params.default"
                importedModelParams = importlib.import_module(importName).MODEL_PARAMS
                if DEBUG:
                    print("No model params exist for '%s'. Using default module params."
                          % metric_family)

            return importedModelParams

    def anomaly_detector(self, metric):
        """
        The main method of the service - whenever a metric is received, it is parsed by the parse_metric_message method
        and then sent to this method for feeding the data to the correct model and detect anomalies
        :param metric:
        :return:
        """

        try:
            model = None
            anomalyLikelihoodCalc = None
            if DEBUG:
                print("DEBUG: Received the metric " + str(base64.b64encode(metric)) + "\n" if DEBUG else "", end="")

            if len(self.models) < self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.max-allowed-models"]:
                models_number_below_configured_limit = True
            else:
                models_number_below_configured_limit = False
                if (time.time() - self._last_logged_message_about_too_many_models) > self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.minimum_seconds_between_model_over_quota_log_messages"]:
                    print("WARN: Currently the number of models/anomaly_likelihood_calculators loaded is exceeds the configured quota. CANNOT CREATE NEW MODELS.")
                    self._last_logged_message_about_too_many_models = time.time()

            if not metric["metric_name"] in self.models:
                create_model_thread_lock.acquire()
                if not metric["metric_name"] in self.models and os.path.isdir(self.get_save_path(metric["metric_name"])):
                    if models_number_below_configured_limit:
                        try:
                            self.models[metric["metric_name"]] = ModelFactory.loadFromCheckpoint(self.get_save_path(metric["metric_name"]))
                            print("LOADED MODEL FOR " + metric["metric_name"] + " FROM DISK")
                        except Exception as ex:
                            self.models[metric["metric_name"]] = self.create_model(self.get_model_params_from_metric_name(metric["metric_family"]))
                            print("WARN: Failed to create a model from disk (" + str(ex) + ")")

                if not metric["metric_name"] in self.models and not os.path.isdir(self.get_save_path(metric["metric_name"])):
                    if models_number_below_configured_limit:
                        self.models[metric["metric_name"]] = self.create_model(self.get_model_params_from_metric_name(metric["metric_family"]))
                        if DEBUG:
                            print("Model for " + metric["metric_name"] + " created from params")
                create_model_thread_lock.release()
    
            if metric["metric_name"] in self.models:
                model = self.models[metric["metric_name"]]
                if DEBUG:
                    print("Model for " + metric["metric_name"] + " loaded from cache")
    
            create_anomaly_likelihood_calc_thread_lock.acquire()
            if not metric["metric_name"] in self.anomaly_likelihood_detectors:
                anomaly_likelihood_calculators_path = self.get_save_path(metric["metric_name"], path_element="anomaly_likelihood_calculator")
    
                if os.path.isfile(os.path.join(anomaly_likelihood_calculators_path, self.anomaly_likelihood_calculator_filename)):
                    if models_number_below_configured_limit:
                        try:
                            if models_number_below_configured_limit:
                                self.anomaly_likelihood_detectors[metric["metric_name"]] = self.create_anomaly_likelihood_calc_from_disk(metric)
                                print("LOADED ANOMALY_LIKELIHOOD_CALC FROM FILE")
                        except Exception as ex:
                            if models_number_below_configured_limit:
                                self.anomaly_likelihood_detectors[metric["metric_name"]] = AnomalyLikelihood()
                                print("WARN: Failed to create an anomaly likelihood calc from disk (" + str(ex) + ")")
                else:
                    if models_number_below_configured_limit:
                        self.anomaly_likelihood_detectors[metric["metric_name"]] = AnomalyLikelihood()

            if metric["metric_name"] in self.anomaly_likelihood_detectors:
                anomalyLikelihoodCalc = self.anomaly_likelihood_detectors[metric["metric_name"]]
            create_anomaly_likelihood_calc_thread_lock.release()
    
            if model:
                result = model.run({
                    "timestamp": datetime.fromtimestamp(metric["metric_timestamp"]),
                    "value": metric["metric_value"]
                })
    
                anomalyScore = result.inferences["anomalyScore"]
                if "multiStepBestPredictions" in result.inferences:
                    prediction = result.inferences["multiStepBestPredictions"][1]
                else:
                    prediction = None
    
                anomalyLikelihood = anomalyLikelihoodCalc.anomalyProbability(
                    value=metric["metric_value"],
                    anomalyScore=anomalyScore,
                    timestamp=datetime.fromtimestamp(metric["metric_timestamp"])
                )
    
                anomaly_reported = False
                anomaly_direction = 0
                if anomalyLikelihood > 0.9 and anomalyScore > 0.9:
                    if prediction > metric["metric_value"]:
                        anomaly_direction = 1
                    else:
                        anomaly_direction = -1
    
                try:
                    self.statsd_client.gauge(self.metrics_prefix + ".anomaly_score." + metric["metric_name"], anomalyScore)
                    self.statsd_client.gauge(self.metrics_prefix + ".anomaly_likelihood." + metric["metric_name"], anomalyLikelihood)
                    self.statsd_client.gauge(self.metrics_prefix + ".anomaly_direction." + metric["metric_name"], anomaly_direction)
                except:
                    print(
                            "WARNING: Failed to report anomaly to statsd. ("
                            "Timestamp: " + str(datetime.fromtimestamp(metric["metric_timestamp"])) + ", " +
                            "Metric: " + str(metric["metric_name"]) + ", " +
                            "Value: " + str(metric["metric_value"]) + ", " +
                            "Anomaly score: " + str(anomalyScore) + ", " +
                            "Prediction: " + str(prediction) + ", " +
                            "AnomalyLikelihood: " + str(anomalyLikelihood) + ", " +
                            "AnomalyReported: " + str(anomaly_reported) + ")"
                    )
                    pass
    
                if anomalyLikelihood > 0.9 and anomalyScore > 0.9:
                    self.report_anomaly(metric=metric, anomaly_info={
                        "htm_anomaly_score": anomalyScore,
                        "htm_anomaly_likelihood": anomalyLikelihood,
                        "anomaly_score": anomalyLikelihood * anomaly_direction * 100,
                        "timestamp: ": datetime.fromtimestamp(metric["metric_timestamp"]),
                        "metric: ": metric["metric_name"],
                        "value: ": metric["metric_value"]
                    })
                    anomaly_reported = True
    
                    print(
                            "Timestamp: " + str(datetime.fromtimestamp(metric["metric_timestamp"])) + ", " +
                            "Metric: " + str(metric["metric_name"]) + ", " +
                            "Value: " + str(metric["metric_value"]) + ", " +
                            "Anomaly score: " + str(anomalyScore) + ", " +
                            "Prediction: " + str(prediction) + ", " +
                            "AnomalyLikelihood: " + str(anomalyLikelihood) + ", " +
                            "AnomalyReported: " + str(anomaly_reported)
                    )
            else:
                if models_number_below_configured_limit:
                    print("ERROR: Could not load/create a model for " + str(metric))
    
            if self.EXIT_ALL_THREADS_FLAG:
                return
            
            self._metrics_successfully_processed.value += 1
            
        except Exception as ex:
            print("WARN: Failed to analyze the metric(b64) " + str(base64.b64encode(metric)) + " due to the following exception: " + str(ex))

    def create_anomaly_likelihood_calc_from_disk(self, metric):
        anomaly_likelihood_calculators_path = self.get_save_path(metric["metric_name"], path_element="anomaly_likelihood_calculator")
        with open(
                os.path.join(anomaly_likelihood_calculators_path, self.anomaly_likelihood_calculator_filename),
                "rb") as anomaly_likelihood_calc_file:
            return AnomalyLikelihood.readFromFile(anomaly_likelihood_calc_file)

    def parse_metric_message(self, metric_raw_info):
        try:
            self._metrics_received.value += 1
            metric_family_hierarchy = ""
            metric_family = ""
    
            if len(metric_raw_info.split(" ")) == 3:
                metric_name = metric_raw_info.split(" ")[0]
                metric_family_raw = metric_name.split(".")
                metric_item = ""
                if len(metric_family_raw) > 1:
                    metric_family_hierarchy = metric_family_raw[:(len(metric_family_raw) - 1)]
                    metric_family = ".".join(metric_family_hierarchy)
                    metric_item = metric_family_raw[(len(metric_family_raw) - 1)]
                else:
                    print("WARN: Failed to parse metric info(b64) (failed to parse metric family. Less than one dot in the family name): " + str(base64.b64encode(metric_raw_info)))
                try:
                    metric_value = float(metric_raw_info.split(" ")[1])
                except:
                    print("WARN: Failed to parse metric info(b64) (failed to convert metric value to float): " + str(base64.b64encode(metric_raw_info)))
                    return
                try:
                    metric_timestamp = int(metric_raw_info.split(" ")[2])
                except:
                    print("WARN: Failed to parse metric info(b64) (failed to convert timestamp value to int): " + str(base64.b64encode(metric_raw_info)))
                    return
    
            else:
                print("WARN: Failed to parse metric info(b64) (raw message contains more or less than two spaces): " + str(base64.b64encode(metric_raw_info)))
                return
    
            self.anomaly_detector({"metric_family_hierarchy" : metric_family_hierarchy, "metric_family": metric_family, "metric_item": metric_item, "metric_name": metric_name, "metric_value": metric_value, "metric_timestamp": metric_timestamp})
            
        except Exception as ex:
            print("WARN: The following unexpected exception has been thrown while parsing the metric message: " + str(ex))

    def run(self, ip='', port=3000, ping_listening_host="127.0.0.1", ping_listening_port=3001, connections_backlog=10, proto="UDP", save_interval=5, models_save_base_path=None, models_params_base_path=None, anomaly_likelihood_detectors_save_base_path=None, alerter_url=None):
        self._ping_listening_host = ping_listening_host
        self._ping_listening_port = ping_listening_port
        self._analyzer_thread = threading.Thread(target=self.run_analyzer, args=[
            save_interval,
            models_save_base_path,
            models_params_base_path,
            anomaly_likelihood_detectors_save_base_path,
            alerter_url
        ])
        print("INFO: Launching the analyzer thread (save_interval=" + str(save_interval) + ";models_save_base_path=" + str(models_save_base_path) + ";models_params_base_path=" + str(models_params_base_path)+ ";anomaly_likelihood_detectors_save_base_path=" + str(anomaly_likelihood_detectors_save_base_path) + ";alerter_url=" + str(alerter_url) + ")")
        self._analyzer_thread.start()
        self._app.run(host=self._ping_listening_host, port=self._ping_listening_port)

    def run_analyzer(self, save_interval=5,
            models_save_base_path=None, models_params_base_path=None,
            anomaly_likelihood_detectors_save_base_path=None, alerter_url=None):

        self.alerter_url = alerter_url
        self._metrics_kafka_topic = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.metrics_topic_name"]

        kafka_consumer = None
        while kafka_consumer is None:
            try:
                kafka_consumer = kafka.KafkaConsumer(self._metrics_kafka_topic,
                                                     bootstrap_servers=self._kafka_server,
                                                     client_id=self._kafka_client_id)
                print("INFO: Loaded a Kafka consumer successfully. (self._metrics_kafka_topic=" + str(self._metrics_kafka_topic) + ";bootstrap_servers=" + str(self._kafka_server) + ";client_id=" + str(self._kafka_client_id) + ")")
            except:
                print(
                    "DEBUG: Waiting on a dedicated thread for the Kafka server to be available... Going to sleep for 10 seconds")
                time.sleep(10)

        # launch the auto-save thread
        if not models_params_base_path:
            self.models_params_base_path = os.path.join(dirname(__file__), "data_model_params")
        else:
            self.models_params_base_path = models_params_base_path

        if not models_save_base_path:
            self.models_save_base_path = os.path.join(dirname(__file__), "data_models")
        else:
            self.models_save_base_path = models_save_base_path

        if not anomaly_likelihood_detectors_save_base_path:
            self.anomaly_likelihood_detectors_save_base_path = os.path.join(dirname(__file__), "anomaly_likelihood_calculators")
        else:
            self.anomaly_likelihood_detectors_save_base_path = anomaly_likelihood_detectors_save_base_path

        autosave_thread = threading.Thread(target=self.auto_save_models, args=[save_interval])
        print("INFO: Launching auto-save thread (save_interval=" + str(save_interval) + ")")
        autosave_thread.start()

        for metric in kafka_consumer:
            self._raw_metrics_downloaded_from_kafka.value += 1

            # If this is an anomaly metric created by this service then there's no need to process it again...
            metric_name = metric.value
            if metric.value is None or metric.value.strip() == "" or len(metric.value.split(" ")) != 3:
                print("DEBUG: Received the following malformed metric. Ignoring: " + str(metric_name) + "\n" if DEBUG else "", end="")
                continue

            metric_name = metric.value.split(" ")[0]
            if re.match(self._allowed_to_work_on_metrics_pattern, str(metric_name)):
                print("DEBUG: Handling the following metric " + str(metric_name) + " since it matches the regex " + self._allowed_to_work_on_metrics_pattern.pattern + "." + "\n" if DEBUG else "", end="")
                self.parse_metric_message(metric_raw_info=metric.value)
            else:
                print("DEBUG: Ignoring the following metric " + str(metric_name) + " since it DOES NOT match the regex " + self._allowed_to_work_on_metrics_pattern.pattern + "." + "\n" if DEBUG else "", end="")



ip = ''
port = 3000
proto = "TCP"
connections_backlog = 10
save_interval=60
models_save_base_path=None
models_params_base_path=None
anomaly_likelihood_detectors_save_base_path=None

script_path = os.path.dirname(os.path.realpath(__file__))
config_file_default_path = "/etc/smart-onion/"
settings_file_name = "metrics_analyzer_settings.json"
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

config_copy = None
alerter_url = None
ping_listening_host = None
ping_listening_port = None
while alerter_url is None:
    try:
        # Contact configurator to fetch all of our config and configure listen-ip and port
        configurator_base_url = str(configurator_proto).strip() + "://" + str(configurator_host).strip() + ":" + str(configurator_port).strip() + "/smart-onion/configurator/"
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.architecture.internal_services.backend.*"
        configurator_response = urllib.urlopen(configurator_final_url).read().decode('utf-8')
        config_copy = json.loads(configurator_response)
        ip = config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.listening-host"]
        port = int(config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.listening-port"])
        proto = config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.protocol"]
        connections_backlog = int(config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.connection-backlog"])
        save_interval = int(config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.save_interval"])
        ping_listening_host = config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.ping-listening-host"]
        ping_listening_port = config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.ping-listening-port"]
        alerter_url = config_copy["smart-onion.config.architecture.internal_services.backend.alerter.protocol"] + "://" + config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-host"] + ":" + str(config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-port"]) + "/smart-onion/alerter/report_alert"
    except Exception as ex:
        print("WARN: Waiting (indefinetly in 10 sec intervals) for the Configurator service to become available (waiting for alerter service info)... (Exception: " + str(ex) + ")")
        time.sleep(10)


utils = Utils()

try:
    ip = utils.extract_args("listen-ip")
except:
    pass
try:
    port = int(utils.extract_args("listen-port"))
except:
    pass
try:
    proto = utils.extract_args("listen-proto").upper()
except:
    pass
try:
    connections_backlog = int(utils.extract_args("conn-backlog"))
except:
    pass
try:
    if int(utils.extract_args("save-interval")) >= 1:
        save_interval = int(utils.extract_args("save-interval"))
except:
    pass
try:
    models_save_base_path = utils.extract_args("models-save-base-path")
except:
    pass
try:
    models_params_base_path = utils.extract_args("models-params-base-path")
except:
    pass
try:
    anomaly_likelihood_detectors_save_base_path = utils.extract_args("anomaly_likelihood_detectors_save_base_path")
except:
    pass

sys.argv = [sys.argv[0]]
MetricsRealtimeAnalyzer(config_copy=config_copy).run(
    ip=ip,
    port=port,
    ping_listening_host=ping_listening_host,
    ping_listening_port=ping_listening_port,
    proto=proto,
    connections_backlog=connections_backlog,
    save_interval=save_interval,
    models_save_base_path=models_save_base_path,
    models_params_base_path=models_params_base_path,
    anomaly_likelihood_detectors_save_base_path=anomaly_likelihood_detectors_save_base_path,
    alerter_url=alerter_url
)
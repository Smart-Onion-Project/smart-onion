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
import socket
import threading
import importlib
from threading import Lock
from os.path import dirname
from datetime import datetime
from nupic.frameworks.opf.model_factory import ModelFactory
from nupic.algorithms.anomaly_likelihood import AnomalyLikelihood


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

    DEBUG = False
    EXIT_ALL_THREADS_FLAG = False
    models = {}
    anomaly_likelihood_detectors = {}
    anomaly_likelihood_detectors_save_base_path = None
    models_save_base_path = None
    models_params_base_path = None
    create_model_thread_lock = Lock()
    create_anomaly_likelihood_calc_thread_lock = Lock()
    anomaly_likelihood_calculator_filename = "anomaly_likelihood_calculator"

    def __init__(self):
        pass

    def report_anomaly(self, metric, anomaly_info):
        pass

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

            for metric, model in self.models.iteritems():
                model_save_path = self.get_save_path(metric=metric, path_element="model")
                model.save(model_save_path)
                if self.EXIT_ALL_THREADS_FLAG:
                    return

            for metric, anomaly_likelihood_calculator in self.anomaly_likelihood_detectors.iteritems():
                anomaly_likelihood_calculators_path = self.get_save_path(metric=metric,
                                                                         path_element="anomaly_likelihood_calculator")

                if not os.path.exists(anomaly_likelihood_calculators_path):
                    os.makedirs(anomaly_likelihood_calculators_path)
                with open(os.path.join(anomaly_likelihood_calculators_path, self.anomaly_likelihood_calculator_filename)
                        , "w") as anomaly_likelihood_calc_file:
                    anomaly_likelihood_calculator.writeToFile(anomaly_likelihood_calc_file)
                if self.EXIT_ALL_THREADS_FLAG:
                    return

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
            if self.DEBUG:
                print "Importing model params from %s" % yaml_filename

            with open(yaml_filename, "r") as yaml_file:
                model_params = yaml.safe_load(yaml_file)
            return model_params
        else:
            importName = "data_model_params.%s" % (
                metric_family.replace(" ", "_").replace("-", "_")
            )
            if self.DEBUG:
                print "Importing model params from %s" % importName
            try:
                importedModelParams = importlib.import_module(importName).MODEL_PARAMS
            except ImportError:
                #Using default model params
                importName = "data_model_params.default"
                importedModelParams = importlib.import_module(importName).MODEL_PARAMS
                if self.DEBUG:
                    print("No model params exist for '%s'. Using default module params."
                          % metric_family)

            return importedModelParams

    def anomaly_detector(self, metric, client_address):
        """
        The main method of the service - whenever a metric is received, it is parsed by the parse_metric_message method
        and then sent to this method for feeding the data to the correct model and detect anomalies
        :param metric:
        :param client_address:
        :return:
        """

        model = None
        anomalyLikelihoodCalc = None
        if self.DEBUG:
            print("Received the metric " + str(metric) + " from " + str(client_address))

        if not metric["metric_name"] in self.models:
            self.create_model_thread_lock.acquire()
            if not metric["metric_name"] in self.models and os.path.isdir(self.get_save_path(metric["metric_name"])):
                try:
                    self.models[metric["metric_name"]] = ModelFactory.loadFromCheckpoint(self.get_save_path(metric["metric_name"]))
                    print("LOADED MODEL FOR " + metric["metric_name"] + " FROM DISK")
                except Exception as ex:
                    self.models[metric["metric_name"]] = self.create_model(self.get_model_params_from_metric_name(metric["metric_family"]))
                    print("WRN: Failed to create a model from disk (" + str(ex) + ")")

            if not metric["metric_name"] in self.models and not os.path.isdir(self.get_save_path(metric["metric_name"])):
                self.models[metric["metric_name"]] = self.create_model(self.get_model_params_from_metric_name(metric["metric_family"]))
                if self.DEBUG:
                    print("Model for " + metric["metric_name"] + " created from params")
            self.create_model_thread_lock.release()

        if metric["metric_name"] in self.models:
            model = self.models[metric["metric_name"]]
            if self.DEBUG:
                print("Model for " + metric["metric_name"] + " loaded from cache")

        self.create_anomaly_likelihood_calc_thread_lock.acquire()
        if not metric["metric_name"] in self.anomaly_likelihood_detectors:
            anomaly_likelihood_calculators_path = self.get_save_path(metric["metric_name"], path_element="anomaly_likelihood_calculator")

            if os.path.isfile(os.path.join(anomaly_likelihood_calculators_path, self.anomaly_likelihood_calculator_filename)):
                try:
                    self.anomaly_likelihood_detectors[metric["metric_name"]] = self.createAnomalyLikelihoodCalcFromDisk(metric)
                    print("LOADED ANOMALY_LIKELIHOOD_CALC FROM FILE")
                except Exception as ex:
                    self.anomaly_likelihood_detectors[metric["metric_name"]] = AnomalyLikelihood()
                    print("WRN: Failed to create an anomaly likelihood calc from disk (" + str(ex) + ")")
            else:
                self.anomaly_likelihood_detectors[metric["metric_name"]] = AnomalyLikelihood()

        anomalyLikelihoodCalc = self.anomaly_likelihood_detectors[metric["metric_name"]]
        self.create_anomaly_likelihood_calc_thread_lock.release()

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

            anomalyReported = False
            if anomalyLikelihood > 0.9 and anomalyScore > 0.9:
                if prediction > metric["metric_value"]:
                    anomaly_direction = 1
                else:
                    anomaly_direction = -1
                self.report_anomaly(metric=metric, anomaly_info={"anomaly_score": anomalyScore, "anomaly_likelihood": anomalyLikelihood, "anomaly_direction": anomaly_direction})
                anomalyReported = True

            print(
                    "Timestamp: " + str(datetime.fromtimestamp(metric["metric_timestamp"])) + ", " +
                    "Metric: " + str(metric["metric_name"]) + ", " +
                    "Value: " + str(metric["metric_value"]) + ", " +
                    "Anomaly score: " + str(anomalyScore) + ", " +
                    "Prediction: " + str(prediction) + ", " +
                    "AnomalyLikelihood: " + str(anomalyLikelihood) + ", " +
                    "AnomalyReported: " + str(anomalyReported)
            )
        else:
            print("ERROR: Could not load a model for " + str(metric))

        if self.EXIT_ALL_THREADS_FLAG:
            return

    def createAnomalyLikelihoodCalcFromDisk(self, metric):
        anomaly_likelihood_calculators_path = self.get_save_path(metric["metric_name"], path_element="anomaly_likelihood_calculator")
        with open(
                os.path.join(anomaly_likelihood_calculators_path, self.anomaly_likelihood_calculator_filename),
                "rb") as anomaly_likelihood_calc_file:
            return AnomalyLikelihood.readFromFile(anomaly_likelihood_calc_file)

    def parse_metric_message(self, metric_raw_info, client_address):
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
                if self.DEBUG:
                    print("Failed to parse metric info from client (failed to parse metric family. Less than one dot in the family name) " + str(client_address) + ": " + str(metric_raw_info))
            try:
                metric_value = float(metric_raw_info.split(" ")[1])
            except:
                if self.DEBUG:
                    print("Failed to parse metric info from client (failed to convert metric value to float) " + str(client_address) + ": " + str(metric_raw_info))
                return
            try:
                metric_timestamp = int(metric_raw_info.split(" ")[2])
            except:
                if self.DEBUG:
                    print("Failed to parse metric info from client (failed to convert timestamp value to int) " + str(client_address) + ": " + str(metric_raw_info))
                return

        else:
            if self.DEBUG:
                print("Failed to parse metric info from client (raw message contains more or less than two spaces) " + str(client_address) + ": " + str(metric_raw_info))
            return

        self.anomaly_detector({"metric_family_hierarchy" : metric_family_hierarchy, "metric_family": metric_family, "metric_item": metric_item, "metric_name": metric_name, "metric_value": metric_value, "metric_timestamp": metric_timestamp}, client_address)

    def tcp_client_handler(self, client_socket, client_address):
        received_msg = client_socket.recv(1024)
        client_socket.close()

        for metric_line in received_msg.split('\n'):
            if len(metric_line.strip()) > 0:
                #If the message is not an empty line send it to the parser. If it is an empty line just ignore it.
                self.parse_metric_message(metric_line, client_address)

    def run(self, ip='', port=3000, connections_backlog=10, proto="UDP", save_interval=5, models_save_base_path=None, models_params_base_path=None, anomaly_likelihood_detectors_save_base_path=None):
        # create an INET, STREAMing socket
        if proto == "UDP":
            serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        elif proto == "TCP":
            serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            raise Exception("The protocol specified is not recognized. Use either TCP or UDP (upper-case only)")

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
        autosave_thread.start()

        # bind the socket to a public host, and a well-known port
        serversocket.bind((ip, port))
        print("Listening on " + str(ip) + ":" + str(port) + "/" + str(proto) + " (if the IP is empty that means all IPs) with connection backlog set to " + str(connections_backlog) + " and " + str(save_interval) + "s auto-save interval")

        clientsocket = None
        try:
            if proto == "TCP":
                # become a server socket
                serversocket.listen(connections_backlog)
                serversocket.settimeout(1.0)

                while True:
                    try:
                        # accept connections from outside
                        (clientsocket, address) = serversocket.accept()
                        # now do something with the clientsocket
                        # in this case, we'll pretend this is a threaded server
                        ct = threading.Thread(target=self.tcp_client_handler, args=[clientsocket, address])
                        ct.start()
                    except socket.timeout:
                        pass
            else:
                while True:
                    metric_line, client_address = serversocket.recvfrom(1024)
                    ct = threading.Thread(target=self.parse_metric_message, args=[metric_line, client_address])
                    ct.start()
        except KeyboardInterrupt:
            self.EXIT_ALL_THREADS_FLAG = True
            if clientsocket:
                clientsocket.close()

ip = ''
port = 3000
proto = "TCP"
connections_backlog = 10
save_interval=60
models_save_base_path=None
models_params_base_path=None
anomaly_likelihood_detectors_save_base_path=None
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

MetricsRealtimeAnalyzer().run(
    ip=ip,
    port=port,
    proto=proto,
    connections_backlog=connections_backlog,
    save_interval=save_interval,
    models_save_base_path=models_save_base_path,
    models_params_base_path=models_params_base_path,
    anomaly_likelihood_detectors_save_base_path=anomaly_likelihood_detectors_save_base_path
)
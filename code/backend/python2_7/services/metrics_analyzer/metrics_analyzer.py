import sys
import socket
import threading
import importlib
from datetime import datetime
from nupic.frameworks.opf.model_factory import ModelFactory


class Utils:

    def __init__(self):
        pass

    def extract_args(self, arg_prefix, error_if_not_found=True):
        for arg in sys.argv:
            if arg.startswith(arg_prefix + "="):
                return arg.split("=")[1]
        if error_if_not_found:
            raise Exception("Argument not found.")
        else:
            return None

class MetricsRealtimeAnalyzer:

    models = {}

    def __init__(self):
        pass

    def createModel(self, modelParams):
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

    def getModelParamsFromMetricName(self, metric_family):
        """
        Given a gym name, assumes a matching model params python module exists within
        the model_params directory and attempts to import it.
        :param metric_family: Gym name, used to guess the model params module name.
        :return: OPF Model params dictionary
        """
        importName = "data_models.%s" % (
            metric_family.replace(" ", "_").replace("-", "_")
        )
        print "Importing model params from %s" % importName
        try:
            importedModelParams = importlib.import_module(importName).MODEL_PARAMS
        except ImportError:
            #Using default module params
            importName = "data_models.default"
            importedModelParams = importlib.import_module(importName).MODEL_PARAMS
            print("No model params exist for '%s'. Using default module params."
                  % metric_family)

        return importedModelParams

    def anomaly_detector(self, metric, client_address):
        print("Received the metric " + str(metric) + " from " + str(client_address))

        if metric["metric_family"] in self.models:
            model = self.models[metric["metric_family"]]
        else:
            model = self.createModel(self.getModelParamsFromMetricName(metric["metric_family"]))
            self.models[metric["metric_family"]] = model

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

            if prediction:
                print("Anomaly score: " + str(anomalyScore) + ", Prediction: " + str(prediction))
            else:
                print("Anomaly score: " + str(anomalyScore) + ", Prediction: None")

    def parse_metric_message(self, metric_raw_info, client_address):
        metric_family_hierarchy = ""
        metric_family = ""
        metric_name = ""
        metric_value = 0.0
        metric_timestamp = 0

        if len(metric_raw_info.split(" ")) == 3:
            metric_name = metric_raw_info.split(" ")[0]
            metric_family_raw = metric_name.split(".")
            if len(metric_family_raw) > 1:
                metric_family_hierarchy = metric_family_raw[:(len(metric_family_raw) - 1)]
                metric_family = ".".join(metric_family_hierarchy)
            else:
                print("Failed to parse metric info from client (failed to parse metric family. Less than one dot in the family name) " + str(client_address) + ": " + str(metric_raw_info))
            try:
                metric_value = float(metric_raw_info.split(" ")[1])
            except:
                print("Failed to parse metric info from client (failed to convert metric value to float) " + str(client_address) + ": " + str(metric_raw_info))
                return
            try:
                metric_timestamp = int(metric_raw_info.split(" ")[2])
            except:
                print("Failed to parse metric info from client (failed to convert timestamp value to int) " + str(client_address) + ": " + str(metric_raw_info))
                return

        else:
            print("Failed to parse metric info from client (raw message contains more or less than two spaces) " + str(client_address) + ": " + str(metric_raw_info))
            return

        self.anomaly_detector({"metric_family_hierarchy":metric_family_hierarchy, "metric_family": metric_family, "metric_name": metric_name, "metric_value": metric_value, "metric_timestamp": metric_timestamp}, client_address)

    def tcp_client_handler(self, client_socket, client_address):
        received_msg = client_socket.recv(1024)
        client_socket.close()

        for metric_line in received_msg.split('\n'):
            if len(metric_line.strip()) > 0:
                #If the message is not an empty line send it to the parser. If it is an empty line just ignore it.
                self.parse_metric_message(metric_line, client_address)

    def run(self, ip='', port=3000, connections_backlog=10, proto="UDP"):
        # create an INET, STREAMing socket
        if proto == "UDP":
            serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        elif proto == "TCP":
            serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            raise Exception("The protocol specified is not recognized. Use either TCP or UDP (upper-case only)")
        # bind the socket to a public host, and a well-known port
        serversocket.bind((ip, port))

        if proto == "TCP":
            # become a server socket
            serversocket.listen(connections_backlog)

            while True:
                # accept connections from outside
                (clientsocket, address) = serversocket.accept()
                # now do something with the clientsocket
                # in this case, we'll pretend this is a threaded server
                ct = threading.Thread(target=self.tcp_client_handler, args=[clientsocket, address])
                ct.run()
        else:
            while True:
                metric_line, client_address = serversocket.recvfrom(1024)
                ct = threading.Thread(target=self.parse_metric_message, args=[metric_line, client_address])
                ct.run()

ip = ''
port = 3000
proto = "UDP"
connections_backlog = 10
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

MetricsRealtimeAnalyzer().run(ip=ip, port=port, proto=proto, connections_backlog=connections_backlog)

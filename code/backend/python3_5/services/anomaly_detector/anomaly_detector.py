#!/usr/bin/python3.5
##########################################################################
# Anomaly Detector                                                       #
# ----------------                                                       #
#                                                                        #
# This service is part of the Smart-Onion package. This micro-service is #
# responsible for detecting anomalies in metrics stored in whisper-files #
# based on statistical and heuristic approach.                           #
#                                                                        #
#                                                                        #
#                                                                        #
#                                                                        #
##########################################################################

import whisper
import time
import sys
import os
import re
import glob
import json
import threading
import statsd
import hashlib
from bottle import Bottle, request
from urllib import request as urllib_req


DEBUG = False
REFERENCE_PAST_SAMPLES = '7,14,21'
METRIC_PATH = '/var/lib/graphite/whisper/'
# METRIC_PATH = '/home/yuval/MYTREE_ext_disk/Programming Projects/Python Projects/smart-onion-resources/whisper-files/whisper/'
TIMESPAN_IN_SEC = 86400
PERCENT_MODE = True
METRICS_CACHE = {}
DISCOVERY_CURRENTLY_RUNNING = {}
metrics_prefix = "smart-onion.anomaly_score.anomaly_detector"


class NormalizedDataSet:
    data = list()
    trimmedFromStart = -1
    trimmedToEnd = -1

    def __init__(self, rawData):
        self.data = list(rawData)

    def max_of(self, num):
        if (len(self.data) > 0 and len(self.data) >= num):
            max = self.data[0]
            for i in range(num - 1, -1, -1):
                if self.data[i] > max:
                    max = self.data[i]
            return max
        else:
            return None

    def min_of(self, num):
        if (len(self.data) > 0 and len(self.data) >= num):
            min = self.data[0]
            for i in range(num - 1, -1, -1):
                if self.data[i] < min:
                    min = self.data[i]
            return min
        else:
            return None

    def average_of(self, num):
        if (len(self.data) > 0 and len(self.data) >= num):
            sum = 0
            for i in range(num - 1, -1, -1):
                sum = sum + self.data[i]
            return sum / float(num)
        else:
            return None

    def max(self):
        return self.max_of(len(self.data))

    def min(self):
        return self.min_of(len(self.data))

    def average(self):
        return self.average_of(len(self.data))

    def count(self):
        return len(self.data)

    def last(self):
        return self.data[len(self.data) - 1]


class AnomalyDetector:
    statsd_client = statsd.StatsClient(prefix=metrics_prefix)

    def __init__(self):
        self._time_loaded = time.time()
        self._app = Bottle()
        self._route()

    def _route(self):
        self._app.route('/smart-onion/discover-metrics/<metric_pattern>', method="GET", callback=self.discover_metrics)
        self._app.route('/smart-onion/get-anomaly-score/<metric_name>', method="GET", callback=self.get_anomaly_score)
        self._app.route('/ping', method="GET", callback=self._ping)

    def run(self, listen_ip, listen_port):
        if DEBUG:
            self._app.run(host=listen_ip, port=listen_port)
        else:
            self._app.run(host=listen_ip, port=listen_port, server="gunicorn", workers=32, timeout=120)

    def _file_as_bytes(self, filename):
        with open(filename, 'rb') as file:
            return file.read()

    def _ping(self):
        return {
            "response": "PONG",
            "file": __file__,
            "hash": hashlib.md5(self._file_as_bytes(__file__)).hexdigest(),
            "uptime": time.time() - self._time_loaded
        }

    def report_anomaly(self, metric, anomaly_info):
        pass

    def normalize_data(self, raw_data):
        last_data_at = -1
        data_points_to_fill = 0
        tmp_res = raw_data
        for i in range(0, len(tmp_res)):
            # print "+ BEFORE: [" + str(i) + "]" + str(tmpRes[i])
            if not (tmp_res[i] is None):
                if (last_data_at >= 0):
                    # print "+ " + str(dataPointsToFill) + " empty datapoints between idx " + str(lastDataAt) + " [" + str(tmpRes[lastDataAt]) + "] and " + str(i) + " [" + str(tmpRes[i]) + "]: Filling them up..."
                    for j in range(last_data_at + 1, i):
                        # print "+ Filling datapoint at idx " + str(j) + " with value of " + str(tmpRes[j - 1]) + " + ((" + str(tmpRes[i]) + " - " + str(tmpRes[lastDataAt]) + ") / " + str(dataPointsToFill) + " + 1))"
                        tmp_res[j] = tmp_res[j - 1] + ((tmp_res[i] - tmp_res[last_data_at]) / data_points_to_fill + 1)
                last_data_at = i

                data_points_to_fill = 0
            else:
                data_points_to_fill = data_points_to_fill + 1

        res = []
        trimmed_from_start = 0
        trimmed_to_end = 0
        data_started_at = -1
        for i in range(0, len(tmp_res)):
            if not (tmp_res[i] is None):
                res.append(tmp_res[i])
                if (data_started_at == -1):
                    data_started_at = i
            else:
                if (data_started_at == -1):
                    trimmed_from_start = trimmed_from_start + 1
                else:
                    trimmed_to_end = trimmed_to_end + 1

        # Returning the un-trimmed version of the array to allow the calling code to trim it as it sees fit
        res_obj = NormalizedDataSet(tmp_res)
        res_obj.trimmedFromStart = trimmed_from_start
        res_obj.trimmedToEnd = trimmed_to_end
        return res_obj

    def ArrContains(self, arr1, arr2):
        # arr1 - Array to be searched
        # arr2 - Array to be searched for
        matchingElements = 0
        for i in arr1:
            for j in arr2:
                if (i.lower() == j.lower()):
                    matchingElements = matchingElements + 1

        return matchingElements == len(arr2)

    def calculate_anomaly_score(self, nowData, referencePastData):
        R = 0.0
        R1 = 0
        R2 = 0
        R3 = 0
        R4 = 0
        R5 = 0
        R6 = 0
        rMAX = 0
        referencePastPeriods = REFERENCE_PAST_SAMPLES.split(",")
        for i in range(0, len(referencePastData)):
            # R1 - Current vs. reference period's max
            if (nowData.last() > referencePastData[i].max()):
                R = R + ((nowData.last() - referencePastData[i].max()) / float(referencePastPeriods[i]))
                R1 = R1 + ((nowData.last() - referencePastData[i].max()) / float(referencePastPeriods[i]))
            # R2 - Current vs. reference period's min
            if (nowData.last() < referencePastData[i].min()):
                R = R + ((nowData.last() - referencePastData[i].min()) / float(referencePastPeriods[i]))
                R2 = R2 + ((nowData.last() - referencePastData[i].min()) / float(referencePastPeriods[i]))
            # R3 - Current vs. reference period's average
            R = R + ((nowData.last() - referencePastData[i].average()) / float(referencePastPeriods[i]))
            R3 = R3 + ((nowData.last() - referencePastData[i].average()) / float(referencePastPeriods[i]))
            # R4 - Current vs. reference period's last
            R = R + ((nowData.last() - referencePastData[i].last()) / float(referencePastPeriods[i]))
            R4 = R4 + ((nowData.last() - referencePastData[i].last()) / float(referencePastPeriods[i]))
            # R5 - Compare the current data's range (i.e max - min) to the past data
            if (R < 0):
                R = R - ((abs(nowData.max_of(60) - nowData.min_of(60)) - abs(
                    referencePastData[i].max_of(60) - referencePastData[i].min_of(60))) / float(referencePastPeriods[i]))
                R5 = R5 - ((abs(nowData.max_of(60) - nowData.min_of(60)) - abs(
                    referencePastData[i].max_of(60) - referencePastData[i].min_of(60))) / float(referencePastPeriods[i]))
            # R6 - Compare the current data's range (i.e max - min) to the past data
            if (R > 0):
                R = R + ((abs(nowData.max_of(60) - nowData.min_of(60)) - abs(
                    referencePastData[i].max_of(60) - referencePastData[i].min_of(60))) / float(referencePastPeriods[i]))
                R6 = R6 + ((abs(nowData.max_of(60) - nowData.min_of(60)) - abs(
                    referencePastData[i].max_of(60) - referencePastData[i].min_of(60))) / float(referencePastPeriods[i]))

        for i in range(0, len(referencePastData)):
            rMAX = rMAX + ((referencePastData[i].max() / float(referencePastPeriods[i])))

        if (PERCENT_MODE):
            RES = R / rMAX * 100
            if RES > 300:
                RES = 300
            if RES < -300:
                RES = -300

            # "Normalize" the response to -100 - 100 range
            RES = RES / 3
            return RES
        else:
            return R

    def metrics_discoverer(self, metrics_pattern):
        if metrics_pattern in DISCOVERY_CURRENTLY_RUNNING and DISCOVERY_CURRENTLY_RUNNING[metrics_pattern] is True:
            return

        else:
            DISCOVERY_CURRENTLY_RUNNING[metrics_pattern] = True

        metrics_pattern_real_path = os.path.join(METRIC_PATH, metrics_pattern.replace(".", "/")) + ".wsp"

        raw_metrics_list = glob.glob(metrics_pattern_real_path, recursive=True)
        res = []
        for metric_file in raw_metrics_list:
            metric_base_path = METRIC_PATH
            if not metric_base_path.endswith("/"):
                metric_base_path = metric_base_path + "/"
            res.append(metric_file.replace(metric_base_path, "").replace("/", "."))

        METRICS_CACHE[metrics_pattern] = res
        DISCOVERY_CURRENTLY_RUNNING[metrics_pattern] = False

    def get_anomaly_score(self, metric_name, cur_time=None, ref_periods=None):
        res = 0

        if "cur_time" in request.query:
            try:
                cur_time = int(request.query["cur_time"])
            except:
                pass

        if "ref_periods" in request.query:
            ref_periods = request.query["ref_periods"]

        anomaly_detector = AnomalyDetector()

        metric_phys_path = os.path.join(METRIC_PATH, (metric_name.replace(".", "/")) + ".wsp")
        if cur_time is None or cur_time <= 0:
            cur_time_epoch = int(time.time())
        else:
            cur_time_epoch = cur_time

        if ref_periods is None or not re.match("^([0-9][0-9\,]+[0-9]|[0-9]+)$", ref_periods):
            reference_past_periods = ref_periods.split(",")
        else:
            reference_past_periods = REFERENCE_PAST_SAMPLES.split(",")

        now_end_epoch = cur_time_epoch
        now_start_epoch = now_end_epoch - TIMESPAN_IN_SEC

        # Verifying that the whisper file at the given location actually exists
        if not os.path.isfile(metric_phys_path):
            raise Exception("Could not find a metric file by the name specified.")

        now_raw_data = whisper.fetch(path=metric_phys_path, fromTime=now_start_epoch, untilTime=now_end_epoch)
        now_data = anomaly_detector.normalize_data(now_raw_data[1])

        # Getting reference past data and normalizing it (that is, filling the blanks with calculated values and marking the beginning and end of the data)
        reference_past_data = []
        for i in range(0, len(reference_past_periods)):
            cur_ref_point_ends = cur_time_epoch - (TIMESPAN_IN_SEC * int(reference_past_periods[i]))
            cur_ref_point_starts = cur_ref_point_ends - TIMESPAN_IN_SEC
            cur_ref_point_raw_data = whisper.fetch(path=metric_phys_path, fromTime=cur_ref_point_starts, untilTime=cur_ref_point_ends)
            cur_ref_point_data = anomaly_detector.normalize_data(cur_ref_point_raw_data)
            reference_past_data.append(cur_ref_point_data)

        # Calculating the amount of datapoints that should be discarded from the end and beginning of all datasets
        trim_all_starts_to = 0
        trim_all_ends_to = 0
        for i in range(0, len(reference_past_data)):
            if reference_past_data[i].trimmedFromStart > trim_all_starts_to:
                trim_all_starts_to = reference_past_data[i].trimmedFromStart
            if reference_past_data[i].trimmedToEnd > trim_all_ends_to:
                trim_all_ends_to = reference_past_data[i].trimmedToEnd
        if now_data.trimmedFromStart > trim_all_starts_to:
            trim_all_starts_to = now_data.trimmedFromStart
        if now_data.trimmedToEnd > trim_all_ends_to:
            trim_all_ends_to = now_data.trimmedToEnd

        if now_data.count() == 0:
            res = -1.111111

        if res == 0:
            # Trimming all the datasets to the same size according to what has been calculated earlier
            for i in range(0, len(reference_past_data)):
                del reference_past_data[i].data[(len(reference_past_data[i].data) - trim_all_ends_to):]
                del reference_past_data[i].data[:trim_all_starts_to]
            del now_data.data[(len(now_data.data) - trim_all_ends_to):]
            del now_data.data[:trim_all_starts_to]

            # Break here if there's no past data available
            reference_past_periods = REFERENCE_PAST_SAMPLES.split(",")
            for i in range(0, len(reference_past_data)):
                if reference_past_data[i].count() < now_data.count() or reference_past_data[i].count() == 0:
                    res = -1.222222
                    break

        if res == 0:
            res = anomaly_detector.calculate_anomaly_score(nowData=now_data, referencePastData=reference_past_data)
            if res > 90:
                anomaly_detector.report_anomaly(metric=metric_name, anomaly_info={"anomaly_score": res})

        try:
            AnomalyDetector().statsd_client.gauge(metrics_prefix + "." + metric_name, res)
        except:
            pass
        
        return "@@RES: " + str(res)

    def discover_metrics(self, metric_pattern):
        anomaly_detector = AnomalyDetector()
        metrics_discoverer_thread = threading.Thread(target=anomaly_detector.metrics_discoverer, args=[metric_pattern])
        metrics_discoverer_thread.start()

        if metric_pattern in METRICS_CACHE:
            res = {
                "data": METRICS_CACHE[metric_pattern]
            }
            return '@@RES: ' + json.dumps(res)
        else:
            return '@@RES: {"data":[]}'
        pass



script_path = os.path.dirname(os.path.realpath(__file__))
listen_ip = "127.0.0.1"
listen_port = 9090
config_file_default_path = "/etc/smart-onion/"
settings_file_name = "alerter_settings.json"
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
while config_copy is None:
    try:
        # Contact configurator to fetch all of our config and configure listen-ip and port
        configurator_base_url = str(configurator_proto).strip() + "://" + str(configurator_host).strip() + ":" + str(configurator_port).strip() + "/smart-onion/configurator/"
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.architecture.internal_services.backend.*"
        configurator_response = urllib_req.urlopen(configurator_final_url).read().decode('utf-8')
        config_copy = json.loads(configurator_response)
        listen_ip = config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.listening-host"]
        listen_port = config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.listening-port"]
    except:
        print("WARN: Waiting (indefinetly in 10 sec intervals) for the Configurator service to become available...")
        time.sleep(10)

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
        else:
            if arg == "--help" or arg == "-h" or arg == "/h" or arg == "/?":
                print("USAGE: " + os.path.basename(os.path.realpath(__file__)) + " [--listen-ip=127.0.0.1 --listen-port=8080]")
                print("")
                print("-h, --help, /h and /q will print this help screen.")
                print("")
                quit(1)

sys.argv = [sys.argv[0]]
AnomalyDetector().run(listen_ip=listen_ip, listen_port=listen_port)

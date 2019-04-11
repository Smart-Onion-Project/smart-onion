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
import kafka
import uuid
from multiprocessing import Value
import syslog
import datetime


DEBUG = False
SINGLE_THREADED = False
PERCENT_MODE = True
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
    def __init__(self, config_copy):
        self._statsd_client = statsd.StatsClient()
        self._time_loaded = time.time()
        self._app = Bottle()
        self._route()
        self._anomalies_reports_attempted = Value('i', 0)
        self._anomalies_reported = Value('i', 0)
        self._metrics_parsed = Value('i', 0)
        self._metrics_successfully_analyzed = Value('i', 0)
        self._analysis_cycles_so_far = Value('i', 0)
        self._raw_metrics_downloaded_from_kafka = Value('i', 0)
        self._config_copy = config_copy
        self._logging_format = self._config_copy["smart-onion.config.common.logging_format"]
        self._kafka_client_id = "SmartOnionAnomalyDetectorService_" + str(uuid.uuid4()) + "_" + str(int(time.time()))
        self._kafka_server = self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"]
        self._metrics_kafka_topic = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-analyzer.metrics_topic_name"]
        self._allowed_to_work_on_metrics_pattern = re.compile(self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.metrics_to_work_on_pattern"])
        self._metrics_base_path = self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.metrics_physical_path"]
        self._metrics_prefix = "smart-onion.anomaly_score.anomaly_detector."
        self._metrics_uniqe_list = {}
        self._metrics_uniqe_list_update_lock = threading.Lock()
        self._reference_past_sample_periods = self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.reference_past_sample_periods"]
        self._reference_timespan_in_secods = self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.reference_timespan_in_seconds"]
        self._anomalies_check_interval = self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.anomalies_check_interval"]
        self._reported_anomalies_kafka_topic = self._config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.reported_anomalies_topic"]
        self._alerter_url = config_copy["smart-onion.config.architecture.internal_services.backend.alerter.protocol"] + "://" + config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-host"] + ":" + str(config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-port"]) + "/smart-onion/alerter/report_alert"
        self._anomaly_detector_proto = config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.published-listening-protocol"]
        self._anomaly_detector_host = config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.published-listening-host"]
        self._anomaly_detector_port = config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.published-listening-port"]
        self._anomaly_detector_url_path = config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.base_urls.get-anomaly-score"]
        self._anomaly_score_threshold_for_reporting_to_alerter = config_copy["smart-onion.config.architecture.internal_services.backend.anomaly-detector.anomaly_score_threshold_for_reporting"]
        self._kafka_producer = None
        while self._kafka_producer is None:
            try:
                self._kafka_producer = kafka.producer.KafkaProducer(bootstrap_servers=self._kafka_server, client_id=self._kafka_client_id)
            except Exception as ex:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "__init__", "WARN", str(None), str(None), str(ex), type(ex).__name__, "Waiting (indefinetly in 10 sec intervals) for the Kafka service to become available..."))
                time.sleep(10)
        self._metrics_discoverer_thread = threading.Thread(target=self.metrics_collector)
        self._metrics_discoverer_thread.start()
        self._automated_anomaly_detection_thread = threading.Thread(target=self.auto_detect_metrics_anomalies_thread)
        self._automated_anomaly_detection_thread.start()

    def _route(self):
        # self._app.route('/smart-onion/get-anomaly-score/<metric_name>', method="GET", callback=self.get_anomaly_score)
        self._app.route('/ping', method="GET", callback=self._ping)

    def run(self, listen_ip, listen_port):
        if SINGLE_THREADED:
            self._app.run(host=listen_ip, port=listen_port)
        else:
            self._app.run(host=listen_ip, port=listen_port, server="gunicorn", workers=32, timeout=120)

    def _file_as_bytes(self, filename):
        with open(filename, 'rb') as file:
            return file.read()

    def _ping(self):
        metrics_in_list = -1
        try:
            with self._metrics_uniqe_list_update_lock:
                metrics_in_list = len(self._metrics_uniqe_list)
        except:
            pass

        return {
            "response": "PONG",
            "file": __file__,
            "hash": hashlib.md5(self._file_as_bytes(__file__)).hexdigest(),
            "uptime": time.time() - self._time_loaded,
            "service_specific_info": {
                "anomalies_reports_attempted": self._anomalies_reports_attempted.value,
                "anomalies_reported": self._anomalies_reported.value,
                "raw_metrics_downloaded_from_kafka": self._raw_metrics_downloaded_from_kafka.value,
                "metrics_parsed": self._metrics_parsed.value,
                "analysis_cycles_so_far": self._analysis_cycles_so_far.value,
                "metrics_in_list": metrics_in_list,
                "metrics_successfully_analyzed": self._metrics_successfully_analyzed.value
            }
        }

    def report_anomaly(self, metric, anomaly_info):
        self._anomalies_reports_attempted.value += 1
        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "report_anomaly", "INFO", str(None), str(metric), str(None), str(None), "The following anomaly reported regarding metric " + str(metric) + ": " + json.dumps(anomaly_info)))

        anomaly_report = {
            "report_id": str(uuid.uuid4()),
            "metric": metric,
            "reporter": "anomaly_detector",
            "meta_data": anomaly_info
        }
        # Disabled direct connection to the alerter service
        # try:
        #     res = urllib_req.urlopen(url=self._alerter_url, data=anomaly_report, context={'Content-Type': 'application/json'}).read().decode('utf-8')
        #     if res is None:
        #         res = "None"
        # except Exception as ex:
        #     syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "report_anomaly", "INFO", str(None), str(metric), str(ex), type(ex).__name__, "Failed to report the following anomaly to the alerter service directly due to the aforementioned exception. Will still try to report the following anomaly to Kafka: " + json.dumps(anomaly_report)))

        try:
            self._kafka_producer.send(topic=self._reported_anomalies_kafka_topic, value=json.dumps(anomaly_report).encode('utf-8'))
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "report_anomaly", "INFO", str(None), str(metric), str(None), str(None), "Reported the following anomaly to the alerter service and to kafka: " + json.dumps(anomaly_report)))
            self._anomalies_reported.value += 1
        except Exception as ex:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "report_anomaly", "WARN", str(None), str(metric), str(ex), type(ex).__name__, "Failed to report the following anomaly to Kafka due to the aforementioned exception. These are the anomaly details: " + json.dumps(anomaly_report)))

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
        referencePastPeriods = self._reference_past_sample_periods.split(",")
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

    def metrics_collector(self):
        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "metrics_collector", "INFO", str(None), str(None), str(None), str(None), "Kafka consumer thread loaded. This thread will subscribe to the " + str(self._metrics_kafka_topic) + " topic on Kafka and will assign the various sampling tasks to the various polling threads"))

        kafka_consumer = None
        while kafka_consumer is None:
            try:
                kafka_consumer = kafka.KafkaConsumer(self._metrics_kafka_topic,
                                                     bootstrap_servers=self._kafka_server,
                                                     client_id=self._kafka_client_id)
            except Exception as ex:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "metrics_collector", "INFO", str(None), str(None), str(ex), type(ex).__name__, "Waiting on a dedicated thread for the Kafka server to be available... Going to sleep for 10 seconds"))
                time.sleep(10)

        for metric_record in kafka_consumer:
            self._raw_metrics_downloaded_from_kafka.value += 1
            metric = str(metric_record.value.decode('utf-8'))
            metric_name = metric
            if metric is None or metric.strip() == "" or len(metric.split(" ")) != 3:
                if metric is None:
                    metric_name = "None"
                if DEBUG:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "metrics_collector", "INFO", str(None), str(metric), str(None), str(None), "Received the following malformed metric. Ignoring: " + str(metric_name)))
                continue

            metric_name = metric.split(" ")[0]
            if re.match(self._allowed_to_work_on_metrics_pattern, str(metric_name)):
                if DEBUG:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "metrics_collector", "DEBUG", str(None), str(None), str(None), str(None), "Handling the following metric " + str(metric_name) + " since it matches the regex " + self._allowed_to_work_on_metrics_pattern.pattern + "."))
                try:
                    with self._metrics_uniqe_list_update_lock:
                        if metric_name not in self._metrics_uniqe_list.keys():
                            self._metrics_uniqe_list[metric_name] = time.time()
                except Exception as ex:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "metrics_collector", "WARN", str(None), str(None), str(ex), type(ex).__name__, "Failed to add the metric to the list due to the aforementioned exception"))
            else:
                if DEBUG:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "metrics_collector", "DEBUG", str(None), str(None), str(None), str(None), "Ignoring the following metric " + str(metric_name) + " since it DOES NOT match the regex " + str(self._allowed_to_work_on_metrics_pattern.pattern) + "."))

    def get_anomaly_score(self, metric_name, cur_time=None, ref_periods=None):
        state = "BEGIN"
        try:
            res = 0

            if "cur_time" in request.query:
                try:
                    cur_time = int(request.query["cur_time"])
                except:
                    pass

            if "ref_periods" in request.query:
                ref_periods = request.query["ref_periods"]

            state = "GetMetricFileFullPath"
            metric_phys_path = os.path.abspath(os.path.join(self._metrics_base_path, (metric_name.replace(".", "/")) + ".wsp"))
            if not metric_phys_path.startswith(self._metrics_base_path):
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "get_anomaly_score", "SEC_EX", str(None), str(metric_name), str(None), str(None), "The metric's name indicated a path outside the configured metrics path. This is probably a security issue. (metric_name:'" + str(metric_name) + "',metric_phys_path:'" + str(metric_phys_path) + "',metrics_base_path:'" + str(self._metrics_base_path) + "')"))
                return "@@RES: -999.999"

            state = "CalcRefTimePeriods"
            if cur_time is None or cur_time <= 0:
                cur_time_epoch = int(time.time())
            else:
                cur_time_epoch = cur_time

            if ref_periods is None or not re.match("^([0-9][0-9\,]+[0-9]|[0-9]+)$", ref_periods):
                reference_past_periods = self._reference_past_sample_periods.split(",")
            else:
                reference_past_periods = ref_periods.split(",")

            now_end_epoch = cur_time_epoch
            now_start_epoch = now_end_epoch - self._reference_timespan_in_secods

            # Verifying that the whisper file at the given location actually exists
            state = "VerifyMetricFileExists"
            if not os.path.isfile(metric_phys_path):
                raise Exception("Could not find a metric file by the name specified.")

            now_raw_data = whisper.fetch(path=metric_phys_path, fromTime=now_start_epoch, untilTime=now_end_epoch)
            now_data = self.normalize_data(now_raw_data[1])

            # Getting reference past data and normalizing it (that is, filling the blanks with calculated values and marking the beginning and end of the data)
            state = "GetPastData"
            reference_past_data = []
            for i in range(0, len(reference_past_periods)):
                cur_ref_point_ends = cur_time_epoch - (self._reference_timespan_in_secods * int(reference_past_periods[i]))
                cur_ref_point_starts = cur_ref_point_ends - self._reference_timespan_in_secods
                cur_ref_point_raw_data = whisper.fetch(path=metric_phys_path, fromTime=cur_ref_point_starts, untilTime=cur_ref_point_ends)
                cur_ref_point_data = self.normalize_data(cur_ref_point_raw_data)
                reference_past_data.append(cur_ref_point_data)

            # Calculating the amount of datapoints that should be discarded from the end and beginning of all datasets
            state = "CalcAmountOfPastData"
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

            state = "VerifyEnoughDataExist"
            if now_data.count() == 0:
                res = -1.111111

            state = "TrimPastDatasets"
            if res == 0:
                # Trimming all the datasets to the same size according to what has been calculated earlier
                for i in range(0, len(reference_past_data)):
                    del reference_past_data[i].data[(len(reference_past_data[i].data) - trim_all_ends_to):]
                    del reference_past_data[i].data[:trim_all_starts_to]
                del now_data.data[(len(now_data.data) - trim_all_ends_to):]
                del now_data.data[:trim_all_starts_to]

                # Break here if there's no past data available
                reference_past_periods = self._reference_past_sample_periods.split(",")
                for i in range(0, len(reference_past_data)):
                    if reference_past_data[i].count() < now_data.count() or reference_past_data[i].count() == 0:
                        res = -1.222222
                        break

            state = "CalcAnomalyScore"
            if res == 0:
                res = self.calculate_anomaly_score(nowData=now_data, referencePastData=reference_past_data)
                if res > self._anomaly_score_threshold_for_reporting_to_alerter:
                    self.report_anomaly(metric=metric_name, anomaly_info={
                        "anomaly_score": float(res),
                        "timestamp: ": datetime.datetime.now().isoformat(),
                        "metric: ": metric_name,
                        "value: ": float(now_data.data[len(now_data.data) - 1])
                    })

            state = "SendMetricDataToStatsD"
            try:
                self._statsd_client.gauge(metrics_prefix + "." + metric_name, res)
            except Exception as ex:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "get_anomaly_score", "ERROR", str(None), str(metric_name), str(ex), type(ex).__name__, "The aforementioned exception has been thrown while sending the metric to statsd."))

            self._metrics_successfully_analyzed.value += 1
            return "@@RES: " + str(res)
        except Exception as ex:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "get_anomaly_score", "WARN", str(None), str(metric_name), str(ex), type(ex).__name__, "An unexpected exception has occurred."))
            return "@@EXCEPTION: " + type(ex).__name__ + ", " + str(ex) + " [State: " + str(state) + "]"

    def auto_detect_metrics_anomalies_thread(self):
        last_cycle_started = time.time()
        while True:
            time_since_last_cycle_started = time.time() - last_cycle_started
            if time_since_last_cycle_started > (self._anomalies_check_interval * 1.5):
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "auto_detect_metrics_anomalies_thread", "WARN", str(None), str(None), str(None), str(None), "Sampling cycles are taking too long to complete. Last cycle took " + str(time_since_last_cycle_started - self._anomalies_check_interval) + " seconds to completr. Either add more threads per CPU, add more CPUs, switch to a faster CPU, improve Graphite's/disk subsystem's performance or add more instances of this service on other servers."))

            last_cycle_started = time.time()
            self._analysis_cycles_so_far.value += 1
            try:
                if DEBUG:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "auto_detect_metrics_anomalies_thread", "DEBUG", str(None), str(None), str(None), str(None), "Starting a sampling cycle."))
                with self._metrics_uniqe_list_update_lock:
                    local_metrics_list_copy = list(self._metrics_uniqe_list.keys())

                for metric in local_metrics_list_copy:
                    cur_url = str(self._anomaly_detector_proto) + "://" + str(self._anomaly_detector_host) + ":" + str(self._anomaly_detector_port) + str(self._anomaly_detector_url_path) + urllib_req.quote(str(metric))
                    if DEBUG:
                        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "auto_detect_metrics_anomalies_thread", "DEBUG", str(None), str(None), str(None), str(None), "Looking for anomalies in metric " + str(metric) + ". Calling " + str(cur_url)))
                    try:
                        urllib_req.urlopen(cur_url)
                    except Exception as ex:
                        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "auto_detect_metrics_anomalies_thread", "WARN", str(None), str(None), str(ex), type(ex).__name__, "Failed to call the url `" + str(cur_url) + "` due to the aforementioned exceptionWaiting."))

                    self._metrics_parsed.value += 1

                if DEBUG:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "auto_detect_metrics_anomalies_thread", "DEBUG", str(None), str(None), str(None), str(None), "Finished sampling cycle. Handled " + str(len(local_metrics_list_copy)) + " metrics."))

                local_metrics_list_copy = None

            except KeyboardInterrupt:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "auto_detect_metrics_anomalies_thread", "INFO", str(None), str(None), str(None), str(None), "Received a keyboard interrupt. Shutting down main loop."))
                break
            except Exception as ex:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "anomaly_detector", "metrics_collector", "ERROR", str(None), str(None), str(ex), type(ex).__name__, "The aforementioned exception has been thrown while looking for anomalies in the collected metrics. Will try again in the next cycle."))

            time.sleep(self._anomalies_check_interval)


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
        generic_config_url = configurator_base_url + "get_config/" + "smart-onion.config.common.*"
        configurator_response = urllib_req.urlopen(generic_config_url).read().decode('utf-8')
        config_copy = dict(config_copy, **json.loads(configurator_response))
        logging_format = config_copy["smart-onion.config.common.logging_format"]
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
AnomalyDetector(config_copy=config_copy).run(listen_ip=listen_ip, listen_port=listen_port)

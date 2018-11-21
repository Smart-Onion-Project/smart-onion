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

#import subprocess
import whisper
import datetime
import time
import json
import sys
import os
import re
from bottle import route, run, template, get, post, request

DEBUG = True
REFERENCE_PAST_SAMPLES = '7,14,21'
METRIC_PATH = '/var/lib/graphite/whisper/'
METRIC_PATH = '/home/yuval/MYTREE_ext_disk/Programming Projects/Python Projects/smart-onion-resources/whisper-files'
TIMESPAN_IN_SEC = 86400
PERCENT_MODE = True



class NormalizedDataSet:
    data = list()
    trimmedFromStart = -1
    trimmedToEnd = -1

    def __init__(self, rawData):
        self.data = list(rawData)

    def MaxOf(self, num):
        if (len(self.data) > 0 and len(self.data) >= num):
            max = self.data[0]
            for i in range(num - 1, -1, -1):
                if self.data[i] > max:
                    max = self.data[i]
            return max
        else:
            return None

    def MinOf(self, num):
        if (len(self.data) > 0 and len(self.data) >= num):
            min = self.data[0]
            for i in range(num - 1, -1, -1):
                if self.data[i] < min:
                    min = self.data[i]
            return min
        else:
            return None

    def AverageOf(self, num):
        if (len(self.data) > 0 and len(self.data) >= num):
            sum = 0
            for i in range(num - 1, -1, -1):
                sum = sum + self.data[i]
            return sum / float(num)
        else:
            return None

    def Max(self):
        return self.MaxOf(len(self.data))

    def Min(self):
        return self.MinOf(len(self.data))

    def Average(self):
        return self.AverageOf(len(self.data))

    def Count(self):
        return len(self.data)

    def Last(self):
        return self.data[len(self.data) - 1]


class AnomalyDetector:

    def __init__(self):
       pass

    def run(self, listen_ip, listen_port):
        if DEBUG:
           run(host=listen_ip, port=listen_port)
        else:
           run(host=listen_ip, port=listen_port, server="gunicorn", workers=32)

    def NormalizeData(self, rawData):
        lastDataAt = -1
        dataPointsToFill = 0
        tmpRes = rawData
        for i in range(0, len(tmpRes)):
            # print "+ BEFORE: [" + str(i) + "]" + str(tmpRes[i])
            if not (tmpRes[i] is None):
                if (lastDataAt >= 0):
                    # print "+ " + str(dataPointsToFill) + " empty datapoints between idx " + str(lastDataAt) + " [" + str(tmpRes[lastDataAt]) + "] and " + str(i) + " [" + str(tmpRes[i]) + "]: Filling them up..."
                    for j in range(lastDataAt + 1, i):
                        # print "+ Filling datapoint at idx " + str(j) + " with value of " + str(tmpRes[j - 1]) + " + ((" + str(tmpRes[i]) + " - " + str(tmpRes[lastDataAt]) + ") / " + str(dataPointsToFill) + " + 1))"
                        tmpRes[j] = tmpRes[j - 1] + ((tmpRes[i] - tmpRes[lastDataAt]) / dataPointsToFill + 1)
                lastDataAt = i

                dataPointsToFill = 0
            else:
                dataPointsToFill = dataPointsToFill + 1

        res = []
        trimmedFromStart = 0
        trimmedToEnd = 0
        dataStartedAt = -1
        for i in range(0, len(tmpRes)):
            if not (tmpRes[i] is None):
                res.append(tmpRes[i])
                if (dataStartedAt == -1):
                    dataStartedAt = i
            else:
                if (dataStartedAt == -1):
                    trimmedFromStart = trimmedFromStart + 1
                else:
                    trimmedToEnd = trimmedToEnd + 1

        # Returning the un-trimmed version of the array to allow the calling code to trim it as it sees fit
        resObj = NormalizedDataSet(tmpRes)
        resObj.trimmedFromStart = trimmedFromStart
        resObj.trimmedToEnd = trimmedToEnd
        return resObj

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
            if (nowData.Last() > referencePastData[i].Max()):
                R = R + ((nowData.Last() - referencePastData[i].Max()) / float(referencePastPeriods[i]))
                R1 = R1 + ((nowData.Last() - referencePastData[i].Max()) / float(referencePastPeriods[i]))
            # R2 - Current vs. reference period's min
            if (nowData.Last() < referencePastData[i].Min()):
                R = R + ((nowData.Last() - referencePastData[i].Min()) / float(referencePastPeriods[i]))
                R2 = R2 + ((nowData.Last() - referencePastData[i].Min()) / float(referencePastPeriods[i]))
            # R3 - Current vs. reference period's average
            R = R + ((nowData.Last() - referencePastData[i].Average()) / float(referencePastPeriods[i]))
            R3 = R3 + ((nowData.Last() - referencePastData[i].Average()) / float(referencePastPeriods[i]))
            # R4 - Current vs. reference period's last
            R = R + ((nowData.Last() - referencePastData[i].Last()) / float(referencePastPeriods[i]))
            R4 = R4 + ((nowData.Last() - referencePastData[i].Last()) / float(referencePastPeriods[i]))
            # R5 - Compare the current data's range (i.e max - min) to the past data
            if (R < 0):
                R = R - ((abs(nowData.MaxOf(60) - nowData.MinOf(60)) - abs(
                    referencePastData[i].MaxOf(60) - referencePastData[i].MinOf(60))) / float(referencePastPeriods[i]))
                R5 = R5 - ((abs(nowData.MaxOf(60) - nowData.MinOf(60)) - abs(
                    referencePastData[i].MaxOf(60) - referencePastData[i].MinOf(60))) / float(referencePastPeriods[i]))
            # R6 - Compare the current data's range (i.e max - min) to the past data
            if (R > 0):
                R = R + ((abs(nowData.MaxOf(60) - nowData.MinOf(60)) - abs(
                    referencePastData[i].MaxOf(60) - referencePastData[i].MinOf(60))) / float(referencePastPeriods[i]))
                R6 = R6 + ((abs(nowData.MaxOf(60) - nowData.MinOf(60)) - abs(
                    referencePastData[i].MaxOf(60) - referencePastData[i].MinOf(60))) / float(referencePastPeriods[i]))

        for i in range(0, len(referencePastData)):
            rMAX = rMAX + ((referencePastData[i].Max() / float(referencePastPeriods[i])))

        if (PERCENT_MODE):
            return R / rMAX * 100
        else:
            return R

    def fetch_raw_data(self):
        pass

    @get('/smart-onion/get-anomaly-score/<metric_name>')
    def get_anomaly_score(metric_name, cur_time=None, ref_periods=None):
        anomaly_detector = AnomalyDetector()

        metric_phys_path = os.path.join(METRIC_PATH, (metric_name.replace(".", "/")) + ".wsp")
        if cur_time is None:
            cur_time_epoch = int(time.time())
        else:
            cur_time_epoch = cur_time

        now_end_epoch = cur_time_epoch
        now_start_epoch = now_end_epoch - TIMESPAN_IN_SEC

        # Verifying that the whisper file at the given location actually exists
        if (not os.path.isfile(metric_phys_path)):
            raise Exception("Could not find a metric file by the name specified.")

        now_raw_data = whisper.fetch(path=metric_phys_path, fromTime=now_start_epoch, untilTime=now_end_epoch)
        now_data = anomaly_detector.NormalizeData(now_raw_data[1])

        # Getting reference past data and normalizing it (that is, filling the blanks with calculated values and marking the beginning and end of the data)
        reference_past_periods = REFERENCE_PAST_SAMPLES.split(",")
        reference_past_data = []
        for i in range(0, len(reference_past_periods)):
            cur_ref_point_ends = cur_time_epoch - (TIMESPAN_IN_SEC * int(reference_past_periods[i]))
            cur_ref_point_starts = cur_ref_point_ends - TIMESPAN_IN_SEC
            cur_ref_point_raw_data = whisper.fetch(path=metric_phys_path, fromTime=cur_ref_point_starts, untilTime=cur_ref_point_ends)
            cur_ref_point_data = anomaly_detector.NormalizeData(cur_ref_point_raw_data)
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

        if now_data.Count() == 0:
            return "@@RES: -1.111111"

        # Trimming all the datasets to the same size according to what has been calculated earlier
        for i in range(0, len(reference_past_data)):
            del reference_past_data[i].data[(len(reference_past_data[i].data) - trim_all_ends_to):]
            del reference_past_data[i].data[:trim_all_starts_to]
        del now_data.data[(len(now_data.data) - trim_all_ends_to):]
        del now_data.data[:trim_all_starts_to]

        # Break here if there's no past data available
        reference_past_periods = REFERENCE_PAST_SAMPLES.split(",")
        for i in range(0, len(reference_past_data)):
            if reference_past_data[i].Count() < now_data.Count() or reference_past_data[i].Count() == 0:
                return "@@RES: -1.222222"

        return "@@RES: " + str(anomaly_detector.calculate_anomaly_score(nowData=now_data, referencePastData=reference_past_data))



script_path = os.path.dirname(os.path.realpath(__file__))
listen_ip = "127.0.0.1"
listen_port = 9090
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

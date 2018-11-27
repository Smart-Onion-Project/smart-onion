#!/usr/bin/python

import subprocess
import datetime
import time
import json
import sys
import os


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


def NormalizeData(rawData):
    lastDataAt = -1
    dataPointsToFill = 0
    tmpRes = list(rawData['values'])
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


def ArrContains(arr1, arr2):
    # arr1 - Array to be searched
    # arr2 - Array to be searched for
    matchingElements = 0
    for i in arr1:
        for j in arr2:
            if (i.lower() == j.lower()):
                matchingElements = matchingElements + 1

    return matchingElements == len(arr2)


# Setting up inital parameters from the command-line args
USAGE = "Usage: " + sys.argv[
    0] + " --metric:<metricName> [--refPast:<pastPeriodsToCompareTo>] [--whisperPath:<metricWhisperPath>] [--timeSpan:<timeSpanToCompare>] [--verbose:<on|off>] [--percent:<on|off>] [--currentTime:<curTimeEpoch>]\n" + \
        "\n" + \
        "metricName - The metric name in Graphite syntax. ex: zabbix.1Discovered_hosts.MVS-SQL7.SQL.SQL_Instance_Page_Life_Expectancy._DEFAULT\n" + \
        "pastPeriodsToCompareTo - Past periods to compare to. Specified in days, delimited by commas. ex: 7,14,21 (To compare current data to the data of last week, a week before that and a week before that). Optional. Defaults to 7,14,21\n" + \
        "metricWhisperPath - The location of the Whisper files. Optional. Defaults to /var/lib/graphite/whisper/\n" + \
        "timeSpanToCompare - The timespan that will be used to compare in seconds. Optional. Defaults to 86400 (The number of seconds in a day)\n" + \
        "verbose - Print more debug related data (about the anomaly detection phases). Optional. Defaults to 'off'\n" + \
        "percent - Specifies whether the script should output the result as a percentage value. Defaults to 'on'\n" + \
        "currentTime - Artificially specify the point in time to start the analysis from. Optional. Defaults to the current time. \n"
if (len(sys.argv) > 1 and sys.argv[1].lower() in ["-h", "--help", "-?"]):
    print
    USAGE
    exit(0)
if (len(sys.argv) < 2):
    print
    "ERROR: Not enough arguments."
    print
    USAGE
    exit(9)
else:
    argOptions = ["--metric:", "--refPast:", "--whisperPath:", "--timeSpan:", "--verbose:", "--percent:",
                  "--currentTime:"]
    argMandatoryOptionsTillIdx = 0
    argNames = []
    argValues = []
    for arg in sys.argv:
        for argOpt in argOptions:
            if arg.lower().startswith(argOpt.lower()):
                if argOpt.lower() in argNames:
                    print
                    "ERROR: The argument '" + argOpt.lower() + "' appears more than once. REMEMBER THAT ARGUMENTS ARE CASE-INSENSITIVES!"
                    print
                    USAGE
                    exit(8)
                else:
                    argNameLength = len(argOpt.lower())
                    argNames.append(argOpt.lower())
                    if (len(arg[argNameLength:]) > 0):
                        argValues.append(arg[argNameLength:])
    if (not ArrContains(argNames, argOptions[:argMandatoryOptionsTillIdx])):
        print
        "ERROR: Some mandatory args are missing!"
        print
        USAGE
        exit(7)
    if (len(argValues) != len(argNames)):
        print
        "ERROR: Some args have no value!"
        print
        USAGE
        exit(6)

METRIC = argValues[
    argNames.index("--metric:")]  # 'zabbix.1Discovered_hosts.MVS-SQL7.SQL.SQL_Instance_Page_Life_Expectancy._DEFAULT'
REFERENCE_PAST_SAMPLES = '7,14,21'
METRIC_PATH = '/var/lib/graphite/whisper/'
TIMESPAN_IN_SEC = 86400
VERBOSE = False
PERCENT_MODE = True
CURRENT_TIME = int(time.time())

if ("--refpast:" in argNames):
    REFERENCE_PAST_SAMPLES = argValues[argNames.index("--refpast:")]  # '7,14,21'
if ("--whisperpath:" in argNames):
    METRIC_PATH = argValues[argNames.index("--whisperpath:")]  # '/var/lib/graphite/whisper/'
if ("--timespan:" in argNames):
    TIMESPAN_IN_SEC = int(argValues[argNames.index("--timespan:")])  # 86400
if ("--verbose:" in argNames):
    VERBOSE = argValues[argNames.index("--verbose:")].lower() in ["true", "on"]  # False
if ("--percent:" in argNames):
    PERCENT_MODE = argValues[argNames.index("--percent:")].lower() in ["true", "on"]  # True
if ("--currenttime:" in argNames):
    CURRENT_TIME = int(argValues[argNames.index("--currenttime:")])  # int(time.time())

# Adding some basic info based on the initial parameters
metricPhysPath = METRIC_PATH + (METRIC.replace(".", "/")) + ".wsp"
curTimeEpoch = CURRENT_TIME

# Verifying that the whisper file at the given location actually exists
if (not os.path.isfile(metricPhysPath)):
    print
    "ERROR: Could not locate a metric whisper file at '" + metricPhysPath + "'"
    print
    USAGE
    exit(5)

# Getting current timespan's metric raw data and normalizing it (that is, filling the blanks with calculated values and marking the beginning and end of the data)
nowEndEpoch = curTimeEpoch
nowStartEpoch = nowEndEpoch - TIMESPAN_IN_SEC
nowRawData = subprocess.check_output(
    ["/usr/bin/whisper-fetch", "--json", "--from=" + str(nowStartEpoch), "--until=" + str(nowEndEpoch), metricPhysPath])
nowData = NormalizeData(json.loads(nowRawData))

# Getting reference past data and normalizing it (that is, filling the blanks with calculated values and marking the beginning and end of the data)
referencePastPeriods = REFERENCE_PAST_SAMPLES.split(",")
referencePastData = []
for i in range(0, len(referencePastPeriods)):
    curRefPointEnds = curTimeEpoch - (TIMESPAN_IN_SEC * int(referencePastPeriods[i]))
    curRefPointStarts = curRefPointEnds - TIMESPAN_IN_SEC
    curRefPointRawData = subprocess.check_output(
        ["/usr/bin/whisper-fetch", "--json", "--from=" + str(curRefPointStarts), "--until=" + str(curRefPointEnds),
         metricPhysPath])
    curRefPointData = NormalizeData(json.loads(curRefPointRawData))
    referencePastData.append(curRefPointData)

# Calculating the amount of datapoints that should be discarded from the end and beginning of all datasets
trimAllStartsTo = 0
trimAllEndsTo = 0
for i in range(0, len(referencePastData)):
    if (referencePastData[i].trimmedFromStart > trimAllStartsTo):
        trimAllStartsTo = referencePastData[i].trimmedFromStart
    if (referencePastData[i].trimmedToEnd > trimAllEndsTo):
        trimAllEndsTo = referencePastData[i].trimmedToEnd
if (nowData.trimmedFromStart > trimAllStartsTo):
    trimAllStartsTo = nowData.trimmedFromStart
if (nowData.trimmedToEnd > trimAllEndsTo):
    trimAllEndsTo = nowData.trimmedToEnd

# Break here if there's no current data
if (nowData.Count() == 0):
    print
    "ERROR: NOT ENOUGH CURRENT DATA AVAILABLE. LEAVING (metricPhysPath=" + metricPhysPath + ", nowStartEpoch=" + str(
        nowStartEpoch) + ", nowEndEpoch=" + str(nowEndEpoch) + ")"
    exit(2)

# Trimming all the datasets to the same size according to what has been calculated earlier
for i in range(0, len(referencePastData)):
    del referencePastData[i].data[(len(referencePastData[i].data) - trimAllEndsTo):]
    del referencePastData[i].data[:trimAllStartsTo]
del nowData.data[(len(nowData.data) - trimAllEndsTo):]
del nowData.data[:trimAllStartsTo]

# Break here if there's no past data available
referencePastPeriods = REFERENCE_PAST_SAMPLES.split(",")
for i in range(0, len(referencePastData)):
    if (referencePastData[i].Count() < nowData.Count() or referencePastData[i].Count() == 0):
        print
        "ERROR: NOT ENOUGH PAST DATA AVAILABLE. LEAVING (metricPhysPath=" + metricPhysPath + ", nowStartEpoch=" + str(
            nowStartEpoch) + ", nowEndEpoch=" + str(nowEndEpoch) + ")"
        exit(1)

# Analyzing the data (The value of R will indicate whether and if so, how big was the anomaly at the current datapoint)
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

if (VERBOSE):
    print
    "+Verbose Info:"
    print
    "+============="
    print
    "+{0:<50} + {1:15} + {2:15} + {3:15} + {4:15} + {5:15} + {6:15} + {7:15} + {8:15} + {9:15} +".format("-" * 50,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15)
    print
    "+{0:<50} | {1:<15} | {2:<15} | {3:<15} | {4:<15} | {5:<15} | {6:<15} | {7:<15} | {8:<15} | {9:<15} |".format(
        "Ref#", "max", "min", "Avg", "max(60)", "min(60)", "Avg(60)", "count", "last", "HourRange")
    for i in range(0, len(referencePastData)):
        print
        "+{0:<50} | {1:15} | {2:15} | {3:15} | {4:15} | {5:15} | {6:15} | {7:15} | {8:15} | {9:15} |".format(
            "referencePastData[" + str(i) + "]", referencePastData[i].Max(), referencePastData[i].Min(),
            referencePastData[i].Average(), referencePastData[i].MaxOf(60), referencePastData[i].MinOf(60),
            referencePastData[i].AverageOf(60), referencePastData[i].Count(), referencePastData[i].Last(),
            abs(referencePastData[i].MaxOf(60) - referencePastData[i].MinOf(60)))
    print
    "+{0:<50} | {1:15} | {2:15} | {3:15} | {4:15} | {5:15} | {6:15} | {7:15} | {8:15} | {9:15} |".format("NOW",
                                                                                                         nowData.Max(),
                                                                                                         nowData.Min(),
                                                                                                         nowData.Average(),
                                                                                                         nowData.MaxOf(
                                                                                                             60),
                                                                                                         nowData.MinOf(
                                                                                                             60),
                                                                                                         nowData.AverageOf(
                                                                                                             60),
                                                                                                         nowData.Count(),
                                                                                                         nowData.Last(),
                                                                                                         abs(
                                                                                                             nowData.MaxOf(
                                                                                                                 60) - nowData.MinOf(
                                                                                                                 60)))
    print
    "+{0:<50} + {1:15} + {2:15} + {3:15} + {4:15} + {5:15} + {6:15} + {7:15} + {8:15} + {9:15} +".format("-" * 50,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15,
                                                                                                         "-" * 15)

    print
    "+R1 impact: {0:15}\t{1}".format(R1, "#R1 - Current vs. reference period's max")
    print
    "+R2 impact: {0:15}\t{1}".format(R2, "#R2 - Current vs. reference period's min")
    print
    "+R3 impact: {0:15}\t{1}".format(R3, "#R3 - Current vs. reference period's average")
    print
    "+R4 impact: {0:15}\t{1}".format(R4, "#R4 - Current vs. reference period's last")
    print
    "+R5 impact: {0:15}\t{1}".format(R5, "#R5 - Compare the current data's range (i.e max - min) to the past data")
    print
    "+R6 impact: {0:15}\t{1}".format(R6, "#R6 - Compare the current data's range (i.e max - min) to the past data")
    print
    "+rMAX.....: {0:15}\t{1}".format(rMAX,
                                     "#The max that is used for percent calculation is the sum of all the max's of each reference past data divided by the number of periods ago it refers to.")
    print
    "+"
    print
    "+R = " + str(R) + " (" + str(R / (rMAX) * 100) + "%)"
else:
    if (PERCENT_MODE):
        print(R / (rMAX) * 100)
    else:
        print
        R

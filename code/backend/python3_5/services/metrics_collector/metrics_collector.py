#!/usr/bin/python3.5
##########################################################################
# Metrics Collector                                                      #
# -----------------                                                      #
#                                                                        #
# This service is part of the Smart-Onion package. This micro-service is #
# responsible for collecting and aggregating information from the        #
# Security Onion ELK. The aggregated and collected data is being used by #
# the service to create metrics like this:                               #
# metric.name.hierarchy value timestamp_in_unix_ms)                      #
#                                                                        #
#                                                                        #
##########################################################################


import sys
from builtins import len, quit, Exception
from bottle import Bottle, request
from urllib import request as urllib_req
import datetime
import json
import base64
from elasticsearch import Elasticsearch
import re
import os
import statsd
import dateutil.parser
import editdistance
import nltk.metrics
from PIL import ImageFont
from PIL import Image
from PIL import ImageDraw
import imagehash
import time
import homoglyphs
import syslog
import urllib.parse
import hashlib
import kafka
import threading
import uuid
import multiprocessing
import random


DEBUG = False
elasticsearch_server = "127.0.0.1"
metrics_prefix = "smart-onion"
config_copy = {}

class QueryNameNotFoundOrOfWrongType(Exception):
    pass

class Utils:
    lst = ""
    perceptive_hashing_algs = ["phash", "ahash", "dhash", "whash"]

    def GenerateLldFromElasticAggrRes(cls, res, macro_list, use_base64, services_urls=None, tiny_url_service_details=None, queries_to_run=None, add_doc_count=True, re_sort_by_value=False, config_copy=None):
        cls.FlattenAggregates(obj=res["aggregations"], idx=0, add_doc_count=add_doc_count)
        # print(cls.lst)
        cls.lst = cls.lst.strip("|")

        res = {
            "data": []
        }
        for line in cls.lst.split("|"):
            line_element = {}
            idx = 0
            line_as_arr = line.split(",")
            for item_b64 in line_as_arr:
                item = base64.b64decode(item_b64.encode('utf-8')).decode('utf-8')
                if use_base64:
                    item_parsed = str(base64.b64encode(str(item).encode('utf-8')).decode('utf-8'))
                else:
                    if cls.is_number(item):
                        item_parsed = float(item)
                    else:
                        item_parsed = str(item)

                if len(macro_list) <= idx:
                    if idx == len(line_as_arr) - 1:
                        if add_doc_count:
                            line_element["{#_DOC_COUNT}"] = item_parsed
                    else:
                        line_element["{#ITEM_" + str(idx) + "}"] = item_parsed
                else:
                    line_element["{#" + macro_list[idx] + "}"] = item_parsed
                idx = idx + 1
            res["data"].append(line_element)

            if re_sort_by_value:
                idx = 0
                res_tmp = {
                    "data": []
                }
                res_tmp_sorted_arr = []
                for item in res["data"]:
                    res_tmp_sorted_arr.append(item["{#ITEM_" + str(idx) + "}"])

                # Sort res_tmp
                res_tmp_sorted_arr.sort()

                for item in res_tmp_sorted_arr:
                    res_tmp["data"].append({"{#ITEM_" + str(idx) + "}": item})

                #TODO: Add support for multi level lists?
                res = res_tmp

        if not queries_to_run is None:
            res_new = {
                "data": []
            }
            for item in res["data"]:
                for query_id in queries_to_run:
                    # Create the URL that need to be accessed by the query_obj type and the number of arguments returned
                    cur_query_obj = queries_conf[query_id]
                    query_type = str(cur_query_obj["type"])

                    cur_url = ""
                    if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol" in config_copy:
                        cur_url = cur_url + config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol"] + "://"
                    if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host" in config_copy:
                        cur_url = cur_url + config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host"] + ":"
                    if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port" in config_copy:
                        cur_url = cur_url + str(config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port"])
                    cur_url = cur_url + services_urls["smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls." + query_type.lower()] + query_id + "?arg1=" + str(item[list(item.keys())[0]])

                    arg_no = 1
                    for key_idx in range(1, len(item.keys())):
                        if list(item.keys())[key_idx] != "{#_DOC_COUNT}":
                            cur_url = cur_url + "&arg" + str(arg_no) + "=" + urllib.parse.quote_plus(str(item[list(item.keys())[key_idx]]))
                            arg_no = arg_no + 1

                    # Translate the url to a tiny url
                    tiny_url_res = cur_url
                    if tiny_url_service_details is not None:
                        tiny_service_url = tiny_url_service_details["protocol"] + "://" + tiny_url_service_details["server"] + ":" + str(tiny_url_service_details["port"]) + services_urls["smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.url2tiny"] + "?url=" + urllib.parse.quote(base64.b64encode(cur_url.encode('utf-8')).decode('utf-8'), safe='')
                        print("Calling " + tiny_service_url)
                        tiny_url_res = urllib_req.urlopen(tiny_service_url).read().decode('utf-8')
                    else:
                        tiny_url_res = cur_url

                    if add_doc_count and "{#_DOC_COUNT}" in item.keys():
                        res_new["data"].append({
                            "{#NAME}": cur_url,
                            "{#URL}": tiny_url_res,
                            "{#QUERY_NAME}": query_id,
                            "{#QUERY_TYPE}": query_type,
                            "{#ARGS}": list(item.keys()),
                            "{#_DOC_COUNT}": item["{#_DOC_COUNT}"]
                        })
                    else:
                        res_new["data"].append({
                            "{#NAME}": cur_url,
                            "{#URL}": tiny_url_res,
                            "{#QUERY_NAME}": query_id,
                            "{#QUERY_TYPE}": query_type,
                            "{#ARGS}": list(item.keys()),
                        })

            res = res_new
        return res

    def FlattenAggregates(cls, obj, idx=0, add_doc_count=True):
        if "key" in obj:
            if len(cls.lst) > 0:
                if cls.lst[len(cls.lst) - 1] == "|":
                    if idx == 0:
                        cls.lst = cls.lst + str(base64.b64encode(str(obj["key"]).encode('utf-8')).decode('utf-8'))
                    else:
                        lst_as_arr = cls.lst.split("|")
                        last_line_as_arr = lst_as_arr[len(lst_as_arr) - 2].split(",")
                        for i in range(0, idx - 1):
                            cls.lst = cls.lst + last_line_as_arr[i] + ","
                        cls.lst = cls.lst + str(base64.b64encode(str(obj["key"]).encode('utf-8')).decode('utf-8'))
                else:
                    cls.lst = cls.lst + "," + str(base64.b64encode(str(obj["key"]).encode('utf-8')).decode('utf-8'))
            else:
                cls.lst = str(base64.b64encode(str(obj["key"]).encode('utf-8')).decode('utf-8'))
        if "field_values" + str(idx) in obj and "buckets" in obj["field_values" + str(idx)]:
            for value in obj["field_values" + str(idx)]["buckets"]:
                cls.FlattenAggregates(obj=value, idx=idx + 1, add_doc_count=add_doc_count)
        else:
            if add_doc_count:
                cls.lst = cls.lst + "," + str(base64.b64encode(str(obj["doc_count"]).encode('utf-8')).decode('utf-8')) + "|"
            else:
                cls.lst = cls.lst + "," + "|"
            return

    def is_number(cls, s):
        try:
            float(s)
            return True
        except (TypeError, ValueError):
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass

        return False

    def HammingDistance(self, cur_value_to_compare_to, value_to_compare):
        if value_to_compare == cur_value_to_compare_to:
            match_rate = 100
        else:
            if len(cur_value_to_compare_to) != len(value_to_compare):
                cur_value_to_compare_to_split = cur_value_to_compare_to.split(".")
                value_to_compare_split = value_to_compare.split(".")
                if len(cur_value_to_compare_to_split) < len(value_to_compare_split):
                    for i in range(0, len(value_to_compare_split) - len(cur_value_to_compare_to_split)):
                        cur_value_to_compare_to_split.append(" ")

                if len(value_to_compare_split) < len(cur_value_to_compare_to_split):
                    for i in range(0, len(cur_value_to_compare_to_split) - len(value_to_compare_split)):
                        value_to_compare_split.append(" ")

                for idx in range(0, len(value_to_compare_split)):
                    if len(value_to_compare_split[idx]) < len(cur_value_to_compare_to_split[idx]):
                        padding = " " * (len(cur_value_to_compare_to_split[idx]) - len(value_to_compare_split[idx]))
                        value_to_compare_split[idx] = value_to_compare_split[idx] + padding
                    if len(cur_value_to_compare_to_split[idx]) < len(value_to_compare_split[idx]):
                        padding = " " * (len(value_to_compare_split[idx]) - len(cur_value_to_compare_to_split[idx]))
                        cur_value_to_compare_to_split[idx] = cur_value_to_compare_to_split[idx] + padding

                cur_value_to_compare_to = ".".join(cur_value_to_compare_to_split)
                value_to_compare = ".".join(value_to_compare_split)

            matched_chars = 0.0
            for idx in range(0, len(cur_value_to_compare_to)):
                if cur_value_to_compare_to[idx] == value_to_compare[idx]:
                    matched_chars = matched_chars + 1
            match_rate = matched_chars / len(cur_value_to_compare_to) * 100.0
        return match_rate

    def LevenshteinDistance(self, cur_value_to_compare_to, value_to_compare):
        if len(cur_value_to_compare_to) > len(value_to_compare):
            match_rate = (len(cur_value_to_compare_to) - editdistance.eval(value_to_compare, cur_value_to_compare_to)) / len(cur_value_to_compare_to) * 100
        else:
            match_rate = (len(value_to_compare) - editdistance.eval(value_to_compare, cur_value_to_compare_to)) / len(
                value_to_compare) * 100

        return match_rate

    def text2png(self, text, color="#000", bgcolor="#FFF", fontfullpath=None, fontsize=13, leftpadding=3,
                 rightpadding=3, width=-1, height=-1):
        REPLACEMENT_CHARACTER = u'\uFFFD'
        NEWLINE_REPLACEMENT_STRING = ' ' + REPLACEMENT_CHARACTER + ' '

        font = ImageFont.load_default()
        if fontfullpath != None:
            if fontfullpath.lower().endswith(".ttf"):
                ImageFont.truetype(fontfullpath, fontsize)
            else:
                ImageFont.load(fontfullpath)
        text = text.replace('\n', NEWLINE_REPLACEMENT_STRING)

        lines = []
        line = u""

        for word in text.split():
            # print(word)
            if word == REPLACEMENT_CHARACTER:  # give a blank line
                lines.append(line[1:])  # slice the white space in the begining of the line
                line = u""
                lines.append(u"")  # the blank line
            elif font.getsize(line + ' ' + word)[0] <= (width - rightpadding - leftpadding):
                line += ' ' + word
            else:  # start a new line
                lines.append(line[1:])  # slice the white space in the begining of the line
                line = u""

                line += ' ' + word  # for now, assume no word alone can exceed the line width

        if len(line) != 0:
            lines.append(line[1:])  # add the last line

        line_height = -1
        if height == -1:
            line_height = font.getsize(text)[1]
        img_height = line_height * (len(lines) + 1)
        if width == -1:
            width = leftpadding + font.getsize(text)[0] + rightpadding
        img_width = width

        img = Image.new("RGBA", (img_width, img_height), bgcolor)
        draw = ImageDraw.Draw(img)

        y = 0
        for line in lines:
            draw.text((leftpadding, y), line, color, font=font)
            y += line_height

        # img.save(fullpath)
        return img

    def VisualSimilarityRate(self, cur_value_to_compare_to, value_to_compare, algorithm="phash"):
        fonts = [
            "/home/yuval/.fonts/Arial_0.ttf",
            "/home/yuval/.fonts/tahoma.ttf",
            "/home/yuval/.fonts/TIMES_0.TTF",
            "/home/yuval/.fonts/Courier New.ttf"
        ]
        max_similarity_rate = 0

        for font in fonts:
            cur_value_to_compare_to_img = self.text2png(cur_value_to_compare_to, fontsize=99, fontfullpath=font)
            value_to_compare_img = self.text2png(value_to_compare, fontsize=99, fontfullpath=font)
            if algorithm == "phash":
                cur_value_to_compare_to_img_hash = imagehash.phash(cur_value_to_compare_to_img)
                value_to_compare_img_hash = imagehash.phash(value_to_compare_img)
            elif algorithm == "ahash":
                cur_value_to_compare_to_img_hash = imagehash.average_hash(cur_value_to_compare_to_img)
                value_to_compare_img_hash = imagehash.average_hash(value_to_compare_img)
            elif algorithm == "whash":
                cur_value_to_compare_to_img_hash = imagehash.whash(cur_value_to_compare_to_img)
                value_to_compare_img_hash = imagehash.whash(value_to_compare_img)
            elif algorithm == "dhash":
                cur_value_to_compare_to_img_hash = imagehash.dhash(cur_value_to_compare_to_img)
                value_to_compare_img_hash = imagehash.dhash(value_to_compare_img)


            cur_similarity_rate = 100.0 - ((cur_value_to_compare_to_img_hash - value_to_compare_img_hash) / 100.0 * 100.0)
            if cur_similarity_rate > max_similarity_rate:
                max_similarity_rate = cur_similarity_rate

        return max_similarity_rate
    
    def HomoglyphsRatio(self, cur_value_to_compare_to, value_to_compare):
        homoglyphs_cache = {}
        homoglyphs_obj = homoglyphs.Homoglyphs()
        homoglyphs_found = 0
        for i in range(0, len(cur_value_to_compare_to)):
            char1 = cur_value_to_compare_to[i]
            if len(value_to_compare) > i:
                char2 = value_to_compare[i]
            else:
                char2 = ""
            if not char1 in homoglyphs_cache:
                homoglyphs_cache[char1] = homoglyphs_obj.get_combinations(char1)
            if char2 != char1 and char2 in homoglyphs_cache[char1]:
                print(char2 + " -> " + char1)
                homoglyphs_found = homoglyphs_found + 1

        return homoglyphs_found / len(cur_value_to_compare_to) * 100


class MetricsCollector:
    queries = {}
    statsd_client = statsd.StatsClient(prefix=metrics_prefix)
    _learned_net_info = None

    def __init__(self, listen_ip, listen_port, learned_net_info, config_copy, queries_config, tiny_url_protocol, tiny_url_server, tiny_url_port, elasticsearch_server, timeout_to_elastic):
        self._time_loaded = time.time()
        self._host = listen_ip
        self._port = listen_port
        self._learned_net_info = learned_net_info
        self._app = Bottle()
        self._route()
        self.queries = queries_config
        self._tiny_url_protocol = tiny_url_protocol
        self._tiny_url_server = tiny_url_server
        self._tiny_url_port = tiny_url_port
        self._config_copy = config_copy
        self.es = Elasticsearch(hosts=[elasticsearch_server], timeout=timeout_to_elastic)
        self.es_hosts = [elasticsearch_server]
        self.timeout_to_elastic = timeout_to_elastic
        self._sampling_tasks = []
        self._polling_threads = []
        self._sampling_tasks_index = {}
        self._is_sampling_tasks_gc_running = False
        self._sampling_tasks_threads_sync_lock = threading.Lock()

    def _route(self):
        self._app.route('/smart-onion/field-query/<queryname>', method="GET", callback=self.fieldQuery)
        self._app.route('/smart-onion/test-similarity/<queryname>', method="GET", callback=self.get_similarity)
        self._app.route('/smart-onion/get-length/<queryname>', method="GET", callback=self.get_length)
        self._app.route('/smart-onion/get-length_b64/<queryname>', method="GET", callback=self.get_length_b64)
        self._app.route('/smart-onion/query-count/<queryname>', method="GET", callback=self.queryCount)
        self._app.route('/smart-onion/list-hash/<queryname>', method="GET", callback=self.list_hash)
        self._app.route('/smart-onion/discover/<queryname>', method="GET", callback=self.discover)
        self._app.route('/smart-onion/dump_queries', method="GET", callback=self.dump_queries)
        self._app.route('/test/similarity/<algo>/<s1>/<s2>', method="GET", callback=self.test_similarity)
        self._app.route('/test/lld', method="GET", callback=self.test_lld_creation)
        self._app.route('/ping', method="GET", callback=self._ping)

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

    def run(self):
        self._kafka_client_id = "SmartOnionMetricsCollectorService_" + str(uuid.uuid4()) + "_" + str(int(time.time()))
        self._kafka_server = self._config_copy["smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers"]
        self._sampling_tasks_kafka_topic = self._config_copy["smart-onion.config.architecture.internal_services.backend.timer.metrics_collection_tasks_topic"]

        # Start a pool of threads (the size is in the config) that will pull sampling tasks from this object's list
        for i in range(0, self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.poller_threads_per_cpu"] * multiprocessing.cpu_count()):
            sampling_tasks_list = {}
            self._sampling_tasks.append(sampling_tasks_list)
            poller_thread = threading.Thread(target=self.sampling_tasks_poller, args=[sampling_tasks_list])
            self._polling_threads.append(poller_thread)
            poller_thread.start()

        self._sampling_tasks_kafka_consumer_thread = threading.Thread(target=self.sampling_tasks_kafka_consumer)
        self._sampling_tasks_kafka_consumer_thread.start()

        self._sampling_tasks_garbage_collector_thread = threading.Thread(target=self._sampling_tasks_garbage_collector)
        self._sampling_tasks_garbage_collector_thread.start()

        if DEBUG:
            self._app.run(host=self._host, port=self._port)
        else:
            self._app.run(host=self._host, port=self._port, server="gunicorn", workers=32, timeout=120)

    def _sampling_tasks_garbage_collector(self):
        while True:
            print("DEBUG: Going to sleep for " + str(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_tasks_gc_interval"]) + " seconds...")
            time.sleep(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_tasks_gc_interval"])
            self._is_sampling_tasks_gc_running = True

            try:
                tombstoned_urls = []

                for sampling_tasks_list in self._sampling_tasks:
                    for url in sampling_tasks_list.keys():
                        if sampling_tasks_list[url]["TTL"] == 0:
                            tombstoned_urls.append(url)

                self._sampling_tasks_threads_sync_lock.acquire()
                for url in tombstoned_urls:
                    for sampling_tasks_list in self._sampling_tasks:
                        if url in sampling_tasks_list.keys():
                            del sampling_tasks_list[url]
            except Exception as ex:
                print("WARN: The following unexpected exception has been thrown during the garbage collection of the sampling tasks lists items: " + str(ex))
            finally:
                try:
                    self._sampling_tasks_threads_sync_lock.release()
                except:
                    pass

            self._is_sampling_tasks_gc_running = False

    def sampling_tasks_poller(self, tasks_list):
        thread_id = "sampling_tasks_poller_" + str(uuid.uuid4())
        last_ran = 0

        sleep_time = random.randint(0, 10)
        print("INFO: Going to sleep for " + str(sleep_time) + " seconds (randomly chosen sleeping time)...")
        time.sleep(sleep_time)

        while True:
            is_sampling_tasks_gc_running = self._is_sampling_tasks_gc_running

            if is_sampling_tasks_gc_running:
                self._sampling_tasks_threads_sync_lock.acquire()

            try:
                if last_ran > 0 and int(time.time() - last_ran) > int(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_interval_ms"] / 1000):
                    print("WARN[" + thread_id + "]: Samples are lagging in " + str(time.time() - last_ran) + " seconds. Either add more threads per CPU, add more CPUs, switch to a faster CPU, improve Elasticsearch's performance or add more instances of this service on other servers.")

                last_ran = time.time()

                # Get a task from the list and complete it.
                for task_url in tasks_list.keys():
                    if tasks_list[task_url]["TTL"] > 0:
                        try:
                            print("DEBUG[" + thread_id + "]: Calling '" + task_url + "'...")
                            urllib_req.urlopen(task_url)
                            tasks_list[task_url]["TTL"] = tasks_list[task_url]["TTL"] - 1
                        except Exception as ex:
                            print("WARN[" + thread_id + "]: Failed to query the URL '" + task_url + "' due to the following exception: " + str(ex))

            except Exception as ex:
                print("WARN[" + thread_id + "]: The following unexpected exception has been thrown while handling sampling tasks: " + str(ex))

            finally:
                try:
                    if is_sampling_tasks_gc_running:
                        self._sampling_tasks_threads_sync_lock.release()
                except:
                    pass

            sleep_time = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_interval_ms"] / 1000.0
            print("INFO[" + thread_id + "]: Going to sleep for " + str(sleep_time) + " seconds...")
            time.sleep(sleep_time)

    def sampling_tasks_kafka_consumer(self):
        try:
            print("DEBUG: Kafka consumer thread loaded. This thread will subscribe to the " + self._sampling_tasks_kafka_topic + " topic on Kafka and will assign the various sampling tasks to the various polling threads")
            kafka_consumer = None
            while kafka_consumer is None:
                try:
                    kafka_consumer = kafka.KafkaConsumer(self._sampling_tasks_kafka_topic, bootstrap_servers=self._kafka_server, client_id=self._kafka_client_id)
                except:
                    print("DEBUG: Waiting on a dedicated thread for the Kafka server to be available... Going to sleep for 10 seconds")
                    time.sleep(10)

            last_task_list_appended = -1
            for sampling_tasks_batch_raw in kafka_consumer:
                sampling_tasks_batch = json.loads(sampling_tasks_batch_raw.value.decode('utf-8'))
                print("DEBUG: Received the following sampling task from Kafka (topic=" + str(sampling_tasks_batch_raw.topic) + ";partition=" + str(sampling_tasks_batch_raw.partition) + ";offset=" + str(sampling_tasks_batch_raw.offset) + ",): " + json.dumps(sampling_tasks_batch))
                for sampling_task in sampling_tasks_batch["batch"]:
                    is_sampling_tasks_gc_running = self._is_sampling_tasks_gc_running

                    if is_sampling_tasks_gc_running:
                        self._sampling_tasks_threads_sync_lock.acquire()

                    try:
                        # If the task exists in the current object's list - reset its TTL
                        # otherwise - add it with the default TTL
                        if sampling_task["URL"] in self._sampling_tasks_index.keys():
                            self._sampling_tasks[self._sampling_tasks_index[sampling_task["URL"]]["list_index"]][sampling_task["URL"]]["TTL"] = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.task_base_ttl"]
                        else:
                            if last_task_list_appended + 1 >= len(self._sampling_tasks):
                                last_task_list_appended = -1

                            self._sampling_tasks[last_task_list_appended + 1][sampling_task["URL"]] = {
                                "TTL": self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.task_base_ttl"],
                                "timestamp": time.time()
                            }

                            last_task_list_appended = last_task_list_appended + 1
                    except Exception as ex:
                        print("WARN: The following unexpected exception has been thrown while processing tasks from Kafka: " + str(ex))
                    finally:
                        try:
                            self._sampling_tasks_threads_sync_lock.release()
                        except:
                            pass

        except Exception as ex:
            print("ERROR: An unexpected exception (" + str(ex) + ") has been thrown while consuming sampling tasks from the Kafka server.")

    def GetQueryTimeRange(self, query_details):
        # if DEBUG:
        #     now = dateutil.parser.parse("2018-06-12T08:00")
        #     yesterday = dateutil.parser.parse("2018-06-11T08:00")
        #     last_month = dateutil.parser.parse("2018-05-11T00:00")
        # else:
        now = datetime.datetime.now()
        yesterday = datetime.date.today() - datetime.timedelta(1)
        last_month = datetime.date.today() - datetime.timedelta(days=30)

        time_range = (now - datetime.timedelta(
            seconds=query_details["time_range"])).isoformat() + " TO " + now.isoformat()

        return {
            "yesterday": yesterday,
            "last_month": last_month,
            "now": now,
            "time_range": time_range
        }

    def resolve_placeholders_in_query(self, raw_query):
        mode = 0
        res_str = ""
        cur_conf_item = ""
        char_idx = 0
        for char in raw_query:
            if char == "{" and len(raw_query) > (char_idx + 1) and raw_query[char_idx + 1] == "#":
                mode = 1
                char_idx = char_idx + 1
            elif char == "#" and mode == 1:
                mode = 2
                char_idx = char_idx + 1
                continue
            elif char == "}" and mode == 2:
                mode = 0
                if cur_conf_item in self._learned_net_info:
                    if isinstance(self._learned_net_info[cur_conf_item], list):
                        res_str = res_str + " ".join(str(x) for x in self._learned_net_info[cur_conf_item])
                    else:
                        res_str = res_str + str(self._learned_net_info[cur_conf_item])
                cur_conf_item = ""
                char_idx = char_idx + 1
            elif mode == 0:
                res_str = res_str + char
                char_idx = char_idx + 1

            if mode == 2:
                cur_conf_item = cur_conf_item + char
                char_idx = char_idx + 1

        return res_str

    def fieldQuery(self, queryname):
        query_details = self.queries[queryname]
        if query_details["type"] != "FIELD_QUERY":
            raise QueryNameNotFoundOrOfWrongType()

        now, time_range, yesterday = self.GetQueryTimeRange(query_details)
        metric_name = query_details["metric_name"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        metric_name = metric_name.replace("{{#query_name}}", queryname)
        query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_base = query_details["query"].replace("{{#query_name}}", queryname)
        base_64_used = query_details["base_64_used"]
        for i in range(1, 10):
            if "{{#arg" + str(i) + "}}" in str(query_base):
                arg = request.query["arg" + str(i)]
                if len(str(arg).strip()) != 0:
                    if base_64_used:
                        query_base = query_base.replace("{{#arg" + str(i) + "}}", base64.b64decode(arg.encode('utf-8')).decode('utf-8'))
                        metric_name = metric_name.replace("{{#arg" + str(i) + "}}", arg)
                    else:
                        query_base = query_base.replace("{{#arg" + str(i) + "}}", arg)
                        metric_name = metric_name.replace("{{#arg" + str(i) + "}}", arg)
            else:
                break


        query_string = "(" + query_base + ") AND @timestamp:[" + time_range + "]"
        query_body = {
                    "query":{
                        "query_string": {
                            "query": self.resolve_placeholders_in_query(query_string)
                        }
                    }
                }

        try:
            res = self.es.search(
                index=query_index,
                body=query_body,
                size=1
            )
            raw_res = res['hits']['hits'][0]['_source'][query_details["field_name"]]
            res = "@@RES: " + raw_res
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        if Utils().is_number(raw_res):
            try:
                metric_object = self.statsd_client.gauge(metric_name, raw_res)
            except:
                pass

        return res

    def get_similarity(self, queryname):
        query_details = self.queries[queryname]
        if query_details["type"] != "SIMILARITY_TEST":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = self.GetQueryTimeRange(query_details)
        yesterday = time_range_args["yesterday"]
        now = time_range_args["now"]
        last_month = time_range_args["last_month"]
        metric_name = query_details["metric_name"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        metric_name = metric_name.replace("{{#query_name}}", queryname)
        query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_list_to_test_similarity_to = query_details["query"].replace("{{#query_name}}", queryname)
        query_list_to_test_similarity_to = query_list_to_test_similarity_to.replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_list_to_test_similarity_to = query_list_to_test_similarity_to.replace("%{last_month}", last_month.strftime("%Y.%m.%d"))
        agg_on_field = query_details["agg_on_field"].replace("{{#query_name}}", queryname)
        base_64_used = query_details["base_64_used"]
        value_to_compare = request.query["arg1"]
        max_list_size = 20
        if "max_list_size" in query_details and str.isdigit(str(query_details["max_list_size"])):
            max_list_size = int(query_details["max_list_size"])
        list_order = "desc"
        if "list_order" in query_details and (query_details["list_order"] == "asc" or query_details["list_order"] == "asc"):
            list_order = query_details["list_order"]

        for i in range(1, 10):
            if "{{#arg" + str(i) + "}}" in str(query_list_to_test_similarity_to):
                arg = request.query["arg" + str(i)]
                if len(str(arg).strip()) != 0:
                    if base_64_used:
                        query_list_to_test_similarity_to = query_list_to_test_similarity_to.replace("{{#arg" + str(i) + "}}", base64.b64decode(arg.encode('utf-8')).decode('utf-8'))
                        metric_name = metric_name.replace("{{#arg" + str(i) + "}}", arg)
                    else:
                        query_list_to_test_similarity_to = query_list_to_test_similarity_to.replace("{{#arg" + str(i) + "}}", arg)
                        metric_name = metric_name.replace("{{#arg" + str(i) + "}}", arg)
            else:
                break

        query_list_to_test_similarity_to_query_body = {
            "size": 0,
            "query": {
                "query_string": {
                    "query": self.resolve_placeholders_in_query(query_list_to_test_similarity_to)
                }
            },
            "aggs": {
                "field_values0": {
                    "terms": {
                        "field": agg_on_field,
                        "size": max_list_size,
                        "order": {
                            "_count": list_order
                        }
                    }
                }
            }
        }


        try:
            print("Running query for the list of items to compare to: `" + json.dumps(query_list_to_test_similarity_to_query_body) + "'")
            raw_res = ""
            res = self.es.search(
                index=query_index,
                body=query_list_to_test_similarity_to_query_body
            )

            highest_match_rate = 0
            highest_match_rate_value = ""
            value_compared = ""
            max_match_rate_algorithm = None
            if 'aggregations' in res and 'field_values0' in res['aggregations'] and 'buckets' in res['aggregations']['field_values0']:
                for bucket in res['aggregations']['field_values0']['buckets']:
                    cur_value_to_compare_to = bucket["key"]
                    edit_distance_match_rate = Utils().LevenshteinDistance(value_to_compare, cur_value_to_compare_to)
                    homoglyphs_ratio = Utils().HomoglyphsRatio(value_to_compare, cur_value_to_compare_to)
                    visual_similarity_rate = []

                    for p_hashing_alg in Utils.perceptive_hashing_algs:
                        visual_similarity_rate.append(Utils().VisualSimilarityRate(value_to_compare=value_to_compare, cur_value_to_compare_to=cur_value_to_compare_to, algorithm=p_hashing_alg))

                    # Create average match rate between all the perceptive hashing algorithms
                    visual_match_rate = sum(visual_similarity_rate) / len(visual_similarity_rate)

                    # Use the highest match rate detected (either visual similarity or textual similarity or homoglyphs ratio)
                    max_match_rate = 0
                    max_match_rate_algorithm = None

                    if visual_match_rate > edit_distance_match_rate:
                        max_match_rate = visual_match_rate
                        max_match_rate_algorithm = "visual_match_rate"
                    else:
                        if homoglyphs_ratio > edit_distance_match_rate:
                            max_match_rate = homoglyphs_ratio
                            max_match_rate_algorithm = "homoglyphs_ratio"
                        else:
                            max_match_rate = edit_distance_match_rate
                            max_match_rate_algorithm = "edit_distance"
                    match_rate = max_match_rate

                    if match_rate > highest_match_rate:
                        highest_match_rate = match_rate
                        highest_match_rate_value = cur_value_to_compare_to
                        value_compared = value_to_compare

                res = "@@RES: " + str(highest_match_rate) + "," + highest_match_rate_value + "(" + max_match_rate_algorithm + ")," + value_compared
            else:
                res = "@@ERROR: Elasticsearch responded with an unexpected response: (" + json.dumps(res) + ")"
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        if Utils().is_number(res):
            try:
                metric_object = self.statsd_client.gauge(metric_name, res)
            except:
                pass

        return res

    def get_length(self, queryname):
        return str(len(queryname))

    def get_length_b64(self, queryname):
        return str(len(base64.b64decode(queryname.encode("utf-8")).decode("utf-8")))

    def queryCount(self, queryname):
        query_details = self.queries[queryname]
        if query_details["type"] != "QUERY_COUNT":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = self.GetQueryTimeRange(query_details)
        yesterday = time_range_args["yesterday"]
        now = time_range_args["now"]
        time_range = time_range_args["time_range"]
        metric_name = query_details["metric_name"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        metric_name = metric_name.replace("{{#query_name}}", queryname)
        query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_base = query_details["query"].replace("{{#query_name}}", queryname)
        base_64_used = query_details["base_64_used"]
        count_unique_values_in = None
        if "count_unique_values_in" in query_details:
            count_unique_values_in = query_details["count_unique_values_in"]

        for i in range(1, 10):
            if "{{#arg" + str(i) + "}}" in str(query_base):
                arg = request.query["arg" + str(i)]
                if len(str(arg).strip()) != 0:
                    if base_64_used:
                        query_base = query_base.replace("{{#arg" + str(i) + "}}", base64.b64decode(arg.encode('utf-8')).decode('utf-8'))
                        metric_name = metric_name.replace("{{#arg" + str(i) + "}}", arg)
                    else:
                        query_base = query_base.replace("{{#arg" + str(i) + "}}", arg)
                        metric_name = metric_name.replace("{{#arg" + str(i) + "}}", arg)
            else:
                break

        query_string = "(" + query_base + ") AND @timestamp:[" + time_range + "]"
        query_body = {
                    "query":{
                        "query_string": {
                            "query": self.resolve_placeholders_in_query(query_string)
                        }
                    }
            }
        if count_unique_values_in is not None:
            query_body = {
                "size": 0,
                "aggs": {
                    "unique_count": {
                        "cardinality": {
                            "field": count_unique_values_in
                        }
                    }
                },
                "query": {
                    "query_string": {
                        "query": self.resolve_placeholders_in_query(query_string)
                    }
                }

            }

        try:
            print("Running query: `" + json.dumps(query_body) + "'")
            raw_res = ""
            if count_unique_values_in != None:
                res = self.es.search(
                    index=query_index,
                    body=query_body
                )
                raw_res = res['aggregations']["unique_count"]["value"]
            else:
                res = self.es.count(
                    index=query_index,
                    body=query_body
                )
                raw_res = res['count']

            res = "@@RES: " + str(raw_res)
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        if Utils().is_number(raw_res):
            try:
                metric_object = self.statsd_client.gauge(metric_name, raw_res)
            except:
                pass

        return res

    def list_hash(self, queryname):
        query_details = self.queries[queryname]
        if query_details["type"] != "LIST_HASH":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = self.GetQueryTimeRange(query_details)
        yesterday = time_range_args["yesterday"]
        now = time_range_args["now"]
        time_range = time_range_args["time_range"]
        query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_base = query_details["query"].replace("{{#query_name}}", queryname)
        agg_on_field = query_details["agg_on_field"].replace("{{#query_name}}", queryname)
        max_list_size = 20
        if "max_list_size" in query_details and str.isdigit(str(query_details["max_list_size"])):
            max_list_size = int(query_details["max_list_size"])
        list_order = "desc"

        for i in range(1, 10):
            if "{{#arg" + str(i) + "}}" in str(query_base):
                arg = request.query["arg" + str(i)]
                if len(str(arg).strip()) != 0:
                    query_base = query_base.replace("{{#arg" + str(i) + "}}", arg)
            else:
                break


        query_string = "(" + query_base + ") AND @timestamp:[" + time_range + "]"
        agg_on_field_list = str.split(agg_on_field, ",")

        query_body = {
                    "size": 0,
                    "query":{
                        "query_string": {
                            "query": self.resolve_placeholders_in_query(query_string)
                        }
                    },
                    "aggs":{
                    }
                }

        agg = query_body["aggs"]
        for i in range(0, len(agg_on_field_list)):
            agg["field_values" + str(i)] = {
                "terms": {
                    "field": agg_on_field_list[i],
                    "size": max_list_size,
                    "order": {
                        "_count": list_order
                    }
                }
            }
            agg["field_values" + str(i)]["aggs"] = {}
            agg = agg["field_values" + str(i)]["aggs"]

        try:
            res = self.es.search(
                index=query_index,
                body=query_body
            )

            res_lld = Utils().GenerateLldFromElasticAggrRes(res=res, macro_list=[], use_base64=False, add_doc_count=False, re_sort_by_value=True)
            res_str = str(hashlib.sha1(json.dumps(res_lld).encode("utf-8")).hexdigest())

            res = "@@RES: " + res_str
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        return res

    def discover(self, queryname):
        query_details = self.queries[queryname]
        if query_details["type"] != "LLD":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = self.GetQueryTimeRange(query_details)
        yesterday = time_range_args["yesterday"]
        now = time_range_args["now"]
        time_range = time_range_args["time_range"]
        query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_base = query_details["query"].replace("{{#query_name}}", queryname)
        agg_on_field = query_details["agg_on_field"].replace("{{#query_name}}", queryname)
        zabbix_macro = query_details["zabbix_macro"].replace("{{#query_name}}", queryname)
        queries_to_run = query_details["queries_to_run"]
        if "encode_json_as_b64" in query_details and (str(query_details["encode_json_as_b64"]).lower() == "true" or str(query_details["encode_json_as_b64"]).lower() == "false"):
            encode_json_as_b64 = bool(query_details["encode_json_as_b64"])
        else:
            encode_json_as_b64 = False
        max_list_size = 20
        if "max_list_size" in query_details and str.isdigit(str(query_details["max_list_size"])):
            max_list_size = int(query_details["max_list_size"])
        list_order = "desc"
        if "list_order" in query_details and (query_details["list_order"] == "asc" or query_details["list_order"] == "asc"):
            list_order = query_details["list_order"]

        use_base64 = False
        if "use_base64" in query_details:
            use_base64 = query_details["use_base64"]
        for i in range(1, 10):
            if "{{#arg" + str(i) + "}}" in str(query_base):
                arg = request.query["arg" + str(i)]
                if len(str(arg).strip()) != 0:
                    query_base = query_base.replace("{{#arg" + str(i) + "}}", arg)
            else:
                break

        query_string = "(" + query_base + ") AND @timestamp:[" + time_range + "]"
        agg_on_field_list = str.split(agg_on_field, ",")

        query_body = {
                    "size": 0,
                    "query":{
                        "query_string": {
                            "query": self.resolve_placeholders_in_query(query_string)
                        }
                    },
                    "aggs":{
                    }
                }

        agg = query_body["aggs"]
        for i in range(0, len(agg_on_field_list)):
            agg["field_values" + str(i)] = {
                "terms": {
                    "field": agg_on_field_list[i],
                    "size": max_list_size,
                    "order": {
                        "_count": list_order
                    }
                }
            }
            agg["field_values" + str(i)]["aggs"] = {}
            agg = agg["field_values" + str(i)]["aggs"]

        try:
            syslog.syslog("metrics_collector: INFO: Executing the following query: hosts: " + json.dumps(self.es_hosts) + ", timeout: " + str(self.timeout_to_elastic) + ", index=" + query_index + ", query_body=" + json.dumps(query_body))
            res = self.es.search(
                index=query_index,
                body=query_body,
                timeout=str(self.timeout_to_elastic * 2) + "s"
            )

            macros_list = str.split(zabbix_macro, ",")
            relevant_service_urls = self.get_config_by_regex(r'smart\-onion\.config\.architecture\.internal_services\.backend\.[a-z0-9\-_]+\.base_urls\.[a-z0-9\-_]+')
            if self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.tinyfy_urls"]:
                tiny_url_service_details = {"protocol": self._tiny_url_protocol, "server": self._tiny_url_server, "port": self._tiny_url_port}
            else:
                tiny_url_service_details = None
            res_lld = Utils().GenerateLldFromElasticAggrRes(res=res, macro_list=macros_list, use_base64=use_base64, queries_to_run=queries_to_run, services_urls=relevant_service_urls, tiny_url_service_details=tiny_url_service_details, config_copy=self._config_copy)

            res_str = str(json.dumps(res_lld))
            if encode_json_as_b64:
                res_str = str(base64.b64encode(str(res_str).encode('utf-8')).decode('utf-8'))
            res = "@@RES: " + res_str
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        return res

    def get_config_by_regex(self, regex):
        res = {}
        for key in self._config_copy.keys():
            if re.match(regex, key):
                res[key] = self._config_copy[key]

        return res

    def dump_queries(self):
        return "@@RES: " + json.dumps(self.queries)

    def test_lld_creation(self):
        simulation_res = {
  "took" : 17,
  "timed_out" : False,
  "_shards" : {
    "total" : 32,
    "successful" : 32,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 941294,
    "max_score" : 0.0,
    "hits" : [ ]
  },
  "aggregations" : {
    "field_values0" : {
      "doc_count_error_upper_bound" : -1,
      "sum_other_doc_count" : 131298,
      "buckets" : [
        {
          "key" : "world",
          "doc_count" : 1
        },
        {
          "key" : "asia",
          "doc_count" : 2
        },
        {
          "key" : "cl",
          "doc_count" : 2
        },
        {
          "key" : "st",
          "doc_count" : 2
        },
        {
          "key" : "com.gt",
          "doc_count" : 3
        },
        {
          "key" : "hu",
          "doc_count" : 3
        },
        {
          "key" : "appspot.com",
          "doc_count" : 4
        },
        {
          "key" : "at",
          "doc_count" : 4
        },
        {
          "key" : "blogspot.co.il",
          "doc_count" : 4
        },
        {
          "key" : "cm",
          "doc_count" : 4
        }
      ]
    }
  }
}
        simulation_zabbix_macro = "DNS_TLD_QRY"
        use_base64 = False
        queries_to_run = [
          "highest_similarity_to_any_top_accessed_tld",
          "number_of_clients_per_dns_tld",
          "number_of_queries_per_dns_tld"
        ]
        encode_json_as_b64 = False

        macros_list = str.split(simulation_zabbix_macro, ",")
        relevant_service_urls = self.get_config_by_regex(r'smart\-onion\.config\.architecture\.internal_services\.backend\.[a-z0-9\-_]+\.base_urls\.[a-z0-9\-_]+')
        tiny_url_service_details = {"protocol": self._tiny_url_protocol, "server": self._tiny_url_server, "port": self._tiny_url_port}
        res_lld = Utils().GenerateLldFromElasticAggrRes(res=simulation_res, macro_list=macros_list, queries_to_run=queries_to_run, use_base64=use_base64, services_urls=relevant_service_urls, tiny_url_service_details=tiny_url_service_details)

        res_str = str(json.dumps(res_lld))
        if encode_json_as_b64:
            res_str = str(base64.b64encode(str(res_str).encode('utf-8')).decode('utf-8'))
        res = "@@RES: " + res_str
        return res

    def test_similarity(self, algo, s1, s2):
        utils = Utils()
        res = "ERROR_UNKNOWN_ALGO"
        try:
            if algo == "levenstein":
                res = str(utils.LevenshteinDistance(s1, s2))
            if algo == "visual-phash":
                res = str(utils.VisualSimilarityRate(s1, s2, algorithm="phash"))
            if algo == "visual-ahash":
                res = str(utils.VisualSimilarityRate(s1, s2, algorithm="ahash"))
            if algo == "visual-dhash":
                res = str(utils.VisualSimilarityRate(s1, s2, algorithm="dhash"))
            if algo == "visual-whash":
                res = str(utils.VisualSimilarityRate(s1, s2, algorithm="whash"))

            return res
        except Exception as ex:
            return "@@EX: " + str(ex)

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
learned_net_info = None
while queries_conf is None:
    try:
        # Contact configurator to fetch all of our config and configure listen-ip and port
        configurator_base_url = str(configurator_proto).strip() + "://" + str(configurator_host).strip() + ":" + str(configurator_port).strip() + "/smart-onion/configurator/"
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.architecture.internal_services.backend.*"
        configurator_response = urllib_req.urlopen(configurator_final_url).read().decode('utf-8')
        config_copy = json.loads(configurator_response)
        listen_ip = config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.listening-host"]
        listen_port = config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.listening-port"]
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.queries"
        configurator_response = urllib_req.urlopen(configurator_final_url).read().decode('utf-8')
        queries_conf = json.loads(configurator_response)
        configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.dynamic.learned.*"
        configurator_response = urllib_req.urlopen(configurator_final_url).read().decode('utf-8')
        learned_net_info = json.loads(configurator_response)
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

            if arg_name == "--elasticsearch_server":
                if not re.match("[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+", arg_value):
                    print("ERROR: The --listen-ip must be a valid IPv4 address.  Using default of " + str(elasticsearch_server) + " (hardcoded)")
                else:
                    elasticsearch_server = arg_value

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

if not os.path.isfile(config_file):
    if config_file_specified_on_cmd:
        print("ERROR: The file specified as the config file does not exist.")
        quit(9)
    else:
        config_file = os.path.join(script_path, config_file_name)

if not os.path.isfile(config_file):
    print ("ERROR: Failed to find the config file in both " + config_file_default_path + " and " + script_path + " (script location)")
    quit(9)


sys.argv = [sys.argv[0]]
MetricsCollector(
    listen_ip=listen_ip,
    listen_port=listen_port,
    learned_net_info=learned_net_info,
    queries_config=queries_conf,
    config_copy=config_copy,
    tiny_url_protocol=config_copy["smart-onion.config.architecture.internal_services.backend.tiny_url.protocol"],
    tiny_url_server=config_copy["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-host"],
    tiny_url_port=int(config_copy["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-port"]),
    elasticsearch_server=elasticsearch_server,
    timeout_to_elastic=int(config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.max_timeout_to_elastic"])).run()


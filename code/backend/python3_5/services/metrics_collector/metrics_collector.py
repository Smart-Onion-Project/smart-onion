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
from urllib import parse as urllib_parse
import datetime
import json
import base64
from elasticsearch import Elasticsearch
import re
import os
import statsd
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
import postgresql
from operator import itemgetter
from multiprocessing import Value


DEBUG = False
SINGLE_THREADED = False
elasticsearch_server = "127.0.0.1"
metrics_prefix = "smart-onion"
config_copy = {}

class QueryNameNotFoundOrOfWrongType(Exception):
    pass

class Utils:
    lst = ""
    perceptive_hashing_algs = ["phash", "ahash", "dhash", "whash"]

    def GenerateLldFromElasticAggrRes(cls, res, macro_list, use_base64, agg_on_field=None, services_urls=None, tiny_url_service_details=None, queries_to_run=None, add_doc_count=True, re_sort_by_value=False, config_copy=None):
        lld = {
            "data": []
        }

        if queries_to_run is None:
            return lld

        urls = []
        for query_id in queries_to_run:
            # Create the URL that need to be accessed by the query_obj type and the number of arguments returned
            cur_query_obj = queries_conf[query_id]
            query_type = str(cur_query_obj["type"])
            agg_on_field = str(agg_on_field).split(",")

            url_base = ""
            if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol" in config_copy:
                url_base = url_base + config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol"] + "://"
            if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host" in config_copy:
                url_base = url_base + config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host"] + ":"
            if config_copy is not None and "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port" in config_copy:
                url_base = url_base + str(config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port"])
            url_base = url_base + services_urls["smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls." + query_type.lower()] + query_id

            if not "aggregations" in res or not "field_values0" in res["aggregations"] or not "buckets" in res["aggregations"]["field_values0"]:
                raise Exception("ERROR: GenerateLldFromElasticAggrRes: The input does not look like a result from Elasticsearch aggregation query.")

            agg0_items = res["aggregations"]["field_values0"]["buckets"]
            for agg0_item_idx in range(len(agg0_items)):
                agg0_item = agg0_items[agg0_item_idx]
                latest_values = []
                arg_idx = 1
                cur_url = url_base + "?"
                if use_base64:
                    cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(base64.b64encode(str(agg0_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                else:
                    cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(str(agg0_item["key"])) + "&"
                latest_values.append(agg0_item["key"])
                if "field_values1" in agg0_item and "buckets" in agg0_item["field_values1"]:
                    agg1_items = agg0_item["field_values1"]["buckets"]
                    for agg1_item_idx in range(len(agg1_items)):
                        agg1_item = agg1_items[agg1_item_idx]
                        arg_idx = 2
                        if use_base64:
                            cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(base64.b64encode(str(agg0_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                            cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(base64.b64encode(str(agg1_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                        else:
                            cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(str(agg0_item["key"])) + "&"
                            cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(str(agg1_item["key"])) + "&"
                        latest_values.append(agg1_item["key"])
                        if "field_values2" in agg1_item and "buckets" in agg1_item["field_values2"]:
                            agg2_items = agg1_item["field_values2"]["buckets"]
                            for agg2_item_idx in range(len(agg2_items)):
                                agg2_item = agg2_items[agg2_item_idx]
                                arg_idx = 3
                                if use_base64:
                                    cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(base64.b64encode(str(agg0_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                    cur_url = cur_url + "arg2=" + urllib_parse.quote(base64.b64encode(str(agg1_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                    cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(base64.b64encode(str(agg2_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                else:
                                    cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(str(agg0_item["key"])) + "&"
                                    cur_url = cur_url + "arg2=" + urllib_parse.quote(str(agg1_item["key"])) + "&"
                                    cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(str(agg2_item["key"])) + "&"
                                latest_values.append(agg2_item["key"])
                                if "field_values3" in agg2_item and "buckets" in agg2_item["field_values3"]:
                                    agg3_items = agg2_item["field_values3"]["buckets"]
                                    for agg3_item_idx in range(len(agg3_items)):
                                        agg3_item = agg3_items[agg3_item_idx]
                                        arg_idx = 4
                                        if use_base64:
                                            cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(base64.b64encode(str(agg0_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                            cur_url = cur_url + "arg2=" + urllib_parse.quote(base64.b64encode(str(agg1_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                            cur_url = cur_url + "arg3=" + urllib_parse.quote(base64.b64encode(str(agg2_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                            cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(base64.b64encode(str(agg3_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                        else:
                                            cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(str(agg0_item["key"])) + "&"
                                            cur_url = cur_url + "arg2=" + urllib_parse.quote(str(agg1_item["key"])) + "&"
                                            cur_url = cur_url + "arg3=" + urllib_parse.quote(str(agg2_item["key"])) + "&"
                                            cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(str(agg3_item["key"])) + "&"
                                        latest_values.append(agg3_item["key"])
                                        if "field_values4" in agg3_item and "buckets" in agg3_item["field_values4"]:
                                            agg4_items = agg3_item["field_values4"]["buckets"]
                                            for agg4_item_idx in range(len(agg4_items)):
                                                agg4_item = agg4_items[agg4_item_idx]
                                                arg_idx = 5
                                                if use_base64:
                                                    cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(base64.b64encode(str(agg0_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                    cur_url = cur_url + "arg2=" + urllib_parse.quote(base64.b64encode(str(agg1_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                    cur_url = cur_url + "arg3=" + urllib_parse.quote(base64.b64encode(str(agg2_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                    cur_url = cur_url + "arg4=" + urllib_parse.quote(base64.b64encode(str(agg3_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                    cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(base64.b64encode(str(agg4_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                else:
                                                    cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(str(agg0_item["key"])) + "&"
                                                    cur_url = cur_url + "arg2=" + urllib_parse.quote(str(agg1_item["key"])) + "&"
                                                    cur_url = cur_url + "arg3=" + urllib_parse.quote(str(agg2_item["key"])) + "&"
                                                    cur_url = cur_url + "arg4=" + urllib_parse.quote(str(agg3_item["key"])) + "&"
                                                    cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(str(agg4_item["key"])) + "&"
                                                latest_values.append(agg4_item["key"])
                                                if "field_values5" in agg4_item and "buckets" in agg4_item["field_values5"]:
                                                    agg5_items = agg4_item["field_values5"]["buckets"]
                                                    for agg5_item_idx in range(len(agg5_items)):
                                                        agg5_item = agg5_items[agg5_item_idx]
                                                        arg_idx = 6
                                                        if use_base64:
                                                            cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(base64.b64encode(str(agg0_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                            cur_url = cur_url + "arg2=" + urllib_parse.quote(base64.b64encode(str(agg1_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                            cur_url = cur_url + "arg3=" + urllib_parse.quote(base64.b64encode(str(agg2_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                            cur_url = cur_url + "arg4=" + urllib_parse.quote(base64.b64encode(str(agg3_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                            cur_url = cur_url + "arg5=" + urllib_parse.quote(base64.b64encode(str(agg4_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                            cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(base64.b64encode(str(agg5_item["key"]).encode('utf-8')).decode('utf-8')) + "&"
                                                        else:
                                                            cur_url = url_base + "?" + "arg1=" + urllib_parse.quote(str(agg0_item["key"])) + "&"
                                                            cur_url = cur_url + "arg2=" + urllib_parse.quote(str(agg1_item["key"])) + "&"
                                                            cur_url = cur_url + "arg3=" + urllib_parse.quote(str(agg2_item["key"])) + "&"
                                                            cur_url = cur_url + "arg4=" + urllib_parse.quote(str(agg3_item["key"])) + "&"
                                                            cur_url = cur_url + "arg5=" + urllib_parse.quote(str(agg4_item["key"])) + "&"
                                                            cur_url = cur_url + "arg" + str(arg_idx) + "=" + urllib_parse.quote(str(agg5_item["key"])) + "&"

                                                        cur_url = cur_url.strip('&')
                                                        urls.append({"URL": cur_url, "query_id": query_id, "query_type": query_type, "doc_count": agg0_item["doc_count"], "macro_values": latest_values, "agg_on_field": agg_on_field})
                                                        latest_values.append(agg5_item["key"])
                                                else:
                                                    cur_url = cur_url.strip('&')
                                                    urls.append({"URL": cur_url, "query_id": query_id, "query_type": query_type, "doc_count": agg4_item["doc_count"], "macro_values": latest_values, "agg_on_field": agg_on_field})
                                        else:
                                            cur_url = cur_url.strip('&')
                                            urls.append({"URL": cur_url, "query_id": query_id, "query_type": query_type, "doc_count": agg3_item["doc_count"], "macro_values": latest_values, "agg_on_field": agg_on_field})
                                else:
                                    cur_url = cur_url.strip('&')
                                    urls.append({"URL": cur_url, "query_id": query_id, "query_type": query_type, "doc_count": agg2_item["doc_count"], "macro_values": latest_values, "agg_on_field": agg_on_field})
                        else:
                           cur_url = cur_url.strip('&')
                           urls.append({"URL": cur_url, "query_id": query_id, "query_type": query_type, "doc_count": agg1_item["doc_count"], "macro_values": latest_values, "agg_on_field": agg_on_field})
                else:
                    cur_url = cur_url.strip('&')
                    urls.append({"URL": cur_url, "query_id": query_id, "query_type": query_type, "doc_count": agg0_item["doc_count"], "macro_values": latest_values, "agg_on_field": agg_on_field})

        for url in urls:
            cur_url = url["URL"]
            query_id = url["query_id"]
            query_type = url["query_type"]
            doc_counts = url["doc_count"]
            latest_values = url["macro_values"]
            agg_on_field = url["agg_on_field"]

            # Translate the url to a tiny url
            tiny_url_res = cur_url
            if tiny_url_service_details is not None:
                tiny_service_url = tiny_url_service_details["protocol"] + "://" + tiny_url_service_details["server"] + ":" + str(tiny_url_service_details["port"]) + services_urls["smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.url2tiny"] + "?url=" + urllib.parse.quote(base64.b64encode(cur_url.encode('utf-8')).decode('utf-8'), safe='')
                tiny_url_res = urllib_req.urlopen(tiny_service_url).read().decode('utf-8')
            else:
                tiny_url_res = cur_url

            if add_doc_count:
                lld_obj = {
                    "{#NAME}": cur_url,
                    "{#URL}": tiny_url_res,
                    "{#QUERY_NAME}": query_id,
                    "{#QUERY_TYPE}": query_type,
                    "{#_DOC_COUNT}": doc_counts
                }
            else:
                lld_obj = {
                    "{#NAME}": cur_url,
                    "{#URL}": tiny_url_res,
                    "{#QUERY_NAME}": query_id,
                    "{#QUERY_TYPE}": query_type,
                }

            if agg_on_field is not None:
                macro_idx = 0
                for field in agg_on_field:
                    if latest_values is not None and len(latest_values) > macro_idx and len(macro_list) > macro_idx:
                        lld_obj["{#" + macro_list[macro_idx] + "}"] = latest_values[macro_idx]
                    elif latest_values is not None:
                        lld_obj["{#ITEM_" + str(macro_idx + 1) + "}"] = latest_values[macro_idx]
                    macro_idx = macro_idx + 1

            if not any(d['{#URL}'] == lld_obj["{#URL}"] for d in lld["data"]):
                lld["data"].append(lld_obj)

        if re_sort_by_value:
            key_field = "{#URL}"
            if len(macro_list) > 0:
                key_field = "{#" + macro_list[0] + "}"
            elif "{#ITEM_1}" in lld["data"][0]:
                key_field = "{#ITEM_1}"
            lld["data"] = sorted(lld["data"], key=itemgetter(key_field))

        return lld

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

        try:
            font = ImageFont.load_default()
        except OSError as ex:
            raise OSError("Failed to load the default font due to the following exception: (" + str(ex) + " (Type=" + type(ex).__name__ + ";ErrNo:" + str(ex.errno) + ";strerr=" + str(ex.strerror) + ")")

        if fontfullpath != None:
            if fontfullpath.lower().endswith(".ttf"):
                try:
                    ImageFont.truetype(fontfullpath, fontsize)
                except OSError as ex:
                    raise OSError("Failed to load the truetype font '" + str(fontfullpath) + "' at size " + str(fontsize) + " due to the following exception: (" + str(ex) + " (Type=" + type(ex).__name__ + ";ErrNo:" + str(ex.errno) + ";strerr=" + str(ex.strerror) + ")")
            else:
                try:
                    ImageFont.load(fontfullpath)
                except OSError as ex:
                    raise OSError("Failed to load the non-truetype font '" + str(fontfullpath) + "' at size " + str(fontsize) + " due to the following exception: (" + str(ex) + " (Type=" + type(ex).__name__ + ";ErrNo:" + str(ex.errno) + ";strerr=" + str(ex.strerror) + ")")

        text = text.replace('\n', NEWLINE_REPLACEMENT_STRING)

        lines = []
        line = u""

        for word in text.split():
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

    def VisualSimilarityRate(self, cur_value_to_compare_to, value_to_compare, fonts_path, algorithm="phash"):
        fonts = [
            os.path.join(fonts_path, "Arial_0.ttf"),
            os.path.join(fonts_path, "tahoma.ttf"),
            os.path.join(fonts_path, "TIMES_0.TTF"),
            os.path.join(fonts_path, "Courier New.ttf")
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
                homoglyphs_found = homoglyphs_found + 1

        if len(cur_value_to_compare_to) == 0:
            return 0

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
        self._metric_requests_processed_successfully = Value('i', 0)
        self._metric_requests_received = Value('i', 0)
        self._similarity_test_requests_processed_successfully = Value('i', 0)
        self._similarity_test_requests_received = Value('i', 0)
        self._field_query_requests_processed_successfully = Value('i', 0)
        self._field_query_requests_received = Value('i', 0)
        self._discovery_requests_received = Value('i', 0)
        self._discovery_requests_processed_successfully = Value('i', 0)
        self._logging_format = self._config_copy["smart-onion.config.common.logging_format"]
        self._metric_item_element_max_size = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_max_length"]
        self._tokenizer_db_type = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbtype"]
        self._tokenizer_db_port = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbport"]
        self._tokenizer_db_host = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbhost"]
        self._tokenizer_db_name = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbname"]
        self._tokenizer_db_user = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbuser"]
        self._tokenizer_db_password = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbpassword"]
        self._fonts_for_similarity_tests_path = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.similarity_test_fonts_path"]

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
        self._app.route('/ping', method="GET", callback=self._ping)
        self._app.route('/lld_test', method="GET", callback=self.simulate_discovery)

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
                "metric_requests_received": self._metric_requests_received.value,
                "metric_requests_processed_successfully": self._metric_requests_processed_successfully.value,
                "similarity_test_requests_received": self._similarity_test_requests_received.value,
                "similarity_test_requests_processed_successfully": self._similarity_test_requests_processed_successfully.value,
                "field_query_requests_received": self._field_query_requests_received.value,
                "field_query_requests_processed_successfully": self._field_query_requests_processed_successfully.value,
                "discovery_requests_received": self._discovery_requests_received.value,
                "discovery_requests_processed_successfully": self._discovery_requests_processed_successfully.value
            }
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

        if SINGLE_THREADED:
            self._app.run(host=self._host, port=self._port)
        else:
            self._app.run(host=self._host, port=self._port, server="gunicorn", workers=32, timeout=120)

    def _sampling_tasks_garbage_collector(self):
        while True:
            if DEBUG:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "_sampling_tasks_garbage_collector", "DEBUG", str(None), str(None), str(None), str(None), "Going to sleep for " + str(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_tasks_gc_interval"]) + " seconds..."))
            time.sleep(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_tasks_gc_interval"])
            self._is_sampling_tasks_gc_running = True

            try:
                tombstoned_urls = []

                for sampling_tasks_list in self._sampling_tasks:
                    for url in sampling_tasks_list.keys():
                        if sampling_tasks_list[url]["TTL"] == 0:
                            tombstoned_urls.append(url)

                with self._sampling_tasks_threads_sync_lock:
                    for url in tombstoned_urls:
                        for sampling_tasks_list in self._sampling_tasks:
                            if url in sampling_tasks_list.keys():
                                del sampling_tasks_list[url]
            except Exception as ex:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "_sampling_tasks_garbage_collector", "WARN", str(None), str(None), str(ex), type(ex).__name__, "An unexpected exception has been thrown during the garbage collection of the sampling tasks lists items"))

            self._is_sampling_tasks_gc_running = False

    def sampling_tasks_poller(self, tasks_list):
        thread_id = "sampling_tasks_poller_" + str(uuid.uuid4())
        last_ran = 0

        sleep_time = random.randint(0, 10)
        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_poller(thread_id:" + str(thread_id) + ")", "INFO", str(None), str(None), str(None), str(None), "Going to sleep for " + str(sleep_time) + " seconds (randomly chosen sleeping time)..."))

        time.sleep(sleep_time)

        while True:
            with self._sampling_tasks_threads_sync_lock:
                try:
                    if last_ran > 0 and int(time.time() - last_ran) > int(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_interval_ms"] * 1.5 / 1000):
                        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_poller(thread_id:" + str(thread_id) + ")", "WARN", str(None), str(None), str(None), str(None), "Samples are lagging in " + str(time.time() - last_ran - int(self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_interval_ms"] / 1000)) + " seconds. Either add more threads per CPU, add more CPUs, switch to a faster CPU, improve Elasticsearch's performance or add more instances of this service on other servers."))

                    last_ran = time.time()

                    # Get a task from the list and complete it.
                    tmp_list = tasks_list.copy()
                    for task_url in tasks_list.keys():
                        if tasks_list[task_url]["TTL"] > 0:
                            try:
                                if DEBUG:
                                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_poller(thread_id:" + str(thread_id) + ")", "DEBUG", str(None), str(None), str(None), str(None), "Calling '" + task_url + "'..."))
                                urllib_req.urlopen(task_url)
                                tasks_list[task_url]["TTL"] = tasks_list[task_url]["TTL"] - 1
                            except Exception as ex:
                                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_poller", "WARN", str(None), str(None), str(ex), type(ex).__name__, "Failed to query the URL '" + str(task_url) + "' due to an exception"))

                except Exception as ex:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_poller(thread_id:" + thread_id + ")", "WARN", str(None), str(None), str(ex), type(ex).__name__, "An unexpected exception has been thrown while handling sampling tasks"))

            sleep_time = self._config_copy["smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_interval_ms"] / 1000.0
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_poller(thread_id:" + str(thread_id) + ")", "INFO", str(None), str(None), str(None), str(None), "Going to sleep for " + str(sleep_time) + " seconds..."))
            time.sleep(sleep_time)

    def sampling_tasks_kafka_consumer(self):
        try:
            if DEBUG:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_kafka_consumer", "DEBUG", str(None), str(None), str(None), str(None), "Kafka consumer thread loaded. This thread will subscribe to the " + str(self._sampling_tasks_kafka_topic) + " topic on Kafka and will assign the various sampling tasks to the various polling threads"))

            kafka_consumer = None
            while kafka_consumer is None:
                try:
                    kafka_consumer = kafka.KafkaConsumer(self._sampling_tasks_kafka_topic, bootstrap_servers=self._kafka_server, client_id=self._kafka_client_id)
                except Exception as ex:
                    if DEBUG:
                        syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_kafka_consumer", "WARN", str(None), str(None), str(ex), type(ex).__name__, "Waiting on a dedicated thread for the Kafka server to be available... Going to sleep for 10 seconds"))

                    time.sleep(10)

            last_task_list_appended = -1
            for sampling_tasks_batch_raw in kafka_consumer:
                sampling_tasks_batch = json.loads(sampling_tasks_batch_raw.value.decode('utf-8'))
                if DEBUG:
                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_kafka_consumer", "DEBUG", str(None), str(None), str(None), str(None), "Received the following sampling task from Kafka (topic=" + str(sampling_tasks_batch_raw.topic) + ";partition=" + str(sampling_tasks_batch_raw.partition) + ";offset=" + str(sampling_tasks_batch_raw.offset) + ",): " + json.dumps(sampling_tasks_batch) + "."))

                for sampling_task in sampling_tasks_batch["batch"]:
                    is_sampling_tasks_gc_running = self._is_sampling_tasks_gc_running

                    with self._sampling_tasks_threads_sync_lock:
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
                            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_kafka_consumer", "WARN", str(None), str(None), str(ex), type(ex).__name__, "An unexpected exception has been thrown while processing tasks from Kafka"))

        except Exception as ex:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "sampling_tasks_kafka_consumer", "ERROR", str(None), str(None), str(ex), type(ex).__name__, "An unexpected exception has been thrown while consuming sampling tasks from the Kafka server"))

    def GetQueryTimeRange(self, query_details):
        # if DEBUG:
        #     now = dateutil.parser.parse("2018-06-12T08:00")
        #     yesterday = dateutil.parser.parse("2018-06-11T08:00")
        #     last_month = dateutil.parser.parse("2018-05-11T00:00")
        # else:
        now = datetime.datetime.utcnow()
        yesterday = now.date() - datetime.timedelta(1)
        last_month = now.date() - datetime.timedelta(days=30)

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

    def resolve_query_args_in_metric_name(self, base_64_used, metric_name, query_base):
        with postgresql.open('pq://' + self._tokenizer_db_user + ':' + self._tokenizer_db_password + '@' + self._tokenizer_db_host + ':' + str(self._tokenizer_db_port) + '/' + self._tokenizer_db_name) as tokenizer_db_conn:
            for i in range(1, 10):
                if "{{#arg" + str(i) + "}}" in str(query_base):
                    arg = request.query["arg" + str(i)]
                    metric_arg = request.query["arg" + str(i)]

                    if len(arg) > self._metric_item_element_max_size:
                        # Tokenizing the element in the DB:
                        element_token_query = tokenizer_db_conn.prepare("select metric_param_token from tokenizer where metric_param_key=convert_from(decode('" + base64.b64encode(str(arg).encode('utf-8')).decode('utf-8') + "', 'base64'), 'UTF8')")
                        element_token = element_token_query()
                        if len(element_token) == 0:
                            # If the token was not found - creating new one and saving it to the DB
                            element_token = [[str(uuid.uuid4())]]
                            try:
                                set_token = tokenizer_db_conn.prepare("insert into tokenizer (metric_param_token, metric_param_key, timestamp) values ('" + element_token[0][0] + "', convert_from(decode('" + base64.b64encode(str(arg).encode('utf-8')).decode('utf-8') + "', 'base64'), 'UTF8'), " + str(time.time()) + ")")
                                set_token()
                            except Exception as ex:
                                # Perhaps another thread preceded us and created it already?
                                element_token_query = tokenizer_db_conn.prepare("select metric_param_token from tokenizer where metric_param_key=convert_from(decode('" + base64.b64encode(str(arg).encode('utf-8')).decode('utf-8') + "', 'base64'), 'UTF8')")
                                element_token = element_token_query()
                                if len(element_token) == 0:
                                    # If we couldn't set the token nor get it... exception!
                                    syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "resolve_query_args_in_metric_name", "ERROR", str(None), str(None), str(ex), type(ex).__name__, "Could not tokenize the argument via the tokenizer DB."))
                                    raise Exception("ERROR: Could not tokenize the argument via the tokenizer DB. (Exception details: (" + str(ex) + " (" + type(ex).__name__ + "))")

                    if len(str(arg).strip()) != 0:
                        if len(arg) > self._metric_item_element_max_size:
                            if base_64_used:
                                query_base = query_base.replace("{{#arg" + str(i) + "}}",
                                                                base64.b64decode(arg.encode('utf-8')).decode('utf-8'))
                                metric_name = metric_name.replace("{{#arg" + str(i) + "}}", element_token[0][0])
                            else:
                                query_base = query_base.replace("{{#arg" + str(i) + "}}", arg)
                                metric_name = metric_name.replace("{{#arg" + str(i) + "}}", element_token[0][0])
                        else:
                            if base_64_used:
                                query_base = query_base.replace("{{#arg" + str(i) + "}}",
                                                                base64.b64decode(arg.encode('utf-8')).decode('utf-8'))
                                metric_name = metric_name.replace("{{#arg" + str(i) + "}}", base64.b64encode(arg.encode('utf-8')).decode('utf-8'))
                            else:
                                query_base = query_base.replace("{{#arg" + str(i) + "}}", arg)
                                metric_name = metric_name.replace("{{#arg" + str(i) + "}}", arg)
                else:
                    break
        return metric_name, query_base

    def fieldQuery(self, queryname):
        self._field_query_requests_received.value += 1
        query_details = self.queries[queryname]
        if query_details["type"] != "FIELD_QUERY":
            raise QueryNameNotFoundOrOfWrongType()

        now, time_range, yesterday = self.GetQueryTimeRange(query_details)
        metric_name = query_details["metric_name"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        metric_name = metric_name.replace("{{#query_name}}", queryname)
        query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_base = query_details["query"].replace("{{#query_name}}", queryname)
        base_64_used = query_details["base_64_used"]
        metric_name, query_base = self.resolve_query_args_in_metric_name(base_64_used, metric_name, query_base)


        query_string = "(" + query_base + ") AND @timestamp:[" + time_range + "]"
        query_body = {
                    "query":{
                        "query_string": {
                            "query": self.resolve_placeholders_in_query(query_string)
                        }
                    }
                }

        raw_res = None
        try:
            res = self.es.search(
                index=query_index,
                body=query_body,
                size=1
            )
            raw_res = res['hits']['hits'][0]['_source'][query_details["field_name"]]
            res = "@@RES: " + raw_res
            self._field_query_requests_processed_successfully.value += 1
        except Exception as e:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "fieldQuery", "WARN", str(None), str(metric_name), str(ex), type(ex).__name__, "An unexpected exception has been thrown while handling a field_query call"))
            res = "@@RES: @@EXCEPTION: " + str(e) + " (" + type(e).__name__ + ")"

        if Utils().is_number(raw_res):
            try:
                metric_object = self.statsd_client.gauge(metric_name, raw_res)
            except:
                pass

        return res

    def get_similarity(self, queryname):
        self._similarity_test_requests_received.value += 1
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

        metric_name, query_base = self.resolve_query_args_in_metric_name(base_64_used, metric_name,
                                                                         query_list_to_test_similarity_to)

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

        state = "RunningElkQuery"
        try:
            if DEBUG:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "get_similarity", "DEBUG", str(state), str(metric_name), str(ex), type(ex).__name__, "Running query for the list of items to compare to: `" + json.dumps(query_list_to_test_similarity_to_query_body) + "'"))

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

                    state = "LevensteinDistance"
                    edit_distance_match_rate = Utils().LevenshteinDistance(value_to_compare, cur_value_to_compare_to)
                    state = "HomoglyphsRatio"
                    homoglyphs_ratio = Utils().HomoglyphsRatio(value_to_compare, cur_value_to_compare_to)
                    state = "VisualSimilarityRate"
                    visual_similarity_rate = []
                    for p_hashing_alg in Utils.perceptive_hashing_algs:
                        visual_similarity_rate.append(Utils().VisualSimilarityRate(value_to_compare=value_to_compare, cur_value_to_compare_to=cur_value_to_compare_to, fonts_path=self._fonts_for_similarity_tests_path, algorithm=p_hashing_alg))

                    state = "AnalyzingResult"
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
                self._similarity_test_requests_processed_successfully.value += 1
            else:
                res = "@@ERROR: Elasticsearch responded with an unexpected response: (" + json.dumps(res) + ")"
        except Exception as ex:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "get_similarity", "WARN", str(state), str(metric_name), str(ex), type(ex).__name__, "An unexpected exception has been thrown while handling a similarity test call"))
            res = "@@RES: @@EXCEPTION: " + str(ex) + " (" + type(ex).__name__ + ") [state=" + state + "]"

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
        self._metric_requests_received.value += 1
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

        metric_name, query_base = self.resolve_query_args_in_metric_name(base_64_used, metric_name, query_base)

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
            if DEBUG:
                syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "queryCount", "DEBUG", str(None), str(metric_name), str(None), str(None), "Running query: `" + json.dumps(query_body) + "'"))

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
            self._metric_requests_processed_successfully.value += 1
        except Exception as ex:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "queryCount", "WARN", str(None), str(metric_name), str(ex), type(ex).__name__, "An unexpected exception has been thrown while handling a query_count call"))
            res = "@@RES: @@EXCEPTION: " + str(ex) + " (" + type(ex).__name__ + ")"

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

            res_lld = Utils().GenerateLldFromElasticAggrRes(res=res, macro_list=[], agg_on_field=agg_on_field, use_base64=False, add_doc_count=False, re_sort_by_value=True)
            res_str = str(hashlib.sha1(json.dumps(res_lld).encode("utf-8")).hexdigest())

            res = "@@RES: " + res_str
        except Exception as e:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "list_hash", "WARN", str(None), str(None), str(ex), type(ex).__name__, "An unexpected exception has been thrown while handling a list_hash call"))
            res = "@@RES: @@EXCEPTION: " + str(e) + " (" + type(e).__name__ + ")"

        return res

    def discover(self, queryname):
        self._discovery_requests_received.value += 1
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
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "discover", "INFO", str(None), str(None), str(None), str(None), "Executing the following query: hosts: " + json.dumps(self.es_hosts) + ", timeout: " + str(self.timeout_to_elastic) + ", index=" + query_index + ", query_body=" + json.dumps(query_body)))
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
            res_lld = Utils().GenerateLldFromElasticAggrRes(res=res, macro_list=macros_list, agg_on_field=agg_on_field, use_base64=use_base64, queries_to_run=queries_to_run, services_urls=relevant_service_urls, tiny_url_service_details=tiny_url_service_details, config_copy=self._config_copy)

            res_str = str(json.dumps(res_lld))
            if encode_json_as_b64:
                res_str = str(base64.b64encode(str(res_str).encode('utf-8')).decode('utf-8'))
            res = "@@RES: " + res_str
            self._discovery_requests_processed_successfully.value += 1
        except Exception as e:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "discover", "WARN", str(None), str(None), str(ex), type(ex).__name__, "An unexpected exception has been thrown while handling a discovery query"))
            res = "@@RES: @@EXCEPTION: " + str(e) + " (" + type(e).__name__ + ")"

        return res

    def get_config_by_regex(self, regex):
        res = {}
        for key in self._config_copy.keys():
            if re.match(regex, key):
                res[key] = self._config_copy[key]

        return res

    def dump_queries(self):
        return "@@RES: " + json.dumps(self.queries)

    def test_similarity(self, algo, s1, s2):
        utils = Utils()
        res = "ERROR_UNKNOWN_ALGO"
        try:
            if algo == "levenstein":
                res = str(utils.LevenshteinDistance(s1, s2))
            if algo == "visual-phash":
                res = str(utils.VisualSimilarityRate(s1, s2, fonts_path=self._fonts_for_similarity_tests_path, algorithm="phash"))
            if algo == "visual-ahash":
                res = str(utils.VisualSimilarityRate(s1, s2, fonts_path=self._fonts_for_similarity_tests_path, algorithm="ahash"))
            if algo == "visual-dhash":
                res = str(utils.VisualSimilarityRate(s1, s2, fonts_path=self._fonts_for_similarity_tests_path, algorithm="dhash"))
            if algo == "visual-whash":
                res = str(utils.VisualSimilarityRate(s1, s2, fonts_path=self._fonts_for_similarity_tests_path, algorithm="whash"))

            return res
        except Exception as ex:
            syslog.syslog(self._logging_format % (datetime.datetime.now().isoformat(), "metrics_collector", "test_similarity", "WARN", str(None), str(None), str(ex), type(ex).__name__, "An unexpected exception has been thrown while using the test method test-similarity"))
            return "@@EX: " + str(ex) + " (" + type(ex).__name__ + ")"

    def simulate_discovery(self):
        config_copy = {
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbname": "smart_onion_metric_collector",
            "smart-onion.config.architecture.internal_services.backend.timer.max_items_in_batch": 10,
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.ping-listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.field_query": "/smart-onion/field-query/",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.metrics_to_work_on_pattern": "^(?!(stats\\.gauges\\.smart\\-onion\\.anomaly_))[^ ]+[A-Za-z0-9\\-_](( [0-9]+(\\.[0-9]+|) [0-9]+)|)$",
            "smart-onion.config.architecture.internal_services.backend.timer.published-listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.save_interval": 10,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.reported_anomalies_topic": "metric-analyzer-detected-anomalies",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.base_urls.get-anomaly-score": "/smart-onion/get-anomaly-score/",
            "smart-onion.config.architecture.internal_services.backend.timer.discover-interval": 3600,
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.anomaly_score_threshold_for_reporting": 0.9,
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.published-listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.proxy_by_tiny": "/so/tiny/",
            "smart-onion.config.architecture.internal_services.backend.timer.protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.configurator.published-listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.timer.listening-port": 9006,
            "smart-onion.config.architecture.internal_services.backend.configurator.listening-port": 9003,
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.alerter.protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.anomaly_score_threshold_for_reporting": 90,
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.published-listening-protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.tiny2url": "/so/tiny2url/",
            "smart-onion.config.architecture.internal_services.backend.alerter.listening-port": 9004,
            "smart-onion.config.architecture.internal_services.backend.tiny_url.listening-port": 9999,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.lld": "/smart-onion/discover/",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.listening-port": 9001,
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.max-allowed-models": 400,
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.published-listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.listening-port": 9002,
            "smart-onion.config.architecture.internal_services.backend.queue.kafka.bootstrap_servers": "10.253.0.141",
            "smart-onion.config.architecture.internal_services.backend.timer.listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.similarity_test": "/smart-onion/test-similarity/",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.tinyfy_urls": False,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.similarity_test_fonts_path": "/opt/smart-onion/resources/similarity-test-fonts/",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.list_hash": "/smart-onion/list-hash/",
            "smart-onion.config.architecture.internal_services.backend.configurator.published-listening-protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.published-listening-port": 9002,
            "smart-onion.config.architecture.internal_services.backend.timer.published-listening-port": 9999,
            "smart-onion.config.architecture.internal_services.backend.timer.published-listening-protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.connection-backlog": 10,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.poller_threads_per_cpu": 3,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbhost": "localhost",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.published-listening-port": 9999,
            "smart-onion.config.common.logging_format": "timestamp=%s;module=smart-onion_%s;method=%s;severity=%s;state=%s;metric/metric_family=%s;exception_msg=%s;exception_type=%s;message=%s",
            "smart-onion.config.architecture.internal_services.backend.configurator.published-listening-port": 9003,
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.protocol": "TCP",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.published-listening-port": 9001,
            "smart-onion.config.architecture.internal_services.backend.alerter.published-listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.metrics_topic_name": "metrics",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbtype": "postgres",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.reference_past_sample_periods": "7,14,21",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.metrics_physical_path": "/data/metrics/whisper/",
            "smart-onion.config.architecture.internal_services.backend.configurator.listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbuser": "smart_onion_metric_collector",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_tasks_gc_interval": 604800,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.alerter.published-listening-port": 9004,
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.listening-port": 9000,
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.minimum_seconds_between_model_over_quota_log_messages": 3600,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.sampling_interval_ms": 600000,
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.reported_anomalies_topic": "anomaly-detector-detected-anomalies",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.published-listening-port": 9000,
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.anomalies_check_interval": 300,
            "smart-onion.config.architecture.internal_services.backend.timer.metrics_collection_tasks_topic": "metric-collection-tasks",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.anomaly_likelihood_threshold_for_reporting": 0.9,
            "smart-onion.config.architecture.internal_services.backend.alerter.listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.configurator.protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.query_count": "/smart-onion/query-count/",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.keep_lost_items_for_days": 30,
            "smart-onion.config.architecture.internal_services.backend.pipeline.statsd.port": 8125,
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.reference_timespan_in_seconds": 86400,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbpassword": "vindeta11",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.backup_file": "/tmp/tiny_url.json.db",
            "smart-onion.config.architecture.internal_services.backend.alerter.published-listening-protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.task_base_ttl": 8640,
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_max_length": 20,
            "smart-onion.config.architecture.internal_services.backend.tiny_url.published-listening-protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.ping-protocol": "http",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.metric_items_tokenizer_dbport": 5432,
            "smart-onion.config.architecture.internal_services.backend.tiny_url.listening-host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.max_timeout_to_elastic": 120,
            "smart-onion.config.architecture.internal_services.backend.tiny_url.backup_interval": 30,
            "smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.url2tiny": "/so/url2tiny",
            "smart-onion.config.architecture.internal_services.backend.pipeline.statsd.host": "127.0.0.1",
            "smart-onion.config.architecture.internal_services.backend.metrics-analyzer.ping-listening-port": 9007,
            "smart-onion.config.architecture.internal_services.backend.tiny_url.published-listening-host": "127.0.0.1"
        }
        tiny_url_service_details = None
        relevant_service_urls = {
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.list_hash": "/smart-onion/list-hash/",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.similarity_test": "/smart-onion/test-similarity/",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.lld": "/smart-onion/discover/",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.proxy_by_tiny": "/so/tiny/",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.field_query": "/smart-onion/field-query/",
            "smart-onion.config.architecture.internal_services.backend.metrics-collector.base_urls.query_count": "/smart-onion/query-count/",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.url2tiny": "/so/url2tiny",
            "smart-onion.config.architecture.internal_services.backend.tiny_url.base_urls.tiny2url": "/so/tiny2url/",
            "smart-onion.config.architecture.internal_services.backend.anomaly-detector.base_urls.get-anomaly-score": "/smart-onion/get-anomaly-score/"
        }
        # queries_to_run = ["number_of_requests_per_url", "number_of_source_hosts_per_url", "number_of_queries_per_dns_tld_subdomain"]
        queries_to_run = ["number_of_requests_per_url"]
        use_base64 = False
        agg_on_field = 'virtual_host.keyword,method.keyword,destination_port,uri.keyword'
        macros_list = ["VIRT_HOST", "HTTP_VERB", "PORT", "SITE_URL"]
        res = {
            "hits": {
                "max_score": 0.0,
                "total": 383456,
                "hits": []
            },
            "took": 721,
            "aggregations": {
                "field_values0": {
                    "sum_other_doc_count": 66065,
                    "buckets": [
                        {
                            "key": "gateway",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 20,
                                                        "buckets": [
                                                            {
                                                                "key": "/umbraco/ping.aspx",
                                                                "doc_count": 1391
                                                            },
                                                            {
                                                                "key": "/",
                                                                "doc_count": 1386
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10153",
                                                                "doc_count": 4
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10168",
                                                                "doc_count": 4
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10115",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10156",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10174",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10048",
                                                                "doc_count": 2
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10056",
                                                                "doc_count": 2
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10100",
                                                                "doc_count": 2
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 2820
                                                },
                                                {
                                                    "key": 8080,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/",
                                                                "doc_count": 1390
                                                            },
                                                            {
                                                                "key": "/umbraco/ping.aspx",
                                                                "doc_count": 1389
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 2779
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 5599
                                    },
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 1947,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/api",
                                                                "doc_count": 365
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 365
                                                },
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/Ribbon.asmx",
                                                                "doc_count": 6
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 6
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 371
                                    },
                                    {
                                        "key": "HEAD",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/",
                                                                "doc_count": 7
                                                            },
                                                            {
                                                                "key": "/robots.txt",
                                                                "doc_count": 2
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 9
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 9
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 92527
                        },
                        {
                            "key": "mvs-sysaid",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 82940,
                                                        "buckets": [
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=HP-CZC43242GQ&register=HP-CZC43242GQ",
                                                                "doc_count": 1963
                                                            },
                                                            {
                                                                "key": "/api/v1/announcements/countUnread",
                                                                "doc_count": 545
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=HP-TRF5360KX3&register=HP-TRF5360KX3",
                                                                "doc_count": 453
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=VMware-56 4d 65 67 20 c5 7e 65-5f 5b 8f 1d ae 87 4a 5c&register=VMware-56 4d 65 67 20 c5 7e 65-5f 5b 8f 1d ae 87 4a 5c",
                                                                "doc_count": 451
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=HP-TRF4240W2C&register=HP-TRF4240W2C",
                                                                "doc_count": 269
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=10-62-E5-03-DC-39&register=10-62-E5-03-DC-39",
                                                                "doc_count": 246
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=HP-4CE812583R&register=HP-4CE812583R",
                                                                "doc_count": 246
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=HP-TRF3260834&register=HP-TRF3260834",
                                                                "doc_count": 243
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=HP-CZC635882J&register=HP-CZC635882J",
                                                                "doc_count": 241
                                                            },
                                                            {
                                                                "key": "/agentmsg?accountID=MVS&computerID=LENOVO-PB006X4A&register=LENOVO-PB006X4A",
                                                                "doc_count": 161
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 236
                                                    },
                                                    "doc_count": 87758
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 87758
                                    },
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 210,
                                                        "buckets": [
                                                            {
                                                                "key": "/agententry?deflate=1&charset=iso-8859-1",
                                                                "doc_count": 112
                                                            },
                                                            {
                                                                "key": "/HelpDesk.jsp?helpdeskfrm&resetParams=YES&fromId=List&ajaxStyleList=YES",
                                                                "doc_count": 44
                                                            },
                                                            {
                                                                "key": "/GetComboBoxItems",
                                                                "doc_count": 39
                                                            },
                                                            {
                                                                "key": "/HelpDesk.jsp?helpdeskfrm&fromId=IncidentsList&ajaxStyleList=YES",
                                                                "doc_count": 24
                                                            },
                                                            {
                                                                "key": "/HelpDesk.jsp?helpdeskfrm&fromId=List&ajaxStyleList=YES",
                                                                "doc_count": 22
                                                            },
                                                            {
                                                                "key": "/SubmitSR.jsp",
                                                                "doc_count": 6
                                                            },
                                                            {
                                                                "key": "/reportsessions?deflate=1&action=reportsessions&accountID=MVS&compID=HP-TRF5360KX3",
                                                                "doc_count": 5
                                                            },
                                                            {
                                                                "key": "/OptionPane.jsp",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/reportsessions?deflate=1&action=reportsessions&accountID=MVS&compID=DELL-JVKHYY1",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/guidedTour",
                                                                "doc_count": 2
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 2
                                                    },
                                                    "doc_count": 470
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 470
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 88228
                        },
                        {
                            "key": "mvs-crm",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 1136,
                                                        "buckets": [
                                                            {
                                                                "key": "/MivtachSimon/XRMServices/2011/Organization.svc",
                                                                "doc_count": 15202
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/XRMServices/2011/Organization.svc/web",
                                                                "doc_count": 8146
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/AppGridWebService.ashx?operation=Refresh",
                                                                "doc_count": 3655
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/LookupMruWebService.asmx",
                                                                "doc_count": 555
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/RecentlyViewedWebService.asmx",
                                                                "doc_count": 550
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/LookupService.asmx",
                                                                "doc_count": 479
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/Ribbon.asmx",
                                                                "doc_count": 203
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/Annotation.asmx",
                                                                "doc_count": 168
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/AppGridWebService.ashx?id=crmGrid&operation=Reset",
                                                                "doc_count": 120
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/AppWebServices/MessageBar.asmx",
                                                                "doc_count": 110
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 7
                                                    },
                                                    "doc_count": 30324
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 30324
                                    },
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 17226,
                                                        "buckets": [
                                                            {
                                                                "key": "/MivtachSimon/WebResources/new_jquery_1.11.1",
                                                                "doc_count": 761
                                                            },
                                                            {
                                                                "key": "/_common/icon.aspx?objectTypeCode=10004&iconType=OutlookShortcutIcon&cache=0",
                                                                "doc_count": 583
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/WebResources/new_MVS.CrmLibNew",
                                                                "doc_count": 516
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/WebResources/new_MVS.GlobalNew",
                                                                "doc_count": 415
                                                            },
                                                            {
                                                                "key": "/MivtachSimon/WebResources/new_XrmServiceToolKit",
                                                                "doc_count": 290
                                                            },
                                                            {
                                                                "key": "/WebResources/new_flag_red",
                                                                "doc_count": 164
                                                            },
                                                            {
                                                                "key": "/WebResources/new_form16",
                                                                "doc_count": 159
                                                            },
                                                            {
                                                                "key": "/WebResources/new_flag_orange",
                                                                "doc_count": 158
                                                            },
                                                            {
                                                                "key": "/WebResources/new_flag_green",
                                                                "doc_count": 120
                                                            },
                                                            {
                                                                "key": "/_static/blank.htm",
                                                                "doc_count": 87
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 68
                                                    },
                                                    "doc_count": 20479
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 20479
                                    },
                                    {
                                        "key": "OPTIONS",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/MivtachSimon/CRMReports/viewer/",
                                                                "doc_count": 3
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 3
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 3
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 50806
                        },
                        {
                            "key": "connectivity.services.fire.glass",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/ConnectivityService/index.html",
                                                                "doc_count": 16611
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 16611
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 16611
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 16611
                        },
                        {
                            "key": "193.16.147.35",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 443,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "http://193.16.147.35/fakeurl.htm",
                                                                "doc_count": 16520
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 16520
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 16520
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 16520
                        },
                        {
                            "key": "mvs-inet2",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 7831,
                                                        "buckets": [
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx",
                                                                "doc_count": 183
                                                            },
                                                            {
                                                                "key": "/formally/InsFormallyDefault.aspx",
                                                                "doc_count": 68
                                                            },
                                                            {
                                                                "key": "/formally/assets/img/remove-icon-small.png",
                                                                "doc_count": 37
                                                            },
                                                            {
                                                                "key": "/formally/assets/plugins/uniform/jquery.uniform.min.js",
                                                                "doc_count": 32
                                                            },
                                                            {
                                                                "key": "/formally/ScriptResource.axd?d=FoiQuTW64IUxwzPAZojt28HCf9sW5euXUKM8tCO44eq-OOKl0U1gvsr4PLsd39meoDPbkYuu7otlCYO5_mmapvv90wlvoU5jN1ytO_Mm__nUEnp0vRA3NUo5ZYMYCgvxHbQhqW57MMLxXqT17pd3hkDZr_FU3wKWLzys_x16eSUpcGzn0&t=633989044800000000",
                                                                "doc_count": 27
                                                            },
                                                            {
                                                                "key": "/formally/css/sigs/acr-style.css",
                                                                "doc_count": 26
                                                            },
                                                            {
                                                                "key": "/formally/JS/browserDetect.js",
                                                                "doc_count": 25
                                                            },
                                                            {
                                                                "key": "/formally/WebResource.axd?d=cXscxvD7spgNSv-C483zzQ_8ZOEZCCtDRqQxg-raUEuwG1mzHiNBEk5DgGaFRPh-E4W6StQ6moyD7UfN7rnBuGLLR2A1&t=636271779501517547",
                                                                "doc_count": 25
                                                            },
                                                            {
                                                                "key": "/formally/assets/InsFormallyDefault/scripts/Default.js?t=29012019",
                                                                "doc_count": 23
                                                            },
                                                            {
                                                                "key": "/formally/assets/InsFormallyDefault/css/tooltipster.bundle.css?t=29012019",
                                                                "doc_count": 22
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 23
                                                    },
                                                    "doc_count": 8299
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 8299
                                    },
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 708,
                                                        "buckets": [
                                                            {
                                                                "key": "/formally/InsFormallyDefault.aspx",
                                                                "doc_count": 2418
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/GetSelectHealthForms",
                                                                "doc_count": 904
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/GetFormsMsgInfo",
                                                                "doc_count": 888
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/GetAge",
                                                                "doc_count": 817
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/CheckThilatBituahStart01",
                                                                "doc_count": 671
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/TransactionDisplay",
                                                                "doc_count": 203
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/Forms_SaveData",
                                                                "doc_count": 121
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/Forms_DisplayTransaction",
                                                                "doc_count": 119
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/Forms_GetDocumentsToSign",
                                                                "doc_count": 106
                                                            },
                                                            {
                                                                "key": "/formally/InsDocumentWizard.aspx/GetDocumentsToSign",
                                                                "doc_count": 102
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 11
                                                    },
                                                    "doc_count": 7057
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 7057
                                    },
                                    {
                                        "key": "OPTIONS",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/formally/Data/Saved/000055/366990/ReasonPdf/",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/formally/assets/plugins/uniform/css/",
                                                                "doc_count": 1
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 2
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 2
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 15358
                        },
                        {
                            "key": "mvs-netcc",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 1336,
                                                        "buckets": [
                                                            {
                                                                "key": "/MOMcc/WebServices/Services.asmx/RTRGetDataTable",
                                                                "doc_count": 2140
                                                            },
                                                            {
                                                                "key": "/MOMCC/AgentsRTRJQGrid.aspx/GetRTData",
                                                                "doc_count": 1944
                                                            },
                                                            {
                                                                "key": "/Momcc/WebServices/Services.asmx/RTRGetDataTable",
                                                                "doc_count": 1461
                                                            },
                                                            {
                                                                "key": "/MOMCC/WebServices/Services.asmx/FunctionGetDataTable",
                                                                "doc_count": 1411
                                                            },
                                                            {
                                                                "key": "/momcc/WebServices/Services.asmx/FunctionGetDataTable",
                                                                "doc_count": 1181
                                                            },
                                                            {
                                                                "key": "/Momcc/WebServices/Services.asmx/FunctionGetDataTable",
                                                                "doc_count": 1084
                                                            },
                                                            {
                                                                "key": "/MOMCC/RTRManagement.aspx/GetSessionId",
                                                                "doc_count": 827
                                                            },
                                                            {
                                                                "key": "/Momcc/WebServices/Services.asmx/BillboardGetDataTable",
                                                                "doc_count": 817
                                                            },
                                                            {
                                                                "key": "/MOMCC/WebServices/Services.asmx/HelloWorld",
                                                                "doc_count": 778
                                                            },
                                                            {
                                                                "key": "/MOMCC/WebServices/Services.asmx/BillboardGetDataTable",
                                                                "doc_count": 749
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 13728
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 13728
                                    },
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 310,
                                                        "buckets": [
                                                            {
                                                                "key": "/MOMCC/WebServices/Services.asmx/js",
                                                                "doc_count": 29
                                                            },
                                                            {
                                                                "key": "/MOMCC/Images/ui-bg_flat_55_fbec88_40x100.png",
                                                                "doc_count": 12
                                                            },
                                                            {
                                                                "key": "/MOMCC/SkillsRTRJQGrid.aspx",
                                                                "doc_count": 10
                                                            },
                                                            {
                                                                "key": "/MOMCC/WebServices/Services.asmx/jsdebug",
                                                                "doc_count": 9
                                                            },
                                                            {
                                                                "key": "/MOMCC/SkillsRTRJQGrid.aspx?uid=814",
                                                                "doc_count": 8
                                                            },
                                                            {
                                                                "key": "/MOMCC/ScriptResource.axd?d=88N2INglQPwPgldgJimW8yQgQHzBoD9LWvTHyIh--lEuMe7hQCR2jx67OGrpvTLzMmJdlJZvngkSmkJP4gusEPpSRKh5l0Mfo7wD60t2cZfuE7IF19ooKDFzzsPSAUMsvoIWmnCltfTB53LGftcBDJYtpc5_6uO3dOJfducBhr1JZdbW0&t=3f4a792d",
                                                                "doc_count": 7
                                                            },
                                                            {
                                                                "key": "/Momcc/App_Themes/TelemarketAdmin_he-il/listview.css",
                                                                "doc_count": 6
                                                            },
                                                            {
                                                                "key": "/Momcc/App_Themes/TelemarketAdmin_he-il/overlib.css",
                                                                "doc_count": 6
                                                            },
                                                            {
                                                                "key": "/Momcc/App_Themes/TelemarketAdmin_he-il/tabs.css",
                                                                "doc_count": 6
                                                            },
                                                            {
                                                                "key": "/Momcc/Design/Hashtable.js",
                                                                "doc_count": 6
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 4
                                                    },
                                                    "doc_count": 409
                                                },
                                                {
                                                    "key": 20000,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/GetRecording/24915187/2019-04-03",
                                                                "doc_count": 1
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 1
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 410
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 14138
                        },
                        {
                            "key": "downloads.ironport.com",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/",
                                                                "doc_count": 8246
                                                            },
                                                            {
                                                                "key": "/vtl/meter.txt",
                                                                "doc_count": 726
                                                            },
                                                            {
                                                                "key": "/vtl/vof_history_year.tgz",
                                                                "doc_count": 717
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 9689
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 9689
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 9689
                        },
                        {
                            "key": "mvs-sccm.mvs.co.il",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 3004,
                                                        "buckets": [
                                                            {
                                                                "key": "/SMS_MP/.sms_pol?{7991fabb-48b7-4ead-8358-3bffafe0f1d1}.2965_00",
                                                                "doc_count": 241
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_pol?{9a131a2a-6ab0-4d01-b13f-e7178a0e6cc8}.943_00",
                                                                "doc_count": 149
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_aut?SMSTRC",
                                                                "doc_count": 103
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/1c4f0866-d966-4a4a-a090-9860dfeb9e66/sccm?/Windows10.0-KB4489886-x64.cab",
                                                                "doc_count": 72
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_aut?MPLIST",
                                                                "doc_count": 62
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_aut?MPKEYINFORMATIONEX",
                                                                "doc_count": 55
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_aut?MPLIST1&MVS",
                                                                "doc_count": 36
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_pol?{7991fabb-48b7-4ead-8358-3bffafe0f1d1}.2964_00",
                                                                "doc_count": 35
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/MVS00002/x64/client.msi",
                                                                "doc_count": 24
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/67b109ab-e31e-490b-b5ba-8290940e48ef/sccm?/Windows6.1-KB4489885-x86.cab",
                                                                "doc_count": 19
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 7
                                                    },
                                                    "doc_count": 3800
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 3800
                                    },
                                    {
                                        "key": "CCM_POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/ccm_system/request",
                                                                "doc_count": 1332
                                                            },
                                                            {
                                                                "key": "/ccm_system_windowsauth/request",
                                                                "doc_count": 792
                                                            },
                                                            {
                                                                "key": "/bgb/handler.ashx?RequestType=Continue",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/bgb/handler.ashx?RequestType=LogIn",
                                                                "doc_count": 1
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 2126
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 2126
                                    },
                                    {
                                        "key": "HEAD",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 1031,
                                                        "buckets": [
                                                            {
                                                                "key": "/SMS_MP/.sms_pol?{9a131a2a-6ab0-4d01-b13f-e7178a0e6cc8}.943_00",
                                                                "doc_count": 73
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_pol?{7991fabb-48b7-4ead-8358-3bffafe0f1d1}.2965_00",
                                                                "doc_count": 62
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_pol?{7991fabb-48b7-4ead-8358-3bffafe0f1d1}.2964_00",
                                                                "doc_count": 16
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_dcm?Id&DocumentId=bf38db4e-68e6-4399-b6f5-ceb35525647f/MANIFEST&Hash=68081A17C7C87F06E89E1405B42804ACCFE5CE3B1C3504CF45A5521101B0673C&Compression=zlib",
                                                                "doc_count": 14
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_dcm?Id&DocumentId=bf38db4e-68e6-4399-b6f5-ceb35525647f/PROPERTIES&Hash=87FE8CBA6F5995F26E8BE489316F5D05232ED3142B8398631FC7174234BBB2D9&Compression=zlib",
                                                                "doc_count": 13
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_dcm?Id&DocumentId=urn:policy-platform:policy.microsoft.com:smlif:ms.dcm.Site_611E7900-9A53-4E46-92FF-20B1838DB5FC.SUM_bf38db4e-68e6-4399-b6f5-ceb35525647f:200:VL&Hash=1FBA6AA03E3D7BEA522B032CEE14D2D26A5579424CDEFFB39AEAE5B7406B4DE3&Compression=zlib",
                                                                "doc_count": 11
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_dcm?Id&DocumentId=urn:policy-platform:policy.microsoft.com:smlif:ms.dcm.ScopeId_611E7900-9A53-4E46-92FF-20B1838DB5FC.Application_5cc46a85-46f4-4664-a0f9-090a2017731a:2&Hash=C132C2D1664B69772ABE90CF64941049119F0686B547A9F78404E52DF84608C6&Compression=zlib",
                                                                "doc_count": 4
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_dcm?Id&DocumentId=ScopeId_611E7900-9A53-4E46-92FF-20B1838DB5FC/ProhibitedApplication_634af815-74e9-4ac0-b596-7996b8d81d09/3/MANIFEST&Hash=F5A8EE3CABA2118238E5301D521A4CB62B56D4CAFF66711633E792E98D289210&Compression=zlib",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_dcm?Id&DocumentId=ScopeId_611E7900-9A53-4E46-92FF-20B1838DB5FC/ProhibitedApplication_634af815-74e9-4ac0-b596-7996b8d81d09/3/PROPERTIES&Hash=AA8D509123F3CBAC741C6D76B7409A56760528B9D645A1FB733FB44E4870D69F&Compression=zlib",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/SMS_MP/.sms_dcm?Id&DocumentId=ScopeId_611E7900-9A53-4E46-92FF-20B1838DB5FC/RequiredApplication_5cc46a85-46f4-4664-a0f9-090a2017731a/2/PROPERTIES&Hash=9DB90D97B1D97ED0F04C66ACE07ECE494D00A62982F26B56ACC2969A74F6166C&Compression=zlib",
                                                                "doc_count": 3
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 5
                                                    },
                                                    "doc_count": 1233
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 1233
                                    },
                                    {
                                        "key": "BITS_POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 59,
                                                        "buckets": [
                                                            {
                                                                "key": "/CCM_Incoming/{51949D56-2D44-4023-A3EE-33D21CA036E4}",
                                                                "doc_count": 6
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{B8B67D7D-F6F0-4BF0-9AB4-E042ECCAD00C}",
                                                                "doc_count": 6
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{0DEBDDFB-FFD0-4A98-8BE5-6F157F3C5728}",
                                                                "doc_count": 5
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{960955E8-8B0F-4D6B-9E11-BEC0907CF2EB}",
                                                                "doc_count": 5
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{BA82CB50-EFEC-484F-B3BA-6DA5B6E12B8B}",
                                                                "doc_count": 5
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{D80E1D34-973C-4827-B425-F0AAE4EE1A0F}",
                                                                "doc_count": 5
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{4F5D4B5E-B020-40D5-A180-0F24F340514A}",
                                                                "doc_count": 4
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{6031F65D-D319-46F3-865A-438F79705EFF}",
                                                                "doc_count": 4
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{698968B0-7393-4F31-9C5A-BB57C927F63A}",
                                                                "doc_count": 4
                                                            },
                                                            {
                                                                "key": "/CCM_Incoming/{865AD052-E41F-465C-B605-5F345EF4C404}",
                                                                "doc_count": 4
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 107
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 107
                                    },
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/SMS_FSP/.sms_fsp",
                                                                "doc_count": 97
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 97
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 97
                                    },
                                    {
                                        "key": "PROPFIND",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 5,
                                                        "buckets": [
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/MVS00002",
                                                                "doc_count": 3
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/05fc3d2a-5e8b-47a8-97b0-fb3b3da8d27f",
                                                                "doc_count": 2
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/1c31290a-ab11-4f19-a41b-53b5779c2a43",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/2e2a2093-5a7e-4223-b6c7-06fed0648173",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/374f8105-0c85-4419-ae80-f227eb431197",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/3cd83981-51c7-490a-8a98-12b4182cc42c",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/4a39e31d-e4ef-4602-95a7-aa7c1eda1b45",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/4f5a1bb1-236f-424a-9427-5c1be2a5d20a",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/5dbd1a0f-218e-46b3-83de-1f6220d5e66a",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/SMS_DP_SMSPKG$/603adbf7-779e-46d0-a21c-c32942294887",
                                                                "doc_count": 1
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 18
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 18
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 7381
                        },
                        {
                            "key": "mvs-earth",
                            "field_values1": {
                                "sum_other_doc_count": 0,
                                "buckets": [
                                    {
                                        "key": "GET",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 3931,
                                                        "buckets": [
                                                            {
                                                                "key": "/cic/Login.aspx",
                                                                "doc_count": 85
                                                            },
                                                            {
                                                                "key": "/cic/Search.aspx",
                                                                "doc_count": 48
                                                            },
                                                            {
                                                                "key": "/CIC/ScriptResource.axd?d=dVI-mRhDcVw4RfMEZwNy_lc-0kw2M5qbzDBrT20NuilHBCNTBL2Vde7_ltINuIE86FvqiaZtELfzFBU5he0LwUlmqq7SYHXxOSw8vI7XjSGDUDxK8cEa65qnq6Rv6QjuVbZ082_FajG-PSNqJvpHnnSHR_OQuyibBL4L-nEUXrBvMPFV23G6UhPfIyCX3_Qe0&t=2b48f70a",
                                                                "doc_count": 34
                                                            },
                                                            {
                                                                "key": "/cic/Images/plus.gif",
                                                                "doc_count": 33
                                                            },
                                                            {
                                                                "key": "/UniformUI/Styles/images/ui-bg_inset-hard_100_fcfdfd_1x100.png",
                                                                "doc_count": 32
                                                            },
                                                            {
                                                                "key": "/cic/Styles/CIC.css",
                                                                "doc_count": 32
                                                            },
                                                            {
                                                                "key": "/UniformUI/Styles/images/ui-bg_inset-hard_100_f5f8f9_1x100.png",
                                                                "doc_count": 29
                                                            },
                                                            {
                                                                "key": "/CIC/ScriptResource.axd?d=b-A0NpVBEnQYXvanvCvWR1ELKKsJvAtPR7av0WwsGPfkPyMSiQrL0dNLszEn1IYPjKmcRKo8HnedXaPYcwX8lyOjqBXdm3ZX6pCQ69WeMHbxw0kJ1_ajYTPdbx8h4Go6nj8-kVIKSKpwfa2IhRXsLPsD3i1SvhY682JRiejE4Etg0E0CcGhOLPs3i6Pc7hQe0&t=ffffffff9300e662",
                                                                "doc_count": 28
                                                            },
                                                            {
                                                                "key": "/UniformUI/Styles/images/ui-bg_gloss-wave_55_5c9ccc_500x100.png",
                                                                "doc_count": 27
                                                            },
                                                            {
                                                                "key": "/cic/Incs/jquery-1.5.1.min.js",
                                                                "doc_count": 26
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 24
                                                    },
                                                    "doc_count": 4305
                                                },
                                                {
                                                    "key": 4080,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 24,
                                                        "buckets": [
                                                            {
                                                                "key": "/MivPel/Image/2018/8525279.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/8856241.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/8922994.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/8968721.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/8975918.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/9061530.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/9094660.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/9094665.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/9096731.pdf",
                                                                "doc_count": 1
                                                            },
                                                            {
                                                                "key": "/MivPel/Image/2019/9124232.pdf",
                                                                "doc_count": 1
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 34
                                                },
                                                {
                                                    "key": 1818,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/bug_list.js",
                                                                "doc_count": 2
                                                            },
                                                            {
                                                                "key": "/jquery/Images/ui-bg_highlight-soft_75_cccccc_1x100.png",
                                                                "doc_count": 1
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 3
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 4342
                                    },
                                    {
                                        "key": "POST",
                                        "field_values2": {
                                            "sum_other_doc_count": 0,
                                            "buckets": [
                                                {
                                                    "key": 80,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 376,
                                                        "buckets": [
                                                            {
                                                                "key": "/sir_ws/SourcesConverter.asmx",
                                                                "doc_count": 268
                                                            },
                                                            {
                                                                "key": "/EmployersWS_Prod/DataMesakem.asmx",
                                                                "doc_count": 256
                                                            },
                                                            {
                                                                "key": "/cic/Search.aspx/ShowClientPolicies",
                                                                "doc_count": 256
                                                            },
                                                            {
                                                                "key": "/cic/Search.aspx",
                                                                "doc_count": 228
                                                            },
                                                            {
                                                                "key": "/CIC/Search.aspx/ShowClientPolicies",
                                                                "doc_count": 115
                                                            },
                                                            {
                                                                "key": "/SIR_WS/Generic.asmx",
                                                                "doc_count": 80
                                                            },
                                                            {
                                                                "key": "/sir_ws/CMarchive.asmx",
                                                                "doc_count": 60
                                                            },
                                                            {
                                                                "key": "/cic/ClientDataRenderServices.aspx/DrawingDocumentsTable",
                                                                "doc_count": 56
                                                            },
                                                            {
                                                                "key": "/cic/ClientDataRenderServices.aspx/DrawingPolicesTable",
                                                                "doc_count": 49
                                                            },
                                                            {
                                                                "key": "/cic/Login.aspx",
                                                                "doc_count": 46
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 6
                                                    },
                                                    "doc_count": 1790
                                                },
                                                {
                                                    "key": 1818,
                                                    "field_values3": {
                                                        "sum_other_doc_count": 0,
                                                        "buckets": [
                                                            {
                                                                "key": "/edit_bug.aspx?id=0",
                                                                "doc_count": 1
                                                            }
                                                        ],
                                                        "doc_count_error_upper_bound": 0
                                                    },
                                                    "doc_count": 1
                                                }
                                            ],
                                            "doc_count_error_upper_bound": 0
                                        },
                                        "doc_count": 1791
                                    }
                                ],
                                "doc_count_error_upper_bound": 0
                            },
                            "doc_count": 6133
                        }
                    ],
                    "doc_count_error_upper_bound": 1359
                }
            },
            "_shards": {
                "successful": 43,
                "total": 43,
                "skipped": 0,
                "failed": 0
            },
            "timed_out": False
        }
        # res = {
        #     "hits": {
        #         "max_score": 0.0,
        #         "total": 383456,
        #         "hits": []
        #     },
        #     "took": 721,
        #     "aggregations": {
        #         "field_values0": {
        #             "sum_other_doc_count": 66065,
        #             "buckets": [
        #                 {
        #                     "key": "gateway",
        #                     "field_values1": {
        #                         "sum_other_doc_count": 0,
        #                         "buckets": [
        #                             {
        #                                 "key": "GET",
        #                                 "field_values2": {
        #                                     "sum_other_doc_count": 0,
        #                                     "buckets": [
        #                                         {
        #                                             "key": 80,
        #                                             "field_values3": {
        #                                                 "sum_other_doc_count": 20,
        #                                                 "buckets": [
        #                                                     {
        #                                                         "key": "/umbraco/ping.aspx",
        #                                                         "doc_count": 1391
        #                                                     },
        #                                                     {
        #                                                         "key": "/",
        #                                                         "doc_count": 1386
        #                                                     },
        #                                                     {
        #                                                         "key": "/MivtachSimon/MivtachSimon/_Common/icon.aspx?cache=1&iconType=NavigationIcon&objectTypeCode=10153",
        #                                                         "doc_count": 4
        #                                                     }
        #                                                 ],
        #                                                 "doc_count_error_upper_bound": 0
        #                                             },
        #                                             "doc_count": 2820
        #                                         },
        #                                         {
        #                                             "key": 8080,
        #                                             "field_values3": {
        #                                                 "sum_other_doc_count": 0,
        #                                                 "buckets": [
        #                                                     {
        #                                                         "key": "/",
        #                                                         "doc_count": 1390
        #                                                     },
        #                                                     {
        #                                                         "key": "/umbraco/ping.aspx",
        #                                                         "doc_count": 1389
        #                                                     }
        #                                                 ],
        #                                                 "doc_count_error_upper_bound": 0
        #                                             },
        #                                             "doc_count": 2779
        #                                         }
        #                                     ],
        #                                     "doc_count_error_upper_bound": 0
        #                                 },
        #                                 "doc_count": 5599
        #                             },
        #                             {
        #                                 "key": "POST",
        #                                 "field_values2": {
        #                                     "sum_other_doc_count": 0,
        #                                     "buckets": [
        #                                         {
        #                                             "key": 1947,
        #                                             "field_values3": {
        #                                                 "sum_other_doc_count": 0,
        #                                                 "buckets": [
        #                                                     {
        #                                                         "key": "/api",
        #                                                         "doc_count": 365
        #                                                     }
        #                                                 ],
        #                                                 "doc_count_error_upper_bound": 0
        #                                             },
        #                                             "doc_count": 365
        #                                         },
        #                                         {
        #                                             "key": 80,
        #                                             "field_values3": {
        #                                                 "sum_other_doc_count": 0,
        #                                                 "buckets": [
        #                                                     {
        #                                                         "key": "/MivtachSimon/AppWebServices/Ribbon.asmx",
        #                                                         "doc_count": 6
        #                                                     }
        #                                                 ],
        #                                                 "doc_count_error_upper_bound": 0
        #                                             },
        #                                             "doc_count": 6
        #                                         }
        #                                     ],
        #                                     "doc_count_error_upper_bound": 0
        #                                 },
        #                                 "doc_count": 371
        #                             }
        #                         ],
        #                         "doc_count_error_upper_bound": 0
        #                     },
        #                     "doc_count": 92527
        #                 }
        #             ],
        #             "doc_count_error_upper_bound": 1359
        #         }
        #     },
        #     "_shards": {
        #         "successful": 43,
        #         "total": 43,
        #         "skipped": 0,
        #         "failed": 0
        #     },
        #     "timed_out": False
        # }

        res_lld = Utils().GenerateLldFromElasticAggrRes(res=res, macro_list=macros_list, agg_on_field=agg_on_field, use_base64=use_base64, queries_to_run=queries_to_run, services_urls=relevant_service_urls, tiny_url_service_details=tiny_url_service_details, config_copy=config_copy)

        res_str = str(json.dumps(res_lld))
        res = "@@RES: " + res_str
        return res

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
        generic_config_url = configurator_base_url + "get_config/" + "smart-onion.config.common.*"
        configurator_response = urllib_req.urlopen(generic_config_url).read().decode('utf-8')
        config_copy = dict(config_copy, **json.loads(configurator_response))
        logging_format = config_copy["smart-onion.config.common.logging_format"]
    except Exception as ex:
        print("WARN: Waiting (indefinetly in 10 sec intervals) for the Configurator service to become available (waiting for queries config)...  (" + str(ex) + " (" + type(ex).__name__ + "))")
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


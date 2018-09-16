#!/usr/bin/python3.5
import re
import sys
import datetime
import json
import base64
from elasticsearch import Elasticsearch
from bottle import route, run, template, get, request
import os
import statsd
import dateutil.parser
import hashlib
import editdistance
import nltk.metrics
from PIL import ImageFont
from PIL import Image
from PIL import ImageDraw
import imagehash


DEBUG=True
elasticsearch_server = "127.0.0.1"
metrics_prefix = "smart-onion"

class QueryNameNotFoundOrOfWrongType(Exception):
    pass

class Utils:
    lst = ""
    perceptive_hashing_algs = ["phash", "ahash", "dhash", "whash"]

    def GenerateLldFromElasticAggrRes(cls, res, macro_list, use_base64, add_doc_count=True, re_sort_by_value=False):
        cls.FlattenAggregates(obj=res["aggregations"], idx=0, add_doc_count=add_doc_count)
#        print(cls.lst)
        cls.lst = cls.lst.strip("|")

        res = {
            "data": []
        }
        for line in cls.lst.split("|"):
            line_element = {}
            idx = 0
            line_as_arr = line.split(",")
            for item_b64 in line_as_arr:
                item = base64.b64decode(item_b64.encode("ascii")).decode("ascii")
                if use_base64:
                    item_parsed = str(base64.b64encode(str(item).encode('ascii')).decode("ascii"))
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

                #Sort res_tmp
                res_tmp_sorted_arr.sort()

                for item in res_tmp_sorted_arr:
                    res_tmp["data"].append({"{#ITEM_" + str(idx) + "}": item})

                #TODO: Add support for multi level lists?

                res = res_tmp
        return res

    def FlattenAggregates(cls, obj, idx=0, add_doc_count=True):
        if "key" in obj:
            if len(cls.lst) > 0:
                if cls.lst[len(cls.lst) - 1] == "|":
                    if idx == 0:
                        cls.lst = cls.lst + str(base64.b64encode(str(obj["key"]).encode('ascii')).decode("ascii"))
                    else:
                        lst_as_arr = cls.lst.split("|")
                        last_line_as_arr = lst_as_arr[len(lst_as_arr) - 2].split(",")
                        for i in range(0, idx - 1):
                            cls.lst = cls.lst + last_line_as_arr[i] + ","
                        cls.lst = cls.lst + str(base64.b64encode(str(obj["key"]).encode('ascii')).decode("ascii"))
                else:
                    cls.lst = cls.lst + "," + str(base64.b64encode(str(obj["key"]).encode('ascii')).decode("ascii"))
            else:
                cls.lst = str(base64.b64encode(str(obj["key"]).encode('ascii')).decode("ascii"))
        if "field_values" + str(idx) in obj and "buckets" in obj["field_values" + str(idx)]:
            for value in obj["field_values" + str(idx)]["buckets"]:
                cls.FlattenAggregates(obj=value, idx=idx + 1, add_doc_count=add_doc_count)
        else:
            if add_doc_count:
                cls.lst = cls.lst + "," + str(base64.b64encode(str(obj["doc_count"]).encode('ascii')).decode("ascii")) + "|"
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


class SmartOnionSampler:
    queries = {}
    es = Elasticsearch(hosts=[elasticsearch_server], timeout=30)
    statsd_client = statsd.StatsClient(prefix=metrics_prefix)

    def __init__(self):
       pass

    def run(self, listen_ip, listen_port, config_file):
        with open(config_file, 'r') as config_file_obj:
            SmartOnionSampler.queries = json.load(config_file_obj)
        if DEBUG:
           run(host=listen_ip, port=listen_port)
        else:
           run(host=listen_ip, port=listen_port, server="gunicorn", workers=32)

    def GetQueryTimeRange(self, query_details):
        if DEBUG:
            now = dateutil.parser.parse("2018-06-12T08:00")
            yesterday = dateutil.parser.parse("2018-06-11T08:00")
            last_month = dateutil.parser.parse("2018-05-11T00:00")
        else:
            now = datetime.datetime.now()
            yesterday = datetime.date.today() - datetime.timedelta(1)
            last_month = datetime.date.today() - datetime.timedelta(months=1)

        time_range = (now - datetime.timedelta(
            seconds=query_details["time_range"])).isoformat() + " TO " + now.isoformat()

        return {
            "yesterday": yesterday,
            "last_month": last_month,
            "now": now,
            "time_range": time_range
        }

    @get('/smart-onion/field-query/<queryname>')
    def fieldQuery(queryname):
        query_details = SmartOnionSampler.queries[queryname]
        if query_details["type"] != "FIELD_QUERY":
            raise QueryNameNotFoundOrOfWrongType()

        now, time_range, yesterday = SmartOnionSampler().GetQueryTimeRange(query_details)
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
                        query_base = query_base.replace("{{#arg" + str(i) + "}}", base64.b64decode(arg.encode("ascii")).decode("ascii"))
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
                            "query": query_string
                        }
                    }
                }

        try:
            res = SmartOnionSampler().es.search(
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
                metric_object = SmartOnionSampler().statsd_client.gauge(metric_name, raw_res)
            except:
                pass

        return res

    @get('/smart-onion/test-similarity/<queryname>')
    def get_similarity(queryname):
        query_details = SmartOnionSampler.queries[queryname]
        if query_details["type"] != "SIMILARITY_TEST":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = SmartOnionSampler().GetQueryTimeRange(query_details)
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
                        query_list_to_test_similarity_to = query_list_to_test_similarity_to.replace("{{#arg" + str(i) + "}}", base64.b64decode(arg.encode("ascii")).decode("ascii"))
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
                    "query": query_list_to_test_similarity_to
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
            res = SmartOnionSampler().es.search(
                index=query_index,
                body=query_list_to_test_similarity_to_query_body
            )

            highest_match_rate = 0
            highest_match_rate_value = ""
            value_compared = ""
            for bucket in res['aggregations']['field_values0']['buckets']:
                cur_value_to_compare_to = bucket["key"]
                edit_distance_match_rate = Utils().LevenshteinDistance(value_to_compare, cur_value_to_compare_to)
                visual_similarity_rate = []

                for p_hashing_alg in Utils.perceptive_hashing_algs:
                    visual_similarity_rate.append(Utils().VisualSimilarityRate(value_to_compare=value_to_compare, cur_value_to_compare_to=cur_value_to_compare_to, algorithm=p_hashing_alg))

                # Create average match rate between all the perceptive hashing algorithms
                match_rate = sum(visual_similarity_rate) / len(visual_similarity_rate)

                # Use the highest match rate detected (either visual similarity or textual similarity
                max_match_rate = 0
                max_match_rate_algorithm = None
                if match_rate > edit_distance_match_rate:
                    max_match_rate = match_rate
                    max_match_rate_algorithm = "visual_match_rate"
                else:
                    max_match_rate = edit_distance_match_rate
                    max_match_rate_algorithm = "edit_distance"
                match_rate = max_match_rate

                if match_rate > highest_match_rate:
                    highest_match_rate = match_rate
                    highest_match_rate_value = cur_value_to_compare_to
                    value_compared = value_to_compare

            res = "@@RES: " + str(highest_match_rate) + "," + highest_match_rate_value + "(" + max_match_rate_algorithm + ")," + value_compared
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        return res

    @get('/smart-onion/get-length/<queryname>')
    def get_length(queryname):
        return str(len(queryname))

    @get('/smart-onion/get-length_b64/<queryname>')
    def get_length_b64(queryname):
        return str(len(base64.b64decode(queryname.encode("utf-8")).decode("utf-8")))

    @get('/smart-onion/query-count/<queryname>')
    def queryCount(queryname):
        query_details = SmartOnionSampler.queries[queryname]
        if query_details["type"] != "QUERY_COUNT":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = SmartOnionSampler().GetQueryTimeRange(query_details)
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
                        query_base = query_base.replace("{{#arg" + str(i) + "}}", base64.b64decode(arg.encode("ascii")).decode("ascii"))
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
                            "query": query_string
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
                        "query": query_string
                    }
                }

            }

        try:
            print("Running query: `" + json.dumps(query_body) + "'")
            raw_res = ""
            if count_unique_values_in != None:
                res = SmartOnionSampler().es.search(
                    index=query_index,
                    body=query_body
                )
                raw_res = res['aggregations']["unique_count"]["value"]
            else:
                res = SmartOnionSampler().es.count(
                    index=query_index,
                    body=query_body
                )
                raw_res = res['count']

            res = "@@RES: " + str(raw_res)
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        if Utils().is_number(raw_res):
            try:
                metric_object = SmartOnionSampler().statsd_client.gauge(metric_name, raw_res)
            except:
                pass

        return res

    @get('/smart-onion/list-hash/<queryname>')
    def list_hash(queryname):
        query_details = SmartOnionSampler.queries[queryname]
        if query_details["type"] != "LIST_HASH":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = SmartOnionSampler().GetQueryTimeRange(query_details)
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
                            "query": query_string
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
            res = SmartOnionSampler().es.search(
                index=query_index,
                body=query_body
            )

            res_lld = Utils().GenerateLldFromElasticAggrRes(res=res, macro_list=[], use_base64=False, add_doc_count=False, re_sort_by_value=True)
            res_str = str(hashlib.sha1(json.dumps(res_lld).encode("utf-8")).hexdigest())

            res = "@@RES: " + res_str
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        return res

    @get('/smart-onion/discover/<queryname>')
    def discover(queryname):
        query_details = SmartOnionSampler.queries[queryname]
        if query_details["type"] != "LLD":
            raise QueryNameNotFoundOrOfWrongType()

        time_range_args = SmartOnionSampler().GetQueryTimeRange(query_details)
        yesterday = time_range_args["yesterday"]
        now = time_range_args["now"]
        time_range = time_range_args["time_range"]
        query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
        query_base = query_details["query"].replace("{{#query_name}}", queryname)
        agg_on_field = query_details["agg_on_field"].replace("{{#query_name}}", queryname)
        zabbix_macro = query_details["zabbix_macro"].replace("{{#query_name}}", queryname)
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
                            "query": query_string
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
            res = SmartOnionSampler().es.search(
                index=query_index,
                body=query_body
            )

            macros_list = str.split(zabbix_macro, ",")
            res_lld = Utils().GenerateLldFromElasticAggrRes(res, macros_list,use_base64)

            res_str = str(json.dumps(res_lld))
            if encode_json_as_b64:
                res_str = str(base64.b64encode(str(res_str).encode('ascii')).decode("ascii"))
            res = "@@RES: " + res_str
        except Exception as e:
            res = "@@RES: @@EXCEPTION: " + str(e)

        return res

    @get('/smart-onion/dump_queries')
    def dump_queries():
        return "@@RES: " + json.dumps(SmartOnionSampler.queries)

    @get('/test/similarity/<algo>/<s1>/<s2>')
    def test_similarity(algo, s1, s2):
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

script_path = os.path.dirname(os.path.realpath(__file__))
listen_ip = "127.0.0.1"
listen_port = 8080
config_file_name = "queries.json"
config_file_default_path = "/etc/smart-onion/"
config_file = os.path.join(config_file_default_path, config_file_name)
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
                    print("EEROR: The --listen-ip must be a valid IPv4 address.  Using default of " + str(elasticsearch_server) + " (hardcoded)")
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
SmartOnionSampler().run(listen_ip=listen_ip, listen_port=listen_port, config_file=config_file)


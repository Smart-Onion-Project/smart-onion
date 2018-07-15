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

DEBUG=True
elasticsearch_server = "127.0.0.1"
metrics_prefix = "smart-onion"

class QueryNameNotFoundOrOfWrongType(Exception):
    pass

class Utils:
    lst = ""

    def GenerateLldFromElasticAggrRes(cls, res, macro_list, use_base64):
        cls.FlattenAggregates(res["aggregations"])
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
                        line_element["{#_DOC_COUNT}"] = item_parsed
                    else:
                        line_element["{#ITEM_" + str(idx) + "}"] = item_parsed
                else:
                    line_element["{#" + macro_list[idx] + "}"] = item_parsed
                idx = idx + 1
            res["data"].append(line_element)
        return res

    def FlattenAggregates(cls, obj, idx=0):
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
                cls.FlattenAggregates(value, idx + 1)
        else:
            cls.lst = cls.lst + "," + str(base64.b64encode(str(obj["doc_count"]).encode('ascii')).decode("ascii")) + "|"
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
            now = dateutil.parser.parse("2018-06-12T00:00")
            yesterday = dateutil.parser.parse("2018-06-11T00:00")
        else:
            now = datetime.datetime.now()
            yesterday = datetime.date.today() - datetime.timedelta(1)

        time_range = (now - datetime.timedelta(
            seconds=query_details["time_range"])).isoformat() + " TO " + now.isoformat()

        return {
            "yesterday": yesterday,
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
            print("Running query: `" + query_string + "'")
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


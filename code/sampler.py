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


elasticsearch_server = "10.10.10.10"
metrics_prefix = "smart-onion"

class QueryNameNotFoundOrOfWrongType(Exception):
    pass

class Utils:
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
         run(host=listen_ip, port=listen_port, server="gunicorn", workers=32)


     @get('/smart-onion/field-query/<queryname>')
     def fieldQuery(queryname):
         query_details = SmartOnionSampler.queries[queryname]
         if query_details["type"] != "FIELD_QUERY":
             raise QueryNameNotFoundOrOfWrongType()

         now = datetime.datetime.now()
         yesterday = datetime.date.today() - datetime.timedelta(1)
         time_range = (now.utcnow() - datetime.timedelta(seconds=query_details["time_range"])).isoformat() + " TO " + now.utcnow().isoformat()
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

         now = datetime.datetime.now()
         yesterday = datetime.date.today() - datetime.timedelta(1)
         time_range = (now.utcnow() - datetime.timedelta(seconds=query_details["time_range"])).isoformat() + " TO " + now.utcnow().isoformat()
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

         now = datetime.datetime.now()
         yesterday = datetime.date.today() - datetime.timedelta(1)
         time_range = (now.utcnow() - datetime.timedelta(seconds=query_details["time_range"])).isoformat() + " TO " + now.utcnow().isoformat()
         query_index = query_details["index_pattern"].replace("%{today}", now.strftime("%Y.%m.%d")).replace("%{yesterday}", yesterday.strftime("%Y.%m.%d"))
         query_base = query_details["query"].replace("{{#query_name}}", queryname)
         agg_on_field = query_details["agg_on_field"].replace("{{#query_name}}", queryname)
         zabbix_macro = query_details["zabbix_macro"].replace("{{#query_name}}", queryname)
         use_base64 = query_details["use_base64"]
         for i in range(1, 10):
             if "{{#arg" + str(i) + "}}" in str(query_base):
                 arg = request.query["arg" + str(i)]
                 if len(str(arg).strip()) != 0:
                     query_base = query_base.replace("{{#arg" + str(i) + "}}", arg)
             else:
                 break


         query_string = "(" + query_base + ") AND @timestamp:[" + time_range + "]"
         query_body = {
                     "size": 0,
                     "query":{
                         "query_string": {
                             "query": query_string
                         }
                     },
                     "aggs": {
                         "field_values": {
                              "terms": { "field": agg_on_field }
                         }
                     }
                 }

         try:
             res = SmartOnionSampler().es.search(
                 index=query_index,
                 body=query_body
             )
             raw_res = res["aggregations"]["field_values"]["buckets"]
             res = {"data":[]}
             for item in raw_res:
                 if use_base64:
                      value = base64.b64encode(item["key"].encode('ascii')).decode("ascii")
                 else:
                      value = item["key"]
                 res["data"].append({"{#" + zabbix_macro + "}": value})
             res = "@@RES: " + str(json.dumps(res))
         except Exception as e:
             raw_res = ""
             res = "@@RES: @@EXCEPTION: " + str(e)

         if Utils().is_number(raw_res):
             try:
                 metric_object = SmartOnionSampler().statsd_client.gauge(metric_name, raw_res)
             except:
                 pass

         return res

     @get('/smart-onion/dump_queries')
     def dump_queries():
         return "@@RES: " + json.dumps(SmartOnionSampler.queries)

script_path = os.path.dirname(os.path.realpath(__file__))
listen_ip = "127.0.0.1"
listen_port = 8080
config_file_name = "queries.conf"
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
                print("USAGE: " + os.path.basename(os.path.realpath(__file__)) + " [--listen-ip=127.0.0.1 --listen-port=8080 --config-file=/etc/smart-onion/queries.conf]")
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


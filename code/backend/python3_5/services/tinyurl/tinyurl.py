#!/usr/bin/python3.5
##########################################################################
# Tiny URL                                                               #
# --------                                                               #
#                                                                        #
# This service is part of the Smart-Onion package. This micro-service is #
# responsible for converting long URLs to tiny URLs.                     #
#                                                                        #
#                                                                        #
##########################################################################

from bottle import Bottle, request, redirect, abort
import base64
import uuid
import os
import sys
import re
import json
import time
import threading
from urllib import request as urllib_req
from threading import Lock
import hashlib

DEBUG = False
auto_save_dictionary_lock = Lock()


class TinyUrl:

    def __init__(self, config_object):
        self._time_loaded = time.time()
        self._config = config_object
        self._host = self._config["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-host"]
        self._port = int(self._config["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-port"])
        self._backup_file = self._config["smart-onion.config.architecture.internal_services.backend.tiny_url.backup_file"]
        self._backup_interval = int(self._config["smart-onion.config.architecture.internal_services.backend.tiny_url.backup_interval"])
        self._key2url_dictionary = {}
        self._url2key_dictionary = {}
        self._app = Bottle()
        self._run_backup_loop = True
        self._route()

    def _route(self):
        self._app.route('/so/tiny2url/<tiny>', method="GET", callback=self.tiny_to_url)
        self._app.route('/so/url2tiny', method="GET", callback=self.url_to_tiny)
        self._app.route('/so/tiny/<url_category>/<url_subcategory>/<tiny>', method="GET", callback=self.proxy_by_tiny)
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

    def url_to_tiny(self):
        if "url" in request.query:
            url_b64 = request.query["url"]
        else:
            return ""
        
        url_to_shorten = base64.b64decode(str(url_b64).encode('utf-8')).decode('utf-8')
        url_category = url_to_shorten.split('/')[3]
        url_sub_category = url_to_shorten.split('/')[4].split('?')[0]
        auto_save_dictionary_lock.acquire()
        url_in_url2key_dictionary = url_to_shorten in self._url2key_dictionary
        auto_save_dictionary_lock.release()
        if url_in_url2key_dictionary:
            auto_save_dictionary_lock.acquire()
            url_key = self._url2key_dictionary[url_to_shorten]
            auto_save_dictionary_lock.release()
        else:
            url_key = str(uuid.uuid4())
            auto_save_dictionary_lock.acquire()
            self._key2url_dictionary[url_key] = url_to_shorten
            self._url2key_dictionary[url_to_shorten] = url_key
            auto_save_dictionary_lock.release()
        return "so/tiny/" + url_category + "/" + url_sub_category + "/" + url_key

    def url_to_tiny_bulk(self, url_b64_bulk):
        pass

    def tiny_to_url(self, tiny):
        url = ""

        auto_save_dictionary_lock.acquire()
        if tiny in self._key2url_dictionary:
            url = self._key2url_dictionary[tiny]
        auto_save_dictionary_lock.release()

        return url

    def proxy_by_tiny(self, url_category, url_subcategory, tiny):
        url = self.tiny_to_url(tiny)
        if url != "":
            # TODO: ERROR - Missing protocol, host and port in the URL
            return urllib_req.urlopen(url).read().decode('utf-8')
        else:
            abort(404, "Url key could not be found.")

    def auto_save_state(self):
        while self._run_backup_loop:
            try:
                auto_save_dictionary_lock.acquire()
                json_to_save = json.dumps(indent=2, obj={
                    "_url2key_dictionary": self._url2key_dictionary,
                    "_key2url_dictionary": self._key2url_dictionary})
                auto_save_dictionary_lock.release()
                with open(self._backup_file, "w") as backup_file:
                    backup_file.write(json_to_save)
            except Exception as ex:
                print("WARN: Could not backup the in-memory database to file (" + str(ex) + "). The next time this service will be invoked ALL url's will become invalid unless the in-memory database will be successfuly saved in the next attempt.")
            time.sleep(self._backup_interval)

    def auto_load_state(self):
        if os.path.isfile(self._backup_file):
            with open(self._backup_file) as backup_file:
                backup_obj_json_string = backup_file.read()
            backup_obj = json.loads(backup_obj_json_string)
            auto_save_dictionary_lock.acquire()
            self._key2url_dictionary = backup_obj["_key2url_dictionary"]
            self._url2key_dictionary = backup_obj["_url2key_dictionary"]
            auto_save_dictionary_lock.release()

    def run(self):
        self.auto_load_state()
        self._backup_thread = threading.Thread(target=self.auto_save_state)
        self._backup_thread.start()
        if DEBUG:
            self._app.run(host=self._host, port=self._port)
        else:
            self._app.run(host=self._host, port=self._port, server="paste")

        self._run_backup_loop = False


script_path = os.path.dirname(os.path.realpath(__file__))
config_file_default_path = "/etc/smart-onion/"
settings_file_name = "tiny_url_settings.json"
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

try:
    # Contact configurator to fetch all of our config and configure listen-ip and port
    configurator_base_url = str(configurator_proto).strip() + "://" + str(configurator_host).strip() + ":" + str(configurator_port).strip() + "/smart-onion/configurator/"
    configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.architecture.internal_services.backend.tiny_url.*"
    configurator_response = urllib_req.urlopen(configurator_final_url).read().decode('utf-8')
    config_copy = json.loads(configurator_response)
    listen_ip = config_copy["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-host"]
    listen_port = int(config_copy["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-port"])
except:
    config_copy = {
        "smart-onion.config.architecture.internal_services.backend.tiny_url.listening-host": "127.0.0.1",
        "smart-onion.config.architecture.internal_services.backend.tiny_url.listening-port": 9999,
        "smart-onion.config.architecture.internal_services.backend.tiny_url.backup_file": "/tmp/tiny_url.json.db",
        "smart-onion.config.architecture.internal_services.backend.tiny_url.backup_interval": 30
    }
    listen_ip = config_copy["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-host"]
    listen_port = config_copy["smart-onion.config.architecture.internal_services.backend.tiny_url.listening-port"]
    print("WARN: No valid configuration could be pulled from the configurator. Using hard-coded defaults of " + listen_ip + ":" + str(listen_port))

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
        else:
            if arg == "--help" or arg == "-h" or arg == "/h" or arg == "/?":
                print("USAGE: " + os.path.basename(os.path.realpath(__file__)) + " [--listen-ip=127.0.0.1 --listen-port=8080 --config-file=/etc/smart-onion/queries.json]")
                print("")
                print("-h, --help, /h and /q will print this help screen.")
                print("")
                quit(1)


sys.argv = [sys.argv[0]]
TinyUrl(config_object=config_copy).run()

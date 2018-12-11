#!/usr/bin/python2.7
import sys
import os
import re
import urllib
from bottle import route, run, template, get, post, request
from bottle import Bottle
import datetime
import json
import base64


DEBUG = True


class SmartOnionAlerter:

    def __init__(self, listen_ip, listen_port):
        self._host = listen_ip
        self._port = listen_port
        self._app = Bottle()
        self._route()

    def _route(self):
        self._app.route('/smart-onion/alerter/report_alert', method="POST", callback=self.report_alert)

    def run(self):
        if DEBUG:
            self._app.run(host=self._host, port=self._port)
        else:
            self._app.run(host=self._host, port=self._port, server="gunicorn", workers=32)

    def report_alert(self):
        #This method should expect to receive a JSON (as an argument in the POST vars) with all the details of the metrics that triggered this alert
        #This method should then search for relevant details for the specific alert that has been triggered and decide whether or not to create a case for
        #the relevant event.
        #
        #Score = highest_base_family_score + pattern_predictability_as_true_positive - pattern_predictability_by_htm - pattern_predictability_as_false_positive
        #Algorithm for detecting priority for each event - Should run after each event is added to the list:
        # - Divide the highest score on the list by 5 => x
        # - Divide each event's score by x => event_priority
        pass


script_path = os.path.dirname(os.path.realpath(__file__))
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

try:
    # Contact configurator to fetch all of our config and configure listen-ip and port
    configurator_base_url = str(configurator_proto).strip() + "://" + str(configurator_host).strip() + ":" + str(configurator_port).strip() + "/smart-onion/configurator/"
    configurator_final_url = configurator_base_url + "get_config/" + "smart-onion.config.architecture.internal_services.backend.*"
    configurator_response = urllib.urlopen(configurator_final_url).read().decode('utf-8')
    config_copy = json.loads(configurator_response)
    listen_ip = config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-host"]
    listen_port = config_copy["smart-onion.config.architecture.internal_services.backend.alerter.listening-port"]
except:
    listen_ip = "127.0.0.1"
    listen_port = 9004

if len(sys.argv) > 1:
    for arg in sys.argv:
        if "=" in arg:
            arg_name = arg.split("=")[0]
            arg_value = arg.split("=")[1]

            if arg_name == "--listen-ip":
                if not re.match("[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+", arg_value):
                    print("ERROR: The --listen-ip must be a valid IPv4 address.  Using default of " + str(listen_ip) + " (hardcoded)")
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
SmartOnionAlerter(listen_ip=listen_ip, listen_port=listen_port).run()


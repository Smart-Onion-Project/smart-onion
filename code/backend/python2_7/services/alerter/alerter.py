#!/usr/bin/python3.5
import sys
from bottle import route, run, template, get, post, request
import datetime
import json
import base64


DEBUG = True


class SmartOnionAlerter:
    def __init__(self):
       pass

    def run(self, listen_ip, listen_port):
        if DEBUG:
           run(host=listen_ip, port=listen_port)
        else:
           run(host=listen_ip, port=listen_port, server="gunicorn", workers=32)

    @post('/smart-onion/alerter/report_alert')
    def report_alert():
        #This method should expect to receive a JSON (as an argument in the POST vars) with all the details of the metrics that triggered this alert
        #This method should then search for relevant details for the specific alert that has been triggered and decide whether or not to create a case for
        #the relevant event.
        #
        #Score = highest_base_family_score + pattern_predictability_as_true_positive - pattern_predictability_by_htm - pattern_predictability_as_false_positive
        #Algorithm for detecting priority for each event - Should run after each event is added to the list:
        # - Divide the highest score on the list by 5 => x
        # - Divide each event's score by x => event_priority
        pass
#!/usr/bin/python3.5
import sys
from bottle import route, run, template, get, post, request
import datetime
import json
import base64


DEBUG = True


class SmartOnionConfigurator:
    def __init__(self):
       pass

    def run(self, listen_ip, listen_port):
        if DEBUG:
           run(host=listen_ip, port=listen_port)
        else:
           run(host=listen_ip, port=listen_port, server="gunicorn", workers=32)

    @get('/smart-onion/configurator/get_config/<config_name>')
    def get_config(config_name):
        pass

    @post('/smart-onion/configurator/update_config/<config_name>')
    def update_config(config_name):
        #This method should expect to receive the requested config value and a proof of an authentication
        pass
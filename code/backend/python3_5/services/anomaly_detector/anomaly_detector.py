#!/usr/bin/python3.5
##########################################################################
# Anomaly Detector                                                       #
# ----------------                                                       #
#                                                                        #
# This service is part of the Smart-Onion package. This micro-service is #
# responsible for detecting anomalies in metrics stored in whisper-files #
# based on statistical and heuristic approach.                           #
#                                                                        #
#                                                                        #
#                                                                        #
#                                                                        #
##########################################################################

import subprocess
import datetime
import time
import json
import sys
import os
from bottle import route, run, template, get, post, request


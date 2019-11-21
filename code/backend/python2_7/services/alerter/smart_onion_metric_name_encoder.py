############################################################
# Smart-Onion                                              #
# -----------                                              #
#                                                          #
# This file should encode a jewish date timestamp. It is   #
# intended to be used by the alerter service to encode the #
# the pattern starts and the time the pattern ends         #
# according to the jewish calendar to allow the system to  #
# predict events that occur according to the jewish        #
# calendar such as religous and national holidays.         #
############################################################

import numpy
from nupic.data import SENTINEL_VALUE_FOR_MISSING_DATA
from nupic.encoders.base import Encoder


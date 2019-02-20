import sys
import os

#if sys.version_info[0]<3:       # require python3
# raise Exception("Python3 required! Current (wrong) version: '%s'" % sys.version_info)

sys.path.insert(0, os.environ["OPUSAPI_PATH"])
from opusapi import app as application

"""
    This file contains a number of utilities for processing CSV files
"""

import os
import pandas as pd
from datetime import datetime

def listFiles(importPath):
    fileList = [f for f in os.listdir(importPath) if os.path.isfile(os.path.join(importPath, f))]
    return fileList

def validateFile(f):
    # Is it a CSV file?
    if f.endswith('.csv') == False:
        return False
    
    # Correct numer of rows?
    # Blah blah blah
    return True

def dumpFailedMessages(path,msgs):
    filename_as_time = datetime.now().strftime("%Y-%m-%d.%H.%M") + ".log"
    f = open(path+filename_as_time,"a")
    f.write(msgs)
    f.close()

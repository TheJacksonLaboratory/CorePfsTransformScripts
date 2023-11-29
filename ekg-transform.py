# -*- coding: utf-8 -*-
"""
Created on August 13, 2023

This module takes one or more EKG files produced
by the X machine and transforms them into a single input file 
suitable for importing into CORE PFS

A detailed algorithm can be found in the document 
Algorithm for EKG transform.docs in this folder.

Example:
python ekg-transform.py -b \\jax\jax\phenotype\EKG-V2\KOMP-UAT\  -i images\ -s transformData\ -l failed\ -d data\

@author: michaelm
"""
import pandas as pd
import csv
import shutil
from datetime import datetime
import platform
import os
from os import listdir
from os import replace
from os.path import basename, splitext, dirname
import argparse
import sys

import transform_functions as tf
import galaxy_db as gdb

basePath = ''
imagePath = ''
srcPath = ''
destPath = ''  # current working directory
logPath = ''

derivedColumnsKomp = [
'EXPT_SAMPLE_BARCODE' , # Filename
'Waveform: Full Trace' , # path to image folder + filename + '-RAW.pdf'
'Waveform: Ensemble Avg', # path to image folder + filename + '-AVERAGE.pdf'
'Tester Name',
'Comments'  # No incoming value
]

derivedColumnsCba = [
'Mouse ID' , # Filename
'Comments'  # No incoming value
]

# Each tuple is destination column, source column, row where data is found
dstSrcColMapKomp = [
('Test Date','TimeDate',0),
('RR Interval (ms)','RR Interval (ms)',3),
('HR (bpm)','Heart Rate (BPM)',3),
('PR Interval (ms)','PR Interval (ms)',3),
('P Duration (ms)','P Duration (ms)',3),
('QRS Interval (ms)','QRS Interval (ms)',3),
('QT Interval (ms)','QT Interval (ms)',3),
('QTc (Mitchell) (ms)','QTc (ms)',3),
('JT Interval (ms)','JT Interval (ms)',3),
('Tpeak Tend Interval (ms)','Tpeak Tend Interval (ms)',3),
('P Amplitude (mV)','P Amplitude (mV)',3),
('Q Amplitude (mV)','Q Amplitude (mV)',3),
('R Amplitude (mV)','R Amplitude (mV)',3),
('S Amplitude (mV)','S Amplitude (mV)',3),
('ST Height (mV)','ST Height (mV)',3),
('T Amplitude (mV)','T Amplitude (mV)',3),
('First Beat','First Beat',3),
('Last Beat','Last Beat',3),
('Number of signals','Used',0),
('Edited','Edited',3)
]

dstSrcColMapCba = [
#('Test Time','TimeDate',0),
('RR Interval (msec)','RR Interval (ms)',3),
('Heart Rate (bpm)','Heart Rate (BPM)',3),
('PR Interval (msec)','PR Interval (ms)',3),
('P Duration (msec)','P Duration (ms)',3),
('QRS Interval (msec)','QRS Interval (ms)',3),
('QT Interval (msec)','QT Interval (ms)',3),
('QTc Interval (msec)','QTc (ms)',3),
('JT Interval (msec)','JT Interval (ms)',3),
('Tpeak Tend Interval (msec)','Tpeak Tend Interval (ms)',3),
('P Amplitude (mV)','P Amplitude (mV)',3),
('Q Amplitude (mV)','Q Amplitude (mV)',3),
('R Amplitude (mV)','R Amplitude (mV)',3),
('S Amplitude (mV)','S Amplitude (mV)',3),
('ST Height (mV)','ST Height (mV)',3),
('T Amplitude (mV)','T Amplitude (mV)',3)
#('First Beat','First Beat',3),
#('Last Beat','Last Beat',3),
#('Number of signals','Used',0),
#('Edited','Edited',3)
]


def add_arguments(argparser):
    argparser.add_argument(
            '-b', '--base', type=str, help='Base directory files', required=True
        )
    argparser.add_argument(
            '-s', '--source', type=str, help='Source directory for raw files', required=True
        )
    argparser.add_argument(
            '-d', '--destination', type=str, help='Destination directory for processed file', required=True
        )
    argparser.add_argument(
            '-l', '--log', type=str, help='Destination directory for error log', required=True
        )
    argparser.add_argument(
            '-i', '--images', type=str, help='Images folder', required=False
        )
        
    args = argparser.parse_args()
    
    global basePath
    global imagePath
    global srcPath
    global destPath
    global logPath
    
    basePath = args.base
    imagePath = basePath + args.images
    srcPath = basePath + args.source
    destPath = basePath + args.destination + datetime.now().strftime("%Y-%m-%d.%H.%M") + ".txt"   # TODO - better filename
    logPath = basePath + args.log
    
    """ print(basePath)
    print(imagePath)
    print(srcPath)
    print(destPath)
    print(logPath) """
    
def addDerivedColumns(row,f):
    # 
    global imagePath
    for key in derivedColumns:
        if key == 'EXPT_SAMPLE_BARCODE':
            if isKompTransform:
                row[key] = splitext(basename(f))[0]
            else:
                idx = f.index('.dat')
                s = f[idx-12:idx]
                row[key] = gdb.getOriginalFilename(s)
        elif key == 'Mouse ID':
            if isKompTransform:
                row[key] = splitext(basename(f))[0]
            else:
                idx = f.index('.dat')
                s = f[idx-12:idx]
                row[key] = gdb.getOriginalFilename(s)
        elif key == 'Waveform: Full Trace':
            row[key] = imagePath + splitext(basename(f))[0] + '-RAW.pdf'
        elif key == 'Waveform: Ensemble Avg':
            row[key] = imagePath + splitext(basename(f))[0] + '-AVERAGE.pdf'
        elif key == 'Tester Name':
            row[key] = 'Not in raw file'
        elif key == 'Comments':
            row[key] = 'Not in raw file'

def listFiles(importPath):
    fileList = [f for f in os.listdir(importPath) if os.path.isfile(os.path.join(importPath, f))]
    return fileList

def validateFile(f):
    # Is it a CSV file?
    if f.endswith('.csv') == False and f.endswith('.txt') == False and f.endswith('.dat') == False:
        return False
    
    # Correct numer of rows?
    # Blah blah blah
    return True

def dumpFailedMessages(msgs):
    global logPath
    filename_as_time = datetime.now().strftime("%Y-%m-%d.%H.%M") + ".log"
    f = open(logPath+filename_as_time,"a")
    f.write(msgs + '\n')
    f.close()

def parseEkgFile(path):
    # If this is a valid EKG file, parse it and return a single row of CSV values
    global basePath
    try:
        returnRow = {}
        if validateFile(path) == True:
            df = pd.read_csv(path)  # TODO - Handle TSV input, too
            # Build up dictionary for destination 
            for tup in dstSrcColMap:
                if tup[0] == 'Test Date':
                   returnRow[tup[0]] = tf. corePfsDateTimeKluge(df.iloc[tup[2]][tup[1]].split(' ',1)[0])
                else:
                   returnRow[tup[0]] = df.iloc[tup[2]][tup[1]]
                
            # Add derived columns
            addDerivedColumns(returnRow,basename(path))
        else:
            dumpFailedMessages("File {0} is not valid.".format(path))
    except Exception as e:
        dumpFailedMessages('ERROR: ' + path + ' ' + str(e))
        raise
    
    return returnRow

def mainGalaxy():
    
    # This is used in the standalone version. In Galaxy it will get the files from the command line.
    # argv[0] is python file name
    # argv[1] is comma separated list of input files
    # argv[2] isoutput file
    
    if len(sys.argv) < 3:
        print("Usage <module name>, <comma separated filelist>, <output file>")
        exit()

    filelist = sys.argv[1].split(',')
    destPath = sys.argv[2]
    global logPath
    logPath = '.'
    
    try:
        # Parse each file into a single CSV string (one per mouse) for the results..
        rowlist = []
        for f in filelist:
            rowlist.append(parseEkgFile(f))
            
        
        # Write the header then the data
        if len(rowlist) > 0:
            with open(destPath,'w',newline='') as csvfile:
                writer = csv.DictWriter(csvfile,fieldnames=rowlist[0].keys(),delimiter='\t')
                writer.writeheader()
                writer.writerows(rowlist)
        
    except Exception as e:
        dumpFailedMessages(str(e))
        
        
def mainStandAlone():
    
    global srcPath
    global destPath
    
    # This is used in the standalone version. 
    args = argparse.ArgumentParser()
    add_arguments(args)
    filelist = listFiles(srcPath)
    filename = ''
    try:
        # Parse each file into a single CSV string.
        rowlist = []
        for f in filelist:
            filename = srcPath+f
            rowlist.append(parseEkgFile(filename))
            
        # Write the header then the data
        if len(rowlist) > 0:
            with open(destPath,'w',newline='') as csvfile:
                writer = csv.DictWriter(csvfile,fieldnames=rowlist[0].keys(),delimiter='\t')
                writer.writeheader()
                writer.writerows(rowlist)
        
            # Move the raw files to the archive folder.
            # Only used in the standalone version
            for f in filelist:
                shutil.move((srcPath+f),(srcPath+"archive/"+f))
            
    except Exception as e:
        dumpFailedMessages(filename + ' : ' + str(e))
        
        
def main():
    
    # TDB - How will I know? argv[0]?
    isKompTransform = 'KOMP' in sys.argv
    isStandAlone = platform.system() == 'Windows'

    global dstSrcColMap
    global derivedColumns
    if isKompTransform:
        dstSrcColMap = dstSrcColMapKomp
        derivedColumns = derivedColumnsKomp
    else:
        dstSrcColMap = dstSrcColMapCba
        derivedColumns = derivedColumnsCba
        
    if(isStandAlone):
        mainStandAlone()
    else:
        mainGalaxy()
                  
if __name__ == '__main__':
    main()

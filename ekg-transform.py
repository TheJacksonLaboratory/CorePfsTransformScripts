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
import os
from os import listdir
from os import replace
from os.path import basename, splitext, dirname
import argparse

import transform_functions as tf

basePath = ''
imagePath = ''
srcPath = ''
destPath = ''
logPath = ''

derivedColumns = [
'EXPT_SAMPLE_BARCODE' , # Filename
'Waveform: Full Trace' , # path to image folder + filename + '-RAW.pdf'
'Waveform: Ensemble Avg', # path to image folder + filename + '-AVERAGE.pdf'
'Tester Name'
]

# Each tuple is destination column, source column, row where data is found
dstSrcColMap = [
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
            row[key] = splitext(basename(f))[0]
        elif key == 'Waveform: Full Trace':
            row[key] = imagePath + splitext(basename(f))[0] + '-RAW.pdf'
        elif key == 'Waveform: Ensemble Avg':
            row[key] = imagePath + splitext(basename(f))[0] + '-AVERAGE.pdf'
        elif key == 'Tester Name':
            row[key] = 'Not in raw file'

def listFiles(importPath):
    fileList = [f for f in os.listdir(importPath) if os.path.isfile(os.path.join(importPath, f))]
    return fileList

def validateFile(f):
    # Is it a CSV file?
    if f.endswith('.csv') == False and f.endswith('.txt') == False:
        return False
    
    # Correct numer of rows?
    # Blah blah blah
    return True

def dumpFailedMessages(msgs):
    filename_as_time = datetime.now().strftime("%Y-%m-%d.%H.%M") + ".log"
    f = open(logPath+filename_as_time,"a")
    f.write(msgs)
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
        dumpFailedMessages(str(e))
        raise
    
    # After the file is parsed successfully it is moved to the achive folder
    # shutil.move(path, dirname(path) + "\\archive\\" + basename(path) )
    return returnRow

def main():
    args = argparse.ArgumentParser()
    add_arguments(args)
    # Get the list of raw files from the source folder
    #filelist = local_utils.listFiles(args.source)
    
    global basePath
    global imagePath
    global srcPath
    global destPath
    global logPath

    #basePath = '\\\\jax\\jax\\phenotype\\EKG-V2\\KOMP-UAT\\'
    #srcPath = basePath + 'transformData\\'
    #imagePath = basePath + 'images\\'
    #destPath = basePath + 'data\\temp.txt'
    #logPath = basePath + 'failed\\'
    
    filelist = listFiles(srcPath)
    
    try:
    
        # Parse each file into a single CSV string.
        rowlist = []
        for f in filelist:
            rowlist.append(parseEkgFile(srcPath+f))
            
        
        # Write the header then the data
        with open(destPath,'w',newline='') as csvfile:
            writer = csv.DictWriter(csvfile,fieldnames=rowlist[0].keys(),delimiter='\t')
            writer.writeheader()
            writer.writerows(rowlist)
        
        # Move the raw files to the archive folder.
        for f in filelist:
            replace((srcPath+f),(srcPath+"archive/"+f))
            
    except Exception as e:
        dumpFailedMessages(str(e))
        
if __name__ == '__main__':
    main()

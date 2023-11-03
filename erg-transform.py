# -*- coding: utf-8 -*-
"""
Created on August 13, 2023

This module takes one or more EKG files produced
by the X machine and transforms them into a single input file 
suitable for importing into CORE PFS

A detailed algorithm can be found in the document 
Algorithm for EKG transform.docs in this folder.

@author: michaelm
"""
import pandas as pd
import csv
import shutil
from datetime import datetime
from os import listdir
from os import path
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
'EXPT_SAMPLE_BARCODE' 
]

destColumns = [ # newcolumn name, row, column
('Tester Name',13,7),
('Protocol',3,7),
('Stimulator',5,7),
('Test Date',6,7),
('Steps',7,7),
('Channels',8,7),
('Mouse ID',9,7),
('C-Wave (cd.s/m²)',3,35),
('Photopic ERG (cd.s/m²)',4,35),
('RE-a (uV) [Scotopic]',2,20),
('RE-b (uV) [Scotopic]',3,20),
('RE-c (uV) [Scotopic]',4,20),
('LE-a (uV) [Scotopic]',6,20),
('LE-b (uV) [Scotopic]',7,20),
('LE-c (uV) [Scotopic]',8,20),
('RE-a (uV) [Photopic]',18,20),
('RE-b (uV) [Photopic]',19,20),
('LE-a (uV) [Photopic]',20,20),
('LE-b (uV) [Photopic]',21,20),
('RE-a (ms) [Scotopic]',2,21),
('RE-b (ms) [Scotopic]',3,21),
('RE-c (ms) [Scotopic]',4,21),
('LE-a (ms) [Scotopic]',6,21),
('LE-b (ms) [Scotopic]',7,21),
('LE-c (ms) [Scotopic]',8,21),
('RE-a (ms) [Photopic]',18,21),
('RE-b (ms) [Photopic]',19,21),
('LE-a (ms) [Photopic]',20,21),
('LE-b (ms) [Photopic]',21,21),
('RE-FO-like (uV) [Scotopic]',5,20),
('RE-FO-like (ms) [Scotopic]',5,21),
('LE-FO-like (uV) [Scotopic]',9,20),
('LE-FO-like (ms) [Scotopic]',9,21)
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
    """
    print(basePath)
    print(imagePath)
    print(srcPath)
    print(destPath)
    print(logPath)
    """
    
    
def addDerivedColumns(row,f):
    # 
    for key in derivedColumns:
        if key == 'EXPT_SAMPLE_BARCODE':
            row[key] = splitext(basename(f))[0]

def listFiles(importPath):
    fileList = [f for f in listdir(importPath) if path.isfile(path.join(importPath, f))]
    return fileList

def validateFile(f):
    # Is it a CSV file?
    if f.lower().endswith('.csv') == False and f.lower().endswith('.txt') == False:
        return False
    
    # Correct numer of rows?
    # Blah blah blah
    return True

def dumpFailedMessages(msgs):
    filename_as_time = datetime.now().strftime("%Y-%m-%d.%H.%M") + ".log"
    f = open(logPath+filename_as_time,"a")
    f.write(msgs)
    f.close()

def parseErgFile(path):
    # If this is a valid EKG file, parse it and return a single row of CSV values
    returnRow = {}
    
    try:
        if validateFile(path) == True:
            # Read in all rows and convert to list so that we have a list of lists
            fileAsList = []
            with open(path) as inputfile:   # TODO make Pythonic
                while line := inputfile.readline():
                    #line = line.rstrip()
                    lineAsList = line.split('\t')  # TSV
                    fileAsList.append(lineAsList) 
            # So now we have a list of lists
            for tup in destColumns:
                if tup[0] == 'Test Date':
                   returnRow[tup[0]] = tf. corePfsDateTimeKluge(fileAsList[tup[1]][tup[2]].split(' ',1)[0])
                else:
                   returnRow[tup[0]] = fileAsList[tup[1]][tup[2]]
                
            # Add derived columns
            addDerivedColumns(returnRow,path)
            
        # After the file is parsed successfully it is moved to the achive folder
        #shutil.move(path, dirname(path) + "\\archive\\" + basename(path) )  # TODO Handle the exception
    except IndexError as e:
        print("File {0} generated an Index Error. Is it TSV?".format(path))
        pass
        
    return returnRow

def main():
    args = argparse.ArgumentParser()
    add_arguments(args)
    
    global basePath
    global imagePath
    global srcPath
    global destPath
    global logPath

    # Get the list of raw files from the source folder
    filelist = listFiles(srcPath)
    if filelist == None:
        exit()
    try:
        # Parse each file into a single CSV string.
        rowlist = []
        for f in filelist:
            rowlist.append(parseErgFile(srcPath+f))
            
        # Write the header then the data
        if len(rowlist) > 0:
            with open(destPath,'w',newline='') as csvfile:
                writer = csv.DictWriter(csvfile,fieldnames=rowlist[0].keys(),delimiter=',')
                writer.writeheader()
                writer.writerows(rowlist)
     
     # Move the raw files to the archive folder.
        for f in filelist:
            replace((srcPath+f),(srcPath+"archive/"+f))
            
    except Exception as e:
        print(str(e))
        dumpFailedMessages(str(e))

if __name__ == '__main__':
    main()

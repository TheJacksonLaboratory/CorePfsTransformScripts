# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 12:45:44 2020

@author: rushn
"""

# import os
# os.chdir('C:/Users/rushn/Documents/Projects/transforms/data_transforms/src')

import pandas as pd
import sys
import shutil
import transform_functions as tf
import itertools


# importPath = 'C:/Users/rushn/Documents/Projects/transforms/Holeboard/import/'
# loadedPath = 'C:/Users/rushn/Documents/Projects/transforms/Holeboard/loaded/'

def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    fileGroups = [list(v) for k, v in itertools.groupby(fileList, key=tf.get_field_sub)]
    for i in fileGroups:
        ComprehensiveDataOutput = [x for x in i if "Comprehensive" in x][0]
        HolePokeStandard = [x for x in i if "HolePoke" in x][0]

        # Grabbing the 3 fields from the header rows
        #        dataHead = pd.read_csv(str(importPath + ComprehensiveDataOutput), skiprows=42, nrows=2)
        dataHead = tf.readcsv(importPath=str(importPath + ComprehensiveDataOutput), skiprows=49, nrow=2)
        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])

        data_original = tf.readcsv(importPath=importPath + ComprehensiveDataOutput, skiprows=55)

        colNameOld = ['CAGE',
                      'SUBJECT ID',
                      'START TIME',
                      'HOLE POKE TOTAL COUNT']

        # Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        # Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)

        # Convert start time to mm/dd/yyyy/hh:mm:ss
        data["START TIME"] = date.split(" ")[0] + " " + data["START TIME"].str.replace(' PM','').str.replace(' AM','')  # TODO
        data['Test Date'] = tf. corePfsDateTimeKluge(date.split(' ',1)[0])
        data['Tester Name'] = user
        data['Comments'] = comment

        to_rename = {'CAGE': 'Arena ID',
                     'SUBJECT ID': 'EXPT_SAMPLE_BARCODE',
                     'HOLE POKE TOTAL COUNT':'Total holepokes'}

        data = data.rename(columns=to_rename)

        #####################################################################
        data_CDO = data

        dataHead = tf.readcsv(importPath=str(importPath + HolePokeStandard), skiprows=26, nrow=2)

        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])

        #        data_original = pd.read_csv(str(importPath + ZoneDataStandard), skiprows=130)
        data_original = tf.readcsv(importPath=importPath + HolePokeStandard, skiprows=32)

        colNameOld = ['SUBJECT ID',
                      'HOLE NAME']

        # Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        # Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)

        data['Test Date'] =  tf. corePfsDateTimeKluge(date.split(' ',1)[0])
        data['Tester Name'] = user
        data['Comments'] = comment

        to_rename = {'SUBJECT ID': 'EXPT_SAMPLE_BARCODE'}

        data = data.rename(columns=to_rename)
        data = data.dropna(subset=['EXPT_SAMPLE_BARCODE'])
        data = data.astype({"EXPT_SAMPLE_BARCODE": int, "HOLE NAME": int})

        ####################################################################
        # Calculations to create required columns for LIMS
        data_HPS = tf.calculations_HB(data)
        #####################################################################
        data_all = pd.merge(data_CDO, data_HPS, on=['EXPT_SAMPLE_BARCODE', 'Test Date',
                                                    'Tester Name', 'Comments'])
        data_all['Arena ID'] = data_all['Arena ID'].str.extract('(\d+)')
        data_all = data_all.drop_duplicates()
        data_all = data_all.round(2)
        data_all.to_csv(loadedPath + 'LOADED_' + HolePokeStandard.split('_')[0] + '.csv', index=False,
                        float_format='%.0f')

        # moving processed file to archive folder
        shutil.move(importPath + ComprehensiveDataOutput, importPath + '\\Archive')
        shutil.move(importPath + HolePokeStandard, importPath + '\\Archive')


if __name__ == '__main__':

    a = str(sys.argv[1])
    b = str(sys.argv[2])
    transform(a, b)

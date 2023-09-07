# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 13:59:00 2020

@author: rushn
"""

# import os
# os.chdir('C:/Users/rushn/Documents/Projects/transforms/data_transforms/src')

import pandas as pd
import pandas.api.types as ptypes
import sys
import shutil
import transform_functions as tf
import itertools

def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    fileGroups = [list(v) for k, v in itertools.groupby(fileList, key=tf.get_field_sub)]
    for i in fileGroups:
        ComprehensiveDataOutput = [x for x in i if "Comprehensive" in x][0]
        ZoneDataStandard = [x for x in i if "ZoneData" in x][0]

        # Grabbing the 3 fields from the header rows
        #        dataHead = pd.read_csv(str(importPath + ComprehensiveDataOutput), skiprows=42, nrows=2)
        dataHead = tf.readcsv(importPath=str(importPath + ComprehensiveDataOutput), skiprows=42, nrow=2)
        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])

        data_original = tf.readcsv(importPath=importPath + ComprehensiveDataOutput, skiprows=48)

        colNameOld = ['CAGE',
                      'SUBJECT ID',
                      'SAMPLE',
                      'START TIME',
                      'DURATION (s)',
                      'TOTAL DISTANCE (cm)',
                      'REST TIME (s)',
                      'AMBULATORY TIME (s)',
                      'VERTICAL EPISODE COUNT',
                      'VERTICAL TIME (s)',
                      'CLOCKWISE REVOLUTIONS',
                      'COUNTER-CLOCKWISE REVOLUTIONS']

        # Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        # Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        # Convert start time to mm/dd/yyyy/hh:mm:ss
        data["START TIME"] = date.split(" ")[0] + " " + data["START TIME"]

        data['Date of test'] = date
        data['Experimenter ID'] = user
        data['Comments'] = comment
        data['SUBJECT ID'] = data['SUBJECT ID'].astype(int)
        data['CAGE'] = data['CAGE'].str.replace('Cage ', '')
        data['CAGE'] = data['CAGE'].astype(int)

        to_rename = {'CAGE': 'Arena ID',
                     'SUBJECT ID': 'EXPT_SAMPLE_BARCODE',
                     'DURATION (s)': 'Sample Duration'}

        data = data.rename(columns=to_rename)

        ####################################################################
        # Calculations to create required columns for LIMS
        data = tf.calculations_CDO_OF(data)
        #####################################################################
        data_CDO = tf.data_CDO_OF(data)

        # Testing columns for proper data type
        string_cols = ['Date of test',
                       'Experimenter ID',
                       'Start Date and Time',
                       'Comments']

        int_cols = ['Number of Rears Total',
                    'Number of Rears First Five Minutes',
                    'Number of Rears Second Five Minutes',
                    'Number of Rears Third Five Minutes',
                    'Number of Rears Fourth Five Minutes',
                    'Clockwise',
                    'Counter Clockwise',
                    'Arena ID']

        float_cols = ['Distance Traveled Total (cm)',
                      'Distance Traveled First Five Minutes (cm)',
                      'Distance Traveled Second Five Minutes (cm)',
                      'Distance Traveled Third Five Minutes (cm)',
                      'Distance Traveled Fourth Five Minutes (cm)',
                      'Whole arena average speed (cm/s)',
                      'Whole arena resting time (s)',
                      'Time Spent Mobile (s)',
                      'Vertical time (s)']

        assert all(ptypes.is_string_dtype(data_CDO[col]) for col in string_cols)
        assert all(ptypes.is_integer_dtype(data_CDO[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_CDO[col]) for col in float_cols)
        assert all(data_CDO['Sample Duration'] == 300)
        assert all(data_CDO['Whole Arena Permanence Time (s)'] == 1200)

        ##################Processing ZoneDataStandard###########################################
        # Grabbing the 3 fields from the header rows
        #        dataHead = pd.read_csv(str(importPath + ZoneDataStandard), skiprows=124, nrows=2)
        dataHead = tf.readcsv(importPath=str(importPath + ZoneDataStandard), skiprows=124, nrow=2)

        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])

        #        data_original = pd.read_csv(str(importPath + ZoneDataStandard), skiprows=130)
        data_original = tf.readcsv(importPath=importPath + ZoneDataStandard, skiprows=130)

        colNameOld = ['SUBJECT ID',
                      'SAMPLE',
                      'ZONE',
                      'DURATION (CENTROID)',
                      'ENTRY COUNT (CENTROID)',
                      'TOTAL DISTANCE (CENTROID)',
                      'REST TIME (CENTROID)',
                      'AMBULATORY TIME (CENTROID)']

        # Ensuring that all of the above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        # Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        data['Date of test'] = date
        data['Experimenter ID'] = user
        data['Comments'] = comment

        to_rename = {'CAGE': 'Arena ID',
                     'SUBJECT ID': 'EXPT_SAMPLE_BARCODE',
                     'DURATION (s)': 'Sample Duration'}

        data = data.rename(columns=to_rename)

        ####################################################################
        # Calculations to create required columns for LIMS
        data = tf.calculations_ZDS_OF(data)
        #####################################################################
        # Selecting final table of columns for LIMS
        data_ZDS = tf.data_ZDS_OF(data)
        # Testing columns for proper data type

        int_cols = ['Number of center entries']

        float_cols = ['Periphery distance traveled (cm)',
                      'Periphery resting time (s)',
                      'Periphery permanence time (s)',
                      'Periphery average speed (cm/s)',
                      'Center distance travelled (cm)',
                      'Center resting time (s)',
                      'Center permanence time (s)',
                      'Center average speed (cm/s)',
                      'PctTime Center (%)',
                      'PctTime Center Habituation Ratio (cm)',
                      'PctTime Corners (%)',
                      'PctTime Corners Habituation Ratio (cm)',
                      'Corners Permanence Time (s)',
                      'PctTime Center Slope (%)',
                      'PctTime Corner Slope (%)']

        assert all(ptypes.is_integer_dtype(data_ZDS[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_ZDS[col]) for col in float_cols)

        data_all = pd.merge(data_CDO, data_ZDS, on='EXPT_SAMPLE_BARCODE')
        data_all = data_all.drop_duplicates()
        data_all = data_all.round(2)
        data_all.to_csv(loadedPath + 'LOADED_' + ZoneDataStandard.split('_')[0] + '.csv', index=False)

        # moving processed file to archive folder
        shutil.move(importPath + ComprehensiveDataOutput, importPath + '\\Archive')
        shutil.move(importPath + ZoneDataStandard, importPath + '\\Archive')


if __name__ == '__main__':
    a = str(sys.argv[1])
    b = str(sys.argv[2])
    transform(a, b)


# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 14:31:10 2020

@author: rushn
This script takes an Light/Dark data file and transforms the file to allow loading into JaxLIMS
"""

from datetime import datetime
import pandas.api.types as ptypes
import sys
import shutil
import transform_functions as tf

#importPath = 'C:/Users/rushn/Documents/Projects/transforms/LightDark/import/'
#loadedPath = 'C:/Users/rushn/Documents/Projects/transforms/LightDark/loaded/'
#importPath = '//jax.org/jax/phenotype/LightDarkv2/KOMP/transform/'
#loadedPath = '//jax.org/jax/phenotype/LightDarkv2/KOMP/processed/'

def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    for i in fileList:
        #Grabbing the 3 fields from the header rows
        dataHead = tf.readcsv(importPath = str(importPath+i), skiprows=124, nrow=2)
#       dataHead = pd.read_csv(str(importPath + i), skiprows=124, nrows=2)
        date = dataHead['CREATION DATE/TIME'].iloc[0]
        user = dataHead['USER NAME'].iloc[0]
        comment = str(dataHead['COMMENT'].iloc[0])
        
        data_original = tf.readcsv(importPath = importPath + i, skiprows=130)
        
        colNameOld = ['CAGE',
                      'SUBJECT ID',
                      'START TIME',
                      'DURATION (s)',
                      'ZONE',
                      'DURATION (CENTROID)',
                      'ENTRY COUNT (CENTROID)',
                      'LATENCY (CENTROID)',
                      'AMBULATORY TIME (CENTROID)']
        
        #Ensuring that all of hte above columns are present
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        data['Test Date'] = tf. corePfsDateTimeKluge(date.split(' ',1)[0])
        data['Tester Name'] = user
        data['Comment'] = comment
        
        to_rename = {'CAGE':'Arena ID',
                        'SUBJECT ID':'EXPT_SAMPLE_BARCODE',
                        'START TIME':'Start time',
                        'DURATION (s)':'Sample Duration'}
        
        data = data.rename(columns=to_rename)
        
        ##################################################################
        #Calculations to create required columns for LIMS
        data = tf.calculations_LD(data)
        #####################################################################
        data_final = tf.final_data_LD(data)
        
        #Testing columns for proper data type
        string_cols = ['Test Date', 
                       'Tester Name', 
                       'Arena ID', 
                       'Start time', 
                       'Comment']
        
        int_cols    = ['Side changes']
        
        float_cols = ['Latency to first transition into dark (sec)',
                      'Light side time spent (sec)',
                      'Time mobile light side (sec)',
                      'Dark side time spent (sec)',
                      'Time mobile dark side (sec)',
                      'Percent time in dark (%)',
                      'Percent time in light (%)']
        
        assert all(ptypes.is_string_dtype(data_final[col]) for col in string_cols)
        assert all(ptypes.is_integer_dtype(data_final[col]) for col in int_cols)
        assert all(ptypes.is_float_dtype(data_final[col]) for col in float_cols)
        assert all(data_final['Sample Duration']==1200)  
		
        for index, row in data_final.iterrows():
            data_final.at[index,'Test Date']= tf.corePfsDateTimeKluge(row['Test Date'])
           
        #print(data_final)
			
        data_final = data_final.round(2)
        data_final.to_csv(loadedPath+'LOADED_'+i, index=False)
        
        #moving processed file to archive folder
        shutil.move(importPath+i, importPath+'\\Archive')

if __name__ == '__main__':
    a = str(sys.argv[1])
    b = str(sys.argv[2])
    transform(a, b)

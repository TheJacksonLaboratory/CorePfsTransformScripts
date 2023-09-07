# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 14:52:32 2020

@author: rushn
"""

import pandas as pd
import sys
import shutil
import transform_functions as tf

#importPath = 'C:/Users/rushn/Documents/Projects/transforms/Hematology/import/'
#loadedPath = 'C:/Users/rushn/Documents/Projects/transforms/Hematology/loaded/'


def transform(importPath, loadedPath):
    fileList = tf.listFiles(importPath)
    fileList.sort()
    for i in fileList:
#        i = fileList[0]
        data_original = tf.readxlsx(importPath = str(importPath+i), skiprows=1)
        
        
        colNameOld = ['Sample No.',
                      'WBC(10^3/uL)',
                      'RBC(10^6/uL)',
                      'HGB(g/dL)',
                      'HCT(%)',
                      'MCV(fL)',
                      'MCH(pg)',
                      'MCHC(g/dL)',
                      'PLT(10^3/uL)',
                      'RDW-CV(%)',
                      'MPV(fL)',
                      'NRBC#(10^3/uL)',
                      'NRBC%(%)',
                      'NEUT#(10^3/uL)',
                      'LYMPH#(10^3/uL)',
                      'MONO#(10^3/uL)',
                      'EO#(10^3/uL)',
                      'BASO#(10^3/uL)',
                      'NEUT%(%)',
                      'LYMPH%(%)',
                      'MONO%(%)',
                      'EO%(%)',
                      'BASO%(%)',
                      'RET%(%)',
                      'RET#(10^9/L)',
                      'IRF(%)',
                      'LFR(%)',
                      'MFR(%)',
                      'HFR(%)',
                      'RET-He(pg)',
                      'PLT-I(10^3/uL)',
                      'PLT-O(10^3/uL)',
                      'Experimenter',
                      'Date-Time Sacrifice/Collection',
                      'Date of Bleed',
                      'Analyst ID',
                      'Comments']
        
        missing = [x for x in colNameOld if x not in list(data_original.columns)]
        error_message = str("Missing required columns: " + ', '.join(missing))
        if len(missing) != 0:
            sys.exit(error_message)

        #Creating table of only necessary columns
        data = data_original[colNameOld].reset_index(drop=True)
        
        data_final = tf.final_data_Hem(data)
        data_final = data_final.round(2)
        data_final.to_csv(loadedPath+'LOADED_'+i.split('.')[0] + '.csv.', index=False)
        
                #moving processed file to archive folder
        shutil.move(importPath+i, importPath+'\\Archive')

if __name__ == '__main__':
    a = str(sys.argv[1])
    b = str(sys.argv[2])
    transform(a, b)


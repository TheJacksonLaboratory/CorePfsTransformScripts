# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 16:08:14 2020

@author: rushn

# Query to get BARCODE from human name : https://jacksonlabstest.platformforscience.com/TEST/odata/EMPLOYEE?$filter=Name eq 'Daniel Pfalzer'
		from requests.auth import HTTPBasicAuth
		import requests

		my_auth = HTTPBasicAuth(email, password)
        query = queryString
        result = requests.get(query, auth=my_auth)
"""
import os
from statistics import mean
import numpy as np
import pandas as pd
from datetime import datetime

def listFiles(importPath):
    fileList = [f for f in os.listdir(importPath) if os.path.isfile(os.path.join(importPath, f))]
    return fileList

def get_field_sub(x): 
    return x.split('_')[0]

def best_fit_slope(ys):
    xs=np.array([300,600,900,1200])
    m = (((mean(xs)*mean(ys)) - mean(xs*ys)) /
         ((mean(xs)**2) - mean(xs**2)))
    return m * 100

def readcsv(importPath, skiprows, nrow=None):
    data = pd.read_csv(str(importPath), skiprows=skiprows, nrows=nrow)
    return data

def readxlsx(importPath, skiprows, nrow=None):
    data = pd.read_excel(str(importPath), skiprows=skiprows, nrows=nrow)
    return data

# Accepts date time in the format m/d/yyyy hh:mm
def corePfsDateTimeKluge(timestr):
    timestr = timestr.replace(' AM','')
    timestr = timestr.replace(' PM','')
    dt = datetime.strptime(timestr, '%m/%d/%Y %H:%M')
    corrected = datetime.strftime(dt, '%m/%d/%Y %H:%M')
    return corrected
	
def calculations_LD(data):
    data['Latency to first transition into dark (sec)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['LATENCY (CENTROID)'].transform('nth', 0)
    data['Side changes'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['ENTRY COUNT (CENTROID)'].transform('nth', 0)
    data['Light side time spent (sec)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION (CENTROID)'].transform('nth', 1)
    data['Time mobile light side (sec)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['AMBULATORY TIME (CENTROID)'].transform('nth', 1)
    data['Dark side time spent (sec)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION (CENTROID)'].transform('nth', 0)
    data['Time mobile dark side (sec)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['AMBULATORY TIME (CENTROID)'].transform('nth', 0)
    data['Percent time in dark (%)'] = data['Dark side time spent (sec)']/(1200 - data['Latency to first transition into dark (sec)'])*100
    data['Percent time in light (%)'] = (data['Light side time spent (sec)'] - data['Latency to first transition into dark (sec)'])/(1200 - data['Latency to first transition into dark (sec)'])*100
    data.loc[(data['DURATION (CENTROID)'] == 0) & (data['ZONE'] == 'Dark'), 'Transition'] = 'No'
    data.loc[(data['DURATION (CENTROID)'] == 1200) & (data['ZONE'] == 'Light'), 'Transition'] = 'No'
    data.loc[(data['Transition'].isnull()), 'Transition'] = 'Yes'
    data.loc[(data['Light side time spent (sec)'] == 1200), 'Latency to first transition into dark (sec)']=1200
    data.loc[(data['Light side time spent (sec)'] == 1200), 'Percent time in dark (%)']=0
    data.loc[(data['Light side time spent (sec)'] == 1200), 'Percent time in light (%)']=100
    return data


def final_data_LD(data):
    colNameNew = ['EXPT_SAMPLE_BARCODE',
                  'Date of test',
                  'Experimenter ID',
                  'Arena ID',
                  'Start time',
                  'Sample Duration',
                  'Latency to first transition into dark (sec)',
                  'Side changes',
                  'Light side time spent (sec)',
                  'Time mobile light side (sec)',
                  'Dark side time spent (sec)',
                  'Time mobile dark side (sec)',
                  'Percent time in dark (%)',
                  'Percent time in light (%)',
                  'Transition',
                  'Comment']
    #Selecting final table of columns for LIMS
    data_final = data[colNameNew].reset_index(drop=True)
    data_final = data_final.drop_duplicates()
    return data_final

def calculations_CDO_OF(data):
    data['Start Date and Time'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['START TIME'].transform('nth', 0)
    data['Whole Arena Permanence Time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['Sample Duration'].transform('sum')
    data['Distance Traveled Total (cm)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].transform('sum')
    data['Distance Traveled First Five Minutes (cm)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].transform('nth', 0)
    data['Distance Traveled Second Five Minutes (cm)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].transform('nth', 1)
    data['Distance Traveled Third Five Minutes (cm)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].transform('nth', 2)
    data['Distance Traveled Fourth Five Minutes (cm)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].transform('nth', 3)
    data['Whole arena average speed (cm/s)'] = data['Distance Traveled Total (cm)']/1200
    data['Whole arena resting time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['REST TIME (s)'].transform('sum')
    data['Time Spent Mobile (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['AMBULATORY TIME (s)'].transform('sum')
    data['Number of Rears Total'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['VERTICAL EPISODE COUNT'].transform('sum')
    data['Number of Rears First Five Minutes'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['VERTICAL EPISODE COUNT'].transform('nth', 0)
    data['Number of Rears Second Five Minutes'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['VERTICAL EPISODE COUNT'].transform('nth', 1)
    data['Number of Rears Third Five Minutes'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['VERTICAL EPISODE COUNT'].transform('nth', 2)
    data['Number of Rears Fourth Five Minutes'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['VERTICAL EPISODE COUNT'].transform('nth', 3)
    data['Vertical time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['VERTICAL TIME (s)'].transform('sum')
    data['Clockwise'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['CLOCKWISE REVOLUTIONS'].transform('sum')
    data['Counter Clockwise'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['COUNTER-CLOCKWISE REVOLUTIONS'].transform('sum')
    data['Distance Traveled Habituation Ratio (cm)'] = ((data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].transform('nth', 3)/300*100)-(data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].transform('nth', 0)/300*100))/3

    distance_avg = data.groupby(['EXPT_SAMPLE_BARCODE'])['TOTAL DISTANCE (cm)'].apply(best_fit_slope)

    distance_avg = distance_avg.rename('Distance Traveled Slope (%)')
    
    data = pd.merge(data, distance_avg, left_on='EXPT_SAMPLE_BARCODE', right_index=True)
    
    return data

def calculations_ZDS_OF(data):
    data[list(data)] = data[list(data)].astype(str)
    data = data.groupby(['EXPT_SAMPLE_BARCODE', 'SAMPLE', 'Date of test', 'Experimenter ID', 'Comments'], \
                        as_index=False, sort=False).agg(','.join)
    data[['DURATION_P','DURATION_CORNER','DURATION_CENTER']] = data['DURATION (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['ENTRY_P','ENTRY_CORNER','ENTRY_CENTER']] = data['ENTRY COUNT (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['DISTANCE_P','DISTANCE_CORNER','DISTANCE_CENTER']] = data['TOTAL DISTANCE (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['REST_P','REST_CORNER','REST_CENTER']] = data['REST TIME (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['AMBULATORY_P','AMBULATORY_CORNER','AMBULATORY_CENTER']] = data['AMBULATORY TIME (CENTROID)'].str.split(',',expand=True).apply(pd.to_numeric)
    data[['EXPT_SAMPLE_BARCODE', 'SAMPLE']] = data[['EXPT_SAMPLE_BARCODE', 'SAMPLE']].apply(pd.to_numeric)
    
    data['Periphery distance traveled (cm)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['DISTANCE_P'].transform('sum')+data.groupby(['EXPT_SAMPLE_BARCODE'])['DISTANCE_CORNER'].transform('sum')
    data['Periphery resting time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['REST_P'].transform('sum')+data.groupby(['EXPT_SAMPLE_BARCODE'])['REST_CORNER'].transform('sum')
    data['Periphery permanence time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_P'].transform('sum')+data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CORNER'].transform('sum')
    data['Periphery average speed (cm/s)'] = data['Periphery distance traveled (cm)']/data['Periphery permanence time (s)']
    data['Center distance travelled (cm)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['DISTANCE_CENTER'].transform('sum')
    data['Center permanence time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CENTER'].transform('sum')
    data['Center resting time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['REST_CENTER'].transform('sum')
    data['Center average speed (cm/s)'] = data['Center distance travelled (cm)']/data['Center permanence time (s)']
    data['Number of center entries'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['ENTRY_CENTER'].transform('sum')
    data['PctTime Center (%)'] = data['Center permanence time (s)']/1200*100
    data['Corners Permanence Time (s)'] = data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CORNER'].transform('sum')
    data['PctTime Corners (%)'] = data['Corners Permanence Time (s)']/1200*100
    data['PctTime Corners Habituation Ratio (cm)'] = ((data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CORNER'].transform('nth', 3)/300*100)-(data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CORNER'].transform('nth', 0)/300*100))/3
    data['PctTime Center Habituation Ratio (cm)'] = ((data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CENTER'].transform('nth', 3)/300*100)-(data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CENTER'].transform('nth', 0)/300*100))/3

    corner_avg = data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CORNER'].apply(best_fit_slope)
    corner_avg = corner_avg.rename('PctTime Corner Slope (%)')
    center_avg = data.groupby(['EXPT_SAMPLE_BARCODE'])['DURATION_CENTER'].apply(best_fit_slope)
    center_avg = center_avg.rename('PctTime Center Slope (%)')
    
    avgs = pd.merge(corner_avg, center_avg, left_index=True, right_index=True)
    
    data = pd.merge(data, avgs, left_on='EXPT_SAMPLE_BARCODE', right_index=True)
    return data

def data_CDO_OF(data):
    colNameNew = ['EXPT_SAMPLE_BARCODE',
                  'Date of test',
                  'Experimenter ID',
                  'Arena ID',
                  'Start Date and Time',
                  'Sample Duration',
                  'Whole Arena Permanence Time (s)',
                  'Distance Traveled Total (cm)',
                  'Distance Traveled First Five Minutes (cm)',
                  'Distance Traveled Second Five Minutes (cm)',
                  'Distance Traveled Third Five Minutes (cm)',
                  'Distance Traveled Fourth Five Minutes (cm)',
                  'Whole arena average speed (cm/s)',
                  'Whole arena resting time (s)',
                  'Time Spent Mobile (s)',
                  'Number of Rears Total',
                  'Number of Rears First Five Minutes',
                  'Number of Rears Second Five Minutes',
                  'Number of Rears Third Five Minutes',
                  'Number of Rears Fourth Five Minutes',
                  'Vertical time (s)',
                  'Clockwise',
                  'Counter Clockwise',
                  'Distance Traveled Habituation Ratio (cm)',
                  'Distance Traveled Slope (%)',
                  'Comments']

        #Selecting final table of columns for LIMS
    data_CDO = data[colNameNew].reset_index(drop=True)
    return data_CDO

def data_ZDS_OF(data):
    colNameNew = ['EXPT_SAMPLE_BARCODE',
                  'Periphery distance traveled (cm)',
                  'Periphery resting time (s)',
                  'Periphery permanence time (s)',
                  'Periphery average speed (cm/s)',
                  'Center distance travelled (cm)',
                  'Center resting time (s)',
                  'Center permanence time (s)',
                  'Center average speed (cm/s)',
                  'Number of center entries',
                  'PctTime Center (%)',
                  'PctTime Center Habituation Ratio (cm)',
                  'PctTime Corners (%)',
                  'PctTime Corners Habituation Ratio (cm)',
                  'Corners Permanence Time (s)',
                  'PctTime Center Slope (%)',
                  'PctTime Corner Slope (%)']
        
        
    data_ZDS = data[colNameNew].reset_index(drop=True)
    return data_ZDS

def final_data_Hem(data):
    colNameNew = ['EXPT_SAMPLE_BARCODE',
                  'White Blood Cells (WBC)',
                  'Red Blood Cells (RBC)',
                  'Measured Hemoglobin (mHGB)',
                  'Hematocrit (HCT)',
                  'Mean Cell Volume (MCV)',
                  'Mean Corpuscular hemoglobin (CHg)',
                  'Mean Cell Hemoglobin Concentration (MCHC)',
                  'Platelet Count (PLT) (Optical)',
                  'Red Cell Distr. Width (RDW)',
                  'Mean Platelet Volume (MPV)',
                  'Nucleated Red Blood Cell Count',
                  'Nucleated Red Blood Cell Percentage',
                  'Neurophil Cell Count',
                  'Lymphocyte Cell Count',
                  'Monocyte Cell Count',
                  'Eosinophil Cell Count',
                  'Basophil Cell Count',
                  'Neurophils (NEUT)',
                  'Lymphocytes (LYM)',
                  'Monocytes (MONO)',
                  'Eosinophils (EOS)',
                  'Basophils (BASO)',
                  'Percent Retic',
                  'Reticulocytes (Retic)',
                  'Percent Immature Retic',
                  'Percent Low Fluorescing Retic',
                  'Percent Middle Fluorescing Retic',
                  'Percent High Fluorescing Retic',
                  'Retic Hemoglobin (CHr)',
                  'Platelet Count (Impedance)',
                  'Platelet Count (Optical)',
                  'Experimenter ID',
                  'Date and Time of Blood Collection',
                  'Date of Measurement',
                  'Analyst ID',
                  'Comments',
                  'Date and time of sacrifice']
    #Selecting final table of columns for LIMS
    data['Date and time of sacrifice'] = data['Date-Time Sacrifice/Collection']
    data.columns = colNameNew
    data_final = data[colNameNew].reset_index(drop=True)
    data_final = data_final.drop_duplicates()
    return data_final


def calculations_HB(data):
    values = dict(data.groupby('EXPT_SAMPLE_BARCODE')['HOLE NAME'].apply(list))
    poke_counts = {}
    for key in values:
        test = {x:values[key].count(x) for x in values[key]}
        poke_counts.update({key:test})
    
    counts = pd.DataFrame.from_dict(poke_counts, orient='index')
    counts = counts.reindex(sorted(counts.columns), axis=1)
    counts.columns = ['Total Hole Pokes Hole ' + str(col) for col in counts.columns]
    data = pd.merge(data, counts, left_on = ['EXPT_SAMPLE_BARCODE'], right_index=True)
    
    data['HOLE NAME'] = data['HOLE NAME'].astype(str)
    sequence = data.groupby(['EXPT_SAMPLE_BARCODE'])['HOLE NAME'].apply('-'.join).reset_index()
    sequence = sequence.rename(columns={'HOLE NAME':'Holepoke sequence'})
    data = pd.merge(data, sequence, on=['EXPT_SAMPLE_BARCODE'])
    data = data.drop(columns = ['HOLE NAME']).drop_duplicates().reset_index(drop=True)
    data.fillna(0, inplace=True)


    return data
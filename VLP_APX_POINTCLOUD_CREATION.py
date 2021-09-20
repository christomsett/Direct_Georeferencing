#!python3
# -*- coding: utf-8 -*-
"""
@author: Chris Tomsett
"""

"""
Modified on 20/09/2021

by: Chris Tomsett
"""



###
###IMPORT RELEVANT MODULES###
###

import time
import os
import pandas as pd
import numpy as np

###set working directory for module import to the same as where the VLP.py fdile with the three function is
os.chdir('')
import VLP
print("Modules Imported")

#time of processing beginning
start = time.time()



###
###SET STANDARD PARAMETERS FOR EACH USE
###
"""
SET FILE LOCATIONS FOR THE DIRECTORY CONTAINING THE VELODYNE CSVs (exported from veloview), THE APPLANIX POSITION FILE (formatted in to a csv), AND THE OUPUT LOCATION AND NAME OF THE PROCESSED POINT CLOUD FILE AS A CSV
"""
#set folder location filepath for velodyne CSVs
velodyne_dir = r'e.g. FileLocation/VLP_CSVs'
print("Velodyne Directory Set")
#set applanix position csv location as above
apx_path = r'e.g. Filelocation/APX_Position.csv'
print("Applanix Position File Location Set")
#set output csv file name based on the above
point_cloud = r'e.g. OutputLocation/FinalPointCloud.csv'

"""
SET THE PARAMETERS BASED ON AQUISITON TIME (e.g. use labstat online time converter) AND SIZE OF DATA CHUNKS (as a guide a size of 100 is good for memory purposes)
"""
#number to split rotation dataset in to for memory reduction, see performing rotation calculations
chunksize = 100
#top of hour gps time in seconds for velodyne adjustment (set as integer), the hour in which take off occured
ToH_UTC_Sec = 0
#gps differenc adjustment, based on leap seconds acquired between GPST and UTC time
TDiffGPS = 18
#velodyne equivelent top of hour time
ToH = ToH_UTC_Sec + TDiffGPS



###
###VELODYNE DATA PREPERATION
###
#set os directory to velodyne packet directory
os.chdir(velodyne_dir)
#list all the files (and directories...) in the current directory
datapacket_list = os.listdir()
print("Files for concatenation listed")

#combine all the files that have been found in the above list in to one large velodyne file
velo = pd.concat([pd.read_csv(f) for f in datapacket_list], sort = False)
print("Files concatenated")

#perform pre processing steps as outlined in the VLP.py package
velo = VLP.VLP_DataPrep(velo, ToH)

#this accounts for crossing of hours where vlp-16 clock resets, it is assumed that any values small than one second past an hour means that the survey was done across two hours
velo = VLP.VLP_HourAdjust(velo)



###
###IMPORTING APPLANIX DATA
###
#read in csv as apx dataframe
apx = pd.read_csv(apx_path)
#sort dataframe by the timestamp for 
apx = apx.sort_values(by =['TIME'])
print("APX-15 data imported and sorted")



###
###MERGE DATA BASED ON NEAREST TIME COMPARISON OF DATA ACQUISITION
###
#combination of velodyne and applanix data
comb = pd.merge_asof(velo, apx, left_on = 'GPSSecWk', right_on = 'TIME', direction = 'nearest')
print("Velodyne and APX-15 data successfuly combined")
#drop items no longer needed past merging
comb = comb.drop(['timestamp', 'SecPastHr', 'TopOfHrSec'], axis = 1)



###
###THESE CAN BE LEFT AS 0 IF EXACT BORESIGHT ANGLE IS KNOWN, USED TO REFINE INTIAL PARAMETERS
###
comb['ROLL'] = comb['ROLL'] + 0
comb['PITCH'] = comb['PITCH'] + 0
comb['HEADING'] = comb['HEADING'] + 0



###
###CLEAR UNREQUIRED VARIABLES FROM MEMORY
###
#remove apx and velo variables now joined
#remove data from velo and apx
velo = None
apx = None
print('variables emptied')
#delete variables velo and apx
del velo
del apx
print('Variables removed')
datapacket_list = None
del datapacket_list



###
###PERFORM ROTATION CALCULATIONS
###
#set number of chunks to be broken in to
comb_df = np.array_split(comb, chunksize)
print('split dataframe created')
#set comb variable to none
comb = None
#create an empty list to append processed chunks to
chunk_list = []
#work through comb_df iterating over each chunked dataframe performing calculations
for number in range(len(comb_df)):
    comb = comb_df[number]
    VLP.VLP_Transformation(comb, chunk_list)
    comb_df[number] = None
    print(str((number + 1) / chunksize * 100) + "% OF CHUNKS PROCESSED")
#concatenate the listed, processed arrays to one file
comb = pd.concat(chunk_list)
#remove chunk list variable
chunk_list = None
del chunk_list



###
###ADJUST POINTS POSITIONING
###
#add new columns that take the relative point position from geogrpahic point position
comb['X_WGS'] = comb['EASTING'] + comb['AdjX']
comb['Y_WGS'] = comb['NORTHING'] + comb['AdjY']
comb['Z_WGS'] = comb['HEIGHT'] + comb['AdjZ']
print('XYZ data now in reference to velodyne coordinate centre')
#drop columns not needed
comb = comb.drop(['EASTING', 'NORTHING', 'HEIGHT', 'AdjX', 'AdjY', 'AdjZ'], axis = 1)

###
###CREATE XYZ POINT FILE
###
#save as a csv file
comb.to_csv(point_cloud)
print("XYZ File created")
print("Script completed")
#time of processing end
finish = time.time()
total_time = finish - start
hours_time = total_time / 3600
print("Total Running Time: ", finish - start, "Seconds; ", hours_time, "Hours.")
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 08:56:23 2020

@author: ct9g13

MAKE IN TO USABLE FUNCTION
"""

###
###IMPORT RELEVANT MODULES
###

import pandas as pd
from datetime import date, timedelta, datetime
import os
from shutil import copyfile


"""

-----
MICASENSE_event_creation takes the original event file that is stored on the board whilst the UAV is in flight and imagery is being taken, alongside the post-processed postion file which includes a time output, and creates a dataframe which contains the position data at the exact (or nearest exact moment) from when the image was 'detected' as being fired through the event in stream. 

This method requires the survey data as a method of input in order to work out the number of seconds (time) since the orignal GPST epoch so that the data can be successfulyl matched togather. 
-----

"""
def MICASENSE_event_creation(position, survey_date):
    
    #create integers of survey date for day month and year seperately

    day = int(survey_date.split("/")[0])
    month = int(survey_date.split("/")[1])
    year = int(survey_date.split("/")[2])
    
    #assign position input file to the variable pos_event for processing
    pos_event = position
    
    #identify GPS week from inputted survey date
    surveyday = date(year, month, day)
    epoch = date(1980, 1, 6) 
    """epoch date set based on first observation of time for satellite data systems"""
    
    #identify start of week for epoch and and survey day to avoid additional week being created or removed due to survey day
    surveymonday = surveyday - timedelta(surveyday.weekday())
    epochmonday = epoch - timedelta(epoch.weekday())
    
    #gps week caclulation based off of survey and epoch monday differece, divided by 7 as expressed as days
    gpsweek = (surveymonday - epochmonday).days/7 - 1
    
    #define seconds since epoch to find seconds passed from epoch to start of the week
    seconds_since_epoch = gpsweek * 604800

    #update event file to inlcude GPST
    #UTC time started 10 year prior which is 315964782 seconds
    UTC_GPST_diff = 315964782
    pos_event['GPS_time'] = pos_event['TIME'] + seconds_since_epoch + UTC_GPST_diff
    
    #account for difference in BST daylight savings affecting filestamp on micasense files
    #time difference in seconds
    bst_offset = 3600
    if month > 3:
        if month < 11:
            pos_event['GPS_time'] = pos_event['GPS_time'] -  bst_offset

    
    GPS_times = pos_event['GPS_time']
    np_GPST = GPS_times.to_numpy(dtype = float)
    np_UTC = []
    for np_time in np_GPST:
        UTC_time = datetime.fromtimestamp(np_time).strftime('%Y/%m/%d %H:%M:%S')
        np_UTC.append(UTC_time)
    
    #create a dataframe  with the np_UTC list and the GPS time, the latter set to be the index for joining
    UTC_date_df = pd.DataFrame({'UTC_date':np_UTC, 'GPS_time': GPS_times, 'GPST_sec': GPS_times}).set_index('GPS_time')
    
    #set pos_event index to UTC time for the join process
    pos_event = pos_event.set_index('GPS_time').join(UTC_date_df)
    
    #return dataframe to be created
    return pos_event


"""
Developing a method in order to create a .txt file for input in to metashape which matches up with the image name to directly georefernce the images
"""
def MICASENSE_metashape_file(image_folder, position_event_file, flight_prefix):
    #create an empty folder to list images in to
    images_blue = []
    images_green = []
    images_red = []
    images_rededge = []
    images_nir = []
    
    #loop through input folder to collect all images within it
    for root, directory, file in os.walk(image_folder):
        
        #loop through these images to seperate bands and append to seperate file lists
        for image_file in file:
            
            #get the image name from the full filepath
            image_name = os.path.split(image_file)[1]
            
            #conditional statement to identify band from _* number, and if true operate statement
            if '_1.' in image_name:
                
                #once band identified, assign full filepath and filename to list for each of the bands
                images_blue.append(os.path.join(root, image_file))
            
            #repeat selection across _2,3,4, and 5 to collate all five bands seperately
            elif '_2.' in image_name:
                images_green.append(os.path.join(root, image_name))
            elif '_3.' in image_name:
                images_red.append(os.path.join(root, image_name))
            elif '_4.' in image_name:
                images_rededge.append(os.path.join(root, image_name))
            else:
                images_nir.append(os.path.join(root, image_name))
    
    
    #create a list of the band lists to loop over and process in the same way
    photo_bands_lists = [images_blue, images_green, images_red, images_rededge, images_nir]
    
    #cycle through each band list, getting time and data infromation, matching to pos_events and saving as individual dataframes
    for band_list in photo_bands_lists:
        #define variables to append to a dataframe
        image_names = []
        image_time_seconds = []
        image_timestamps = []
        
        #loop through list to obtain information and assign to these variables
        for indv_image in band_list:
            
            #extract and append image name
            indv_image_name = os.path.split(indv_image)[1].split('.')[0]
            image_names.append(indv_image_name)
            
            #obtain image aquisiton time as hour:min:sec and UTC timestamp
            image_time = os.path.getmtime(indv_image)
            image_timestamp = datetime.fromtimestamp(image_time).strftime('%Y/%m/%d %H:%M:%S')
            
            #append UTC string and UTC timestamps to variable lists 
            image_time_seconds.append(image_time)
            image_timestamps.append(image_timestamp)
        
        
        #create dataframe of image name, time, and timestamp
        MS_image_event = pd.DataFrame({'Image_Name':image_names, 'Capture_Time_sec':image_time_seconds, 'Capture_Timestamp':image_timestamps}).set_index('Capture_Time_sec')
        
        #obtain first index which is the first timestanp to match against
        first_timestamp = MS_image_event.first_valid_index()
        
        #obtain length of dataframe, aka how many rows after the intial row to obtain for matching
        no_of_rows = len(MS_image_event.index)
        
        #find position of index closest to first timestamp
        idx_event1 = position_event_file['GPST_sec'].sub(first_timestamp).abs().idxmin()
        
        #get pd.loc position of closest time match identified above
        pos_loc = position_event_file.index.get_loc(idx_event1)
        
        #create a dataframe of event positions, for the number of rows starting at the first index
        last_row = pos_loc + no_of_rows
        event_df = position_event_file.iloc[pos_loc : last_row, :]
        
        #remove indexes of all dataframes
        MS_image_event.reset_index(drop = True, inplace = True)
        event_df.reset_index(drop = True, inplace = True)
        
        #INSERT IF STATEMTRN DEPENDENT ON EXTENSION IN FILENAME COLUMN
        first_name = MS_image_event.iloc[0,0]
        
        #create dataframes which correspond to the 5 bands for future concatenation
        if first_name.endswith('_1'):
            blue_df = pd.concat([MS_image_event, event_df], axis = 1)
        if first_name.endswith('_2'):
            green_df = pd.concat([MS_image_event, event_df], axis = 1)
        if first_name.endswith('_3'):
            red_df = pd.concat([MS_image_event, event_df], axis = 1)
        if first_name.endswith('_4'):
            rededge_df = pd.concat([MS_image_event, event_df], axis = 1)
        if first_name.endswith('_5'):
            nir_df = pd.concat([MS_image_event, event_df], axis = 1)
            
        
    #combine the two dataframes so that successive photos match up with successive event files based around the intial match
    final_df = pd.concat([blue_df, green_df, red_df, rededge_df, nir_df], axis = 0)
        
    #drop unneccessary columns
    final_df.drop(['TIME', 'DISTANCE', 'GPST_sec'], axis = 1, inplace = True)
    
    #add flight prefix and .tif extension to filename
    final_df['Image_Name'] = flight_prefix + "_" + final_df['Image_Name'] + '.tif'
    
    #return output
    return final_df   

"""
IN ORDER TO USE THESE IN METASHAPE, THE NAME TAGS HAVE TO BE DIFFERENT OTHERWISE IMPORTING THE EXIF DATA IN METASHAPE UPDATES CAMERAS WITH THE SAME NAME BUT FROM DIFFERENT FLIGHTS. THIS TOOL ALLOCATES A FLIGHT PREFIX TO THE START OF THE NAME, THE SAME AS IS GIVEN IN THE CSV CREATION OF POSITIONAL DATA ABOVE.
"""
def Add_Prefix(image_folder, flight_prefix):
    #create subfolder name to store non-matching outputs in, a.k.a missing timestamp files
    subfolder = "\\Prefix_Updates"
    #make a directory containing this sub folder in the updated time 
    os.mkdir(image_folder + subfolder)
    #redefine subfolder to a variable that os.path.join() can read, i.e. remove escape characters
    subfolder = "Prefix_Updates"
    
    #loop through folder appending file names
    for root, directory, file in os.walk(image_folder):
        #loop through the image files
        for image in file:
            #check only editing the .tif files in the folder
            if image.endswith(".tif"):
                
                #create a new name based on the prefix
                prefix_name = flight_prefix + "_" + image
                
                #get filepath for each individual image
                original_file = os.path.join(root, image)
                
                #create a new filepath to base the copy on
                new_file = os.path.join(root, subfolder, prefix_name)
                
                #perform the file copy 
                copyfile(original_file, new_file)
                
                #print confirmation for each file
                print(new_file + " copied to new folder")
        #including a break stops a recursive dig in to the newly created folder
        break
    
    

#survey date, this should be dd/mm/yyyy format
surveydate = 'e.g. 03/06/2021'
#flight prefix, e.g. F1, flight 1, flightbar etc. this is to differentiate between two sets of images that may be from differenmt flights but may have the same name (e.g. 0001.jpeg from two flights)
flight_prefix = 'F1'
#specify processed position file
pos = pd.read_csv('e.g. FileLocation/APX_PositionFile.csv')
#specify raw image folder
image_folder = 'e.g. FileLocation/ImageFolder'
#output position file for Metashape
out_pos = 'OutputInformation/ImageLocations_For_Metashape.csv'


pos_event = MICASENSE_event_creation(pos, surveydate)
photo_times = MICASENSE_metashape_file(image_folder, pos_event, flight_prefix)
photo_times.to_csv(out_pos, index = False)
Add_Prefix(image_folder, flight_prefix)



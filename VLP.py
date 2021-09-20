# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 16:26:15 2019

@author: geocomp_user


MODULE CREATED TO ALLOW FOR THE STORAGE OF MULTIPLE FUNCTIONS 
THAT CAN BE EDITED FOR USE WITH VELODYNE VLP-16 AND APPLANIX DATA

THESE FUNCTION ARE CALLED FORM THE POINT CLOUD CREATION PYTHON SCRIPT

"""    



def VLP_DataPrep(Datafile, TopOfHour):
    #PREPARING RAW CONCATENATED CSVs FOR PROCESSING
    
    #remove any unrequired fields
    velo = Datafile.drop(['Points_m_XYZ:0','Points_m_XYZ:1','Points_m_XYZ:2','laser_id','azimuth','adjustedtime','vertical_angle','dual_distance','dual_intensity','dual_return_matching'], axis = 1)

    #calculate seconds past the hour (timestamp divided by 1 million)
    velo['SecPastHr'] = velo.timestamp/1000000
    
    #input top of the hour value gps time
    velo['TopOfHrSec'] = TopOfHour
    
    #input GPS Seconds of that week time to match APX-15
    velo['GPSSecWk'] = velo.SecPastHr + velo.TopOfHrSec
    print("Timestamp configured to match APX-15 data")
    
    #convert GPSSecWk to floating point number for future joining
    velo['GPSSecWk'] = velo['GPSSecWk'].astype('float64')
    
    #sort by GPSSecWk for joining by nearest time period
    velo = velo.sort_values(by = ['GPSSecWk'])
    print("Velodyne Data Sorted")
    
    return velo


def VLP_HourAdjust(df):
    #import relevant packages
    import pandas as pd
    
    #check to see if the smallest seconds past the hour is less than one second (e.g. will have corssed the hour mark)
    if df.SecPastHr.min() < 1:
        
        #create two new dataframes, splitting at the 30 min mark as no flight will start before 30 mins and cross the hour or vice versa
        df1 = df[(df['SecPastHr'] >= 0) & (df['SecPastHr'] <= 1800)].copy()
        df2 = df[(df['SecPastHr'] >= 1800) & (df['SecPastHr'] <= 3600)].copy()
        
        #for those just passing the hour, add a full hours of seconds (3600) to their value
        df1['GPSSecWk'] = df1['GPSSecWk'] + 3600
        
        #rejoin the two dataframes
        df_adj = pd.concat([df1, df2])
        print("Cross hour data adjusted")
        
        #resort values by time
        df_adj = df_adj.sort_values(by = ['GPSSecWk'])
        print("Velodyne Data Resorted")
        
        #return that joined and adjusted dataframe
        return df_adj
        
    else:
        print("Velodyne data does not cross the hour mark")
        return df
        
        

def VLP_Transformation (dataframe, outputlist):
    ###
    ###ACCOUNT FOR ORIENTATION OF SENSOR
    ###
    
    #creation of a rotation matrix column
    #use of right handed orthogonal with z vertical opposite which opposes vehicle body frame of applanix
    #negative one multiplication due to anticlockwise calculations of x and z
    
    #import relevant files
    from scipy.spatial.transform import Rotation as R
    import pandas as pd
    
    #combine XYZ fields
    dataframe['XYZ_combined'] = list(zip(dataframe.X, dataframe.Y, dataframe.Z))
    
    #combininig x y and z fields to create a rotational matrix, where x reperesents each row (hence x['roll,'] means that rows roll), and axis=1 means it iterates across rows rather than columns, using the apply function on the dataframe 'comb'
    dataframe['RotationMatrix'] = dataframe.apply(lambda x: R.from_euler('xyz', [(x['PITCH']), (x['ROLL']), (x['HEADING'] * -1)], degrees = True), axis = 1)
    
    #use lambda apply function to apply rotation matrix to original xyz files
    dataframe['XYZ_adjusted'] = dataframe.apply(lambda x: x['RotationMatrix'].apply(x['XYZ_combined']), axis = 1)
    
    #create three new columns based off of x,y, and z
    dataframe[['AdjX', 'AdjY', 'AdjZ']] = pd.DataFrame(dataframe['XYZ_adjusted'].tolist(), index=dataframe.index)
    
    #drop unneccessary data
    dataframe = dataframe.drop(['X', 'Y', 'Z', 'GPSSecWk', 'DISTANCE', 'ROLL', 'PITCH', 'HEADING', 'XYZ_combined', 'RotationMatrix', 'XYZ_adjusted'], axis = 1)

    #append chunk list data to chunk list
    outputlist.append(dataframe)
    
    #delete variable from memory
    dataframe = None
    del dataframe
    
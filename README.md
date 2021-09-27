# Direct_Georeferencing
Taking APX-15 INS data and using it to provide metadata for Metashape direct georeferencing of MicaSense imagery and Velodyne VLP-16 laser scan data.

For the VLP_APX_POINTCLOUD_CREATION.py script to run successfully, it needs to be in the same folder location as the VLP script which contains some functions that this script uses. 

The VLP folder must contain csv files for outputs of all the scans exported from veloview or be imported as a continous pandas dataframe with columns of SecPastHr, X, Y, Z

APX data must inlcude 'TIME', 'EASTING' 'NORTING', 'HEIGHT', 'ROLL', 'PITCH', 'HEADING' as a post processed position file (shoudl work for other INS systems that can output this data in metres and degrees) and be a .csv folder

Final point cloud to be saved as .csv file and location and name needs specifying

ToH_UTC_Sec can be identified as the sec of the gps week for the hour in which take off occured. i.e both 11:10 and 11:59 would give a top of hour value of 11:00, use a tool such as GPS to UTC time calculator from labstat https://www.labsat.co.uk/index.php/en/gps-time-calculator setting leap seconds to 0 as this is accounted for in the script

For the MS_APX_METASHAPE_FILE.py script, the date of the survey, a user defined prefix for each flight if there are multiple flights, the locations of the APX position file whereby each row is the tag of an image input (the joining is done not on nearest time but photo order), the folder containign all the images, and the output metashape posiitonal information file. 

Both scripts are deisgned to be run from an environment such as spyder, on python 3.X, with pandas, datetime, os, shutil, numpy, and scipy.spatial.transform installed.

There is the possibility to parrallise the chunks for processing in the VLP script, but this has not been done as of yet. 

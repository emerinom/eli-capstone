
# Copyright (C) 2016 by The HDF Group.
#  All rights reserved.
#
# This PySpark example parallizes handling multiple HDF4 files.
#
# Put a bunch of AIRS.YYY.MM.DD.L3.RetStd_H031.v4.0.21.0.G06104133732.hdf files
# like [1] in the current working directory.
#
# Then, run this script like:
#
# %python summary_hdf.py .
#
# At the end, it will create summary.csv file that computes mean, median, stdev.
#
# This is tested with PySpark-Notebook Docker [2] on CentOS 7.
#
# Last updated: 2016-09-08
#
import os, sys
import numpy as np
import h5py as h5py
from pyspark import SparkContext

if __name__ == "__main__":
    """
    Usage: doit [partitions]
    """
    sc = SparkContext(appName="GPM")
    base_dir = str(sys.argv[1]) if len(sys.argv) > 1 else exit(-1)
    partitions = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    hdf5_path = ''

    ###################################################################
    ### get the precipitation datasets o
    ### it has to be changed accordingly for reading out the needed information seen Notebook Spark_HDF
    def summarize(x):
    
        FILE_NAME = x
        dataset = h5py.File(FILE_NAME, 'r')
        
        # List available SDS datasets.
        # print(list(dataset.keys())) 
        
        # Read precipitation dataset.
        precip = dataset.get('precipitation')
        #precip = np.transpose(precip)
        
        # Read geolocation dataset.
        #theLats= dataset['lat'][:]
        #theLons = dataset['lon'][:]
        
        # Extract the date from the file name.
        # 3B-MO.MS.MRG.3IMERG.20000601-S000000-E235959.06.V06B.HDF5.SUB.hdf5
        key = "".join(FILE_NAME[25:32])
        #return [(key, precip, theLats, theLons)]
        return [(key, precip)]
    #####################################################################################

    # traverse the base directory and pick up HDFEOS files
    file_list = filter(lambda s: s.endswith(".hdf5"),
                       [ "%s%s%s" % (root, os.sep, file)
                         for root, dirs, files in os.walk(base_dir)
                         for file in files])
 
    # partition the list
    file_paths = sc.parallelize(file_list, partitions)
    
    # compute per file summaries
    rdd = file_paths.flatMap(summarize)

    # collect the results and write the time series to a CSV file
    results = rdd.collect()
    #print(list(results))
    
    with open("precip.csv", "w") as text_file:
        #text_file.write("Date,Precip,Lat,Long\n")
        text_file.write("Date,Precip\n")
        #print(len(results))
        #for k in sorted(results):
        for k in results:
            dat = k[0]
            # To acces the sub-arrays of Precipitation datasets
            precip = list(k[1][0][0])
            #print(precip)
            #print(len(precip))
            for i in range(len(precip)):
                text_file.write(
                    "{0},{1}\n".format(
                        dat,precip[i]))
                
    sc.stop()
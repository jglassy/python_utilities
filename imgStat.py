# -*- coding: utf-8 -*-
'''
 script  : imgStat.py
 revised : 2017-01-16T15:17:00
 author  : Joe Glassy
 purpose : Implement sample python code to perform image analysis via
           statistical summary of one chosen HDF5, GeoTIFF, or 
           raw binary dataset
 version : 1.0           
 notes   : A major reason for developing this sample app is to exercise
           a variety of GIT version control activities via my www.github.com
           account.
'''

import sys,os
import time, random, uuid
import numpy as np
# FUTURE : import h5py

# ----------------------------------------------------------------------
def imgDriver(imgPath):
    '''
    imgDriver
    inputs: imgPath
    return: tupl (mean,min,max)
    notes : We currently ignore the images 2D dimensions
    notes : Only BYTE (uint8) raw binary images supported so far.
    '''
    # Read the image into a NUMPY array
    imgAry = np.fromfile(imgPath,dtype='uint8')
    # Perform representative analytics
    imgMean = imgAry.mean()
    imgMin  = imgAry.min()
    imgMax  = imgAry.max()
    # Organize results into a tuple to return
    resultTupl= (imgMean,imgMin,imgMax)
    return resultTupl
    # end::imgDriver()

# ----------------------------------------------------------------------
def sessionClose():
    '''
    sessionClose()
    '''
    print("%s session completed %s"%(os.path.basename(sys.argv[0]),time.ctime()))
    return 0
    # end::sessionClose()

# ----------------------------------------------------------------------
if __name__ == "__main__":
    '''
    main()
    '''
    # parse command line: <inputImgPath> <opModes-future>
    if len(sys.argv)>= 2:
        inputImgPath = sys.argv[1]
        # more inputs will be supported in future
    #
    print("%s session started %s"%(os.path.basename(sys.argv[0]),time.ctime()))
    # Assert: only BYTE raw binary images are supported thus far
    iMean,iMin,iMax = imgDriver(inputImgPath)
    print("Image: %s mean %.2f min %.2f max %.2f"%(inputImgPath, iMean,iMin,iMax))
    #
    sessionClose()
    #

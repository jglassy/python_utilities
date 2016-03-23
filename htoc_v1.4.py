# -*- coding: utf-8 -*-
'''
# script : htoc.py
# invocation: python2.7 htoc.py MyAncillaryFile.h5 {FULL|BRIEF} <OUTPUT-UserOverride.toc>
# revised   : 2015-06-22T16:38:00, pending extension to visit all of a 
#             GROUP's attributes (currently omitted!),which we'll call GROUPATTRIB, 
#             since this is where all the ISO metadata resides!
# revised   : 2016-02-29T14:12, working instance of new htoc under development, goal
#             is to make this htoc.py more robust working with VLEN, DIMENSION SCALES
#             and OBJECT REFERENCES
#
# revised   : 2015-06-22T16:40:00
# author    : joe glassy
# version   : See 'versTag' in code.
# history   : Clean up only, removed redundant cmd line parse section accidentally left
#             in on an earlier session.
# history   : Changed DATASETATTRIB to 'DATAATTRIB' to better distinguish from DATASET
#             when grep'ing nested parses etc.
# purpose   : Produce a fully qualified table-of-contents from an HDF5 file
#          that prefaces dataobjects or attributes with their owner. 
#          And where any new-line separated attribute values are converted
#          to string out on a single line to allow grep to do line wise filtering
# context   : SMAP L4_C Nature Run v4 era
# notes     : Group Attribute reporting is now enabled but we need a consistent
#             way to enable/disable this, as the ISO metadata tree produces a LOT of
#             output that may not always be wanted. For now it is just enabled
#             in the FULL mode.
# notes     : Only 1 arg needed at minimum: htoc.py <myFile.h5> where the result
#             is routed to myFile.toc when user does not override output filename
# notes     : Currently, if you want to explicitly name the output file, you MUST specify either
#             FULL or BRIEF (as the 2nd argument) to set the mode for now
# notes     : Planned future changes: use python's argparse to define option
#             switches e.g. -q "QUIET" suppresses output to console
#             -f enabled FULL qualification, includes filename in each line
#             and, allow user to selectively ENABLE or DISABLE output of any class
#             of content, e.g. +d (include DATASET), -d (exclude DATASET)
#             +fa(include FILE ATTRIBUTES) -fa(exclude FILE ATTRIBUTES)
#             -da(include DATASET ATTRIBUTES) +da(include DATASET attributes)
#             -c(exclude output to CONSOLE) +c(include output to console=default)
#             +s SQLite, include writing output to an Sqlite database named <myFile.toc.sqlite>
#             +t TEXT, include writing output to a text file (default)
# ----------------------------------------------------------------------
# versTag = "1.3-2015-06-22T16:39:00-07:00"
'''
versTag = "1.4-2016-02-29T14:12:00-07:00"

import os,sys
import time, uuid
import random
import math
import hashlib

import numpy as np
# added 5/4/2015 for better filtering out of missing values
import numpy.ma as ma

import h5py

from datetime import date
import datetime

# ----------------------------------------------------------------------
def genHDF5_TableOfContentsBrief(inPath,outf):
    '''
    function: genHDF5_TableOfContentsBrief()
    inputs  : inPath,outf
    purpose : yields a more  abbreviated output form, where each HDF5 object 
              is not fully qualified.
    '''
    tocDic = {} 
    fileAttribDic = {}
    rawLis = []
    ithGrp = 0 
    ithDta = 0
    #
    
    if os.path.exists(inPath):
        hObj = h5py.File(inPath,"r")
        # Recursively descend through all of files objects
        hObj.visit(rawLis.append)
        contentLis = [r.encode('ASCII') for r in rawLis]
        # Add filename itself to the dictionary
        baseFilename = hObj.filename
        tocDic['FILENAME'] = baseFilename
        fileFmtStr = ("FILE,%s"%(baseFilename))
        outf.write("%s\n"%(fileFmtStr))
        print("%s"%(fileFmtStr))
        #
        for itm in sorted(contentLis):
            cls = str(hObj[itm].__class__)
            if cls.find(".group.Group") >= 0:
                clsLbl = "GROUP"
                groupName = itm
                ithGrp += 1
                contentKey = clsLbl + "."+ str(ithGrp).zfill(6)
                tocDic[contentKey] = groupName
                outf.write("%s,%s\n"%(clsLbl,itm))
                # for now, we have DISABLED group attrib display in BRIEF mode
                '''
                # <JMG> 6/22/2015, added to visit all group attributess
                # SWEEP DOWN THROUGH ALL GROUP Attributes here!
                grpAttribNameLis = [a.encode("ASCII") for a in hObj[groupName].attrs]
                nGroupAttribs = len(grpAttribNameLis)
                if nGroupAttribs > 0 and enableGroupAttrib:
                    for grpAtrName in grpAttribNameLis:
                        grpAtrValu = hObj[itm].attrs[grpAtrName]
                        grpAtrKey  = "GROUPATTRIB,"+ grpAtrName
                        tocDic[grpAtrKey] = grpAtrValu
                        # Output
                        grpAtrFmtStr =("GROUPATTRIB,%s,%s,%s"%(groupName,grpAtrName,grpAtrValu))
                        print("%s"%(grpAtrFmtStr))
                        outf.write("%s\n"%(grpAtrFmtStr))                    
                '''
                
            elif cls.find(".dataset.Dataset") >=0:
                clsLbl = "DATASET"
                dsObj = hObj[itm]
                dType = dsObj.dtype
                dShap = dsObj.shape
                datasetName = itm
                ithDta += 1
                contentKey = clsLbl + "."+ str(ithDta).zfill(6)
                tocDic[contentKey] = itm
                # display DATASET name. Add datatype,shape, as well later
                dsFmtStr = ("%s,%s,%s,%s"%(clsLbl,datasetName,dType,dShap))
                tocDic[contentKey] = dsFmtStr
                print("%s"%(dsFmtStr))
                outf.write("%s\n"%(dsFmtStr))
                
                # sweep through dataset level attributes
                dtaAttribNameLis = [a.encode("ASCII") for a in hObj[itm].attrs]
                for dsAtr in dtaAttribNameLis:
                    dsAtrValu = hObj[itm].attrs[dsAtr]
                    dsAtrKey = "DATAATTRIB,"+ dsAtr
                    tocDic[dsAtrKey] = dsAtrValu
                    # Output: dataset's name, datasetAttribute name, and attributes value
                    dsAtrFmtStr =("DATAATTRIB,%s,%s,%s"%(datasetName,dsAtr,dsAtrValu))
                    print("%s"%(dsAtrFmtStr))
                    outf.write("%s\n"%(dsAtrFmtStr))
            
        # Next, sweep through file level attributes
        
        # Collect names of file level attributes
        objAttribNameLis = [a.encode("ASCII") for a in hObj.attrs]
        # Collect and store attrib values based on these names
        for fileAttrName in objAttribNameLis:
            valu = hObj.attrs[fileAttrName]
            fAtrKey = "FILEATTRIB," + fileAttrName
            # store (add) to master TOC dictionary
            tocDic[fAtrKey] = valu
            fmtStr =("%s,%s"%(fAtrKey,valu))
            print("%s"%(fmtStr))
            outf.write("%s\n"%(fmtStr))
            
    else:
        print("@F ERROR Cannot find or open input HDF5 %s"%(inPath))
        sys.exit(-1)
    
    outf.close()
    hObj.close()
    # return completed dictionary
    return tocDic 
    # end::genHDF5_TableOfContentsBrief()

# ----------------------------------------------------------------------
def genHDF5_TableOfContents(inPath,outf):
    '''
    function: genHDF5_TableOfContents() 
    inputs  : inPath -- full path to input HDF5 file
              outf -- file handle to output TOC text file
    purpose : "FULL MODE" yields a fully qualified output form
        where the filename (parent) is included on each line. This allows
        this utility to spin through a collection of files where the results
        may be distinguished (e.g. easily separated) a file wise basis
    returns: TBD
    '''
    tocDic = {} 
    fileAttribDic = {}
    rawLis = []
    ithGrp = 0 
    ithDta = 0
    
    if os.path.exists(inPath):
        hObj = h5py.File(inPath,"r")
        # Recursively descend through all of files objects
        hObj.visit(rawLis.append)
        contentLis = [r.encode('ASCII') for r in rawLis]
        
        # Future use: create a list of each (root-level) object's class identifier
        #  noting that when a dataset or group has attributes, the class of these
        #  attributes is not yet recorded in this list since we don't recurse here
        clsObjLis = [ hObj[itm].__class__ for itm in sorted(contentLis)]
        #  Ideally we want to store these classifiers in a dictionary keyed by 'itm'
        #  probably using a dict comprehension
        # clsDic = {} ; clsDic[itm] = clsObjLis[xxxxx]
        
        # Add filename itself to the secondary output, a dictionary
        currFilename = hObj.filename
        tocDic['FILENAME'] = currFilename
        fileFmtStr = ("FILE,%s"%(hObj.filename))
        outf.write("%s\n"%(fileFmtStr))
        print("%s"%(fileFmtStr))
        #
        for itm in sorted(contentLis):
            cls = str(hObj[itm].__class__)
            if cls.find(".group.Group") >= 0:
                clsLbl = "GROUP"
                groupName = itm                
                ithGrp += 1
                contentKey = clsLbl + "."+ str(ithGrp).zfill(6)
                tocDic[contentKey] = itm
                outf.write("%s,%s,%s\n"%(clsLbl,currFilename,itm))
                # <JMG> 6/22/2015, added to visit all group attributess
                # SWEEP DOWN THROUGH ALL GROUP Attributes here!
                grpAttribNameLis = [a.encode("ASCII") for a in hObj[groupName].attrs]
                nGroupAttribs = len(grpAttribNameLis)
                if nGroupAttribs > 0:
                    for grpAtrName in grpAttribNameLis:
                        grpAtrValu = hObj[itm].attrs[grpAtrName]
                        grpAtrKey  = "GROUPATTRIB,"+ grpAtrName
                        tocDic[grpAtrKey] = grpAtrValu
                        # Output
                        grpAtrFmtStr =("GROUPATTRIB,%s,%s,%s"%(groupName,grpAtrName,grpAtrValu))
                        print("%s"%(grpAtrFmtStr))
                        outf.write("%s\n"%(grpAtrFmtStr))

            elif cls.find(".dataset.Dataset") >=0:
                clsLbl = "DATASET"
                dsObj = hObj[itm]
                dType = dsObj.dtype
                dShap = dsObj.shape
                datasetName = itm
                ithDta += 1
                contentKey = clsLbl + "."+ str(ithDta).zfill(6)
                tocDic[contentKey] = itm
                # display DATASET name. Add datatype,shape, as well later
                dsFmtStr = ("%s,%s,%s,%s,%s"%(clsLbl,currFilename,itm,dType,dShap))
                tocDic[contentKey] = dsFmtStr
                print("%s"%(dsFmtStr))
                outf.write("%s\n"%(dsFmtStr))
                
                # sweep through dataset level attributes
                dtaAttribNameLis = [a.encode("ASCII") for a in hObj[datasetName].attrs]
                for dsAtr in dtaAttribNameLis:
                    dsAtrValu = hObj[datasetName].attrs[dsAtr]
                    # Note: "unwind" any string attributes (contains embedded newlines)
                    # to all fit on one longer line
                    if isinstance(dsAtrValu,str):
                        dsAtrValu = dsAtrValu.replace("\n","")
                    dsAtrKey = "DATAATTRIB,"+ dsAtr
                    tocDic[dsAtrKey] = dsAtrValu
                    dsAtrFmtStr =("DATAATTRIB,%s,%s,%s,%s"%(currFilename,datasetName,dsAtr,dsAtrValu))
                    print("%s"%(dsAtrFmtStr))
                    outf.write("%s\n"%(dsAtrFmtStr))
            
        # Next, sweep through file level attributes
        
        # Collect names of file level attributes
        objAttribNameLis = [a.encode("ASCII") for a in hObj.attrs]
        # Collect and store attrib values based on these names
        for fileAttrName in objAttribNameLis:
            valu = hObj.attrs[fileAttrName]
            # Unwind any multi-line string attributes onto a single line
            if isinstance(valu,str):
                valu = valu.replace("\n","")
            fAtrKey = "FILEATTRIB," + currFilename + "," + fileAttrName
            # store (add) to master TOC dictionary
            tocDic[fAtrKey] = valu
            fmtStr =("%s,%s"%(fAtrKey,valu))
            print("%s"%(fmtStr))
            outf.write("%s\n"%(fmtStr))
            
    else:
        print("@F ERROR Cannot find or open input HDF5 %s"%(inPath))
        sys.exit(-1)
    
    outf.close()
    hObj.close()
    # return completed dictionary
    return tocDic 
    # end::genHDF5_TableOfContents()
    
# ----------------------------------------------------------------------
if __name__ == "__main__":
    '''
    main
    '''
    sesCtlDic = {}
    strtSec = time.time()
    endiSec = time.time()
    elapSec = 0.0
    opMode  = "FULL"   # choices are "FULL" or "
    enableGroupAttrib = False
    #
    hdf5Path = 'undefined-path'
    outPath  = 'undefined-path'

    if len(sys.argv) >= 2:
        hdf5Path = sys.argv[1]
        if not os.path.exists(hdf5Path):
            print("Required HDF5 path (%s) cannot be determined!"%(hdf5Path))
            sys.exit(-1)
            
        if len(sys.argv) >= 3:
            # mode choices: FULL | BRIEF where FULL is the default
            userMode = sys.argv[2].upper()
            opMode = userMode 
            # In this mode, user is allowed to name TOC file whatever is needed
            if len(sys.argv) >= 4:
                outPath = sys.argv[3]

    # retrieve extension, which MIGHT be .h5, or .nc, or .nc4 etc
    if outPath == "undefined-path":
        if len(os.path.split(hdf5Path)[0]) == 0:
            # this is a LOCAL path, e.g. in "." directory
            baseNam = os.path.splitext(hdf5Path)[0]
        else:
            baseNam = os.path.split(hdf5Path)[1]

    outPath = baseNam + ".toc"
        
    print("\n%s (%s) session started %s"%(sys.argv[0],versTag,time.ctime()))
    print("Input HDF5 file %s"%(hdf5Path))
    print("Output TOC path %s"%(outPath))

    # Open (create) output text TOC file    
    outf = open(outPath,"w")
    
    if opMode == "FULL":
        print("Producing FULL  mode output")
        tocDic = genHDF5_TableOfContents(hdf5Path,outf)
    else:
        print("Producing BRIEF mode output")
        tocDoc = genHDF5_TableOfContentsBrief(hdf5Path,outf)

    endiSec = time.time()
    elapSec = endiSec - strtSec
    elapMin = elapSec / 60.0
    elapHrs = elapMin / 60.0

    if opMode == "FULL":
        print("\nProduced FULL  mode output to table-of-contents file: %s"%(outPath))
    else:
        print("\nProduced BRIEF mode output to table-of-contents file %s"%(outPath))
    print("%s version %s session completed %s requiring %.2f sec"%(sys.argv[0],versTag,time.ctime(),elapSec))
    print("%s version %s elapsed minutes %.2f (%.2f hours)"%(sys.argv[0],versTag,elapMin,elapHrs))
    
    sys.exit(0)
    # end::main()

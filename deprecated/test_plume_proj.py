
import os
import sys
import os.path
import numpy as np
import xml.etree.ElementTree as ET
import uuid
import requests
import json
from lxml import etree
import boto3
from boto.s3.key import Key
from osgeo import gdal,ogr,osr
import glob
import argparse
from PIL import Image
import tempfile



#https://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
def GetExtent(gt,cols,rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner
    '''
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
        yarr.reverse()
    return ext

#https://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
def ReprojectCoords(coords,src_srs,tgt_srs):
    ''' Reproject a list of x,y coordinates.

        @type geom:     C{tuple/list}
        @param geom:    List of [[x,y],...[x,y]] coordinates
        @type src_srs:  C{osr.SpatialReference}
        @param src_srs: OSR SpatialReference object
        @type tgt_srs:  C{osr.SpatialReference}
        @param tgt_srs: OSR SpatialReference object
        @rtype:         C{tuple/list}
        @return:        List of transformed [[x,y],...[x,y]] coordinates
    '''
    trans_coords=[]
    transform = osr.CoordinateTransformation( src_srs, tgt_srs)
    for x,y in coords:
        x,y,z = transform.TransformPoint(x,y)
        trans_coords.append([x,y])
    return trans_coords


#https://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
def geotiff_spatial(tiffpath):
    ds = gdal.Open(tiffpath)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize

    extent = GetExtent(gt, cols, rows)

    src_srs = osr.SpatialReference()

    print src_srs
    print ds.GetProjection()


    src_srs.ImportFromWkt(ds.GetProjection())

    print  src_srs
    tgt_srs = src_srs.CloneGeogCS()
    geo_ext = ReprojectCoords(extent, src_srs, tgt_srs)

    lats = [e[1] for e in geo_ext]
    lons = [e[0] for e in geo_ext]

    uy = np.max(lats)
    ly = np.min(lats)

    lx = np.min(lons)
    rx = np.max(lons)

    ul = (lx, uy)
    ll = (lx, ly)
    lr = (rx, ly)
    ur = (rx, uy)

    shape_extents = [
        ul,
        ll,
        lr,
        ur
    ]
    return shape_extents




if __name__ == "__main__":
    geotiff = sys.argv[1]

    shape = geotiff_spatial(geotiff)
    print shape
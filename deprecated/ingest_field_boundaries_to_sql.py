import utm
import numpy as np
import os
import sys
import os.path
import requests
import json
import argparse
import ingestutils.s3util as s3util
from osgeo import gdal,ogr,osr

import psycopg2


DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""

def envelope_to_polygon(envelope):
    ring = ogr.Geometry(ogr.wkbLinearRing)

    ring.AddPoint(envelope[0], envelope[3])
    ring.AddPoint(envelope[0], envelope[2])
    ring.AddPoint(envelope[1], envelope[2])
    ring.AddPoint(envelope[1], envelope[3])
    ring.AddPoint(envelope[0], envelope[3])
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    poly.FlattenTo2D()
    return poly


def process_feature(cur, feature, transform, layerDefinition):
    geom = feature.GetGeometryRef()
    geom.Transform(transform)

    feature_type = geom.GetGeometryName().lower()
    centroid = geom.Centroid()

    geom.FlattenTo2D()
    if geom.GetGeometryType() == ogr.wkbPolygon:
        geom = ogr.ForceToMultiPolygon(geom)
    elif geom.GetGeometryType() == ogr.wkbPoint:
        bufferDistance = 0.001
        geom = geom.Buffer(bufferDistance)
        geom = ogr.ForceToMultiPolygon(geom)

    envelope = envelope_to_polygon(geom.GetEnvelope())

    feature_name = feature.GetField("NAME")
    feature_field_code = feature.GetField("FIELD_CODE")
    area_sq_mi = feature.GetField("AREA_SQ_MI")
    area_acre = feature.GetField("AREA_ACRE")
    perimeter = feature.GetField("PERIMETER")
    district = feature.GetField("District")

    sql = """
    insert into field_boundaries 
      (
        feature_name,
        field_code,
        area_sq_mi,
        area_acre,
        perimeter,
        district,
        field_location,
        field_envelope,
        field_shape
      )
    values 
      (
        %s, %s, %s, %s, %s, %s, 
        ST_GeomFromText('POINT(%s %s)', 4326),
	    ST_GeomFromText(%s, 4326),
	    ST_GeomFromText(%s, 4326)
      )    
    """

    cur.execute(sql, (
        feature_name,
        feature_field_code,
        area_sq_mi,
        area_acre,
        perimeter,
        district,
        centroid.GetX(),
        centroid.GetY(),
        envelope.ExportToWkt(),
        geom.ExportToWkt()
    ))



def process_input_file(input_file, verbose=False, test_only=False):
    file = ogr.Open(input_file)

    targetSpatialRef = osr.SpatialReference()
    targetSpatialRef.ImportFromEPSG(4326)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWD, host=DB_ENDPOINT, port=DB_PORT)
    cur = conn.cursor()

    for layer_num in range(0, file.GetLayerCount()):
        layer = file.GetLayer(layer_num)

        layerDefinition = layer.GetLayerDefn()

        spatialRef = layer.GetSpatialRef()
        transform = osr.CoordinateTransformation(spatialRef, targetSpatialRef)

        for feature_num in range(0, layer.GetFeatureCount()):

            feature = layer.GetFeature(feature_num)
            process_feature(cur, feature, transform, layerDefinition)

    conn.commit()
    cur.close()
    conn.close()


def gdal_error_handler(err_class, err_num, err_msg):
    errtype = {
            gdal.CE_None:'None',
            gdal.CE_Debug:'Debug',
            gdal.CE_Warning:'Warning',
            gdal.CE_Failure:'Failure',
            gdal.CE_Fatal:'Fatal'
    }
    err_msg = err_msg.replace('\n',' ')
    err_class = errtype.get(err_class, 'None')
    print 'Error Number: %s' % (err_num)
    print 'Error Type: %s' % (err_class)
    print 'Error Message: %s' % (err_msg)

gdal.PushErrorHandler(gdal_error_handler)

if __name__ == "__main__":


    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input Field Boundary file(s)", required=True, type=str, nargs='+')
    parser.add_argument("-e", "--endpoint", help="Target PostgreSQL endpoint", type=str, default="localhost",
                        required=False)
    parser.add_argument("-p", "--port", help="Target PostgreSQL port", type=str, default="8983", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test by parsing and assembling upload document. Don't actually upload to Solr.",
                        required=False, action="store_true")

    args = parser.parse_args()
    input_files = args.data
    test_only = args.test
    verbose = args.verbose

    for input_file in input_files:
        process_input_file(input_file, verbose, test_only)
import os
import sys
import os.path
import uuid
import requests
import json
from osgeo import gdal,ogr,osr
import argparse
import psycopg2


DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""



def ingest_counties_shapefile(shpfile_path,  solr_host=None, test_only=False, verbose=False):
    if verbose is True:
        print "Processing input file:", shpfile_path

    if verbose is True:
        print "Loading file into memory..."

    file = ogr.Open(shpfile_path)

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

            geom = feature.GetGeometryRef()
            geom.Transform(transform)
            centroid = geom.Centroid()

            name = feature.GetField("NAME")
            coname = feature.GetField("CONAME")
            area = feature.GetField("AREA")
            perimeter = feature.GetField("PERIMETER")
            cacoa = feature.GetField("CACOA_")
            cacoa_id = feature.GetField("CACOA_ID")
            conum = feature.GetField("CONUM")
            dsslv = feature.GetField("dsslv")

            if verbose:
                print name, coname, type(area), type(perimeter)

            geom.FlattenTo2D()
            if geom.GetGeometryType() == ogr.wkbPolygon:
                geom = ogr.ForceToMultiPolygon(geom)

            sql = """
            insert into counties
              (
                name,
                coname,
                area,
                perimeter,
                cacoa,
                cacoa_id,
                conum,
                dsslv,
                geojson,
                county_location,
                county_shape
              ) values (
              %s, %s, %s, %s, %s, %s, %s, %s, %s,
                ST_GeomFromText(%s, 4326),
                ST_GeomFromText(%s, 4326)
              )
            """

            cur.execute(sql,
                        (
                            name,
                            coname,
                            area,
                            perimeter,
                            cacoa,
                            cacoa_id,
                            conum,
                            dsslv,
                            feature.ExportToJson(),
                            centroid.ExportToWkt(),
                            geom.ExportToWkt()
                        )
                    )


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

if __name__ == "__main__":

    os.environ["GDAL_DATA"] = '/Users/kgill/srv/share/gdal/'

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input California Counties Shapefile(s)", required=True, type=str)
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
    input_file = args.data
    test_only = args.test
    verbose = args.verbose

    ingest_counties_shapefile(input_file, test_only=test_only, verbose=verbose)



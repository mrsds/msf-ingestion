import os
import sys
import os.path
import numpy as np
import uuid
import requests
import json
import boto3
from boto.s3.key import Key
from osgeo import gdal,ogr,osr
import glob
import argparse
import tempfile
from io import BytesIO
import ingestutils.s3util as s3util
import utm
import traceback


SOLR_URL = "http://%s:%s"

CATEGORY_MAP = {
    "Oil & Gas Wells": 0,
    "Oil_NG_Wells": 0,
    "Livestock Dairies": 1,
    "Anaerobic Lagoons": 2,
    "CNG Fueling Stations": 3,
    "CNG_fueling_stations": 3,
    "LNG Fueling Stations": 4,
    "LNG_fueling_stations": 4,
    "Natural Gas Storage Fields": 5,
    "NG_Storage_Fields": 5,
    "Natural Gas Processing Plants": 6,
    "NG_Processing_Plants": 6,
    "Natural Gas Compressor Stations": 7,
    "NG_Compressor_Stations": 7,
    "Petroleum Refineries": 8,
    "Petroleum_Refinery": 8,
    "Wastewater Treatment Plants": 9,
    "Wastewater_Treatment_Plants": 9,
    "Power Plants": 10,
    "Power_Plants": 10,
    "Landfills": 11,
    "Disposal": 11,
    "In-Vessel Digestion": 11,
    "EMSW Conversion": 11,
    "Natural Gas Pipelines": 12,
    "NG_Pipelines": 12,
    "NG_Pipelines_CEC": 12,
    "SoCAB Boundary": 13,
    "petrorefinery_CARB_SFbayarea": 14,
    "Dairy_Kern": 1,
    "Field Boundary": 16,
    "Dairy_California": 1
}



def process_shape(feature, transform, layerDefinition, category, category_id, solr_host=None, test_only=False, verbose=False):
    geom = feature.GetGeometryRef()
    geom.Transform(transform)

    site_name = feature.GetField("LSiteName")

    feature_type = geom.GetGeometryName().lower()

    centroid = geom.Centroid()

    doc = {
        "id": str(uuid.uuid4()),
        "record_type": "vista",
        "category": category,
        "category_id": category_id,
        "name": site_name,
        "description": None,
        "shape_type": feature_type,
        "shape_centroid_location": centroid.ExportToWkt(),
        "shape_poly_shape_t": geom.ExportToWkt(),
        "geojson_t": feature.ExportToJson()
    }


    for i in range(layerDefinition.GetFieldCount()):
        field_name = layerDefinition.GetFieldDefn(i).GetName()
        field_value = feature.GetField(field_name)
        doc["map_%d_name_s" % i] = field_name
        doc["map_%d_value_s" % i] = field_value

        if field_name == "LLong":
            doc["l_lon_f"] = float(field_value)
        elif field_name == "LLat":
            doc["l_lat_f"] = float(field_value)
        elif field_name == "LOperator":
            doc["l_operator_s"] = str(field_value)
        elif field_name == "LSiteName":
            doc["l_sitename_s"] = str(field_value)
        elif field_name == "LState":
            doc["l_state_s"] = str(field_value)
        elif field_name == "LAddress":
            doc["l_address_s"] = str(field_value)
        elif field_name == "LSector":
            doc["l_sector_s"] = str(field_value)
        elif field_name == "LRelDate":
            doc["l_reldate_s"] = str(field_value)
        elif field_name == "LCity":
            doc["l_city_s"] = str(field_value)


    if test_only is False:
        if solr_host is not None:
            r = requests.post('%s/solr/MSF/update/json/docs?commit=true'%solr_host, data=json.dumps(doc),
                                  headers={"Content-Type": "application/json"})

            if r.status_code != 200:
                print r.status_code, r.text
        else:
            print "Solr host not specified, cannot upload documents"
            sys.exit(1)


def parse_category_from_filename(input_path):
    bn = os.path.basename(input_path)

    if bn == "Field_Boundaries.shp":
        return "Field Boundary", CATEGORY_MAP["Field Boundary"]

    category = bn[0:bn.index("_L1")]
    category_id = CATEGORY_MAP[category]
    return category, category_id


def process_vista_shapefile(input_path, solr_host=None, test_only=False, verbose=False):

    if verbose is True:
        print "Processing input file:", input_path

    category, category_id = parse_category_from_filename(input_path)

    if verbose is True:
        print "Category:", category
        print "Category ID:", category_id

    if verbose is True:
        print "Loading file into memory..."

    file = ogr.Open(input_path)

    targetSpatialRef = osr.SpatialReference()
    targetSpatialRef.ImportFromEPSG(4326)

    for layer_num in range(0, file.GetLayerCount()):
        layer = file.GetLayer(layer_num)

        layerDefinition = layer.GetLayerDefn()

        spatialRef = layer.GetSpatialRef()
        transform = osr.CoordinateTransformation(spatialRef, targetSpatialRef)

        for feature_num in range(0, layer.GetFeatureCount()):

            feature = layer.GetFeature(feature_num)

            try:
                process_shape(feature, transform, layerDefinition, category, category_id, solr_host=solr_host, test_only=test_only, verbose=verbose)
            except:
                traceback.print_exc()

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

    os.environ["GDAL_DATA"] = '/Users/kgill/srv/share/gdal/'

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input VISTA Shapefile(s)", required=True, type=str, nargs='+')
    parser.add_argument("-s", "--solrhost", help="Target Solr host", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--solrport", help="Target Solr port", type=str, default="8983", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test by parsing and assembling upload document. Don't actually upload to Solr.",
                        required=False, action="store_true")

    args = parser.parse_args()
    input_files = args.data
    test_only = args.test
    solr_host = SOLR_URL % (args.solrhost, args.solrport)
    verbose = args.verbose

    for input_file in input_files:
        process_vista_shapefile(input_file, solr_host=solr_host, test_only=test_only, verbose=verbose)
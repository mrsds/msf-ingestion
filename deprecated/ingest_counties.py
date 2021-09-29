import os
import sys
import os.path
import uuid
import requests
import json
from osgeo import gdal,ogr,osr
import argparse

SOLR_URL = "http://%s:%s"



def ingest_counties_shapefile(shpfile_path,  solr_host=None, test_only=False, verbose=False):
    if verbose is True:
        print "Processing input file:", shpfile_path

    if verbose is True:
        print "Loading file into memory..."

    file = ogr.Open(shpfile_path)

    targetSpatialRef = osr.SpatialReference()
    targetSpatialRef.ImportFromEPSG(4326)

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

            doc = {
                "id": str(uuid.uuid4()),
                "record_type": "county",
                "name_s": name,
                "coname_s": coname,
                "area_f": area,
                "perimeter_f": perimeter,
                "cacoa_s": cacoa,
                "cacoa_id_s": cacoa_id,
                "conum_s": conum,
                "dsslv_s": dsslv,
                "shape_centroid_location": centroid.ExportToWkt(),
                "shape_poly_shape_t": geom.ExportToWkt(),
                "geojson_t": feature.ExportToJson()
            }

            if verbose:
                print json.dumps(doc)

            if test_only is False:
                if solr_host is not None:
                    r = requests.post('%s/solr/MSF/update/json/docs?commit=true' % solr_host, data=json.dumps(doc),
                                      headers={"Content-Type": "application/json"})

                    if r.status_code != 200:
                        print r.status_code, r.text
                else:
                    print "Solr host not specified, cannot upload documents"
                    sys.exit(1)


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
    parser.add_argument("-s", "--solrhost", help="Target Solr host", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--solrport", help="Target Solr port", type=str, default="8983", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test by parsing and assembling upload document. Don't actually upload to Solr.",
                        required=False, action="store_true")

    args = parser.parse_args()
    input_file = args.data
    test_only = args.test
    solr_host = SOLR_URL % (args.solrhost, args.solrport)
    verbose = args.verbose

    ingest_counties_shapefile(input_file, solr_host=solr_host, test_only=test_only, verbose=verbose)


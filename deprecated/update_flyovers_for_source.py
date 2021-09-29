import requests
import json
import argparse
import ingestutils.counts as counts
import sys
import traceback
import math
import re
from math import radians, cos, sin, asin, sqrt
from ingestutils.progress import printProgress
from osgeo import gdal,ogr,osr

#SOLR_URL = "http://100.64.114.155:8983"
#SOLR_URL = "http://localhost:8983"

SOLR_URL = "http://%s:%s"



def get_source_objects(solr_host, items_per_page=1000, page=0):
    params= {
            "q": "record_type:sources-source",
            "indent": "on",
            "wt": "json",
            "rows": items_per_page,
            "start": (items_per_page * page)
        }

    url = "%s/solr/MSF/select"%(solr_host)
    r = requests.get(url, params=params)
    response = json.loads(r.text)
    d = response["response"]["docs"]

    num_found = response["response"]["numFound"]
    return d, num_found


def get_flyovers_at_point(min_lat, min_lon, max_lat, max_lon, solr_host):
    field = "flight_shape"
    params = {
        "q": "record_type:aviris-flightline",
        "indent": "on",
        "fq": "%s:[%f,%f TO %f,%f]" % (field, min_lat, min_lon, max_lat, max_lon),
        "wt": "json",
        "rows": 1000
    }

    url = "%s/solr/MSF/select" % solr_host
    r = requests.get(url, params=params)

    response = json.loads(r.text)
    d = response["response"]["docs"]

    return d


def get_plumes_for_flightlight(flight_name, source_id, solr_host):
    params = {
        "q": "record_type:aviris-plume",
        "indent": "on",
        "fq": [
            "source_id_s:%s"%source_id,
            "name:%s*" % (flight_name)
            ],
        "wt": "json",
        "rows": 1000
    }

    url = "%s/solr/MSF/select" % solr_host
    r = requests.get(url, params=params)

    response = json.loads(r.text)
    d = response["response"]["docs"]

    return d

def process_source_object(source_object, solr_host, test_only):


    #print source_object

    shape_poly = "POINT ({lon} {lat})".format(lat=source_object["source_latitude_f"], lon=source_object["source_longitude_f"])
    poly = ogr.CreateGeometryFromWkt(shape_poly)
    min_lon, max_lon, min_lat, max_lat = poly.GetEnvelope()

    flyovers = get_flyovers_at_point(min_lat, min_lon, max_lat, max_lon, solr_host)

    print source_object["source_identifier_s"], len(flyovers)
    for flyover in flyovers:
        plumes = get_plumes_for_flightlight(flyover["flight_name"], source_object["source_identifier_s"], solr_host)

        print "\t", flyover["flight_name"], len(plumes)



def process_source_objects(solr_host, test_only=False, num_items_per_page = 1000):
    foo, num_sources_found = get_source_objects(solr_host, items_per_page=0, page=0)
    num_source_pages = num_sources_found / num_items_per_page + 1


    for page in range(0, num_source_pages):
        #printProgress(page, num_source_pages)
        source_objects, count = get_source_objects(solr_host, num_items_per_page, page=page)
        for source_object in source_objects:
            process_source_object(source_object, solr_host, test_only)
            #break
        break

    #printProgress(num_source_pages, num_source_pages)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--solrhost", help="Target Solr host", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--solrport", help="Target Solr port", type=str, default="8983", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test. Don't actually upload to Solr.",
                        required=False, action="store_true")


    args = parser.parse_args()
    solr_host = SOLR_URL % (args.solrhost, args.solrport)
    verbose = args.verbose
    test_only = args.test

    process_source_objects(solr_host=solr_host, test_only=test_only)
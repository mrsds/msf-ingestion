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

#SOLR_URL = "http://100.64.114.155:8983"
#SOLR_URL = "http://localhost:8983"

SOLR_URL = "http://%s:%s"






MEAN_RADIUS_EARTH_METERS = 6371010.0
EQUATORIAL_RADIUS_EARTH_METERS = 6378140.0
POLAR_RADIUS_EARTH_METERS = 6356752.0
FLATTENING_EARTH = 298.257223563
MEAN_RADIUS_EARTH_MILES = 3958.8

class DistanceUnit(object):
    METERS = 0
    MILES = 1


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

# Equirectangular approximation for when performance is key. Better at smaller distances
def equirectangularApprox(x0, y0, x1, y1):
    R = 6371000.0 # Meters
    x0r = x0 * (math.pi / 180.0) # To radians
    x1r = x1 * (math.pi / 180.0)
    y0r = y0 * (math.pi / 180.0)
    y1r = y1 * (math.pi / 180.0)

    x = (y1r - y0r) * math.cos((x0r + x1r) / 2.0)
    y = x1r - x0r
    d = math.sqrt(x*x + y*y) * R
    return d


def get_solr_objects(solr_host, query="*:*", items_per_page=1000, page=0):
    params= {
            "q": query,
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


def get_vista_objects(solr_host, items_per_page=1000, page=0):
    return get_solr_objects(solr_host, query="record_type:vista", items_per_page=items_per_page, page=page)


def get_sources_objects(solr_host, items_per_page=1000, page=0):
    return get_solr_objects(solr_host, query="record_type:sources-source", items_per_page=items_per_page, page=page)


def get_vista_object_at_coords(find_lat, find_lon, solr_host):

    params = {
        "q": "record_type:vista",
        "fq": "{!geofilt}",
        "sfield": "shape_centroid_location",
        "pt": ", ".join(map(str, [find_lat, find_lon])),
        #"facet.query": ["{!frange l=0 u=5}geodist()", "{!frange l=5.001 u=3000}geodist()"],
        "d": 10,
        "bf": "recip(geodist(),2,200,20)",
        "sort": "score desc",
        "indent": "on",
        "wt": "json",
        "rows": 1000
    }

    url = "%s/solr/MSF/select" % solr_host
    r = requests.get(url, params=params)
    response = json.loads(r.text)
    num_results = response["response"]["numFound"]
    docs = response["response"]["docs"]
    return docs, num_results

def get_source_object_at_coords(find_lat, find_lon, solr_host):

    params = {
        "q": "record_type:souces-source",
        "fq": "{!geofilt}",
        "sfield": "shape_centroid_location",
        "pt": ", ".join(map(str, [find_lat, find_lon])),
        #"facet.query": ["{!frange l=0 u=5}geodist()", "{!frange l=5.001 u=3000}geodist()"],
        "d": 10,
        "bf": "recip(geodist(),2,200,20)",
        "sort": "score desc",
        "indent": "on",
        "wt": "json",
        "rows": 1000
    }

    url = "%s/solr/MSF/select" % solr_host
    r = requests.get(url, params=params)
    response = json.loads(r.text)
    num_results = response["response"]["numFound"]
    docs = response["response"]["docs"]
    return docs, num_results


def parse_point(s):
    g = re.search("(?<=\().*(?<!\))", s)
    if g is None:
        raise Exception("Could not understand POINT string")

    ps = map(float, g.group(0).split(" "))
    return ps


def distance(source, vista_doc):
    vista_obj_centroid = parse_point(vista_doc["shape_centroid_location"])
    source_est_dist = haversine(source["source_longitude_f"], source["source_latitude_f"], vista_obj_centroid[0], vista_obj_centroid[1])
    return source_est_dist

def sort_vista_docs(source, vista_docs):
    sorted_list = sorted(vista_docs, key=lambda vista_doc: distance(source, vista_doc))
    return sorted_list


"""
def process_vista_page(page_num, num_items_per_page=1000, solr_host=None):
    vista_objects, num_vista_found = get_vista_objects(solr_host, items_per_page=num_items_per_page, page=page_num)

    for vista_object in vista_objects:
        vista_longitude, vista_latitude = parse_point(vista_object["shape_centroid_location"])
        get_source_object_at_coords(vista_latitude, vista_longitude, solr_host)
"""


def get_appended_field_value(vista_object, field, new_value):
    if field in vista_object:
        field_value = vista_object[field]
        if type(field_value) == list:
            field_value.append(new_value)
    else:
        field_value = [new_value]

    return field_value



def clear_source_relationship_for_vista_object(id, solr_host):
    update_fields = {
        "source_identifier_t": None,
        "source_latitude_fs": None,
        "source_longitude_fs": None,
        "source_nearest_facility_t": None,
        "source_type_t": None,
        "source_area_t": None,
        "source_internal_identifier_t": None,
        "source_sector_t": None,
        "source_estimated_distance_from_centroid_fs": None
    }
    counts.update_fields(id, update_fields, solr_host, record_type="vista")



def clear_all_source_relationships(solr_host, num_items_per_page = 1000):
    foo, num_vista_found = get_vista_objects(solr_host, items_per_page=0, page=0)
    num_vista_pages = num_vista_found / num_items_per_page + 1


    for page in range(0, num_vista_pages):
        printProgress(page, num_vista_pages)
        vista_objects, count = get_vista_objects(solr_host, num_items_per_page, page=page)
        for vista_object in vista_objects:
            clear_source_relationship_for_vista_object(vista_object["id"], solr_host)

    printProgress(num_vista_pages, num_vista_pages)

    counts.commit_changes_to_solr(solr_host=solr_host, solr_core="MSF")

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
    parser.add_argument("-c", "--clearrelationships",
                        help="Clear all existing source/vista relationships", required=False, action="store_true")

    args = parser.parse_args()
    solr_host = SOLR_URL % (args.solrhost, args.solrport)
    verbose = args.verbose
    test_only = args.test
    clearrelationships = args.clearrelationships

    num_items_per_page = 1000

    if clearrelationships and not test_only:
        print "Clearing all Vista/Source Relationships in Solr..."
        clear_all_source_relationships(solr_host, 100)
        print "Done"
        sys.exit()



    foo, num_sources_found = get_sources_objects(solr_host, items_per_page=0, page=0)
    num_sources_pages = num_sources_found / num_items_per_page + 1

    #foo, num_vista_found = get_vista_objects(solr_host, items_per_page=0, page=0)
    #num_vista_pages = num_vista_found / num_items_per_page + 1

    sources, num_sources_found = get_sources_objects(solr_host, items_per_page=num_items_per_page, page=0)



    for source in sources:
        source_lat = source["source_latitude_f"]
        source_lon = source["source_longitude_f"]

        matching_vista_objects, num_found = get_vista_object_at_coords(source_lat, source_lon, solr_host=solr_host)
        matching_vista_objects = sort_vista_docs(source, matching_vista_objects)
        for vista_object in matching_vista_objects:

            source_est_dist = distance(source, vista_object)

            source_identifier = None if "source_identifier_s" not in source else source["source_identifier_s"]
            nearest_facility = None if "nearest_facility_s" not in source else source["nearest_facility_s"]
            source_type = None if "source_type_s" not in source else source["source_type_s"]
            source_area = None if "area_s" not in source else source["area_s"]
            source_id = None if "id" not in source else source["id"]
            source_sector = None if "sectors_s" not in source else source["sectors_s"]
            vista_site_name = None if "l_sitename_s" not in vista_object else vista_object["l_sitename_s"]

            print "Source: ", source_identifier, nearest_facility, ", Facility: ", vista_object["id"], vista_site_name, " - Estimated Distance: ", source_est_dist, " km"


            source_identifier_t = get_appended_field_value(vista_object, "source_identifier_t", source_identifier)
            source_latitude_fs = get_appended_field_value(vista_object, "source_latitude_fs", source_lat)
            source_longitude_fs = get_appended_field_value(vista_object, "source_longitude_fs", source_lon)
            source_nearest_facility_t = get_appended_field_value(vista_object, "source_nearest_facility_t",  nearest_facility)
            source_type_t = get_appended_field_value(vista_object, "source_type_t", source_type)
            source_area_t = get_appended_field_value(vista_object, "source_area_t", source_area)
            source_internal_identifier_t = get_appended_field_value(vista_object, "source_internal_identifier_t", source_id)
            source_sector_t = get_appended_field_value(vista_object, "source_sector_t", source_sector)
            source_estimated_distance_from_centroid_fs = get_appended_field_value(vista_object, "source_estimated_distance_from_centroid_fs", source_est_dist)

            update_fields = {
                    "source_identifier_t": source_identifier_t,
                    "source_latitude_fs": source_latitude_fs,
                    "source_longitude_fs": source_longitude_fs,
                    "source_nearest_facility_t": source_nearest_facility_t,
                    "source_type_t":  source_type_t,
                    "source_area_t": source_area_t,
                    "source_internal_identifier_t": source_internal_identifier_t,
                    "source_sector_t": source_sector_t,
                    "source_estimated_distance_from_centroid_fs": source_estimated_distance_from_centroid_fs
                }

            if verbose:
                print update_fields

            if not test_only:
                counts.update_fields(vista_object["id"], update_fields, solr_host)
            break # Take only the closest

        #break

    if not test_only:
        counts.commit_changes_to_solr(solr_host=solr_host, solr_core="MSF")
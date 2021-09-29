
import requests
import json
from osgeo import ogr
import traceback

def query_aviris_count_at_point(min_lat, min_lon, max_lat, max_lon, solr_host, record_type="aviris-flightline"):

    field = "flight_shape" if record_type == "aviris-flightline" else "plume_shape"

    params = {
        "q": "record_type:%s"%(record_type),
        "indent": "on",
        "fq": "%s:[%f,%f TO %f,%f]" % (field, min_lat, min_lon, max_lat, max_lon),
        "wt": "json",
        "rows": 0
    }

    url = "%s/solr/MSF/select" % solr_host
    r = requests.get(url, params=params)

    num_results = json.loads(r.text)["response"]["numFound"]
    return num_results


def update_fields(id, properties, solr_host, record_type=None):
    _update_solr_fields(id, properties, solr_host, "MSF", record_type)


def update_vista_fields(id, properties, solr_host, record_type=None):
    _update_solr_fields(id, properties, solr_host, "vista", record_type)


def update_aviris_fields(id, properties, solr_host, record_type=None):
    _update_solr_fields(id, properties, solr_host, "aviris", record_type)


def update_sources_fields(id, properties, solr_host, record_type=None):
    _update_solr_fields(id, properties, solr_host, "sources", record_type)


def _update_solr_fields(id=None, properties={}, solr_host="localhost", solr_core="MSF", record_type=None):

    update_props = {}

    if id is not None:
        update_props["id"] = id

    if record_type is not None:
        update_props["record_type"] = record_type

    for key in properties:
        update_props[key] = {
            "set": properties[key]
        }

    update = [
     update_props
    ]

    url = "%s/solr/%s/update" % (solr_host, solr_core)

    r = requests.post(url=url, json=update)
    if r.status_code == 200:
        pass
    else:
        raise Exception("Failed to update VISTA fields on remote Solr host (status code: %s)"%r.status_code)


def update_vista_counts(id, num_flights_matching, num_plumes_matching, solr_host):

    has_flights = 1 if num_flights_matching > 0 else 0
    has_plumes = 1 if num_plumes_matching > 0 else 0
    update_fields(id, {
        "num_flights_matching_i": num_flights_matching,
        "num_plumes_matching_i": num_plumes_matching,
        "has_flights_i": has_flights,
        "has_plumes_i": has_plumes
    }, solr_host)



def commit_changes_to_solr(solr_host, solr_core="MSF"):
    url = "%s/solr/%s/update" % (solr_host, solr_core)
    r = requests.get(url=url, params={"stream.body": "<commit/>"})


def process_vista_object(vista_object, solr_host, test_only=False, verbose=False):

    shape_poly = vista_object["shape_poly_shape_t"][0]
    poly = ogr.CreateGeometryFromWkt(shape_poly)
    min_lon, max_lon, min_lat, max_lat = poly.GetEnvelope()


    num_flights_matching = query_aviris_count_at_point(min_lat, min_lon, max_lat, max_lon, solr_host, record_type="aviris-flightline")
    num_plumes_matching = query_aviris_count_at_point(min_lat, min_lon, max_lat, max_lon, solr_host, record_type="aviris-plume")

    if verbose:
        print "Vista Object", vista_object["name"], "(", vista_object["id"], ") has", num_flights_matching, "matching flights and", num_plumes_matching, "matching plumes"

    if not test_only:
        update_vista_counts(vista_object["id"], num_flights_matching, num_plumes_matching, solr_host)
        #print vista_object["name"], " (", vista_object["category"], ") has", num_flights_matching, "matching fly-overs, and", num_plumes_matching, "matching plumes"


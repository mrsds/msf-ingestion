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




def get_vista_objects(solr_host, items_per_page=1000, page=0):
    params= {
            "q": "record_type:vista",
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

def process_vista_object(vista_object, solr_host, test_only):
    property_map = {}

    for i in range(0, 60):
        if "map_%d_name_s" % i in vista_object and "map_%d_value_s" % i in vista_object:
            key = vista_object["map_%d_name_s" % i]
            value = vista_object["map_%d_value_s" % i]
            property_map[key] = value

    if "Status" in property_map:
        status = property_map["Status"]
    else:
        status = None

    update_fields = {
        "facility_status_s": status
    }

    if not test_only:
        counts.update_fields(vista_object["id"], update_fields, solr_host)



def process_vista_objects(solr_host, test_only=False, num_items_per_page = 1000):
    foo, num_vista_found = get_vista_objects(solr_host, items_per_page=0, page=0)
    num_vista_pages = num_vista_found / num_items_per_page + 1


    for page in range(0, num_vista_pages):
        printProgress(page, num_vista_pages)
        vista_objects, count = get_vista_objects(solr_host, num_items_per_page, page=page)
        for vista_object in vista_objects:
            process_vista_object(vista_object, solr_host, test_only)

        break

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


    args = parser.parse_args()
    solr_host = SOLR_URL % (args.solrhost, args.solrport)
    verbose = args.verbose
    test_only = args.test

    process_vista_objects(solr_host=solr_host, test_only=test_only)
import requests
import json
import argparse
import ingestutils.counts as counts
import sys
import traceback




#SOLR_URL = "http://100.64.114.155:8983"
#SOLR_URL = "http://localhost:8983"

SOLR_URL = "http://%s:%s"




#http://localhost:8983/solr/vista/select?fl=id,%20map_*&indent=on&q=id:e48277e7-5f68-416f-9fb5-e4e3eca75949&wt=json
def get_vista_objects(solr_host):
    params= {
            "q": "record_type:vista",
            "fl": "id,map_*",
            "indent": "on",
            "wt": "json",
            "rows": 300000
        }

    url = "%s/solr/vista/select"%solr_host
    r = requests.get(url, params=params)
    d = json.loads(r.text)["response"]["docs"]
    return d


def process_vista_object(vista_object_doc, solr_host, test_only=False, verbose=False):

    property_map = {}

    for i in range(0, 60):
        if "map_%d_name_s" % i in vista_object_doc and "map_%d_value_s" % i in vista_object_doc:
            key = vista_object_doc["map_%d_name_s" % i]
            value = vista_object_doc["map_%d_value_s" % i]
            property_map[key] = value

    llong = property_map["LLong"] if "LLong" in property_map else None
    loperator = property_map["LOperator"] if "LOperator" in property_map else None
    lsitename = property_map["LSiteName"] if "LSiteName" in property_map else None
    lstate = property_map["LState"] if "LState" in property_map else None
    laddress = property_map["LAddress"] if "LAddress" in property_map else None
    llat = property_map["LLat"] if "LLat" in property_map else None
    lsector = property_map["LSector"] if "LSector" in property_map else None
    lreldate = property_map["LRelDate"] if "LRelDate" in property_map else None
    lcity = property_map["LCity"] if "LCity" in property_map else None

    if verbose:
        print vista_object_doc["id"]

    counts.update_fields(vista_object_doc["id"], {
        "l_lon_f": float(llong),
        "l_lat_f": float(llat),
        "l_operator_s": str(loperator),
        "l_sitename_s": str(lsitename),
        "l_state_s": str(lstate),
        "l_address_s": str(laddress),
        "l_sector_s": str(lsector),
        "l_reldate_s": str(lreldate),
        "l_city_s": str(lcity)
    }, solr_host)



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

    vista_objects = get_vista_objects(solr_host)

    for vista_object in vista_objects:
        try:
            process_vista_object(vista_object, solr_host=solr_host, test_only=test_only, verbose=verbose)
        except:
            traceback.print_exc()
            print "Unexpected error:", sys.exc_info()[0]

    counts.commit_changes_to_solr(solr_host=solr_host)
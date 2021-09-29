import requests
import json
import argparse
import ingestutils.counts as counts
import sys
import traceback

#SOLR_URL = "http://100.64.114.155:8983"
#SOLR_URL = "http://localhost:8983"

SOLR_URL = "http://%s:%s"


def get_aviris_objects(solr_host):
    params= {
            "q": "record_type:aviris-plume",
            #"fl": "id,map_*",
            "indent": "on",
            "wt": "json",
            "rows": 300000
        }

    url = "%s/solr/MSF/select"%solr_host
    r = requests.get(url, params=params)
    d = json.loads(r.text)["response"]["docs"]
    return d


def process_aviris_object(aviris_object_doc, solr_host, test_only=False, verbose=False):

    property_map = {}

    for i in range(0, 60):
        if "map_%d_name_s" % i in aviris_object_doc and "map_%d_value_s" % i in aviris_object_doc:
            key = aviris_object_doc["map_%d_name_s" % i]
            value = aviris_object_doc["map_%d_value_s" % i]
            property_map[key] = value


    ime_20_f = property_map["IME20 (kg)"] if "IME20 (kg)" in property_map else None
    ime_10_f = property_map["IME10 (kg)"] if "IME10 (kg)" in property_map else None
    ime_5_f = property_map["IME5 (kg)"] if "IME5 (kg)" in property_map else None
    source_id_s = property_map["Source id"] if "Source id" in property_map else None
    candidate_id_s = property_map["# Candidate id"] if "# Candidate id" in property_map else None

    if verbose:
        print "For AVIRIS with id", aviris_object_doc["id"]
        print "    IME20:", float(ime_20_f)
        print "    IME10:", float(ime_10_f)
        print "    IME5:", float(ime_5_f)
        print "    Source ID:", source_id_s
        print "    Candidate ID:", candidate_id_s

    if not test_only:
        counts.update_fields(aviris_object_doc["id"], {
            "ime_20_f": float(ime_20_f),
            "ime_10_f": float(ime_10_f),
            "ime_5_f": float(ime_5_f),
            "source_id_s": str(source_id_s),
            "candidate_id_s": str(candidate_id_s)
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

    aviris_objects = get_aviris_objects(solr_host)

    for aviris_object in aviris_objects:
        try:
            process_aviris_object(aviris_object, solr_host=solr_host, test_only=test_only, verbose=verbose)
        except:
            traceback.print_exc()
            print "Unexpected error:", sys.exc_info()[0]

    counts.commit_changes_to_solr(solr_host=solr_host, solr_core="aviris")


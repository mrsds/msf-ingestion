
import requests
import json
import argparse
import ingestutils.counts as counts
import sys
import traceback
from ingestutils.progress import printProgress

#SOLR_URL = "http://100.64.114.155:8983"
#SOLR_URL = "http://localhost:8983"

SOLR_URL = "http://%s:%s"



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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--solrhost", help="Target Solr host", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--solrport", help="Target Solr port", type=str, default="8983", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test flyover count calculations. Don't actually upload to Solr.",
                        required=False, action="store_true")


    args = parser.parse_args()
    solr_host = SOLR_URL % (args.solrhost, args.solrport)
    verbose = args.verbose
    test_only = args.test

    num_items_per_page = 100
    foo, num_vista_found = get_vista_objects(solr_host, items_per_page=0, page=0)
    num_vista_pages = num_vista_found / num_items_per_page + 1

    for page in range(0, num_vista_pages):
        printProgress(page, num_vista_pages)
        vista_objects, count = get_vista_objects(solr_host, num_items_per_page, page=page)
        for vista_object in vista_objects:
            try:
                counts.process_vista_object(vista_object, solr_host=solr_host, test_only=test_only, verbose=verbose)
            except:
                traceback.print_exc()
                print "Unexpected error:", sys.exc_info()[0]

    printProgress(num_vista_pages, num_vista_pages)

    counts.commit_changes_to_solr(solr_host=solr_host, solr_core="MSF")



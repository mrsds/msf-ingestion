
import os
import sys
import os.path
import numpy as np
import xml.etree.ElementTree as ET
import uuid
import requests
import json
from lxml import etree
import argparse

#SOLR_URL = "http://100.64.114.155:8983"
#SOLR_URL = "http://localhost:8983"

SOLR_URL = "http://%s:%s"

CATEGORY_MAP = {
    "Oil & Gas Wells": 0,
    "Livestock Dairies": 1,
    "Anaerobic Lagoons": 2,
    "CNG Fueling Stations": 3,
    "LNG Fueling Stations": 4,
    "Natural Gas Storage Fields": 5,
    "Natural Gas Processing Plants": 6,
    "Natural Gas Compressor Stations": 7,
    "Petroleum Refineries": 8,
    "Wastewater Treatment Plants": 9,
    "Power Plants": 10,
    "Landfills": 11,
    "Natural Gas Pipelines": 12,
    "SoCAB Boundary": 13,
    "petrorefinery_CARB_SFbayarea": 14
}



def parse_coordinates(coords):
    coordinates = np.array(coords.text.split(",")).astype(np.float)
    return coordinates


def parse_polygon(polyElement):
    coordinates = polyElement.find("{http://www.opengis.net/kml/2.2}outerBoundaryIs/{http://www.opengis.net/kml/2.2}LinearRing/{http://www.opengis.net/kml/2.2}coordinates")
    c = [np.array(f.split(",")).astype(np.float) for f in coordinates.text.strip().split(" ")]
    return c

def parse_linestring(polyElement):
    coordinates = polyElement.find("{http://www.opengis.net/kml/2.2}coordinates")
    c = [np.array(f.split(",")).astype(np.float) for f in coordinates.text.replace("#QNAN", "0").strip().split(" ")]
    return c


def parse_placemark_properties(placemark):
    placemarkProperties = {}
    print placemark, placemark.findall(".//{http://www.opengis.net/kml/2.2}SimpleData")
    for property in placemark.findall(".//{http://www.opengis.net/kml/2.2}SimpleData"):
        print property
        placemarkProperties[property.attrib["name"]] = property.text

    return placemarkProperties


def parse_placemark(placemark):

    placemarkProperties = parse_placemark_properties(placemark)
    name = placemarkProperties["SiteName"]

    shape = None
    polyElement = placemark.find("{http://www.opengis.net/kml/2.2}Polygon")
    if polyElement is not None:
        poly = parse_polygon(polyElement)

        shape = {
            "type": "polygon",
            "path": poly
        }

    multiPolygons = placemark.find("{http://www.opengis.net/kml/2.2}MultiGeometry/{http://www.opengis.net/kml/2.2}Polygon")
    if multiPolygons is not None:
        #for p in multiPolygons:
        poly = parse_polygon(multiPolygons)

        shape = {
            "type": "polygon",
            "path": poly
        }


    return name, shape, placemarkProperties




def parse_folder(folder):
    folderName = folder.find("{http://www.opengis.net/kml/2.2}name").text
    folderDescription = None


    placemarks = []

    for placemark in folder.findall(".//{http://www.opengis.net/kml/2.2}Placemark"):
        name, shape, placemarkProperties = parse_placemark(placemark)
        placemarks.append({
            "name": name,
            "description": None,
            "shape": shape,
            "properties": placemarkProperties
        })

    return folderName, placemarks



def parse_document(kml_path):
    tree = ET.parse(kml_path)
    root = tree.getroot()

    folders = []

    for folder in root.findall(".//{http://www.opengis.net/kml/2.2}Folder"):
        folderName, placemarks = parse_folder(folder)

        folders.append({
            "name": folderName,
            "description": None,
            "placemarks": placemarks
        })

    return folders


def upload_document(folder, placemark, force_category=None, solr_host=None, test_only=False):

    if force_category is not None:
        category = force_category
        category_id = CATEGORY_MAP[force_category]
    else:
        category = folder["name"]
        category_id = CATEGORY_MAP[folder["name"]]

    doc = {
        "id": str(uuid.uuid4()),
        "category": category,
        "category_id": category_id,
        "name": placemark["name"],
        "description": placemark["description"],
        "shape_type":  placemark["shape"]["type"],
        "shape_location": None
    }

    if placemark["name"] is None:
        doc["name"] = placemark["properties"]["FACILITY"]

    m = 0
    for key in placemark["properties"]:
        value = placemark["properties"][key]
        doc["map_%d_name_s"%m] = key
        doc["map_%d_value_s"%m] = value
        m = m + 1


    if placemark["shape"]["type"] == "point":
        doc["shape_location"] = " ".join(map(str, placemark["shape"]["coordinates"][:2].tolist()))
    elif placemark["shape"]["type"] == "polygon":
        mean_lat = np.array([p[0] for p in placemark["shape"]["path"]]).mean()
        mean_lon = np.array([p[1] for p in placemark["shape"]["path"]]).mean()
        doc["shape_location"] = "%f %f"%(mean_lat, mean_lon)
        doc["shape_poly_fs"] = [p.tolist()[:2] for p in placemark["shape"]["path"]]
    elif placemark["shape"]["type"] == "line":
        mean_lat = np.array([p[0] for p in placemark["shape"]["path"]]).mean()
        mean_lon = np.array([p[1] for p in placemark["shape"]["path"]]).mean()
        doc["shape_location"] = "%f %f" % (mean_lat, mean_lon)
        doc["shape_poly_fs"] = [p.tolist()[:2] for p in placemark["shape"]["path"]]

    if test_only is False:
        if solr_host is not None:
            r = requests.post('%s/solr/vista/update/json/docs?commit=true'%solr_host, data=json.dumps(doc),
                                  headers={"Content-Type": "application/json"})

            if r.status_code != 200:
                print r.status_code, r.text
        else:
            print "Solr host not specified, cannot upload documents"
            sys.exit(1)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input Vista KML file(s)", required=True, type=str, nargs='+')
    parser.add_argument("-c", "--category", help="Force category override", type=str)
    parser.add_argument("-s", "--solrhost", help="Target Solr host", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--solrport", help="Target Solr port", type=str, default="8983", required=False)
    parser.add_argument("-t", "--test", help="Test by parsing and assembling upload document. Don't actually upload to Solr.", required=False, action="store_true")

    args = parser.parse_args()

    input_files = args.data
    force_category = args.category
    test_only = args.test
    solr_host = SOLR_URL%(args.solrhost, args.solrport)

    for input_file in input_files:
        if not os.path.exists(input_file):
            print "Specified file '%s' not found or is inaccessible" % input_file
            sys.exit(1)

        print "Parsing %s:"%input_file
        folders = parse_document(input_file)

        print "Loading..."
        for folder in folders:
            print folder["name"]
            for placemark in folder["placemarks"]:
                upload_document(folder, placemark, force_category=force_category, solr_host=solr_host, test_only=test_only)


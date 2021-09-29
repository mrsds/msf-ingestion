
import os
import os.path
import numpy as np
import uuid
import requests
import json
from osgeo import gdal,ogr,osr
import glob
import argparse
from PIL import Image
import tempfile
import ingestutils.s3util as s3util
import math

#SOLR_URL = "http://100.64.114.155:8983"
#SOLR_URL = "http://localhost:8983"

SOLR_URL = "http://%s:%s"

S3_BUCKET = "bucket"

#https://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
def GetExtent(gt,cols,rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner
    '''
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
        yarr.reverse()
    return ext

#https://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
def ReprojectCoords(coords,src_srs,tgt_srs):
    ''' Reproject a list of x,y coordinates.

        @type geom:     C{tuple/list}
        @param geom:    List of [[x,y],...[x,y]] coordinates
        @type src_srs:  C{osr.SpatialReference}
        @param src_srs: OSR SpatialReference object
        @type tgt_srs:  C{osr.SpatialReference}
        @param tgt_srs: OSR SpatialReference object
        @rtype:         C{tuple/list}
        @return:        List of transformed [[x,y],...[x,y]] coordinates
    '''
    trans_coords=[]
    transform = osr.CoordinateTransformation( src_srs, tgt_srs)
    for x,y in coords:
        x,y,z = transform.TransformPoint(x,y)
        trans_coords.append([x,y])
    return trans_coords


#https://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
def geotiff_spatial(tiffpath):
    ds = gdal.Open(tiffpath)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize

    extent = GetExtent(gt, cols, rows)

    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(ds.GetProjection())

    tgt_srs = src_srs.CloneGeogCS()
    geo_ext = ReprojectCoords(extent, src_srs, tgt_srs)

    a0 = geo_ext[1]
    a1 = geo_ext[0]
    slope = (a1[0] - a0[0]) / (a1[1] - a0[1])

    rotation_angle = np.arctan(slope)

    if (a1[0] - a0[0]) < 0.0:
        rotation_angle *= -1.0

    lats = [e[1] for e in geo_ext]
    lons = [e[0] for e in geo_ext]

    uy = np.max(lats)
    ly = np.min(lats)

    lx = np.min(lons)
    rx = np.max(lons)

    ul = (lx, uy)
    ll = (lx, ly)
    lr = (rx, ly)
    ur = (rx, uy)

    shape_extents = [
        ul,
        ll,
        lr,
        ur
    ]

    return shape_extents, math.degrees(rotation_angle)



"""
    metadata = {
        "mid_lat": mid_lat,
        "mid_lon": mid_lon,
        "shape": extents,
        "date": data_date,
        "vlds": vlds,
        "power_cycle_number": pnn,
        "airborne_flight_run_number": rnn,
        "json_s3_url": json_s3_url,
        "ime_s3_url": ime_s3_url,
        "rgb_png_s3_url": rgb_png_s3_url,
        "plume_png_s3_url": plume_png_s3_url
    }
"""

def upload_document_to_solr(name, metadata, solr_host, test_only=False, verbose=False):
    shape = metadata["shape"]
    shape.append(shape[0])
    poly = "POLYGON((" + ",".join([" ".join(map(str, p)) for p in shape]) + "))"

    id = str(uuid.uuid4())
    if verbose:
        print "Uploading with name", name
        print "Uploading with id", id

    doc = {
        "id": id,
        "name": name,
        "description": "",
        "shape_type": "",
        "shape_location": None,
        "json_url": metadata["json_s3_url"],
        "png_url": metadata["rgb_png_s3_url"],
        "plume_url":metadata["plume_png_s3_url"],
        "rgbqlctr_url":metadata["rgbqlctr_png_s3_url"],
        "png_url_thumb": metadata["rgb_png_s3_url_thumb"],
        "plume_url_thumb": metadata["plume_png_s3_url_thumb"],
        "rgbqlctr_url_thumb": metadata["rgbqlctr_png_s3_url_thumb"],
        "plume_tiff_url": metadata["plume_tiff_s3_url"],
        "rgb_tiff_url": metadata["rgb_tiff_s3_url"],
        "plume_shape":poly,
        "data_date_dt":metadata["date"],
        "vlds": metadata["vlds"],
        "power_cycle_number": metadata["power_cycle_number"],
        "airborne_flight_run_number": metadata["airborne_flight_run_number"],
        "ul_pixel_coordinate_row": metadata["ul_pixel_coordinate_row"],
        "ul_pixel_coordinate_col": metadata["ul_pixel_coordinate_col"],
        "mergedist": metadata["mergedist"],
        "record_type": "aviris-plume"
    }

    if "Source id" in metadata["ime_properties"]:
        doc["source_id_s"] = metadata["ime_properties"]["Source id"]
    if "IME20 (kg)" in metadata["ime_properties"]:
        doc["ime_20_f"] = float(metadata["ime_properties"]["IME20 (kg)"])
    if "IME10 (kg)" in metadata["ime_properties"]:
        doc["ime_10_f"] = float(metadata["ime_properties"]["IME10 (kg)"])
    if "IME5 (kg)" in metadata["ime_properties"]:
        doc["ime_5_f"] = float(metadata["ime_properties"]["IME5 (kg)"])
    if "# Candidate id" in metadata["ime_properties"]:
        doc["candidate_id_s"] = str(metadata["ime_properties"]["# Candidate id"])


    doc["shape_location"] = "%f,%f" % (metadata["mid_lat"], metadata["mid_lon"])

    ime_properties = metadata["ime_properties"]
    if ime_properties is not None:
        m = 0
        for key in ime_properties:
            value = ime_properties[key]
            doc["map_%d_name_s"%m] = key
            doc["map_%d_value_s"%m] = value
            m = m + 1

    if not test_only:
        print "Uploading %s to Solr"%name
        r = requests.post('%s/solr/MSF/update/json/docs?commit=true'%solr_host, data=json.dumps(doc),
                              headers={"Content-Type": "application/json"})

        if r.status_code != 200:
            print r.status_code, r.text
            raise Exception("Failed to upload document. Solr status: %s, %s"%(r.status_code, r.text))
    else:
        print
        print json.dumps(doc, indent=4)
        print
        print "Test of %s to Solr" % name





def find_ime_file(src_dir):
    parent_dir = os.path.dirname(src_dir)
    files = glob.glob("%s/*ime.txt"%parent_dir)
    if len(files) > 0:
        return files[0]
    else:
        return None

def parse_ime_file(ime_file):
    f = open(ime_file, "r")
    data = f.read()
    f.close()
    lines = data.split("\n")
    headers = None
    value_rows = []
    for i in range(0, len(lines)):
        if i == 0:
            headers = lines[i].split(", ")
        else:
            value_list = lines[i].split(", ")
            values = {}

            for j in range(0, len(headers)):
                values[headers[j]] = value_list[j] if j < len(value_list) else None
            value_rows.append(values)
    return headers, value_rows

def get_ime_for_source_id(ime_rows, source_id):
    if ime_rows is None or source_id is None:
        return None

    for row in ime_rows:
        if row["Source id"] == source_id:
            return row

    return None

def overlay_image_over(bottom_image, top_image, dest_image):
    top_img = Image.open(top_image)
    bottom_img = Image.open(bottom_image)

    top_img = top_img.convert('RGBA')
    bottom_img = bottom_img.convert('RGBA')

    #top_img.thumbnail((bottom_img.width, bottom_img.height), Image.ANTIALIAS)

    x = 0#bottom_img.width - top_img.width
    y = 0#bottom_img.height - top_img.height

    bottom_img.paste(top_img, (x, y, top_img.width, top_img.height), top_img)
    bottom_img.save(dest_image, format="PNG")



def convert_to_png(src_img, dest_img):
    img = Image.open(src_img)
    img.save(dest_img, format="PNG")


def rotate_image(src_img, dest_img, rotation_angle):
    img = Image.open(src_img)
    img2 = img.rotate(-1.0 * rotation_angle, expand=True, resample=Image.BICUBIC)
    img2.save(dest_img, format="PNG")


def ingest_aviris_plume_geojson(geojson, solr_host=None, s3_bucket=S3_BUCKET, test_only=False, images_only=False, verbose=False):


    if verbose:
        print "Ingesting", geojson
    file_base = geojson[0:-12]


    plume_tiff = "%s_ctr.tif"%file_base
    plume_png = "%s/%s_ctr.png" % (tempfile.gettempdir(), os.path.basename(file_base))

    rgb_tiff = "%s_rgb.tif"%file_base
    rgb_png = "%s/%s_rgb.png" % (tempfile.gettempdir(), os.path.basename(file_base))

    rgbqlctr_png = "%s/%s_rgbgl-ctr.png" % (tempfile.gettempdir(), os.path.basename(file_base))

    convert_to_png(plume_tiff, plume_png)
    convert_to_png(rgb_tiff, rgb_png)
    overlay_image_over(rgb_tiff, plume_tiff, rgbqlctr_png)

    bn = os.path.basename(geojson)
    flight_id = bn[:bn.find("_")]

    year = bn[3:7]
    month = bn[7:9]
    day = bn[9:11]
    hour = bn[12:14]
    minute = bn[14:16]
    second = bn[16:18]

    vlds = None
    pnn = None
    rnn = None

    bn_parts = bn.split("_")
    source_id = bn_parts[1]
    ul_pixel_coordinate_row = bn_parts[2]
    ul_pixel_coordinate_col = bn_parts[3]

    dn = os.path.dirname(geojson)
    md = dn[dn.rindex("/")+1:]

    if md[0:9] == "mergedist":
        mergedist = md[9:]
    else:
        mergedist = -1


    ime_file = find_ime_file(os.path.dirname(geojson))
    ime_properties = {}
    if ime_file is not None:
        ime_headers, ime_rows = parse_ime_file(ime_file)
        ime_properties = get_ime_for_source_id(ime_rows, source_id)

    extents, rotation_angle = geotiff_spatial(rgb_tiff)

    plume_rotated_png = "%s/%s_ctr_rotated.png" % (tempfile.gettempdir(), os.path.basename(file_base))
    rotate_image(plume_tiff, plume_rotated_png, rotation_angle)
    #sys.exit(0)
    f = open(geojson, "r")
    j = json.load(f)
    f.close()

    features = j["features"]
    if "features" in features: # 'features' is two deep in older versions
        features = features["features"]
    coords_list = features[0]["geometry"]["coordinates"]

    lons = []
    lats = []

    for coords_chunk in coords_list:
        for coords_item in coords_chunk:
            coords_lon = coords_item[0]
            coords_lat = coords_item[1]
            lats.append(coords_lat)
            lons.append(coords_lon)
            #coords = map(float, coords_item.split(", "))

    mid_lon = np.median(np.array(lons))
    mid_lat = np.median(np.array(lats))


    json_s3_url = s3util.upload_file_to_s3(geojson, s3_bucket=s3_bucket, test_only=test_only)

    rgb_s3_url, rgb_s3_url_thumb = s3util.upload_image_to_s3(rgb_png, s3_bucket=s3_bucket, test_only=test_only)
    plume_s3_url, plume_s3_url_thumb = s3util.upload_image_to_s3(plume_rotated_png, s3_bucket=s3_bucket, test_only=test_only)
    rgbqlctr_s3_url, rgbqlctr_s3_url_thumb = s3util.upload_image_to_s3(rgbqlctr_png, s3_bucket=s3_bucket, test_only=test_only)

    rgb_tiff_s3_url, rgb_tiff_s3_url_thumb = s3util.upload_image_to_s3(rgb_tiff, s3_bucket=s3_bucket, test_only=test_only, upload_thumbnail=False)
    plume_tiff_s3_url, plume_tiff_s3_url_thumb = s3util.upload_image_to_s3(plume_tiff, s3_bucket=s3_bucket, test_only=test_only, upload_thumbnail=False)

    os.unlink(rgbqlctr_png)

    data_date = "{YYYY}-{MM}-{DD}T{HH}:{mm}:{ss}Z".format(YYYY=year, MM=month, DD=day, HH=hour, mm=minute,ss=second)
    metadata = {
        "record_type": "plume",
        "mid_lat": mid_lat,
        "mid_lon": mid_lon,
        "shape": extents,
        "date": data_date,
        "vlds": vlds,
        "power_cycle_number": pnn,
        "airborne_flight_run_number": rnn,
        "json_s3_url": json_s3_url,

        "rgb_png_s3_url": rgb_s3_url,
        "plume_png_s3_url": plume_s3_url,
        "rgbqlctr_png_s3_url": rgbqlctr_s3_url,

        "rgb_png_s3_url_thumb": rgb_s3_url_thumb,
        "plume_png_s3_url_thumb": plume_s3_url_thumb,
        "rgbqlctr_png_s3_url_thumb": rgbqlctr_s3_url_thumb,

        "rgb_tiff_s3_url": rgb_tiff_s3_url,
        "plume_tiff_s3_url": plume_tiff_s3_url,

        "ul_pixel_coordinate_row": ul_pixel_coordinate_row,
        "ul_pixel_coordinate_col": ul_pixel_coordinate_col,
        "mergedist": mergedist,
        "ime_properties": ime_properties
    }

    if verbose:
        print json.dumps(metadata, indent=4)

    if not images_only:
        upload_document_to_solr(os.path.basename(file_base), metadata, solr_host=solr_host, test_only=test_only, verbose=verbose)



def ingest_aviris_flight_geojson(geojson, solr_host=None, s3_bucket=S3_BUCKET, test_only=False, verbose=False):
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input AVIRIS GeoJson file(s)", required=True, type=str, nargs='+')
    parser.add_argument("-s", "--solrhost", help="Target Solr host", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--solrport", help="Target Solr port", type=str, default="8983", required=False)
    parser.add_argument("-b", "--bucket", help="Target S3 Bucket", type=str, default="bucket", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test by parsing and assembling upload document. Don't actually upload to Solr.",
                        required=False, action="store_true")
    parser.add_argument("-i", "--imagesonly",
                        help="Only upload images to S3. Don't actually upload to Solr.",
                        required=False, action="store_true")

    args = parser.parse_args()
    input_files = args.data
    s3_bucket = args.bucket
    test_only = args.test
    solr_host = SOLR_URL % (args.solrhost, args.solrport)
    verbose = args.verbose
    images_only = args.imagesonly

    for input_file in input_files:
        ingest_aviris_plume_geojson(geojson=input_file, solr_host=solr_host, s3_bucket=s3_bucket, test_only=test_only, images_only=images_only, verbose=verbose)

    #geojson = sys.argv[1]
    #ingest_aviris_geojson(geojson)







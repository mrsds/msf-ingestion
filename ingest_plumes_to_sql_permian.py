import os
import os.path
import sys
import numpy as np
import uuid
import requests
import json
from osgeo import gdal,ogr,osr, gdalconst
import glob
import argparse
from PIL import Image
import tempfile
import ingestutils.s3util as s3util
import math
import psycopg2


DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""




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
    if (a1[1] - a0[1]) == 0:
        slope = 0
    else:
        slope = (a1[0] - a0[0]) / (a1[1] - a0[1])

    rotation_angle = np.arctan(slope)

    if (a1[0] - a0[0]) < 0.0:
        rotation_angle *= -1.0

    lats = [e[0] for e in geo_ext]
    lons = [e[1] for e in geo_ext]

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



# """
#     metadata = {
#         "mid_lat": mid_lat,
#         "mid_lon": mid_lon,
#         "shape": extents,
#         "date": data_date,
#         "vlds": vlds,
#         "power_cycle_number": pnn,
#         "airborne_flight_run_number": rnn,
#         "json_s3_url": json_s3_url,
#         "ime_s3_url": ime_s3_url,
#         "rgb_png_s3_url": rgb_png_s3_url,
#         "plume_png_s3_url": plume_png_s3_url
#     }
# """

"""
    metadata = {
        "shape": extents,
        "date": data_date,
        "vlds": vlds,
        "power_cycle_number": pnn,
        "airborne_flight_run_number": rnn,
        "ime_s3_url": ime_s3_url,
        "rgb_png_s3_url": rgb_png_s3_url,
        "plume_png_s3_url": plume_png_s3_url
    }
"""




def does_plume_exist_in_db(cur, candidate_id):
    sql = """
    select count(1) from aviris_plumes where candidate_id = %s;
    """
    cur.execute(sql, (candidate_id,))
    row = cur.fetchone()
    return row[0] >= 1


def get_plume_db_id_from_candidate_id(cur, candidate_id):
    sql = """
    select plume_id from aviris_plumes where candidate_id = %s
    """
    cur.execute(sql, (candidate_id,))
    row = cur.fetchone()
    return row[0]

def get_plume_data_from_candidate_id(cur, candidate_id):
    sql = """
    select * from plumes where plume_id = %s
    """
    cur.execute(sql, (candidate_id,))
    row = cur.fetchone()
    return row

def upload_to_db(name, metadata, candidate_id, test_only=False, verbose=False):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWD, host=DB_ENDPOINT, port=DB_PORT)
    cur = conn.cursor()

    shape = metadata["shape"]
    shape.append(shape[0])
    poly = "POLYGON((" + ",".join([" ".join(map(str, p)) for p in shape]) + "))"

    # candidate_id = str(metadata["ime_properties"]["# Candidate id"])

    exists = does_plume_exist_in_db(cur, candidate_id)

    if exists:
        update_in_db(cur, poly, name, metadata, candidate_id, verbose)
    else:
        insert_to_db(cur, poly, name, metadata, candidate_id, verbose)

    if test_only:
        if verbose:
            print("Testing only, rolling back changes")
        conn.rollback()
    else:
        if verbose:
            print("Committing changes")
        conn.commit()
    cur.close()
    conn.close()


def update_in_db(cur, poly, name, metadata, candidate_id, verbose=False):
    # candidate_id = str(metadata["ime_properties"]["# Candidate id"])
    if verbose:
        print("     Updating vista plume %s" % candidate_id)

    plume_db_id = get_plume_db_id_from_candidate_id(cur, candidate_id)
    if verbose:
        print("     Internal db identifier: %s"%plume_db_id)

    plume_data = get_plume_data_from_candidate_id(cur, candidate_id)

    sql = """
    update aviris_plumes
    set
        name = %s,
        json_url = %s,
        png_url = %s, 
        plume_url = %s,
        rgbqlctr_url = %s,
        png_url_thumb = %s,
        plume_url_thumb = %s,
        rgbqlctr_url_thumb = %s,
        plume_tiff_url = %s,
        rgb_tiff_url = %s,
        ul_pixel_coordinate_row = %s,
        ul_pixel_coordinate_col = %s,
        mergedist = %s,
        source_id = %s,
        ime_20 = %s,
        ime_10 = %s,
        ime_5 = %s,
        candidate_id = %s,
        
        aviris_plume_id = %s,
        detid5 = %s,
        detid10 = %s,
        detid20 = %s,
        fetch5 = %s,
        fetch10 = %s,
        fetch20 = %s,
        
        plume_longitude = %s,
        plume_latitude = %s,
        plume_location=ST_GeomFromText('POINT(%s %s)', 4326),
        plume_shape=ST_GeomFromText(%s, 4326),
        data_date=to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')
    where 
      plume_id = %s
    """

    cur.execute(sql,
                (
                    name,
                    None,
                    metadata["rgb_png_s3_url"],
                    metadata["plume_png_s3_url"],
                    metadata["rgbqlctr_png_s3_url"],
                    metadata["rgb_png_s3_url_thumb"],
                    metadata["plume_png_s3_url_thumb"],
                    metadata["rgbqlctr_png_s3_url_thumb"],
                    metadata["plume_tiff_s3_url"],
                    metadata["rgb_tiff_s3_url"],
                    metadata["ul_pixel_coordinate_row"],
                    metadata["ul_pixel_coordinate_col"],
                    metadata["mergedist"],
                    plume_data[10], #source_id
                    None, #ime_20
                    None, #ime_10
                    None, #ime_5
                    candidate_id,
                    None, #aviris_plume_id
                    None, #det_id_5
                    None, #det_id_10
                    None, #det_id_20
                    None, #fetch5
                    None, #fetch10
                    None, #fetch20
                    plume_data[8], #plume_lon
                    plume_data[7], #plume_lat
                    plume_data[8], #plume_lon
                    plume_data[7], #plume_lat
                    poly,
                    metadata["date"],
                    plume_db_id
                )
            )


def insert_to_db(cur, poly, name, metadata, candidate_id, verbose=False):
    # candidate_id = str(metadata["ime_properties"]["# Candidate id"])
    if verbose:
        print("     Inserting vista plume %s" % candidate_id)

    plume_data = get_plume_data_from_candidate_id(cur, candidate_id)

    sql = """
    insert into aviris_plumes
      (
        name,
        json_url,
        png_url, 
        plume_url,
        rgbqlctr_url,
        png_url_thumb,
        plume_url_thumb,
        rgbqlctr_url_thumb,
        plume_tiff_url,
        rgb_tiff_url,
        ul_pixel_coordinate_row,
        ul_pixel_coordinate_col,
        mergedist,
        source_id,
        ime_20,
        ime_10,
        ime_5,
        candidate_id,
        
        aviris_plume_id,
        detid5,
        detid10,
        detid20,
        fetch5,
        fetch10,
        fetch20,
        
        plume_longitude,
        plume_latitude,
        plume_location,
        plume_shape,
        data_date
      ) values (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        ST_GeomFromText('POINT(%s %s)', 4326),
        ST_GeomFromText(%s, 4326),
        to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')
      )
    """
    cur.execute(sql,
                (
                    name,
                    # metadata["json_s3_url"],
                    None,
                    metadata["rgb_png_s3_url"],
                    metadata["plume_png_s3_url"],
                    metadata["rgbqlctr_png_s3_url"],
                    metadata["rgb_png_s3_url_thumb"],
                    metadata["plume_png_s3_url_thumb"],
                    metadata["rgbqlctr_png_s3_url_thumb"],
                    metadata["plume_tiff_s3_url"],
                    metadata["rgb_tiff_s3_url"],
                    metadata["ul_pixel_coordinate_row"],
                    metadata["ul_pixel_coordinate_col"],
                    metadata["mergedist"],
                    plume_data[10], #source id
                    None, #plume_data[0], #ime_20
                    None, #lume_data[20], #ime_10
                    None, #plume_data[19], #ime_5
                    candidate_id, #candidate_id
                    None, #aviris_plume_id
                    None, #plume_data[23], #detid5
                    None, #plume_data[24], #detid10
                    None, #plume_data[25], #detid20
                    None, #plume[1], #fetch5
                    None, #plume_data[21], #fetch10
                    None, #plume_data[22], #fetch20
                    plume_data[8], #plumelon
                    plume_data[7], #plumelat
                    plume_data[8], #plumelon
                    plume_data[7], #plumelat
                    poly,
                    metadata["date"]
                )
            )



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

    x_diff = (abs(bottom_img.width - top_img.width) // 2)
    y_diff = (abs(bottom_img.height - top_img.height) // 2)
    x = -x_diff
    y = -y_diff
    x2 = top_img.width - x_diff
    y2 = top_img.height - y_diff

    bottom_img.paste(top_img, (x, y, x2, y2), top_img)
    bottom_img.save(dest_image, format="PNG")



def convert_to_png(src_img, dest_img):
    img = Image.open(src_img)
    img.save(dest_img, format="PNG")


def rotate_image(src_img, dest_img, rotation_angle):
    img = Image.open(src_img)
    img2 = img.rotate(-1.0 * rotation_angle, expand=True, resample=Image.BICUBIC)
    img2.save(dest_img, format="PNG")


def reproject(src_image, match_image, dest_image):
    src_filename = src_image
    src = gdal.Open(src_filename, gdalconst.GA_ReadOnly)
    src_proj = src.GetProjection()
    src_geotrans = src.GetGeoTransform()
    src_wide = src.RasterXSize
    src_high = src.RasterYSize

    match_filename = match_image
    match_ds = gdal.Open(match_filename, gdalconst.GA_ReadOnly)
    match_proj = match_ds.GetProjection()
    match_geotrans = match_ds.GetGeoTransform()

    wide = match_ds.RasterXSize
    high = match_ds.RasterYSize
    #print match_geotrans, wide , src_wide
    #match_geotrans = list(match_geotrans)
    #match_geotrans[2] *= (float(src_high) / float(high))
    #match_geotrans[4] *= (float(src_high) / float(high))

    dst_filename = dest_image
    dst = gdal.GetDriverByName('GTiff').Create(dst_filename, wide, high, 3, gdalconst.GDT_Byte)
    dst.SetGeoTransform(match_geotrans)
    dst.SetProjection(match_proj)

    # Do the work
    gdal.ReprojectImage(src, dst, src_proj, match_proj, gdalconst.GRA_Bilinear)

    del dst

def ingest_aviris_plume_geojson(geojson, s3_bucket="bucket", test_only=False, images_only=False, verbose=False):


    if verbose:
        print ("Ingesting", geojson)
    file_base = geojson[0:-8]


    plume_tiff_input = "%s_ctr.tif"%file_base
    plume_tiff = "tmp/%s_rotated.tif"%os.path.basename(plume_tiff_input[:plume_tiff_input.rindex(".")])
    plume_png = "%s/%s_ctr.png" % (tempfile.gettempdir(), os.path.basename(file_base))
    print(plume_tiff_input, plume_tiff, plume_png)

    rgb_tiff_input = "%s_rgb.tif"%file_base
    rgb_tiff = "tmp/%s_rotated.tif" % os.path.basename(rgb_tiff_input[:rgb_tiff_input.rindex(".")])
    rgb_png = "%s/%s_rgb.png" % (tempfile.gettempdir(), os.path.basename(file_base))

    rgbqlctr_png = "%s/%s_rgbgl-ctr.png" % ("tmp", os.path.basename(file_base))

    print(rgb_tiff_input, rgb_tiff, rgb_png)

    candidate_id = os.path.basename(plume_tiff_input[:plume_tiff_input.rindex(".")])
    candidate_id = candidate_id[0 : candidate_id.index("_")]

    gdal.Warp(rgb_tiff, rgb_tiff_input, dstSRS="EPSG:3857")
    gdal.Warp(plume_tiff, plume_tiff_input, dstSRS="EPSG:3857")

    #reproject(rgb_tiff, plume_tiff,rgb_tiff)

    overlay_image_over(rgb_tiff, plume_tiff, rgbqlctr_png)
    convert_to_png(plume_tiff, plume_png)
    convert_to_png(rgb_tiff, rgb_png)
    #sys.exit(0)

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
    source_id = bn_parts[0]
    # ul_pixel_coordinate_row = bn_parts[2][1:]
    # ul_pixel_coordinate_col = bn_parts[3][1:]
    ul_pixel_coordinate_row = bn_parts[1][1:]
    ul_pixel_coordinate_col = bn_parts[2][1:]

    dn = os.path.dirname(geojson)
    #md = dn[dn.rindex("/")+1:]

    #if md[0:9] == "mergedist":
    #    mergedist = md[9:]
    #else:
    mergedist = -1


    # ime_file = find_ime_file(os.path.dirname(geojson))
    # ime_properties = {}
    # if ime_file is not None:
    #     ime_headers, ime_rows = parse_ime_file(ime_file)
    #     ime_properties = get_ime_for_source_id(ime_rows, source_id)

    extents, rotation_angle = geotiff_spatial(plume_tiff)

    plume_rotated_png = plume_png
    #plume_rotated_png = "%s/%s_ctr_rotated.png" % (tempfile.gettempdir(), os.path.basename(file_base))
    #rotate_image(plume_tiff, plume_rotated_png, rotation_angle)
    #sys.exit(0)


    # f = open(geojson, "r")
    # j = json.load(f)
    # f.close()

    # features = j["features"]
    # if "features" in features: # 'features' is two deep in older versions
    #     features = features["features"]
    # coords_list = features[0]["geometry"]["coordinates"]

    # lons = []
    # lats = []

    # for coords_chunk in coords_list:
    #     for coords_item in coords_chunk:
    #         coords_lon = coords_item[0]
    #         coords_lat = coords_item[1]
    #         lats.append(coords_lat)
    #         lons.append(coords_lon)
    #         #coords = map(float, coords_item.split(", "))

    # mid_lon = np.median(np.array(lons))
    # mid_lat = np.median(np.array(lats))

    # json_s3_url = s3util.upload_file_to_s3(geojson, s3_bucket=s3_bucket, test_only=test_only)

    rgb_s3_url, rgb_s3_url_thumb = s3util.upload_image_to_s3(rgb_png, s3_bucket=s3_bucket, test_only=True)
    plume_s3_url, plume_s3_url_thumb = s3util.upload_image_to_s3(plume_rotated_png, s3_bucket=s3_bucket, test_only=True)
    rgbqlctr_s3_url, rgbqlctr_s3_url_thumb = s3util.upload_image_to_s3(rgbqlctr_png, s3_bucket=s3_bucket, test_only=True)

    rgb_tiff_s3_url, rgb_tiff_s3_url_thumb = s3util.upload_image_to_s3(rgb_tiff, s3_bucket=s3_bucket, test_only=True, upload_thumbnail=False)
    plume_tiff_s3_url, plume_tiff_s3_url_thumb = s3util.upload_image_to_s3(plume_tiff, s3_bucket=s3_bucket, test_only=True, upload_thumbnail=False)


    data_date = "{YYYY}-{MM}-{DD} {HH}:{mm}:{ss}".format(YYYY=year, MM=month, DD=day, HH=hour, mm=minute,ss=second)
    metadata = {
        "record_type": "plume",
        # "mid_lat": mid_lat,
        # "mid_lon": mid_lon,
        "shape": extents,
        "date": data_date,
        "vlds": vlds,
        "power_cycle_number": pnn,
        "airborne_flight_run_number": rnn,
        # "json_s3_url": json_s3_url,

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
        # "ime_properties": ime_properties
    }

    if verbose:
        print (json.dumps(metadata, indent=4))

    if not images_only:
        upload_to_db(os.path.basename(file_base), metadata, candidate_id, test_only=test_only, verbose=verbose)


    try:
        os.unlink(plume_tiff)
    except:
        print ("Error deleting plume_tiff file ", plume_tiff) # TODO: Why, tho?

    try:
        os.unlink(rgb_tiff)
    except:
        print ("Error deleting rgb_tiff file ", rgb_tiff) # TODO: Why, tho?

    try:
        os.unlink(rgbqlctr_png)
    except:
        print ("Error deleting rgbqlctr_png file ", rgbqlctr_png) # TODO: Why, tho?


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input AVIRIS GeoJson file(s)", required=True, type=str, nargs='+')
    parser.add_argument("-e", "--endpoint", help="Target PostgreSQL endpoint", type=str, default="localhost",
                        required=False)
    parser.add_argument("-p", "--port", help="Target PostgreSQL port", type=str, default="8983", required=False)
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
    verbose = args.verbose
    images_only = args.imagesonly

    if not os.path.exists("tmp"):
        os.mkdir("tmp")

    for input_file in input_files:
        ingest_aviris_plume_geojson(geojson=input_file, s3_bucket=s3_bucket, test_only=test_only, images_only=images_only, verbose=verbose)

    #geojson = sys.argv[1]
    #ingest_aviris_geojson(geojson)








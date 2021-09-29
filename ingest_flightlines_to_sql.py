import utm
import numpy as np
import os
import os.path
import requests
import json
import argparse
import ingestutils.s3util as s3util

import psycopg2


DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""



def compute_bounds(easting, northing, samples, lines, easting_scale, northing_scale, zone, rotation):
    tl_m = [0.0, 0.0]
    tr_m = [(samples * easting_scale), 0]
    bl_m = [0, -(lines * northing_scale)]
    br_m = [(samples * easting_scale), -(lines * northing_scale)]

    theta = np.radians(rotation)
    R = np.array([[np.cos(theta), -np.sin(theta)], [np.sin(theta), np.cos(theta)]])

    tl_m = np.dot(R, tl_m)
    tr_m = np.dot(R, tr_m)
    bl_m = np.dot(R, bl_m)
    br_m = np.dot(R, br_m)

    tl_m += [easting, northing]
    tr_m += [easting, northing]
    bl_m += [easting, northing]
    br_m += [easting, northing]

    tl = utm.to_latlon(tl_m[0], tl_m[1], zone, 'U')
    tr = utm.to_latlon(tr_m[0], tr_m[1], zone, 'U')
    bl = utm.to_latlon(bl_m[0], bl_m[1], zone, 'U')
    br = utm.to_latlon(br_m[0], br_m[1], zone, 'U')

    return tl, tr, bl, br


def hackish_utm_string_parse(s):
    parts = s[2:-2].split(", ")
    easting = float(parts[3])
    northing = float(parts[4])
    easting_scale = float(parts[5])
    northing_scale = float(parts[6])
    zone = parts[7]
    northing_hemi = parts[8]
    rotation = float(parts[11].split("=")[1])

    return {
        "easting": easting,
        "northing": northing,
        "easting_scale": easting_scale,
        "northing_scale": northing_scale,
        "zone": int(zone),
        "northing_hemi" : northing_hemi,
        "rotation": rotation
    }


def read_hdr_file(image_path):
    hdr_file_path = image_path[:-4] + ".hdr"
    f = open(hdr_file_path, "r")
    d = f.readlines()
    f.close()

    hdr_props = {}

    for line in d:
        parts = line.split(" = ")
        if len(parts) == 2:
            hdr_props[parts[0]] = parts[1].strip()
    return hdr_props

def parse_flight_date_from_filename(image_path):
    bn = os.path.basename(image_path)
    year = bn[3:7]
    month = bn[7:9]
    day = bn[9:11]
    hour = bn[12:14]
    minute = bn[14:16]
    second = bn[16:18]

    data_date = "{YYYY}-{MM}-{DD} {HH}:{mm}:{ss}".format(YYYY=year, MM=month, DD=day, HH=hour, mm=minute, ss=second)

    return data_date



def compute_bounds_from_hdr_props(image_path):
    hdr_props = read_hdr_file(image_path)

    lines = int(hdr_props["lines"])
    samples = int(hdr_props["samples"])
    map_info_string = hdr_props["map info"]

    projection = hackish_utm_string_parse(map_info_string)

    easting = projection["easting"]
    northing = projection["northing"]
    easting_scale = projection["easting_scale"]
    northing_scale = projection["northing_scale"]
    zone = projection["zone"]
    rotation = projection["rotation"]

    tl, tr, bl, br = compute_bounds(easting, northing, samples, lines, easting_scale, northing_scale, zone, rotation)

    return tl, tr, bl, br



def does_flightline_exist_in_db(cur, flight_name):
    sql = """
    select count(1) from flightlines where flight_name = %s;
    """
    cur.execute(sql, (flight_name,))
    row = cur.fetchone()
    return row[0] >= 1



def update_in_db(cur, flight_name, data_date, image_url, poly):
    sql = """
    update flightlines
        set image_url=%s, 
          flightline_shape=ST_GeomFromText(%s, 4326),
          flight_timestamp=to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')
    where
      flight_name = %s
    """

    cur.execute(sql,
                (
                    image_url,
                    poly,
                    data_date,
                    flight_name
                )
            )

def insert_to_db(cur, flight_name, data_date, image_url, poly):
    sql = """
    insert into flightlines
        (
          flight_name, 
          image_url,
          flightline_shape,
          flight_timestamp
        )
    values
        (
           %s,
           %s,
           ST_GeomFromText(%s, 4326),
           to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')

        )
    """

    cur.execute(sql,
                (
                    flight_name,
                    image_url,
                    poly,
                    data_date
                )
            )

def insert_flightline_to_db(cur, flight_name, data_date, image_url, poly):
    exists = does_flightline_exist_in_db(cur, flight_name)

    if exists:
        update_in_db(cur, flight_name, data_date, image_url, poly)
    else:
        insert_to_db(cur, flight_name, data_date, image_url, poly)




def ingest_flightline_image(image_path, s3_bucket="bucket", test_only=False, verbose=False):
    tl, tr, bl, br = compute_bounds_from_hdr_props(image_path)

    tl = [tl[1], tl[0]]
    tr = [tr[1], tr[0]]
    bl = [bl[1], bl[0]]
    br = [br[1], br[0]]

    flight_name = os.path.basename(image_path)[:18]
    data_date = parse_flight_date_from_filename(image_path)
    image_url = s3util.upload_file_to_s3(image_path, s3_bucket=s3_bucket, test_only=test_only)

    shape = [tl, bl, br, tr, tl]
    poly = "POLYGON((" + ",".join([" ".join(map(str, p)) for p in shape]) + "))"

    # Create cursor
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWD, host=DB_ENDPOINT, port=DB_PORT)
    cur = conn.cursor()

    insert_flightline_to_db(cur, flight_name, data_date, image_url, poly)

    if test_only:
        conn.rollback()
    else:
        conn.commit()
    cur.close()
    conn.close()



if __name__ == "__main__":


    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input AVIRIS Flightline file(s)", required=True, type=str, nargs='+')
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

    args = parser.parse_args()
    input_files = args.data
    s3_bucket = args.bucket
    test_only = args.test
    verbose = args.verbose

    for input_file in input_files:
        ingest_flightline_image(input_file, s3_bucket=s3_bucket, test_only=test_only, verbose=verbose)
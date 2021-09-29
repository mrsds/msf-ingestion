import argparse
import psycopg2
import csv
import traceback

DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""

class PlumeHeaders:
    LINE_NAME = "# Line name"
    CANDIDATE_ID = "Candidate ID"
    SOURCE_ID = "Source ID"
    PLUME_LATITUDE = "Plume Latitude (deg)"
    PLUME_LONGITUDE = "Plume Longitude (deg)"
    CMF_MIN = "CMF Min (ppmm)"
    CMF_MAX = "CMF Max (ppmm)"
    CMF_MEDIAN = "CMF Median (ppmm)"
    CMF_MAD = "CMF MAD (ppmm)"
    SALIENCE_MIN = "Salience Min (%)"
    SALIENCE_MAX = "Salience Max (%)"
    SALIENCE = "Salience"
    CLOUD = "Cloud"
    SPECULAR = "Specular"
    FLARE = "Flare"
    DARK = "Dark"
    IME_10 = "IME10 (kg)"
    IME_20 = "IME20 (kg)"
    IME_50 = "IME50 (kg)"
    FETCH_10 = "Fetch10 (m)"
    FETCH_20 = "Fetch20 (m)"
    FETCH_50 = "Fetch50 (m)"
    DET_ID_10 = "DetId10"
    DET_ID_20 = "DetId20"
    DET_ID_50 = "DetId50"
    ASPECT_RATIO_10 = "Aspect ratio10"
    ASPECT_RATIO_20 = "Aspect ratio20"
    ASPECT_RATIO_50 = "Aspect ratio50"
    TOTAL_PIXELS_10 = "Total pixels10"
    TOTAL_PIXELS_20 = "Total pixels20"
    TOTAL_PIXELS_50 = "Total pixels50"
    AVG_IME_10 = "AvgIME10 (kg/m)"
    AVG_IME_20 = "AvgIME20 (kg/m)"
    AVG_IME_50 = "AvgIME50 (kg/m)"
    AVG_FETCH_10 = "AvgFetch10 (kg/m)"
    AVG_FETCH_20 = "AvgFetch20 (kg/m)"
    AVG_FETCH_50 = "AvgFetch50 (kg/m)"
    AVG_IME_DIV_FETCH_10 = "AvgIMEdivFetch10 (kg/m)"
    AVG_IME_DIV_FETCH_20 = "AvgIMEdivFetch20 (kg/m)"
    AVG_IME_DIV_FETCH_50 = "AvgIMEdivFetch50 (kg/m)"
    STD_IME_DIV_FETCH_10 = "StdIMEdivFetch10 (kg/m)"
    STD_IME_DIV_FETCH_20 = "StdIMEdivFetch20 (kg/m)"
    STD_IME_DIV_FETCH_50 = "StdIMEdivFetch50 (kg/m)"
    MINIMUM_THRESHOLD = "Minimum Threshold (ppmm)"
    WIND_MEAN_HRRR_10M = "Wind Mean (m/s) [HRRR 10 m, 10 nearest points for each of 3 closest times]"
    WIND_STD_HRRR_10M = "Wind Std (m/s) [HRRR 10 m, 10 nearest points for each of 3 closest times]"
    WIND_MEAN_HRRR_80M = "Wind Mean (m/s) [HRRR 80 m, 10 nearest points for each of 3 closest times]"
    WIND_STD_HRRR_80M = "Wind Std (m/s) [HRRR 80 m, 10 nearest points for each of 3 closest times]"
    WIND_MEAN_RTMA_10M = "Wind Mean (m/s) [RTMA 10 m, 10 nearest points for each of 3 closest times]"
    WIND_STD_RTMA_10M = "Wind Std (m/s) [RTMA 10 m, 10 nearest points for each of 3 closest times]"
    ASPECT_RATIO_FLAG = "Aspect Ratio Flag (0=valid, 1=invalid)"
    EMISSION_RATE_HRRR_10M = "Emission Rate (kg/hr) [HRRR 10 m]" #flux
    EMISSION_UNCERTAINTY_HRRR_10M = "Emission Uncertainty (kg/hr) [HRRR 10 m]" #flux uncertainty
    EMISSION_RATE_RTMA_10M = "Emission Rate (kg/hr) [RTMA 10 m]"
    EMISSION_UNCERTAINTY_RTMA_10M = "Emission Uncertainty (kg/hr) [RTMA 10 m]"
    DISTANCE_TO_NEAREST_STATION = "Distance to Nearest Station (km)"
    AVERAGE_WINDSPEED_AT_NEAREST_STATION = "Average Windspeed at Nearest Station (m/s)"
    STATION_SEARCH_RADIUS = "Station search radius (km)"
    STATION_SEARCH_TIME_DELTA = "Station search time delta (+/- minutes)"

def process_row(headers, row):
    datamap = {}

    for i in range(0, len(headers)):
        header = headers[i].strip()
        value = row[i]
        datamap[header] = value

    return datamap


def parse_csv(input_file, verbose=False):
    datamap = []
    with open(input_file) as csvfile:
        data = csv.reader(csvfile, delimiter=',', quotechar='\"')
        row_num = 0
        headers = []
        for row in data:
            if row_num == 0:
                headers = row
            else:
                row_data = process_row(headers, row)
                datamap.append(row_data)
            row_num += 1
    return datamap


def str_to_float(s):
    if s == "TBD":
        return -9999.0
    elif s != "TBD" and s is not None and len(s) > 0:
        return float(s)
    else:
        return None

def upload_plume(plume, cur, verbose=False, test_only=False):


    if does_plume_exist(plume[PlumeHeaders.CANDIDATE_ID], cur) is True:
        if verbose:
            print("Plume already exists, updating: ", plume[PlumeHeaders.CANDIDATE_ID])
        update_plume(plume, cur, verbose, test_only)
    elif verbose:
        print("Ingesting plume", plume[PlumeHeaders.CANDIDATE_ID])
        insert_plume(plume, cur, verbose, test_only)


def insert_plume(plume, cur, verbose=False, test_only=False):
    # get from candidate ID
    time_strings = plume[PlumeHeaders.CANDIDATE_ID].split("t")
    detection_time = time_strings[1][:2] + ":" + time_strings[1][2:4] + ":" + time_strings[1][4:6]
    detection_date = time_strings[0][3:7] + "-" + time_strings[0][7:9] + "-" + time_strings[0][9:11]
    # this simplified version is not making geom points atm, will revisit later, refer to ingest_plume_csv_to_sql.py
    sql = """
        INSERT INTO plumes
          (
    	    plume_id, 
    	    plume_latitude_deg, 
    	    plume_longitude_deg, 
    	    source_id, 
    	    source_latitude_deg, 
    	    source_longitude_deg, 
    	    candidate_id, 
    	    line_name, 
            flux,
            flux_uncertainty,
            vista_id,
    	    source_location, 
    	    plume_location,
    	    detection_timestamp)
    	VALUES (
    	  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s,
          %s,
    	   to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')
    	  );
        """

    cur.execute(sql,
                (
                    plume[PlumeHeaders.CANDIDATE_ID],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.SOURCE_ID][0]+plume[PlumeHeaders.SOURCE_ID][-5:],
                    # plume[PlumeHeaders.SOURCE_LATITUDE],
                    # plume[PlumeHeaders.SOURCE_LONGITUDE],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.CANDIDATE_ID],
                    plume[PlumeHeaders.LINE_NAME],
                    str_to_float(plume[PlumeHeaders.EMISSION_RATE_HRRR_10M]),
                    str_to_float(plume[PlumeHeaders.EMISSION_UNCERTAINTY_HRRR_10M]),
                    # plume[PlumeHeaders.VISTA_ID],
                    None,
                    # str_to_float(plume[PlumeHeaders.SOURCE_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.SOURCE_LATITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LATITUDE]),
                    None,
                    None,
                    "%s %s" % (detection_date, detection_time)
                ))


def update_plume(plume, cur, verbose=False, test_only=False):
    time_strings = plume[PlumeHeaders.CANDIDATE_ID].split("t")
    detection_time = time_strings[1][:2] + ":" + time_strings[1][2:4] + ":" + time_strings[1][4:6]
    detection_date = time_strings[0][3:7] + "-" + time_strings[0][7:9] + "-" + time_strings[0][9:11]
    # this simplified version is not making geom points atm, will revisit later, refer to ingest_plume_csv_to_sql.py
    sql = """update plumes set 
            plume_id=%s, 
    	    plume_latitude_deg=%s, 
    	    plume_longitude_deg=%s, 
    	    source_id=%s, 
    	    source_latitude_deg=%s, 
    	    source_longitude_deg=%s, 
    	    candidate_id=%s, 
    	    line_name=%s, 
            flux=%s,
            flux_uncertainty=%s,
            vista_id=%s,
    	    source_location=%s, 
    	    plume_location=%s,
    	    detection_timestamp=to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')
    	where 
    	plume_id = %s
    """

    cur.execute(sql,
                (
                    plume[PlumeHeaders.CANDIDATE_ID],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.SOURCE_ID][0]+plume[PlumeHeaders.SOURCE_ID][-5:],
                    # plume[PlumeHeaders.SOURCE_LATITUDE],
                    # plume[PlumeHeaders.SOURCE_LONGITUDE],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.CANDIDATE_ID],
                    plume[PlumeHeaders.LINE_NAME],
                    str_to_float(plume[PlumeHeaders.EMISSION_RATE_HRRR_10M]),
                    str_to_float(plume[PlumeHeaders.EMISSION_UNCERTAINTY_HRRR_10M]),
                    # plume[PlumeHeaders.VISTA_ID],
                    None,
                    # str_to_float(plume[PlumeHeaders.SOURCE_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.SOURCE_LATITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LATITUDE]),
                    None,
                    None,
                    "%s %s" % (detection_date, detection_time),
                    plume[PlumeHeaders.CANDIDATE_ID]
                ))

def does_plume_exist(candidate_id, cur):
    cur.execute("select count(candidate_id) as plume_count from plumes where candidate_id=%s",
                (candidate_id,))
    row = cur.fetchone()
    return row[0] >= 1


def upload_plumes(plumes, verbose=False, test_only=False):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWD, host=DB_ENDPOINT, port=DB_PORT)
    cur = conn.cursor()

    try:
        for plume in plumes:
            upload_plume(plume, cur, verbose, test_only)
    except:
        traceback.print_exc()

    if test_only:
        conn.rollback()
    else:
        conn.commit()

    cur.close()
    conn.close()


def process_csv(input_file, verbose=False, test_only=False):
    data = parse_csv(input_file, verbose)
    upload_plumes(data, verbose, test_only)





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input Plumes CSV (csv)", required=True, type=str, nargs='+')
    parser.add_argument("-e", "--endpoint", help="Target PostgreSQL endpoint", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--port", help="Target PostgreSQL port", type=str, default="5432", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test. Don't actually upload to PostgreSQL.",
                        required=False, action="store_true")

    args = parser.parse_args()
    input_files = args.data
    verbose = args.verbose
    test_only = args.test

    for input_file in input_files:
        process_csv(input_file, verbose=verbose, test_only=test_only)

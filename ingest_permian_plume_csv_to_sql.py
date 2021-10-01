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
    SOURCE_ID = "source_id"
    PLUME_LATITUDE = "plume_lat"
    PLUME_LONGITUDE = "plume_lon"
    CANDIDATE_ID ="candidate_id"
    DATE_OF_DETECTION = "date_of_detection"
    TIME_OF_DETECTION = "time_of_detection"
    SOURCE_TYPE = "source_type"
    IPCC = "ipcc"
    Q_PLUME = "qplume"
    SIGMA_QPLUME = "sigma_qplume"
    
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
            vista_id,
    	    source_location, 
    	    plume_location,
            flux,
            flux_uncertainty,
    	    detection_timestamp
            )
    	VALUES (
    	  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    	   to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')
    	  );
        """

    cur.execute(sql,
                (
                    plume[PlumeHeaders.CANDIDATE_ID],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.SOURCE_ID],
                    # plume[PlumeHeaders.SOURCE_LATITUDE],
                    # plume[PlumeHeaders.SOURCE_LONGITUDE],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.CANDIDATE_ID],
                    #plume[PlumeHeaders.LINE_NAME],
                    # plume[PlumeHeaders.VISTA_ID],
                    plume[PlumeHeaders.CANDIDATE_ID].split("-")[0],
                    None,
                    # str_to_float(plume[PlumeHeaders.SOURCE_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.SOURCE_LATITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LATITUDE]),
                    None,
                    None,
                    plume[PlumeHeaders.Q_PLUME],
                    plume[PlumeHeaders.SIGMA_QPLUME],
                    "%s %s" % (plume[PlumeHeaders.DATE_OF_DETECTION], plume[PlumeHeaders.TIME_OF_DETECTION])
                ))


def update_plume(plume, cur, verbose=False, test_only=False):
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
            vista_id=%s,
    	    source_location=%s, 
    	    plume_location=%s,
            flux=%s,
            flux_uncertainty=%s,
    	    detection_timestamp=to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss')
    	where 
    	plume_id = %s
    """

    cur.execute(sql,
                (
                    plume[PlumeHeaders.CANDIDATE_ID],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.SOURCE_ID],
                    # plume[PlumeHeaders.SOURCE_LATITUDE],
                    # plume[PlumeHeaders.SOURCE_LONGITUDE],
                    plume[PlumeHeaders.PLUME_LATITUDE],
                    plume[PlumeHeaders.PLUME_LONGITUDE],
                    plume[PlumeHeaders.CANDIDATE_ID],
                    #plume[PlumeHeaders.LINE_NAME],
                    # plume[PlumeHeaders.VISTA_ID],
                    plume[PlumeHeaders.CANDIDATE_ID].split("-")[0],
                    None,
                    # str_to_float(plume[PlumeHeaders.SOURCE_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.SOURCE_LATITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LONGITUDE]),
                    # str_to_float(plume[PlumeHeaders.PLUME_LATITUDE]),
                    None,
                    None,
                    plume[PlumeHeaders.Q_PLUME],
                    plume[PlumeHeaders.SIGMA_QPLUME],
                    "%s %s" % (plume[PlumeHeaders.DATE_OF_DETECTION], plume[PlumeHeaders.TIME_OF_DETECTION]),
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

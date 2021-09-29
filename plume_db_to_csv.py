import argparse
import psycopg2
import csv
import traceback

DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""

def get_plumes(cur):
    sql = """
    select * from plumes
    """
    cur.execute(sql)
    rows = cur.fetchall()
    return rows

def build_csv(output_file):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWD, host=DB_ENDPOINT, port=DB_PORT)
    cur = conn.cursor()
    with open(output_file, 'w', newline='') as csvfile:
        rowWriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        rowWriter.writerow(['Source identifier', 'Plume latitude (deg)', 'Plume longitude (deg)', 'Candidate identifier', 'Date of detection', 'Time of detection (UTC)', 'Source type (best estimate)', 'Sectors (IPCC)', 'Qplume (kg/hr): Plume emissions', 'Sigma Qplume (kg/hr): Uncertainty for plume emissions'])
        plumes = get_plumes(cur)
        for row in plumes:
            rowWriter.writerow([row[10], row[7], row[8], row[15], row[47].date(), row[47].time(), row[16], row[18], row[48], row[49]])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Output Plumes CSV (csv)", required=True, type=str)
    parser.add_argument("-e", "--endpoint", help="Target PostgreSQL endpoint", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--port", help="Target PostgreSQL port", type=str, default="5432", required=False)

    args = parser.parse_args()
    output_file = args.data

    build_csv(output_file)


import argparse
import psycopg2

DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""




if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", help="Target PostgreSQL endpoint", type=str, default=DB_ENDPOINT, required=False)
    parser.add_argument("-p", "--port", help="Target PostgreSQL port", type=str, default=DB_PORT, required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test by parsing and assembling upload document. Don't actually upload to PostgreSQL.",
                        required=False, action="store_true")
    args = parser.parse_args()
    test_only = args.test
    verbose = args.verbose
    db_endpoint = args.endpoint
    db_port = args.port

    # TODO: Use correct credentials
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWD, host=db_endpoint, port=db_port)
    cur = conn.cursor()

    sql = """
    begin;
    select msf_purge_plumes_and_sources();
    """
    cur.execute(sql)

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



import argparse
import psycopg2

DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""


def execute_procedure(stmt, cur, verbose=False):
    if verbose is True:
        print "Running procedure:", stmt, "...."
    sql = """
    begin;
    select {statement}();
    """.format(statement=stmt)
    cur.execute(sql)
    if verbose is True:
        print "        Done"


def msf_purge_joins_and_clean_inactive_entries(cur, verbose=False):
    execute_procedure("msf_purge_joins_and_clean_inactive_entries", cur, verbose)


def msf_build_vista_sources_relationship(cur, verbose=False):
    execute_procedure("msf_build_vista_sources_relationship", cur, verbose)


def msf_build_vista_aviris_plumes_relationship(cur, verbose=False):
    execute_procedure("msf_build_vista_aviris_plumes_relationship", cur, verbose)


def msf_build_vista_flightlines_relationship(cur, verbose=False):
    execute_procedure("msf_build_vista_flightlines_relationship", cur, verbose)


def msf_build_vista_oil_wells_field_boundary_relationship(cur, verbose=False):
    execute_procedure("msf_build_vista_oil_wells_field_boundary_relationship", cur, verbose)


def msf_build_vista_counties_relationship(cur, verbose=False):
    execute_procedure("msf_build_vista_counties_relationship", cur, verbose)


def msf_build_sources_flightlines_relationship(cur, verbose=False):
    execute_procedure("msf_build_sources_flightlines_relationship", cur, verbose)


def msf_build_sources_counties_relationship(cur, verbose=False):
    execute_procedure("msf_build_sources_counties_relationship", cur, verbose)


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

    msf_purge_joins_and_clean_inactive_entries(cur, verbose)
    msf_build_vista_sources_relationship(cur, verbose)
    msf_build_vista_aviris_plumes_relationship(cur, verbose)
    msf_build_vista_flightlines_relationship(cur, verbose)
    msf_build_vista_oil_wells_field_boundary_relationship(cur, verbose)
    msf_build_vista_counties_relationship(cur, verbose)
    msf_build_sources_flightlines_relationship(cur, verbose)
    msf_build_sources_counties_relationship(cur, verbose)

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

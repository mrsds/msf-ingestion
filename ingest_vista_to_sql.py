
import os.path
from osgeo import gdal,ogr,osr
import argparse
import psycopg2
from ingestutils import sectors

DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""



CATEGORY_MAP = {
    "Compressed Natural Gas Fueling Station" : 1000,
    "Compressor Station": 1001,
    "Dairy": 1002,
    "Digester": 1003,
    "Liquefied Natural Gas Fueling Station": 1004,
    "Natural Gas Processing Plants": 1005,
    "Natural Gas Storage Field": 1006,


    "Oil_and_Gas_Facility_Boundaries": 1007,
    "Oil and Gas Facility Boundary": 1007,
    "Oil and Gas Well": 1008,
    "Pipeline": 1009,
    "Distribution Pipeline": 1009,
    "Power Plant": 1010,
    "Refinery": 1011,

    "Solid Waste Disposal Site": 1012,
    "Landfill": 1012,

    "Wastewater Treatment Plant": 1013,
    "Composting Sites": 1014,
    "Oil and Gas Field Boundary": 1015,
    "Oil_and_Gas_Field_Boundaries": 1015,
    "Field Boundary": 1015,
    "Feed Lots": 1016,
    "Feed Lot": 1016
}




def get_definition_field_value(layerDefinition, feature, reqd_field_name):
    for i in range(layerDefinition.GetFieldCount()):
        field_name = layerDefinition.GetFieldDefn(i).GetName()
        field_value = feature.GetField(field_name)
        if field_name == reqd_field_name:
            return field_value

    return None

def envelope_to_polygon(envelope):
    ring = ogr.Geometry(ogr.wkbLinearRing)

    ring.AddPoint(envelope[0], envelope[3])
    ring.AddPoint(envelope[0], envelope[2])
    ring.AddPoint(envelope[1], envelope[2])
    ring.AddPoint(envelope[1], envelope[3])
    ring.AddPoint(envelope[0], envelope[3])
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    poly.FlattenTo2D()
    return poly


def get_vista_table_sequence_value(cur):
    sql = """
    select currval('vista_id_seq');
    """
    cur.execute(sql)
    row = cur.fetchone()
    return row[0]


def shape_exists_in_db(cur, vista_id):
    sql = """
    select count(1) from vista where vista_id = %s;
    """
    cur.execute(sql, (vista_id,))
    row = cur.fetchone()
    return row[0] >= 1


def get_vista_db_id_from_vista_id(cur, vista_id):
    sql = """
    select id from vista where vista_id = %s;
    """
    cur.execute(sql, (vista_id, ))
    row = cur.fetchone()
    return row[0]

def clear_shape_metadata_in_db(cur, vista_db_id):
    sql = """
    delete from vista_metadata where vista_id = %s
    """
    cur.execute(sql, (vista_db_id,))


def update_in_db(cur, category, category_id, site_name, feature_type, feature,
                    site_center_lon, site_center_lat,
                    loperator, lsitename, lstate, laddress, lsector, lcity,
                    geom_poly, geom_line, envelope_wkt, sector_level_1, sector_level_2, sector_level_3, vista_id, layerDefinition, verbose=False):
    if verbose:
        print("     Updating vista %s"%vista_id)

    vista_db_id = get_vista_db_id_from_vista_id(cur, vista_id)
    if verbose:
        print("     Internal db identifier: %s"%vista_db_id)

    sql = """
    update vista set
          category=%s,
    	  category_id=%s, 
    	  name=%s, 
    	  description=%s,
    	  shape_type=%s, 
    	  geojson=%s, 
    	  longitude=%s, 
    	  latitude=%s, 
    	  operator=%s, 
    	  site_name=%s, 
    	  state=%s, 
    	  address=%s, 
    	  sector=%s, 
    	  city=%s, 
    	  facility_location=ST_GeomFromText('POINT(%s %s)', 4326),
    	  facility_shape=ST_GeomFromText(%s, 4326),
    	  facility_shape_line=ST_GeomFromText(%s, 4326),
    	  facility_envelope=ST_GeomFromText(%s, 4326),
    	  sector_level_1=%s,
    	  sector_level_2=%s,
    	  sector_level_3=%s,
    	  vista_id=%s,
    	  is_active=true
    where
      id=%s;
    """
    cur.execute(sql,
                (
                    category,
                    category_id,
                    site_name,
                    None,
                    feature_type,
                    feature.ExportToJson(),
                    site_center_lon,
                    site_center_lat,
                    loperator,
                    lsitename,
                    lstate,
                    laddress,
                    lsector,
                    lcity,
                    site_center_lon,
                    site_center_lat,
                    geom_poly,
                    geom_line,
                    envelope_wkt,
                    sector_level_1,
                    sector_level_2,
                    sector_level_3,
                    vista_id,
                    vista_db_id
                )
            )
    clear_shape_metadata_in_db(cur, vista_db_id)
    insert_vista_metadata(cur, vista_db_id, layerDefinition, feature)


def insert_to_db(cur, category, category_id, site_name, feature_type, feature,
                    site_center_lon, site_center_lat,
                    loperator, lsitename, lstate, laddress, lsector, lcity,
                    geom_poly, geom_line, envelope_wkt, sector_level_1, sector_level_2, sector_level_3, vista_id, layerDefinition, verbose=False):

    if verbose:
        print("     Inserting vista %s" % vista_id)

    sql = """
        INSERT INTO vista
        (
    	  category,
    	  category_id, 
    	  name, 
    	  description,
    	  shape_type, 
    	  geojson, 
    	  longitude, 
    	  latitude, 
    	  operator, 
    	  site_name, 
    	  state, 
    	  address, 
    	  sector, 
    	  city, 
    	  facility_location,
    	  facility_shape,
    	  facility_shape_line,
    	  facility_envelope,
    	  sector_level_1,
    	  sector_level_2,
    	  sector_level_3,
    	  vista_id,
    	  is_active
    	  ) VALUES (
    	   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    	   ST_GeomFromText('POINT(%s %s)', 4326),
    	   ST_GeomFromText(%s, 4326),
    	   ST_GeomFromText(%s, 4326),
    	   ST_GeomFromText(%s, 4326),
    	   %s, %s, %s, %s, true
    	  );
        """
    cur.execute(sql,
                (
                    category,
                    category_id,
                    site_name,
                    None,
                    feature_type,
                    feature.ExportToJson(),
                    site_center_lon,
                    site_center_lat,
                    loperator,
                    lsitename,
                    lstate,
                    laddress,
                    lsector,
                    lcity,
                    site_center_lon,
                    site_center_lat,
                    geom_poly,
                    geom_line,
                    envelope_wkt,
                    sector_level_1,
                    sector_level_2,
                    sector_level_3,
                    vista_id
                )
            )

    vista_db_id = get_vista_table_sequence_value(cur)

    insert_vista_metadata(cur, vista_db_id, layerDefinition, feature)



def process_shape(cur, feature, transform, layerDefinition, test_only=False, verbose=False):
    geom = feature.GetGeometryRef()
    geom.Transform(transform)

    try:
        site_name = get_definition_field_value(layerDefinition, feature, "VistaName")
    except ValueError:
        site_name = get_definition_field_value(layerDefinition, feature, "LSiteName")

    feature_type = geom.GetGeometryName().lower()

    centroid = geom.Centroid()

    geom.FlattenTo2D()
    if geom.GetGeometryType() == ogr.wkbPolygon:
        geom = ogr.ForceToMultiPolygon(geom)
    elif geom.GetGeometryType() == ogr.wkbPoint:
        bufferDistance = 0.001
        geom = geom.Buffer(bufferDistance)
        geom = ogr.ForceToMultiPolygon(geom)
    elif geom.GetGeometryType() == ogr.wkbLineString:
        geom = ogr.ForceToMultiLineString(geom)


    envelope = envelope_to_polygon(geom.GetEnvelope())

    category = get_definition_field_value(layerDefinition, feature, "VistaSType")

    if category not in CATEGORY_MAP:
        raise Exception("Category '%s' not found."%category)

    category_id = CATEGORY_MAP[category]

    site_center_lat = centroid.GetY()
    site_center_lon = centroid.GetX()
    vista_id = get_definition_field_value(layerDefinition, feature, "Vista_ID")
    vista_ipcc = get_definition_field_value(layerDefinition, feature, "VistaIPCC")
    loperator = get_definition_field_value(layerDefinition, feature, "LOperator")
    lsitename = site_name
    lstate = get_definition_field_value(layerDefinition, feature, "LState")
    laddress = get_definition_field_value(layerDefinition, feature, "LAddress")
    lsector = get_definition_field_value(layerDefinition, feature, "VistaIPCC")
    lcity = get_definition_field_value(layerDefinition, feature, "LCity")
    try:
        sector_level_3, sector_level_2, sector_level_1 = sectors.get_sectors_by_level_3(vista_ipcc)
    except:
        sector_level_3, sector_level_2, sector_level_1 = (None, None, None)
    if verbose is True:
        print "Ingesting facility %s (%s) - Category %s (%s)"%(lsitename, vista_id, category, category_id)


    geom_poly = None #geom.ExportToWkt()
    geom_line = None #geom.ExportToWkt()


    if feature_type in ("multilinestring", "linestring"):
        geom_line = geom.ExportToWkt()
    else:
        geom_poly = geom.ExportToWkt()

    does_exist = shape_exists_in_db(cur, vista_id)
    if does_exist:
        update_in_db(cur, category, category_id, site_name, feature_type, feature,
                     site_center_lon, site_center_lat,
                     loperator, lsitename, lstate, laddress, lsector, lcity,
                     geom_poly, geom_line, envelope.ExportToWkt(), sector_level_1, sector_level_2, sector_level_3, vista_id,
                     layerDefinition)
    else:
        insert_to_db(cur, category, category_id, site_name, feature_type, feature,
                     site_center_lon, site_center_lat,
                     loperator, lsitename, lstate, laddress, lsector, lcity,
                     geom_poly, geom_line, envelope.ExportToWkt(), sector_level_1, sector_level_2, sector_level_3, vista_id,
                     layerDefinition)





def insert_vista_metadata(cur, vista_id, layerDefinition, feature):
    sql = """
    insert into vista_metadata
      (
        vista_id,
        property_name,
        property_value
      ) values (
        %s, %s, %s
      );
    """
    for i in range(layerDefinition.GetFieldCount()):
        field_name = layerDefinition.GetFieldDefn(i).GetName()
        field_value = feature.GetField(field_name)
        cur.execute(sql,(vista_id, field_name, field_value))


def set_categories_inactive(cur, categories_list, verbose=False):
    for category_id in categories_list:
        if verbose:
            print("Setting category id %s as inactive"%category_id)
        sql = """
        update vista set is_active=false where category_id = %s
        """
        cur.execute(sql, (category_id,))


def fetch_categories_for_load(file):
    categories = []

    for layer_num in range(0, file.GetLayerCount()):
        layer = file.GetLayer(layer_num)
        layerDefinition = layer.GetLayerDefn()
        for feature_num in range(0, layer.GetFeatureCount()):
            feature = layer.GetFeature(feature_num)
            category = get_definition_field_value(layerDefinition, feature, "VistaSType")

            if category in CATEGORY_MAP:
                category_id = CATEGORY_MAP[category]
                if category_id not in categories:
                    categories.append(category_id)

    return categories

def fetch_ipcc_list(file):
    ipcc_list = []

    for layer_num in range(0, file.GetLayerCount()):
        layer = file.GetLayer(layer_num)
        layerDefinition = layer.GetLayerDefn()
        for feature_num in range(0, layer.GetFeatureCount()):
            feature = layer.GetFeature(feature_num)
            ipcc = get_definition_field_value(layerDefinition, feature, "VistaIPCC")
            sector_level_3, sector_level_2, sector_level_1 = sectors.get_sectors_by_level_3(ipcc)

            if sector_level_1 not in ipcc_list:
                ipcc_list.append(sector_level_1)

    return ipcc

def process_vista_shapefile(input_path, noinactive=False, quickcheck=False, test_only=False, verbose=False):

    if verbose is True:
        print "Processing input file:", input_path

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWD, host=DB_ENDPOINT, port=DB_PORT)
    cur = conn.cursor()

    if verbose is True:
        print "Loading file into memory..."

    file = ogr.Open(input_path)

    categories_list = fetch_categories_for_load(file)

    if not noinactive:
        set_categories_inactive(cur, categories_list, verbose=verbose)

    targetSpatialRef = osr.SpatialReference()
    targetSpatialRef.ImportFromEPSG(4326)

    n = 0
    for layer_num in range(0, file.GetLayerCount()):
        layer = file.GetLayer(layer_num)

        layerDefinition = layer.GetLayerDefn()

        spatialRef = layer.GetSpatialRef()
        transform = osr.CoordinateTransformation(spatialRef, targetSpatialRef)

        for feature_num in range(0, layer.GetFeatureCount()):

            feature = layer.GetFeature(feature_num)

            #try:
            process_shape(cur, feature, transform, layerDefinition, test_only=test_only, verbose=verbose)

            n = n + 1
            if quickcheck is True and n >= 10:
                break
            #except:
            #   traceback.print_exc()
            #break
        #break


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


def gdal_error_handler(err_class, err_num, err_msg):
    errtype = {
            gdal.CE_None:'None',
            gdal.CE_Debug:'Debug',
            gdal.CE_Warning:'Warning',
            gdal.CE_Failure:'Failure',
            gdal.CE_Fatal:'Fatal'
    }
    err_msg = err_msg.replace('\n',' ')
    err_class = errtype.get(err_class, 'None')
    print 'Error Number: %s' % (err_num)
    print 'Error Type: %s' % (err_class)
    print 'Error Message: %s' % (err_msg)

gdal.PushErrorHandler(gdal_error_handler)

if __name__ == "__main__":

    os.environ["GDAL_DATA"] = '/Users/kgill/srv/share/gdal/'

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help="Input VISTA Shapefile(s)", required=True, type=str, nargs='+')
    parser.add_argument("-e", "--endpoint", help="Target PostgreSQL endpoint", type=str, default="localhost", required=False)
    parser.add_argument("-p", "--port", help="Target PostgreSQL port", type=str, default="8983", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")
    parser.add_argument("-t", "--test",
                        help="Test by parsing and assembling upload document. Don't actually upload to Solr.",
                        required=False, action="store_true")
    parser.add_argument("-n", "--noinactive", help="Don't flag existing entries as inactive", required=False, action="store_true")
    parser.add_argument("-q", "--quick", help="Quick check. Only ingest ten entries then quit", required=False,
                        action="store_true")
    args = parser.parse_args()
    input_files = args.data
    test_only = args.test
    verbose = args.verbose
    noinactive = args.noinactive
    quickcheck = args.quick

    for input_file in input_files:
        process_vista_shapefile(input_file, noinactive=noinactive, quickcheck=quickcheck, test_only=test_only, verbose=verbose)
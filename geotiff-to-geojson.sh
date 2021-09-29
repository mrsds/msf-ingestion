#!/bin/bash
#
# Requires:
#   - gdal_sieve.py
#   - ogr2ogr (GDAL)
#		- topojson (node.js)

# Grab the relative directory for source file.
SRC_DIR=`dirname $0`

# Which raster to compress.
ORG_FILE="$SRC_DIR/permian-thumb.tiff"

# Final output file.
OUTPUT_FILE="$SRC_DIR/permian-thumb.json"

echo "Processing $ORG_FILE."

# Where to output the new file.
TMP_DIR=./tmp

# The amount of times the file should be passed over.
ITERATIONS=3

# Threshold for each iteration.
THRESHOLD=40

# TopoJSON area threshold for simplification.
TOPO_COMPRESSION=0.000005

# Setup internal vars.
_CUR=$THRESHOLD
_COMPRESSION=$(($ITERATIONS * $THRESHOLD))

rm -rf $TMP_DIR
mkdir -p $TMP_DIR

# Start sieve passes.
gdal_sieve.py -st $THRESHOLD -4 $ORG_FILE $TMP_DIR/output-"$THRESHOLD".tiff

while [ $_CUR -le $_COMPRESSION ]; do
	let _PREV=$_CUR
	let _CUR=$_CUR+$THRESHOLD
	echo "Compressing output-$_PREV.tiff into $_CUR.tiff"
	gdal_sieve.py -st $THRESHOLD -4 "$TMP_DIR/output-$_PREV.tiff" \
		"$TMP_DIR/output-$_CUR.tiff"
	rm "$TMP_DIR/output-$_PREV.tiff"
done

# Raster to vector.
gdal_polygonize.py $TMP_DIR/output-"$_CUR".tiff \
	-f "ESRI Shapefile" $TMP_DIR vector n

# Change shapefile to geojson without the 0 layer, which is water.
ogr2ogr -f "GeoJSON" -where "n != 0" "$TMP_DIR/geojson.json" $TMP_DIR/vector.shp

# Convert to compressed TopoJSON.
geo2topo -o $OUTPUT_FILE \
	--no-stitch-poles \
	-s $TOPO_COMPRESSION \
	-p -- "$TMP_DIR/geojson.json"

# Clean up.
rm -rf $TMP_DIR
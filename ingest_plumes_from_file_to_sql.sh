#!/bin/bash
plumefilelist=$1
for f in `cat $plumefilelist`; do
	echo $f
	python ingest_plumes_to_sql_permian.py -d $f -v
done
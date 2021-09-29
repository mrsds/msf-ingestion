#!/usr/bin/env bash

for v in `ls  VISTA/*L1*shp`; do
    echo Ingesting $v ...
    python ingest_vista_to_sql.py -d $v $@
done
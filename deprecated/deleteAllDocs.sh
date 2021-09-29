#!/bin/bash


host=$1
core=$2

if [ "x$3" == "x" ]; then
    query='*:*'
else
    query=$3
fi



echo Warning: This is nuclear. Deletes all documents

curl http://${host}:8983/solr/${core}/update --data "<delete><query>${query}</query></delete>" -H 'Content-type:text/xml; charset=utf-8'
curl http://${host}:8983/solr/${core}/update --data '<commit/>' -H 'Content-type:text/xml; charset=utf-8'



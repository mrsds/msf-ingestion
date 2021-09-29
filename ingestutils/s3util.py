import os
import sys
import os.path
import numpy as np
import xml.etree.ElementTree as ET
import uuid
import requests
import json
from lxml import etree
import boto3
from boto.s3.key import Key
from osgeo import gdal,ogr,osr
import glob
import argparse
from PIL import Image
import tempfile
from io import BytesIO


S3_BUCKET = "bucket"


def upload_data_to_s3(filename, data, s3_bucket=S3_BUCKET, test_only=False):
    key = "AVIRIS/%s" % os.path.basename(filename)

    if not test_only:
        s3 = boto3.resource('s3')

        print "Uploading as", key
        s3.Bucket(s3_bucket).put_object(Key=key, Body=data)
        s3.Object(s3_bucket, key).Acl().put(ACL='public-read')
    else:
        print "Test upload as", key

    return "https://s3-us-gov-west-1.amazonaws.com/%s/%s"%(s3_bucket, key)


def upload_file_to_s3(file, s3_bucket=S3_BUCKET, test_only=False):
    data = open(file, 'rb')
    return upload_data_to_s3(file, data, s3_bucket=s3_bucket, test_only=test_only)


def create_thumbnail(file, thumbnail_size=(100, 100)):
    im = Image.open(file)
    im.thumbnail(thumbnail_size)
    img_bytes = BytesIO()
    im = im.convert("RGB")
    im.save(img_bytes, "JPEG")
    return img_bytes.getvalue()


def calculate_size_from_max_dimension(im, max_dim=100):
    pass

def upload_image_to_s3(file, s3_bucket=S3_BUCKET, test_only=False, thumbnail_size=(100, 100), upload_thumbnail=True):
    image_url = upload_file_to_s3(file, s3_bucket=s3_bucket, test_only=test_only)

    if upload_thumbnail:
        thumbnail_name = os.path.basename(file).replace(".png", "_thumbnail.jpg")
        thumbnail_data = create_thumbnail(file, thumbnail_size)
        thumbnail_url = upload_data_to_s3(thumbnail_name, thumbnail_data, s3_bucket=s3_bucket, test_only=test_only)
    else:
        thumbnail_url = None

    return image_url, thumbnail_url



if __name__ == "__main__":
    pass
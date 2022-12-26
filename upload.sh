#!/bin/bash

aws --profile=storage-upload
  s3 --endpoint-url=https://storage.yandexcloud.net \
  cp --recursive \
  $1/ s3://querifylabs.private/datasets/tpcds/1000g/parquet/$1

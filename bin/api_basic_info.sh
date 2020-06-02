#!/usr/bin/env bash


spark-submit \
--name "api_basic_info" \
--class smk.api.ApiBasicInfo \
--queue suyan \
--master yarn \
~/chenzhihao/smartmarketing.jar "$@"

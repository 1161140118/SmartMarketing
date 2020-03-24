#!/usr/bin/env bash
# $1: data date / "*" or "all" : all over the date.

spark-submit \
--class smk.dataops.LoadTiUbGsmBsD \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
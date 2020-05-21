#!/usr/bin/env bash

# $1 : start date[, end date or - ]
# split by space

spark-submit \
--class smk.target.MallIntentionPortrait \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster

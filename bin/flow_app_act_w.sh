#!/usr/bin/env bash

# $1 : start date[, end date, pre_start date, pre_end date]
# split by space

spark-submit \
--class smk.flow.FlowActivateHourW \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster

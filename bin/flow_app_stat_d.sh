#!/usr/bin/env bash

# $1: data date / "*" or "all" : all over the date.

spark-submit \
--name "flow_app_stat_d_${1}" \
--class smk.flow.FlowAppStatD \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" "$2" \
--deploy-mode cluster
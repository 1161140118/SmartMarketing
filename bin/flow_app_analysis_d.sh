#!/usr/bin/env bash

# $1: data date / "*" or "all" : all over the date.

spark-submit \
--name "flow_app_analysis_d_${1}" \
--class smk.flow.FlowAppAnalysis \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
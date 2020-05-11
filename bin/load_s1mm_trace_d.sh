#!/usr/bin/env bash
# $1: data date / "*" or "all" : all over the date.

spark-submit \
--name "load_s1mm_trace_d_${1}" \
--class smk.dataops.LoadTraceD \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
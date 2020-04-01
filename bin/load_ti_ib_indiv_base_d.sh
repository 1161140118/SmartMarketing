#!/usr/bin/env bash

# $1: data date / "*" or "all" : all over the date.

spark-submit \
--name "load_ti_ib_indiv_base_d_${1}" \
--class smk.dataops.LoadTiIbIndivBaseD \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
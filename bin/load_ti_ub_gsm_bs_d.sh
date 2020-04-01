#!/usr/bin/env bash
# $1: data date / "*" or "all" : all over the date.

spark-submit \
--name "load_ti_ub_gsm_bs_d_${1}" \
--class smk.dataops.LoadTiUbGsmBsD \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
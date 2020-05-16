#!/usr/bin/env bash

# $1: data date

spark-submit \
--name "mall_target_act_d_${1}" \
--class smk.target.TargetActivity \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
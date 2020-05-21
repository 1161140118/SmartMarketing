#!/usr/bin/env bash

# $1: data date

spark-submit \
--name "flow_app_portrait_${1}" \
--class smk.flow.FlowAppPortrait \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
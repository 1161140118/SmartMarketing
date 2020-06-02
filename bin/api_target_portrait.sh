#!/usr/bin/env bash


spark-submit \
--name "api_target_portrait" \
--class smk.api.ApiTargetPortrait \
--queue suyan \
--master yarn \
~/chenzhihao/smartmarketing.jar "$@"

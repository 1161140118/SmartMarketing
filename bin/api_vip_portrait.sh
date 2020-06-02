#!/usr/bin/env bash


spark-submit \
--name "api_vip_portrait" \
--class smk.api.ApiVipPortrait \
--queue suyan \
--master yarn \
~/chenzhihao/smartmarketing.jar "$@"

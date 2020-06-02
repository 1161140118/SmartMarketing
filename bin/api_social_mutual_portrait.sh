#!/usr/bin/env bash


spark-submit \
--name "api_social_mutual_portrait" \
--class smk.api.ApiSocialMutualPortrait \
--queue suyan \
--master yarn \
~/chenzhihao/smartmarketing.jar "$@"

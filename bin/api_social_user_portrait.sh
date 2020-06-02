#!/usr/bin/env bash


spark-submit \
--name "api_social_user_portrait" \
--class smk.api.ApiSocialUserPortrait \
--queue suyan \
--master yarn \
~/chenzhihao/smartmarketing.jar "$@"

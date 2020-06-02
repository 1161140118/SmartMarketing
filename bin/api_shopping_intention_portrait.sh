#!/usr/bin/env bash


spark-submit \
--name "api_shopping_intention_portrait" \
--class smk.api.ApiShoppingIntentionPortrait \
--queue suyan \
--master yarn \
~/chenzhihao/smartmarketing.jar "$@"

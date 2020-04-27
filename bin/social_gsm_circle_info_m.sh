#!/usr/bin/env bash

# $1: month to specify social_gsm_stat_m part.

spark-submit \
--name "social_gsm_circle_info_m_${1}" \
--class smk.social.SocialGsmCircleInfoM \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
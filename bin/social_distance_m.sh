#!/usr/bin/env bash

# $1: month

spark-submit \
--name "social_distance_m_${1}" \
--class smk.social.SocialDistanceM \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
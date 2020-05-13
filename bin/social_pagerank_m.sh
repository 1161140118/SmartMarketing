#!/usr/bin/env bash

# $1: month
# $2: iters

spark-submit \
--name "social_pagerank_m_${1}" \
--class smk.social.SocialPageRankM \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" "$2" \
--deploy-mode cluster
#!/usr/bin/env bash

# $1 : start date
# $2 : end date / '-' : 6 days later
# $3 : mall name / default: 'outlets'

spark-submit \
--class smk.vip.VipInfoWeekly \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" "$2" "$3" \
--deploy-mode cluster
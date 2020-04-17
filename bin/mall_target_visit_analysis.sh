#!/usr/bin/env bash

# $1 : start date
# $2 : end date / '-' : 6 days later

spark-submit \
--class smk.target.TargetVisitAnalysis \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" "$2" \
--deploy-mode cluster
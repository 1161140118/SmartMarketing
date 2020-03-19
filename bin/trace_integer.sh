#!/usr/bin/env bash

spark-submit \
--class smk.dataops.TraceInteger \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar $1 $2 \
--deploy-mode cluster
#!/usr/bin/env bash

# $1: data date / "*" or "all" : all over the date.

spark-submit \
--class smk.dataops.LoadTiIbIndivBaseD \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 8G \
--deploy-mode cluster
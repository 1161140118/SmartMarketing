#!/usr/bin/env bash

spark-submit \
--class smk.target.MallCellMatch \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar \
--deploy-mode cluster
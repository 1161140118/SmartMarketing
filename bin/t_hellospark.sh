#!/usr/bin/env bash

spark-submit \
--class helloworld.HelloSpark \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar $1 \
--deploy-mode cluster
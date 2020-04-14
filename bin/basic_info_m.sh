#!/usr/bin/env bash

# $1: data date to specify ti_ib_indiv_base_d part.

spark-submit \
--name "basic_info_m_${1}" \
--class smk.basicInfo.BasicInfoMonthly \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" \
--deploy-mode cluster
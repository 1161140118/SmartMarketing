#!/usr/bin/env bash

# $1 : start date
# $2 : end date

start_date=${1}
end_date=${2}

start=$start_date
end=`date -d "1 day ${end_date}" +%Y%m%d`	# 日期自增
while [ "${start}" != "${end}" ]
do
  echo $start
  echo -e "sh ~/chenzhihao/load_ti_ub_gsm_bs_d.sh $start "
  pwd
  sh ~/chenzhihao/load_ti_ub_gsm_bs_d.sh $start
  start=`date -d "1 day ${start}" +%Y%m%d`	# 日期自增
done
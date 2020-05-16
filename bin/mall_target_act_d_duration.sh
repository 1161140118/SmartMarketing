#!/usr/bin/env bash

# $1 : start date
# $2 : end date
# $3 : mode

start_date=${1}
end_date=${2}

start=$start_date
end=`date -d "1 day ${end_date}" +%Y%m%d`	# 日期自增
while [ "${start}" != "${end}" ]
do
  echo $start
  echo -e "sh ~/chenzhihao/mall_target_act_d.sh $start "
  pwd
  sh ~/chenzhihao/mall_target_act_d.sh "$start"
  start=`date -d "1 day ${start}" +%Y%m%d`	# 日期自增
done
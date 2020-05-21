#!/usr/bin/env bash

# $1 : start date
# $2 : end date

start_date=${1}
end_date=${2}

start=$start_date
end=`date -d "1 day ${end_date}" +%Y%m%d`	# 日期自增
while [ "${start}" != "${end}" ]
do
  echo $start $mode
  echo -e "sh ~/chenzhihao/flow_app_portrait.sh $start "
  pwd
  sh ~/chenzhihao/flow_app_portrait.sh "$start"
  start=`date -d "1 day ${start}" +%Y%m%d`	# 日期自增
done
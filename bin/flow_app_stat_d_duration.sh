#!/usr/bin/env bash

# $1 : start date
# $2 : end date
# $3 : mode

start_date=${1}
end_date=${2}
mode=${3}

start=$start_date
end=`date -d "1 day ${end_date}" +%Y%m%d`	# 日期自增
while [ "${start}" != "${end}" ]
do
  echo $start $mode
  echo -e "sh ~/chenzhihao/flow_app_stat_d.sh $start $mode "
  pwd
  sh ~/chenzhihao/flow_app_stat_d.sh "$start" "$mode"
  start=`date -d "1 day ${start}" +%Y%m%d`	# 日期自增
done
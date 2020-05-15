# $1 : start date
# $2 : end date

spark-submit \
--name "mall_target_trace_info_${1}_${2}" \
--class smk.target.TargetTraceInfo \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" "$2" \
--deploy-mode cluster
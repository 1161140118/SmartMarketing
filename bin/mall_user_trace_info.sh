# $1 : start date
# $2 : end date
# $3 : mall name / default outlets

spark-submit \
--class smk.vip.UserTraceInfo \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" "$2" "$3" \
--deploy-mode cluster
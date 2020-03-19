# $1 : start date
# $2 : end date
# $3 : mall name / default outlets

spark-submit \
--class smk.target.TargetTraceStat \
--queue suyan \
--master yarn ~/chenzhihao/smartmarketing.jar "$1" "$2" \
--deploy-mode cluster
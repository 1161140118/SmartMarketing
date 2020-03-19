spark-submit \
--class helloworld.patient.CLASS \
--queue suyan \
--files XXX \
--master yarn JAR  PARAMS \
--deploy-mode cluster \
> /data1/hlj_spark/dabl.log 2>&1



# original demo
spark-submit \
--class helloworld.patient.CLASS \
--queue suyan \
--conf spark.streaming.kafka.maxRatePerPartition=10000 \
--jars /opt/hadoop_client/Spark/spark/lib/gson-2.2.4.jar,/opt/hadoop_client/Spark/spark/lib/ojdbc6-11.2.0.3.0.jar,/opt/hadoop_client/Spark/spark/lib/jredisclient-0.0.1.jar,/opt/hadoop_client/Spark/spark/lib/commons-pool2-2.3.jar,/opt/hadoop_client/Spark/spark/lib/streamingClient/spark-streaming-kafka_2.10-1.5.1.jar,/opt/hadoop_client/Spark/spark/lib/streamingClient/kafka_2.10-0.8.2.1.jar,/opt/hadoop_client/Spark/spark/lib/streamingClient/kafka-clients-0.8.2.1.jar \
--master yarn /data1/hlj_spark/hlj3_dataBal.jar \
--deploy-mode cluster > /data1/hlj_spark/dabl.log 2>&1


# sensedw
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 32g \
    --executor-memory 32g \
    --num-executors 8 \
    --properties-file conf/spark/spark-yarn.conf \
    --files /usr/local/spark/conf/hive-site.xml,conf/application/application.conf,conf/spark/log4j.properties \
    --conf "spark.yarn.maxAppAttempts=1" \
    --class com.sensetime.bi.edw.ods.FindUniquePartition \
    didaDataFactory-assembly-1.0.0.jar ${table_name} ${target_partition} "${sql_condition}"
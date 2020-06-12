201907流量数据暂做202001用：

``` sql
CREATE TABLE if not exists `ts_hit_app`(
    `userid`    string,
    `flow`  int,
    `appid` string,
    `appname`   string,
    `lon`   double,
    `lat`   double,
    `hour`  string
)
COMMENT ' 用户APP流量数据日表 '
PARTITIONED BY (
    `part_date` string COMMENT '数据分区日期 <partition field>'
)
stored as parquet;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table suyanli.ts_hit_app partition(part_date)
select
  userid, flow, appid, appname, lon, lat, hour,
  concat('202001', substr(yearday,-2)) as part_date
from suyanli.ts_hit_app_201907
distribute by part_date

```
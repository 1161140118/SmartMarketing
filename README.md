# SmartMarketing
# 基于移动大数据的场景化智能营销系统

## 简介

数据预处理、数据分析、用户画像构建，推荐计算。

## 程序执行方法

1. 通过Spark-shell 直接运行代码。
2. 通过`bin`目录下shell文件提交spark job（绝大部分job的执行方式）。

## 编译及运行环境依赖

代码使用scala语言，通过sbt编译构建，项目目录结构遵循sbt标准目录。

* JDK 8
* Spark 1.5.1
* Scala 2.10.4
* SBT 0.13.16 (见 `project/build.properties`)

## 主要程序说明

主要程序目录：`src/main/scala/smk/`，下面对各package分别说明。

### vip
核心客户数据分析。产出核心客户画像。

#### UserTraceInfo
匹配用户和基站，过滤得到轨迹记录。

#### UserTraceStat
根据连接记录级和日级统计信息，过滤合法记录。

#### VipInfoW

根据规则，提取 mall_user_trace_stat 中 user，去除商场员工等特殊人群，剩余重点客户为vip。

按周统计该周内总访问时间，访问次数写入表 mall_vip_info_week。

#### **VipPortrait**

核心客户画像（周）：

1. 周访问次数

2. 累计到该周，当月访问次数

3. 周访问总/平均时长

4. 累计到该周，当月访问总/平均时长

5. 客户忠诚度打分：

   次数： 2  3  3.5

   时间： 0-1-2-3-4

6. 客户忠诚度过滤：

   本周结算累计忠诚度低于0.3被过滤：

   1. 连续三周未出现的低活跃新顾客
   2. 连续五周未出现的高活跃新顾客
   3. 连续六周以上未出现的老顾客

### target

目标客户数据分析，产出目标客户画像与购物意图画像。

#### MallCellMatch

商场-基站匹配，双向选择渐进匹配算法。

#### TargetTraceInfo

计算用户访问浏览信息：
1. 根据规则过滤连接记录
2. 用户连接有效基站即达到商场附近，连接记录标定商场名称
3. 对每条用户访问记录，标定商场名

#### TargetTraceStat

对每个 用户-商场 聚集，计算持续时间，开始、结束时间。

处理时间交叠的记录。

#### TargetVisitAnalysis

对每个用户聚集，标定当天浏览商场列表，count，平均持续时间， 加和持续时间等数据。

得到日级和周级两个分析汇总数据表。

#### TargetActivity

目标客户活跃度：

1. 当日到访商场基站数，区分工作日/周末
2. 周内日均到访商场基站数
3. 月内日均到访商场基站数

#### **TargetPortrait**

目标客户画像（周）：

1. 周访问次数
2. 累计到该周，当月访问次数
3. 周访问总/平均时长
4. 累计到该周，当月访问总/平均时长

#### **MallIntentionPortrait**

购物意图画像

<b>注意：依赖前一周数据.</b>

1. 线上购物意图
   1. 购物类APP使用强度：从flow analysis拉出日数据，取周平均
   2. 近期线上购物倾向（周）：本周取平均与上周数据取平均对比
   3. APP使用活跃时段（周）：从 flow_app_act_w 拉取

2. 线下购物意图
   1. 商场访问频次：从画像表中拉出，综合目标、核心客户

   2. 商场访问强度：同上

   3. 近期线下消费倾向：总时长（次数*平均时长）变化量

   4. 近期线下活跃倾向：

      vip: 忠诚度变化量

      target: 活跃度变化量

### social

社交（通话记录）数据处理与分析，产出社交关系画像。

#### SocialGsmStatM

用户语音按基站月表：

1. 筛选哈尔滨
2. 按 serv_no 聚集，避免基站不同的影响
3. 计算通话时长描述属性和通话次数描述属性
4. 计算上述属性的对数正态分布的均值与方差
5. 根据上述均值和方差，判定用户特殊性（有效性）：
   1. 对 calling_avg_ln, called_avg_ln 使用 4σ 标准
   2. 对 calling_cnt_ln, called_cnt_ln, one_min_calling_cnt_ln 使用 3σ 标准
6. 按月份和有效性，分区存储

#### SocialGsmCircleInfoM

用户语音交往圈月表:

1. 数据清洗
   1. 过滤非手机号
   2. 过滤对端非哈尔滨
   3. 过滤本机到本机
   4. 关联语音信息统计月表，过滤离群点手机号记录
2. 数据汇总
   1. 各种通话时长描述
   2. 各种通话次数描述
   3. 通话频繁度描述

#### SocialPageRankM

通过GraphX 计算PageRank和入度与出度。

#### **SocialUserPortrait**

用户社交关系单方画像：

1. pagerank 评分、排名
2. 通话记录统计

#### SocialDistanceM

计算社交亲密度

#### **SocialMutualPortrait**

构成社交关系双方画像

### flow

流量数据处理与分析，产出消费偏好画像。

#### FlowAppStatD

预过滤目标用户的流量数据记录，得到时段表和日表。

#### FlowActivateHourW

计算周活跃时段。

#### **FlowAppPortrait**

消费偏好画像（日级）

<b>注意：依赖前一日</b>

APP 使用情况分析

1.  网购类APP：淘宝等
2.  支付类APP
3.  金融类APP
4.  以上三类使用情况较前一日增加量
5.  APP使用活跃时段

评估策略：

1. 每个APP使用流量对数化，并根据偏离对数均值程度打分: 1-5, 0表示未使用
2. 同类APP使用取打分最高者作为该类APP使用情况得分

### recommend

推荐计算，通过结合用户画像属性构建输入特征并进行模型训练与数据预测。

#### RecoVipCluster

基于核心客户聚类，簇属性计算。

#### RecSimCalculate

目标客户特征/相似度属性 构建。

#### RecoLogisticRegression

逻辑回归模型组装、训练、评估。

注：由于sbt暂不能正常处理spark-millib，此部分代码在spark-shell执行。

#### RecoRandomForest

随进森林模型组装、训练、评估。

注：由于sbt暂不能正常处理spark-millib，此部分代码在spark-shell执行。

#### RecoItemGenerator

利用随机森林模型进行预测，获得推荐计算结果。

注：由于sbt暂不能正常处理spark-millib，此部分代码在spark-shell执行。

### api

画像及推荐结果表查询api。

### basicInfo

用户基本信息数据处理。

### dataops

部分大文件导入Hive的job。

## 辅助程序说明

辅助程序目录：`util`。

* metadata 元数据目录，用于数据字段参考
* raw_data 部分原始数据
* data  raw_date处理得到的数据，用于输入hive或spark运算
* utils  一些辅助代码
* markdowns
  * core_user.md  vip相关记录和hive建库语句
  * target_user.md  target相关记录和hive建库语句
  * shopping_intention 流量数据hive建库语句
  * s1mm_trace.md  移动轨迹数据到分区表
  * ti_data_load2hive.md  部分数据量较小的表导入hive的执行语句




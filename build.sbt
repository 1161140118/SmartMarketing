name := "SmartMarketing"

version := "0.1"

scalaVersion := "2.10.4"


// 配置jar包名字
assemblyJarName in assembly := "smartmarketing.jar"

// 在打包时，跳过测试
test in assembly := {}

// 阿里云仓库
resolvers += "aliyun-nexus" at "http://dl.bintray.com/spark-packages/maven"
resolvers+="aliyun Maven Repository" at "http://maven.aliyun.com/nexus/content/groups/public"
externalResolvers:= Resolver.withDefaultResolvers(resolvers.value,mavenCentral =false)

// withCachedResolution: sbt启动后第一次执行update后，会缓存所有的依赖解析信息
// withLatestSnapshots:  这样 SBT 就直接使用从远程仓库拉取到的第一个 SNAPSHOT 依赖(版本号最新，时间未必最新）
//updateOptions := updateOptions.value.withCachedResolution(true).withLatestSnapshots(false)


lazy val sparkVersion = "1.5.1"

// 依赖项，%%表示测试时需要，一般%； % "provided"表示此jar不打入最终的jar文件内
// 由于spark集群上已经有spark相关库，所以不打包
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)


// 当出现多个相同的依赖时，指定merge策略
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


/*
sbt 命令：
  
  
  
 */
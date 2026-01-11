```
1.flink安装部署

2.

```

growingIO:  https://accounts.growingio.com/login

诸葛IO : https://zhugeio.com/demo

易观：https://demo.analysysdata.com/dashboard/detail/1584

神策：https://www.sensorsdata.cn/demo/demo.html



# 1 Flink简介

## 背景介绍

第一代引擎：MR

第二代引擎：Tez

第三代引擎：Spark

第四代引擎：Flink



纯实时计算引擎较少、流批统一引擎没有？？？

做一个流批统一的计算引擎



## 定义

Apache Flink is a framework and distributed processing engine for stateful computations 
over unbounded and bounded data streams. Flink has been designed to run in all common 
cluster environments, perform computations at in-memory speed and at any scale.

Apache Flink是一个框架和分布式处理引擎，用于对无界和有界数据流进行有状态计算。



## 发展历程

2008，柏林理工大学一个研究性项目Stratosphere，Next Generation Big Data Analytics Platform（目标是建立下一代大数据分析引擎）；
2014-04-16，Stratosphere成为Apache孵化项目，从Stratosphere 0.6开始，正式更名为Flink。由Java语言编写；
2014-08-26，Flink 0.6发布；
2014-11-04，Flink 0.7.0发布，介绍了最重要的特性：Streaming API；
2016-03-08，Flink 1.0.0，支持Scala；
2016-08-08，Flink 1.1.0；
2017-02-06，Flink 1.2.0；
2017-11-29，Flink 1.4.0；
2018-05-25，Flink 1.5.0；
2018-08-08，Flink 1.6.0；
2018-11-30，Flink 1.7.0；
2019-02-15，Flink 1.7.2；
2019-04-09，Flink 1.8.0；
2019-07-10，Flink 1.8.1；
2019-09-12，Flink 1.8.2；
2019-08-22，Flink 1.9.0；
2019-10-18，Flink 1.9.1；
2020-02-11，Flink 1.10.0；
2020-05-08，Flink 1.10.1-rc3；

2020-07，Flink 1.11.1；

2019年初，阿里收购flink产品所属公；不久必然会升级到2.x。



## 为什么选择flink

- 流式数据更为真实地反映了我们的生活方式
- 流批计算融合
- 基于事件进行纯实时计算(连续事件处理)



**应用场景：**

纯实时计算的指标(实时数仓) --------

[监控和风控]的也会用到它  ------

===

Event-driven Applications
Stream & Batch Analytics
Data Pipelines & ETL

由上可知，flink也常用于离线和实时数仓中！！！重温下数仓架构演变！！！

首先我们来看看数仓架构演变(借用阿里云数仓架构)。演变如下图：

![1588960441004](Flink%E7%AC%94%E8%AE%B0.assets/1588960441004.png)

离线数仓架构：

![1588960483467](Flink%E7%AC%94%E8%AE%B0.assets/1588960483467.png)



Lambda架构：实时和离线计算融于一体。



![1588960673043](Flink%E7%AC%94%E8%AE%B0.assets/1588960673043.png)

Kappa架构：Lambda架构的简化版本，去掉其离线部分。

flink认为批次也是实时的特例！！！

![1588960921766](Flink%E7%AC%94%E8%AE%B0.assets/1588960921766.png)



# 2 Flink的安装部署

**下载地址：**

https://flink.apache.org/downloads.html



**部署模式：**

![1602214558997](Flink%E7%AC%94%E8%AE%B0.assets/1602214558997.png)



**提交模式：**

- in Session Mode,  ===>基于yarn
- in a Per-Job Mode, or  ===>基于yarn
- in Application Mode.

====>

- Local Mode



## 2.1、单机模式

flink的local模式运行在单个jvm中。同时local方便快速测试。

安装方式：

需求：

- **Java 1.8.x** or higher,
- **ssh**

1、下载

2、解压

```
[root@hadoop01 local]# tar -zxvf /home/flink-1.9.1-bin-scala_2.11.tgz -C /usr/local/
[root@hadoop01 local]# cd ./flink-1.9.1/
```

3、配置环境变量

```
export FLINK_HOME=/usr/local/flink-1.9.1/


export PATH=$PATH:$JAVA_HOME/bin:$ZK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$KAFKA_HOME/bin:$FLINK_HOME/bin:

```

4、刷新环境变量

```
[root@hadoop01 flink-1.9.1]# source /etc/profile
[root@hadoop01 flink-1.9.1]# which flink
```

5、启动测试

```
./bin/start-cluster.sh
```

6、测试：

```
jps
```

![1582910426138](Flink%E7%AC%94%E8%AE%B0.assets/1582910426138.png)

访问web地址：http://hadoop01:8081

![1582910461986](Flink%E7%AC%94%E8%AE%B0.assets/1582910461986.png)





启动流作业：

监控输入数据

```
[root@hadoop01 flink-1.9.1]# nc -l 6666
lorem ipsum
ipsum ipsum ipsum
bye
```

启动job

```
[root@hadoop01 flink-1.9.1]# ./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 6666
```

![1582910932981](Flink%E7%AC%94%E8%AE%B0.assets/1582910932981.png)

监控结果

```
[root@hadoop01 ~]# tail -f /usr/local/flink-1.9.1/log/flink-*-taskexecutor-*.out
lorem : 1
bye : 1
ipsum : 4
```



启动批次作业：

```
[root@hadoop01 flink-1.9.1]# flink run ./examples/batch/WordCount.jar --input /home/words --output /home/2002/out/00
Starting execution of program
Program execution finished
Job with JobID 8b258e1432dde89060c4acbac85f57d4 has finished.
Job Runtime: 3528 ms
```

web控制台如下图：

![1589042795331](Flink%E7%AC%94%E8%AE%B0.assets/1589042795331.png)



7、关闭local模式

```
./bin/stop-cluster.sh
```



## 2.2 standalone集群模式

flink的集群也是主从架构。主是jobManager,,从事taskManager。

![1589014192579](Flink%E7%AC%94%E8%AE%B0.assets/1589014192579.png)



规划：

| ip              | 服务                    | 描述 |
| --------------- | ----------------------- | ---- |
| 192.168.216.111 | jobManager、taskManager |      |
| 192.168.216.112 | taskManager             |      |
| 192.168.216.113 | taskManager             |      |



1、下载

2、解压

```
[root@hadoop01 local]# tar -zxvf /home/flink-1.9.1-bin-scala_2.11.tgz -C /usr/local/
[root@hadoop01 local]# cd ./flink-1.9.1/
```

3、配置环境变量

```
export FLINK_HOME=/usr/local/flink-1.9.1/


export PATH=$PATH:$JAVA_HOME/bin:$ZK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$KAFKA_HOME/bin:$FLINK_HOME/bin:

```

4、刷新环境变量

```
[root@hadoop01 flink-1.9.1]# source /etc/profile
[root@hadoop01 flink-1.9.1]# which flink
```



5、集群配置

```properties
配置三个配置文件
-rw-r--r--. 1 yarn games 10327 Jul 18  2019 flink-conf.yaml
-rw-r--r--. 1 yarn games    15 Jul 15  2019 masters
-rw-r--r--. 1 yarn games    10 Jul 15  2019 slaves
```

配置flink-conf.yaml：

```
#修改几个地方：

jobmanager.rpc.address: hadoop01

rest.port: 8081

rest.address: hadoop01
```

配置masters：

```
hadoop01:8081
```

配置slaves：

```
hadoop01
hadoop02
hadoop03
```



6、分发到别的服务器

```shell
[root@hadoop01 flink-1.9.1]# scp -r ../flink-1.9.1/ hadoop02:/usr/local/
[root@hadoop01 flink-1.9.1]# scp -r ../flink-1.9.1/ hadoop03:/usr/local/

并配置好其他服务器的环境变量。。。。。。
```



7、启动集群

```shell
[root@hadoop01 flink-1.9.1]# start-cluster.sh
[root@hadoop01 flink-1.9.1]# jps
2080 ResourceManager
1684 NameNode
5383 StandaloneSessionClusterEntrypoint
1803 DataNode
2187 NodeManager
5853 TaskManagerRunner
```

访问web：http://hadoop01:8081/#/overview

![1589164174984](Flink%E7%AC%94%E8%AE%B0.assets/1589164174984.png)

![1589164320780](Flink%E7%AC%94%E8%AE%B0.assets/1589164320780.png)



8、运行作业

```shell
[root@hadoop01 flink-1.9.1]# flink run ./examples/batch/WordCount.jar --input /home/words --output /home/1907/out/02
Starting execution of program
Program execution finished
Job with JobID dd30661b01cc6f663fe22dab7d7ef542 has finished.
Job Runtime: 6432 ms

查看结果：
[root@hadoop01 flink-1.9.1]# cat /home/1907/out/02
```



生产中：

1、jobmamager配置到单独服务器即可，，，本身使用不了多少内存。

2、taskmamager配置多台服务器，内存充足，能够满足业务即可。



## 2.3、standalone cluster HA部署

官网路径：

![1602224971372](Flink%E7%AC%94%E8%AE%B0.assets/1602224971372.png)

基于standalone cluster集群升级部署。

1、修改配置：flink-conf.yaml

```properties
high-availability: zookeeper
high-availability.zookeeper.quorum: hadoop01:2181,hadoop02:2181,hadoop03:2181
high-availability.zookeeper.path.root: /flink
high-availability.cluster-id: /cluster_flink
high-availability.storageDir: hdfs://hadoop01:9000/flink/recovery

```

2、修改配置：masters

```properties
hadoop01:8081
hadoop02:8081

```

远程发送：

```shell
[root@hadoop01 flink-1.9.1]# scp -r ./conf/flink-conf.yaml ./conf/masters hadoop02:/usr/local/flink-1.9.1/conf/
[root@hadoop01 flink-1.9.1]# scp -r ./conf/flink-conf.yaml ./conf/masters hadoop03:/usr/local/flink-1.9.1/conf/
```



3、启动集群

启动顺序：先启动zk和hdfs、再启动flink。

```
拷贝hdfs的依赖包：
[root@hadoop01 ~]# cp /home/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar /usr/local/flink-1.9.1/lib/
[root@hadoop01 ~]# scp /home/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar hadoop02:/usr/local/flink-1.9.1/lib/
[root@hadoop01 ~]# scp /home/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar hadoop03:/usr/local/flink-1.9.1/lib/

启动集群：
[root@hadoop01 ~]# start-cluster.sh
```



4、测试提交作业：

![1583758549991](Flink%E7%AC%94%E8%AE%B0.assets/1583758549991.png)

```shell
[root@hadoop01 ~]# flink run /usr/local/flink-1.9.1/examples/batch/WordCount.jar --input /home/words --output /home/out/fl00

```

结果：

```shell
[root@hadoop01 ~]# cat /home/out/fl00
1813 4
gp1813 3
hello 2
hi 1

```



5、并测试HA的切换：

通过log查看leader还是standby状态：

hadoop01的日志

![1585299888626](Flink%E7%AC%94%E8%AE%B0.assets/1585299888626.png)

由上可以看出hadoop01是leader。也就是active状态。

hadoop02的日志

![1585300058687](Flink%E7%AC%94%E8%AE%B0.assets/1585300058687.png)

hadoop02的日志没有leadership标识，也就是为standby状态。



手动杀死hadoop01激活状态的jobmanager：

```shell
[root@hadoop01 ~]# jps
3840 TaskManagerRunner
2454 NodeManager
1959 NameNode
3385 StandaloneSessionClusterEntrypoint
1802 QuorumPeerMain
4026 Jps
2092 DataNode
2350 ResourceManager
[root@hadoop01 ~]# kill -9 3385    ##或者使用jobmanager.sh stop
```



再次查看hadoop02的log：

![1585300200555](Flink%E7%AC%94%E8%AE%B0.assets/1585300200555.png)

显示hadoop02为leader状态。



测试是否能跑作业：

![1585327945976](Flink%E7%AC%94%E8%AE%B0.assets/1585327945976.png)

结果查看：

![1585328122720](Flink%E7%AC%94%E8%AE%B0.assets/1585328122720.png)



6、重启hadoop01的jobmanager：

```shell
[root@hadoop01 flink-1.9.1]# jobmanager.sh start
Starting standalonesession daemon on host hadoop01.
[root@hadoop01 flink-1.9.1]# jps
3840 TaskManagerRunner
5408 StandaloneSessionClusterEntrypoint

```

查看hadoop01的日志状态:

![1585301220521](Flink%E7%AC%94%E8%AE%B0.assets/1585301220521.png)

没有那个授权leader信息，代表就是一个standby状态咯。

HA的正常切换功能就可以咯。



## 2.4 yarn cluster 模式部署

mr和spark都可以基于yarn模式部署，flink也不例外，生产中很多也基于yarn模式部署。

flink的yarn模式部署也分为两种方式，一种是yarn-session，一种是yarn-per-job。大致如下图：

![1589045348992](Flink%E7%AC%94%E8%AE%B0.assets/1589045348992.png)



### 2.4.1 flink session HA模式

需要先启动集群，然后在提交作业，接着会向yarn申请一块资源空间后，资源永远保持不变。如果资源满了，下一个作业就无法提交，只能等到yarn中的其中一个作业执行完成后，释放了资源，那下一个作业才会正常提交。

适合场景：

当作业很少并且都较小，能快速执行完成时，可以使用。否则一般不会使用该模式。

这种模式，不需要做任何配置，直接将任务提价到yarn集群上面去，我们需要提前启动hdfs以及yarn集群即可。



两个进程：

运行yarn-session的主机上会运行FlinkYarnSessionCli和YarnSessionClusterEntrypoint两个进程。

在yarn-session提交的主机上必然运行FlinkYarnSessionCli，这个进场代表本节点可以命令方式提交job，而且可以不用指定-m参数。

YarnSessionClusterEntrypoint进场代表yarn-session集群入口，实际就是jobmanager节点，也是yarn的ApplicationMaster节点。

这两个进程可能会出现在同一节点上，也可能在不同的节点上。



1、配置

```
[root@hadoop01 flink-1.9.1]# vi ./conf/flink-conf.yaml

追加如下内容：
# flink yarn HA settings
high-availability: zookeeper
high-availability.zookeeper.quorum: hadoop01:2181,hadoop02:2181,hadoop03:2181
high-availability.zookeeper.path.root: /flink_yarn_2002
high-availability.cluster-id: /flink_yarn_2002
high-availability.storageDir: hdfs://hadoop01:9000/flink_yarn_2002/recovery

```

hadoop02 和 hadoop03分别做如上的配置。



2、启动flink session

先确保zookeeper、hdfs、yarn是启动okay。

```
[root@hadoop01 flink-1.9.1]# yarn-session.sh -n 3 -jm 1024 -tm 1024
...................................
2020-04-14 11:52:59,248 INFO  org.apache.flink.shaded.curator.org.apache.curator.framework.state.ConnectionStateManager  - State change: CONNECTED
2020-04-14 11:52:59,753 INFO  org.apache.flink.runtime.rest.RestClient                      - Rest client endpoint started.
Flink JobManager is now running on hadoop02:41674 with leader id 04caacdd-23c6-4e79-acd5-6db3b1014be0.
JobManager Web Interface: http://hadoop02:8081   ##代表jobmanager启动到hadoop02
```



报错：

```
Diagnostics: Container [pid=9528,containerID=container_1586835850522_0001_03_000001] is running beyond virtual memory limits. Current usage: 316.1 MB of 1 GB physical memory used; 2.3 GB of 2.1 GB virtual memory used. Killing container.

解决方法：
在hadoop01、hadoop02、hadoop03中的yarn-site.xml中配置如下：
<!--关闭nm的虚拟内存检测-->
<property>
         <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
</property>
```



3、环境检测

```
根据启动的信息可知，flink启动到咯hadoop02,则使用jps测试一下：

```

jps检测进程：

```
[root@hadoop02 flink-1.9.1]# jps
8992 DataNode
9985 YarnSessionClusterEntrypoint

```



web页面查看：http://hadoop02:8081

![1589128407010](Flink%E7%AC%94%E8%AE%B0.assets/1589128407010.png)





查看yarn的web控制台：http://hadoop01:8088

![1589128528251](Flink%E7%AC%94%E8%AE%B0.assets/1589128528251.png)





4、提交作业测试：

提交作业和standalone一样正常提交即可。

```shell
[root@hadoop02 flink-1.9.1]# flink run /usr/local/flink-1.9.1/examples/batch/WordCount.jar --input /home/words --output /home/out/fl01
Starting execution of program
Program execution finished
Job with JobID c3fd22587744bc54a6d69af6573a3183 has finished.
Job Runtime: 20642 ms

```

5、HA切换检测

杀死YarnSessionClusterEntrypoint服务，，看看还能不能在集群中找到该服务。

```
[root@hadoop02 flink-1.9.1]# jps
8992 DataNode
9985 YarnSessionClusterEntrypoint
8901 QuorumPeerMain
9096 SecondaryNameNode
9720 NodeManager
11210 Jps
#杀死进程造成异常退出
[root@hadoop02 flink-1.9.1]# kill -9 9985
```

当是HA时，，则一个挂掉后，则JM将会失败转移到另外的服务器上。如下是转移到hadoop01上。

```shell
[root@hadoop01 flink-1.9.1]# jps
9408 NameNode
12017 YarnSessionClusterEntrypoint

再次测试job：
[root@hadoop02 flink-1.9.1]# flink run /usr/local/flink-1.9.1/examples/batch/WordCount.jar --input /home/words --output /home/out/fl03
Starting execution of program
Program execution finished
Job with JobID 779689f706af7a9cb05e771a80e89128 has finished.
Job Runtime: 11462 ms


yarn session提交的作业，，在yarn的web平台中看不到。可以通过flink --list来查看。
```



6、如何停止运行的程序
通过cancel命令进行停止:

```
flink cancel -s hdfs:///flink/savepoints /savepoints-* -yid application_1586836326559_0002
```

或者通过 flink list 获得 jobId 

```
flink list
flink cancel -s hdfs:///flink/savepoints/savepoint-* jobId 
```

其中-s为可选操作



7、关闭jobmanager

直接将yarn-session停止掉:

```
yarn application -kill applicationId 
```



### 2.4.2 flink-per-job模式

```
yarn session需要先手动启动一个集群，然后在提交作业。
但是Flink per-job直接提交作业即可，不需要额外的去启动一个flink-session集群。直接提交作业，即可完成Flink作业。
```

适合场景：

作业多、且每个作业运行时长不定。生产推荐使用该模式运行作业。

1、直接使用flink run运行即可

```shell
[root@hadoop01 flink-1.9.1]# flink run -m yarn-cluster /usr/local/flink-1.9.1/examples/batch/WordCount.jar --input /home/words --output /home/out/fl05
```

![1589130867238](Flink%E7%AC%94%E8%AE%B0.assets/1589130867238.png)



查看yarn的web平台：

![1589130909448](Flink%E7%AC%94%E8%AE%B0.assets/1589130909448.png)





注意：

一定配置HADOOP_HOME  或者 YARN_HOME.





## 2.5、job historyserver配置

配置：vi ./conf/flink-conf.yaml,,,,追加如下内容

```yaml
# The HistoryServer is started and stopped via bin/historyserver.sh (start|stop)

# Directory to upload completed jobs to. Add this directory to the list of
# monitored directories of the HistoryServer as well (see below).
jobmanager.archive.fs.dir: hdfs://hadoop01:9000/flink_completed_jobs/

# The address under which the web-based HistoryServer listens.
historyserver.web.address: 192.168.216.111

# The port under which the web-based HistoryServer listens.
historyserver.web.port: 8082

# Comma separated list of directories to monitor for completed jobs.
historyserver.archive.fs.dir: hdfs://hadoop01:9000/flink_completed_jobs/

# Interval in milliseconds for refreshing the monitored directories.
historyserver.archive.fs.refresh-interval: 10000

```

启动历史服务：

```shell
[root@hadoop01 flink-1.9.1]# historyserver.sh start

```

查看进程：

```shell
[root@hadoop01 flink-1.9.1]# jps

```

访问web：http://hadoop01:8082/





注意:

启动flink集群报错：

```properties
Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file system implementation for scheme 'hdfs'. The scheme is not directly supported by Flink and no Hadoop file system to support this scheme could be loaded.
        at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:447)
        at org.apache.flink.core.fs.FileSystem.get(FileSystem.java:359)
        at org.apache.flink.core.fs.Path.getFileSystem(Path.java:298)
        at org.apache.flink.runtime.blob.BlobUtils.createFileSystemBlobStore(BlobUtils.java:116)
        ... 10 more
Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Hadoop is not in the classpath/dependencies.
        at org.apache.flink.core.fs.UnsupportedSchemeFactory.create(UnsupportedSchemeFactory.java:58)
        at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:443)
        ... 13 more

解决办法：
我使用的hadoop的版本是2.7.1，flink的版本是1.9.1
因为flink的checkpoint是需要记录在hdfs上，但是应该是flink1.8之后的就把hadoop的一些依赖删除了，所以报错找不到相应的jar包。

手动将flink对hadoop的依赖包进行导入到flink的lib目录下即可。
flink-shaded-hadoop-2-uber-2.7.5-10.0.jar

下载地址：
https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar

```

历史服务配置到此结束。



## 2.6、flink web控制台提交

1、选择需要运行的jar包

2、填写运行job需要的参数即可

![1589131793861](Flink%E7%AC%94%E8%AE%B0.assets/1589131793861.png)



注：

默认情况下，Job重启后就会删除掉之前上传的jar包。生产环境玩肯定是不行的，所以我们还是要指定一个目录来存储所有的上传 jar 包，并且不能够被删除，要配置固定的目录（Flink 重启也不删除的话）需要配置如下：

```bash
可设置web.upload.dir: /usr/local/flink-1.9.1/jars
```

默认位置：

![1589132332404](Flink%E7%AC%94%E8%AE%B0.assets/1589132332404.png)



在服务器中查看到结果如下：

![1589132409258](Flink%E7%AC%94%E8%AE%B0.assets/1589132409258.png)







## 2.7 批次和流式案例

pom.xml依赖：

```xml
<!--自定义版本信息-->
    <properties>
        <java.version>1.8</java.version>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <scala.compat.version>2.11</scala.compat.version>
        <hadoop.version>2.7.1</hadoop.version>
        <flink.version>1.9.1</flink.version>
        <kafka.version>0.9.0.1</kafka.version>
    </properties>

    <dependencies>
        <!--scala的依赖库-->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <!--flink的client依赖-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-scala_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-scala_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <!--flink的java的依赖-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_2.11</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
</dependencies>
```



### 2.7.1 流批代码案例

流式scala版本：

```java
package com.qianfeg.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 1、获取流式执行环境
 * 2、初始化数据源
 * 3、转换
 * 4、sink到目的地
 * 5、触发执行
 */
object Demo01_WordCount {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、初始化数据
    val ds: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //3、转换
    import org.apache.flink.api.scala._
    val sumed: DataStream[(String, Int)] = ds
        //.filter()
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    //4、sink持久化
    sumed.print("wc->")
    //5、触发执行
    env.execute("flink scala wc ")

    /**
    //一行代码
    env.socketTextStream("hadoop01", 6666)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print("wc->")

    env.execute("flink scala wc ")
     */
  }
}

```





# 作业day01

1、将自己的流式作业提交到自己的flink集群上运行。

2、编写java版本的流式词频统计

3、编写scala版本的批次词频统计

4、编写java版本的批次词频统计



流式Java版本案例：

```java
package com.qianfeng.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo01_WordCount_java {
    public static void main(String[] args) throws Exception {
        //1、获取flink的上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、初始化数据源 ---> source
        DataStreamSource<String> ds = env.socketTextStream("hadoop01", 6666);
        //3、对数据源ds进行转换 ---> transformation
       /* SingleOutputStreamOperator<WordWithCount> flatMapDS = ds.flatMap(new FlatMapFunction<String, WordWithCount>() {
            //实现切分--
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                //先对value进行切分
                String[] words = value.split(" ");
                //循环输出
                WordWithCount wordWithCount = new WordWithCount();
                for (String word : words) {
                    //搜集返回值
                    wordWithCount.word = word;
                    wordWithCount.count = 1;
                    out.collect(wordWithCount);
                }
            }
        });

         //4、进行分组累加
        SingleOutputStreamOperator<WordWithCount> sumed = flatMapDS.keyBy("word")
                .sum("count");


        //5、进行sink
        sumed.print("wc->");*/

        /*SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                //先对value进行切分
                String[] words = value.split(" ");
                //循环输出
                for (String word : words) {
                    //搜集返回值
                    out.collect(new Tuple2<String, Long>(word, 1l));
                }
            }
        });*/

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = ds.flatMap(new MyFlatMapFunction());

        SingleOutputStreamOperator<Tuple2<String, Long>> sumed = flatMapDS.keyBy(0).sum(1);
        sumed.print("wc->");

        //6、触发执行
        env.execute("java wc");
    }

    //自定义封装数据对象的内部类
    public static class WordWithCount {
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" + "word='" + word +", count=" + count +'}';
        }
    }


    //自定义flatmap函数
    public static class MyFlatMapFunction implements FlatMapFunction<String,Tuple2<String,Long>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            //先对value进行切分
            String[] words = value.split(" ");
            //循环输出
            for (String word : words) {
                //搜集返回值
                out.collect(new Tuple2<String, Long>(word, 1l));
            }
        }
    }
}
```



### 2.7.2 批次代码案例

scala版本的案例：

```java
package com.qianfeg.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * scala的版本的批次案例
 * 步骤：
 * 1、获取批次的执行环境
 * 2、初始化数据源
 * 3、转换
 * 4、sink
 */
object Demo01_WordCount_scala {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    import org.apache.flink.api.scala._
    env.fromElements("i like flink","flink is nice","flink nice")
      .flatMap(_.split(" "))
      .filter(_.size>=2)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}

```



java版本的案例：

```java
package com.qianfeng.batch;

import com.qianfeng.stream.Demo01_WordCount_java;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

//java版本的批次的api统计
public class Demo01_WordCount_batch {
    public static void main(String[] args) throws Exception {
        //获取批次执行上下文环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("i like flink","flink is nice","flink nice")
                .flatMap(new Demo01_WordCount_java.MyFlatMapFunction())
               // .filter(new FilterFunction<Tuple2<String, Long>>() {}
                .groupBy(0)
                .sum(1)
                .print();
    }
}
```



### 2.7.3 代码命令行和web提交

需要添加打包插件(如果有就可以忽略)：

```xml
 <build>
        <!--scala待编译的文件目录-->
        <sourceDirectory>src/main/java</sourceDirectory>
        <testSourceDirectory>src/test/java</testSourceDirectory>
        <plugins>

            <!-- Java Compiler -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>

            <!-- Scala Compiler -->
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```



上传jar包，然后执行如下命令即可

```shell
[root@hadoop02 flink-1.9.1]# flink run -m yarn-cluster --class com.qianfeng.day01.Demo01_Batch_WC /home/gp1907_flink-1.0.jar /home/out/fl00

[root@hadoop02 flink-1.9.1]# flink run -m yarn-cluster -c com.qianfeng.day01.Demo01_Batch_WC /home/gp1907_flink-1.0.jar /home/out/fl01


不不不不不不不能执行如下命令：
[root@hadoop02 flink-1.9.1]# flink run -m yarn-cluster /home/gp1907_flink-1.0.jar com.qianfeng.day01.Demo01_Batch_WC /home/out/fl00
```

![1589194720369](Flink%E7%AC%94%E8%AE%B0.assets/1589194720369.png)



web提交：

![1589204535356](Flink%E7%AC%94%E8%AE%B0.assets/1589204535356.png)

运行结果：

![1589204595139](Flink%E7%AC%94%E8%AE%B0.assets/1589204595139.png)



### 2.7.4 实时作业web监控案介绍

提交流式作业：

![1589526533058](Flink%E7%AC%94%E8%AE%B0.assets/1589526533058.png)

![1589526741149](Flink%E7%AC%94%E8%AE%B0.assets/1589526741149.png)

![1589526848660](Flink%E7%AC%94%E8%AE%B0.assets/1589526848660.png)

![1589527277210](Flink%E7%AC%94%E8%AE%B0.assets/1589527277210.png)

![1589527471369](Flink%E7%AC%94%E8%AE%B0.assets/1589527471369.png)





## Flink高级

## 2.8 Standalone模式任务调度

### 2.8.1 standalone服务启动流程

![1594142586128](Flink%E7%AC%94%E8%AE%B0.assets/1594142586128.png)

可以此参考源码看看每一个方法及其作用。



### 2.8.2 Standalone 运行原理

![1589212091958](Flink%E7%AC%94%E8%AE%B0.assets/1589212091958.png)

官网地址：https://ci.apache.org/projects/flink/flink-docs-release-1.10/concepts/runtime.html

运行原理图中的俗语：

1.Program Code：我们编写的 Flink 应用程序代码。

2.Job Client：Job Client 不是 Flink 程序执行的内部部分，但它是任务执行的起点。 Job Client 负责接受用户的程序代码，然后创建数据流，将数据流优化并提交给 Job Manager 以便进一步执行。 执行完成后，Job Client 将结果返回给用户。

3.JobManager：主进程（也称为作业管理器）协调和管理程序的执行。 它的主要职责包括安排任务，管理checkpoint ，故障恢复等。机器集群中至少要有一个 master，master 负责调度 task，协调 checkpoints 和容灾，高可用设置的话可以有多个 master，但要保证一个是active, 其他是 standby; Job Manager 包含 Actor system(通信系统)、Scheduler（调度）、Check pointing 三个重要的组件。

4.Task Manager：从 Job Manager 处接收需要部署的 Task。Task Manager 是在 JVM 中的一个或多个线程中执行任务的工作节点。 任务执行的并行性由每个 Task Manager 上可用的任务槽（task slot）决定。 每个任务代表分配给任务槽的一组资源。 例如，如果 Task Manager 有四个插槽，那么它将为每个插槽分配 25％ 的内存。 可以在任务槽中运行一个或多个线程。 同一插槽中的线程共享相同的 JVM。 同一 JVM 中的任务共享 TCP 连接和心跳消息。Task Manager 的一个 Slot 代表一个可用线程，该线程具有固定的内存，注意 Slot 只对内存隔离，没有对 CPU 隔离。默认情况下，Flink 允许子任务共享 Slot，即使它们是不同 task 的 subtask，只要它们来自相同的 job。这种共享可以有更好的资源利用率。



### 2.8.3 standalone job提交流程

![1589213501501](Flink%E7%AC%94%E8%AE%B0.assets/1589213501501.png)

1、用户提交程序到jobClient

2、JobClient进行处理、解析、优化提交到JobManager

3、jobmanager给与jobclient回馈提交结果

4、jobmanager将接收到的jobgraph继续进行处理、优化，解析成task拓扑，并发送给taskmanager中

5、taskmanager执行接收到的task，并定时汇报task的状态给jm

6、jobmanager将job的任务结果返回给jobclient

7、jobclient将结果返回给用户



## 2.9 flink on yarn的内部实现

### 2.9.1 flink和yarn的交互

![1589214324354](Flink%E7%AC%94%E8%AE%B0.assets/1589214324354.png)

官网地址：https://ci.apache.org/projects/flink/flink-docs-release-1.10/ops/deployment/yarn_setup.html#setup-for-application-priority-on-yarn



### 2.9.2 yarn模式下提交流程

![1589215433199](Flink%E7%AC%94%E8%AE%B0.assets/1589215433199.png)

















# 第三章 Flink的流批API实践



![1594109483940](Flink%E7%AC%94%E8%AE%B0.assets/1594109483940.png)

官网地址：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/datastream_api.html



1.10.*及以后：

![1602732873491](Flink%E7%AC%94%E8%AE%B0.assets/1602732873491.png)

## 3.1 流式的API操作

### 3.1.1 DataStream的source

```
flink的流式source分为4类：
基于文件：
readTextFile(path)
readFile(fileInputFormat, path)
readFile(fileInputFormat, path, watchType, interval, pathFilter)

基于socket： 不能设置2个及以上的并行度
socketTextStream

基于集合：
fromCollection(Seq)
fromCollection(Iterator)
fromElements(elements: _*)
fromParallelCollection(SplittableIterator)
generateSequence(from, to)

自定义source：
.addSource()
```



#### 自带source类型

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

/**
 * flink指带的source
 */
object Demo02_DataStreamSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1、集合source
    //创建一个集合
    val list: ListBuffer[Int] = new ListBuffer[Int]()
    list += 15
    list += 20
    list += 25

    //然后对list中的数据进行流式处理
    import org.apache.flink.api.scala._
    //集合作为源
    val dStream: DataStream[Int] = env.fromCollection(list)
    val filtered: DataStream[Int] = dStream.map(x => x * 100).filter(x => x >= 2000)
    //打印
    filtered.print("fromCollection->").setParallelism(1)
    println("============")

    //2、元素source
    val dStream1 = env.fromElements("flink is nice and flink nice") //字符串作为源
    dStream1.print("fromElements->").setParallelism(1)
    println("============")


    //3、文件source
    val dStream2 = env.readTextFile("E:\\flinkdata\\words.txt","utf-8")
    dStream2.print("readTextFile->").setParallelism(1)
    println("============")


    //4、读取hdfs文本文件，hdfs文件以hdfs://开头,不指定master的短URL
    val dStream3: DataStream[String] = env.readTextFile("hdfs://hadoop01:9000/words")
    dStream3.print("readTextFile->")

    //5、读取CSV文件,,,json格式自己读取即可
    val path = "E:\\flinkdata\\test.csv"
    val dStream4 = env.readTextFile(path)
    dStream4.print("readTextFile-csv->")

    //6、基于socket的sopurce
    val dStream6: DataStream[String] = env
        .socketTextStream("hadoop01", 6666)
        .setParallelism(1)  //不能设置为2及以上
    dStream6.print()

    //启动env
    env.execute("collection source")
  }
}
```



#### 自定义source

```java
package scala.com.scala.stream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

/**
 * flink自定义source
 * 自定义source分为3类：
 * 1、SourceFunction: 实现该接口，该类型的source不能设置并行度；同样没有open()和close()方法
 *  典型的就是SocketTextStreamFunction就是实现SourceFunction。
 * 2、ParallelSourceFunction : 实现该接口，能设置并行度；但没有open()和close()方法
 * 3、RichParallelSourceFunction ： 实现该接口，能设置并行度；同样有open()和close()方法
 */
object Demo03_DataStream_defineSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取自定义的数据源
    import org.apache.flink.api.scala._

    //1、自定义sourcefunction
    /*val ds: DataStream[String] = env.addSource(new SourceFunction[String] {
      //负责将生成数据发送到下游
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random: Random = new Random()
        // 循环可以不停的读取静态数据
        while (true) {
          val nextInt = random.nextInt(30)
          ctx.collect("random : " + nextInt)
          Thread.sleep(500);
        }
      }
      //负责什么时候结束run方法
      override def cancel(): Unit = ???
    })  //.setParallelism(2)  //不能在source上设置并行度
    ds.print("sourcefunction=>").setParallelism(2)*/

    //2、自定义ParallelSourceFunction
    /*val ds: DataStream[String] = env.addSource(new MyParallelSourceFunction).setParallelism(2)
    ds.print("ParallelSourceFunction->")*/

    val ds: DataStream[String] = env.addSource(new MyRichParallelSourceFunction).setParallelism(2)
    ds.print("RichParallelSourceFunction->")

    //启动env
    env.execute("collection source")
  }
}

//自定义ParallelSourceFunction
class MyParallelSourceFunction extends ParallelSourceFunction[String]{
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random: Random = new Random()
    // 循环可以不停的读取静态数据
    while (true) {
      val nextInt = random.nextInt(30)
      ctx.collect("random : " + nextInt)
      Thread.sleep(500);
    }
  }

  override def cancel(): Unit = ???
  
  
}


/**
 * 自定义RichParallelSourceFunction
 */
class MyRichParallelSourceFunction extends RichParallelSourceFunction[String]{

  var index:Int = _
  override def open(parameters: Configuration): Unit = {
    index = 50
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random: Random = new Random()
    // 循环可以不停的读取静态数据
    while (true) {
      val nextInt = random.nextInt(index)
      ctx.collect("random : " + nextInt)
      Thread.sleep(500);
    }
  }

  override def cancel(): Unit = ???

  override def close(): Unit = super.close()
}

```



#### 自定义Mysql的Source

添加mysql依赖：

```xml
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.38</version>
</dependency>
```



编写Stu实体

```java
package scala.com.scala.bean
//学生实体样例类
case class Stu(uid:Int,uname:String)

```



mysql的连接核心：

```java
package scala.com.scala.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.com.scala.bean.Stu

/**
 * flink自定义source
 *
 * CREATE TABLE `stu1` (
 * `id` int(11) DEFAULT NULL,
 * `name` varchar(32) DEFAULT NULL
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * ???思考多个并行度读取一份数据？？？
 * 1、就想并行读取一份数据===》考虑读出来的数据顺序问题
 *
 * 一个并行度读的好处？？所有数据按照顺序读出来
*/
object Demo04_DataStream_MysqlSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)  //全局设置

    //获取自定义的数据源
    import org.apache.flink.api.scala._

    val ds: DataStream[Stu] = env.addSource(new MyRichMysqlSource)
    ds.print("mysqlSource->")

    //启动env
    env.execute("mysqlSource")
  }
}

/**
 * 自定义RichParallelSourceFunction
 */
class MyRichMysqlSource extends RichParallelSourceFunction[Stu]{
  //获取mysql的连接的
  var ps: PreparedStatement = _
  var connection: Connection = _
  var resultSet: ResultSet = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"  //sql连接不用ssl将会报警告
    //val url = "jdbc:mysql://hadoop01:3306/test?useSSL=true"  //用ssl需要配置
    val username = "root"
    val password = "root"
    Class.forName(driver)
    try {
      connection = DriverManager.getConnection(url, username, password)
      val sql = "select * from stu1;"
      ps = connection.prepareStatement(sql)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  //读取mysql的数据的
  override def run(ctx: SourceFunction.SourceContext[Stu]): Unit = {
    //获取ps中的数据
    resultSet = ps.executeQuery()
    while (resultSet.next()) {
      var stu = new Stu(
        resultSet.getInt(1),
        resultSet.getString(2).trim)
      ctx.collect(stu)
    }
  }

  override def cancel(): Unit = ???

  //关闭mysql的连接
  override def close(): Unit = {
    if (resultSet != null) {
      resultSet.close()
    }
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }
}
```



#### Flink-jdbc-inputformat定义

```java
package scala.com.scala.stream

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row

/**
 * flink自定义source
 *
 * CREATE TABLE `stu1` (
 * `id` int(11) DEFAULT NULL,
 * `name` varchar(32) DEFAULT NULL
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * 自定义source方式获取数据有延迟；；而使用inputformat方式没有延迟
*/
object Demo05_DataStream_FlinkJDBC {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)  //全局设置

    //获取自定义的数据源
    import org.apache.flink.api.scala._

    //定义数据库中的字段类型
    val fieldTypes:Array[TypeInformation[_]] = Array[TypeInformation[_]](
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    //将基础类型转换成行类型
    val rowTypeInfo = new RowTypeInfo(fieldTypes:_*)

    val jdbcInputFormat:JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
   	.setDrivername("com.mysql.jdbc.Driver")
   	.setDBUrl("jdbc:mysql://hadoop01:3306/test")
   	.setQuery("select * from stu1")
    .setUsername("root")
    .setPassword("root")
   	.setRowTypeInfo(rowTypeInfo)
   	.finish()

    //使用createInput方式读取
    val ds: DataStream[Row] = env.createInput(jdbcInputFormat)
    ds.print("flink jdbc->")

    //启动env
    env.execute("flink jdbc")
  }
}
```





# 作业day02

1、编写flink的kafka source，并测试

2、复习一下flink的启动流程、作业提交流程(最好画一个图)

3、提前预习flink的流式算子和sink相关代码



#### Flink的连接器(Connector)

官网地址：https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/dev/connectors/kafka.html

是flink用于和第三方系统进行连接操作的接口代码。分为两类：

第一类Bundled Connectors：

- [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kafka.html) (source/sink)
- [Apache Cassandra](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/cassandra.html) (sink)
- [Amazon Kinesis Streams](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kinesis.html) (source/sink)
- [Elasticsearch](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/elasticsearch.html) (sink)
- [Hadoop FileSystem](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/filesystem_sink.html) (sink)
- [RabbitMQ](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/rabbitmq.html) (source/sink)
- [Apache NiFi](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/nifi.html) (source/sink)
- [Twitter Streaming API](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/twitter.html) (source)
- [Google PubSub](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/pubsub.html) (source/sink)

第二类Apache Bahir：

- [Apache ActiveMQ](https://bahir.apache.org/docs/flink/current/flink-streaming-activemq/) (source/sink)
- [Apache Flume](https://bahir.apache.org/docs/flink/current/flink-streaming-flume/) (sink)
- [Redis](https://bahir.apache.org/docs/flink/current/flink-streaming-redis/) (sink)
- [Akka](https://bahir.apache.org/docs/flink/current/flink-streaming-akka/) (sink)
- [Netty](https://bahir.apache.org/docs/flink/current/flink-streaming-netty/) (source)

flink提供flink和kafka整合依赖，依赖中提供生产和消费两大模块，注意不同的依赖版本所使用的kafka的client端，注意冲突。



#### Flink-Kafka连接器

先引入依赖：

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-kafka_2.11</artifactId>
	<version>1.9.1</version>
</dependency>
```

代码：

```java
package scala.com.scala.stream

import java.util.Properties

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * flink自定义
*/
object Demo06_DataStream_KafkaSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置kafka的相关消费信息
    val from_topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty("group.id", "test-group")
    import org.apache.flink.api.scala._
    //val ds = env.addSource(new FlinkKafkaConsumer[String](from_topic, new SimpleStringSchema(), properties))
    val ds = env.addSource(new FlinkKafkaConsumer[String](from_topic, new aa(), properties))
    //操作
    val fitered: DataStream[String] = ds.filter(_.length >= 5)
    /*ds.map(par=>{
      //复杂的实时的清洗
    })*/
    fitered.print("kafka-connector->")

    //启动env
    env.execute("kafka connector")
  }
}
```



#### flink-kafka-connect额外设置

```java
package scala.com.scala.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * flink自定义
*/
object Demo07_DataStream_KafkaSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.enableCheckpointing(10)
    //配置kafka的相关消费信息
    val from_topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty("group.id", "test-group")
    import org.apache.flink.api.scala._
    /*val flinkConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      from_topic,
      new SimpleStringSchema(),
      properties)*/
    val flinkConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      from_topic,
      new MyStringDes(),
      properties)
    //对消费者进行设置
    //flinkConsumer.setStartFromSpecificOffsets()  //指定分区，指定offset进行消费
/*    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition(from_topic, 0), 23L)
    specificStartOffsets.put(new KafkaTopicPartition(from_topic, 1), 31L)
    specificStartOffsets.put(new KafkaTopicPartition(from_topic, 2), 43L)
    flinkConsumer.setStartFromSpecificOffsets(specificStartOffsets)*/

    flinkConsumer.setStartFromLatest() //消费最新数据

    //正则指定分区
    /*val myConsumer = new FlinkKafkaConsumer[String](java.util.regex.Pattern.compile("test-[0-9]"),
      new SimpleStringSchema,
      properties)*/

    val ds = env.addSource(flinkConsumer)

    //操作
    val fitered: DataStream[String] = ds.filter(_.length >= 5)
    /*ds.map(par=>{
      //复杂的实时的清洗
    })*/
    fitered.print("kafka-connector->")

    //启动env
    env.execute("kafka connector")
  }
}

//自定义kafka的消费时的反序列化  --- 泛型，该序列化返回类型
class MyStringDes extends KafkaDeserializationSchema[String] {
  override def isEndOfStream(nextElement: String): Boolean = false

  //反序列化的实现  ConsumerRecord ： key-value
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    //也可以做数据的转换清洗等
    new String(record.value())+"_shcema"
  }

  //获取生产类型
  override def getProducedType: TypeInformation[String] = {
    TypeInformation.of(classOf[String])
  }
}
```



#### flink kafka offset

官网地址：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kafka.html#kafka-consumers-offset-committing-behaviour-configuration

在说flink kafka的offset的时候，大家可以先想想spark streaming或者structured streaming中kafka的offset怎么存储？？？

```
1、依赖于kafka的本身的自动提交机制
2、存储于外部系统
```

flink中kafka的offset提交主要有2种：

第一种：如果Checkpointing disabled，则完全依赖于kafka自身的API。
第二种：如果Checkpointing enabled， 则当checkpoint做完成时，会将offset提交给kafka or zk(0.8),这样保障提交的offset和checkpoint中的是一致。方便容错恢复的时候使用checkpoint中的offset而已。flink默认也使用该方式。

基于checkpoint的offset提交流程：

第一：初始化每个分区的消费者offset为0.

![1594137959131](Flink%E7%AC%94%E8%AE%B0.assets/1594137959131.png)

第二：0分区消费A，offset变更。

![1594138216695](Flink%E7%AC%94%E8%AE%B0.assets/1594138216695.png)

第三步：0分区消费到B，变更offset为2；1分区消费A1，变更offset为1；同时jm触发checkpoint。

![1594138636609](Flink%E7%AC%94%E8%AE%B0.assets/1594138636609.png)



第四步：0分区的source做完checkpoint，然后将barrier传递到下一个opertor，及Map将开始做checkpoint

![1594139158488](Flink%E7%AC%94%E8%AE%B0.assets/1594139158488.png)

第五步：map optertor将做完checkpoint，但还未完成。

![1594139469375](Flink%E7%AC%94%E8%AE%B0.assets/1594139469375.png)



第六步：所有opreator做完checkpoint

![1594139708900](Flink%E7%AC%94%E8%AE%B0.assets/1594139708900.png)

第七步：checkpoint完成之后，将checkpoint中存储的offset提交到kafka中。如果任务有失败情况，则会根据checkpoint中的offset来恢复，保障处理完成的事件一定是处理完成过。如果恢复则如下：

![1594140046395](Flink%E7%AC%94%E8%AE%B0.assets/1594140046395.png)

至此为止，flink kafka的offset提交完成。大家也可以参考起源码，主要是3个方法：

```
1、notifyCheckpointComplete(long checkpointId) : 通知checkpoint完成。

2、doCommitInternalOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets,@Nonnull KafkaCommitCallback commitCallback)：开始转到kafkaFetcher中，然后将offset提交到kafka中。

3、setOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,@Nonnull KafkaCommitCallback commitCallback)：该方法是一个单独线程，是实际操作offset提交的逻辑。
```





### 3.1.2 DataStream的Opertor

官网：https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/index.html

基础算子：

**FlatMap**

map

**Filter**

keyBy

**Split**

select

**Reduce**

**Aggregations**

union



#### flatMap|map|filter：

类型转换：DataStream--->DataStream

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.com.scala.bean.WordCount

/**
 * flink自定义
*/
object Demo08_DataStream_FlatMapMapFilter {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //转换统计之前必须进行隐式转换（引入整个scala的api包）
    import org.apache.flink.api.scala._
    val res: DataStream[WordCount] = dStream
      .flatMap(_.split(" "))
      .filter(x => x.length > 5) //长度大于5位的单词
      .map(w => WordCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(5))  //滑动窗口
      .sum("count")

    //sink地方
    res.print().setParallelism(1)

    //启动env
    env.execute("Map|FlatMap|Filter")
  }
}
```

#### SideOutput侧输流

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.com.scala.bean.Temp

/**
 * flink自定义
 *
 * sideOutput:
 * 1、先定义outputTag，即侧输标识
 * 2、使用process函数进行数据流输出
 * 3、使用getSideOutput获取侧输流
*/
object Demo10_DataStream_SideOutput {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //将温度正常和异常的旅客拆分
    //输入数据：uid temp uname timestamp location
    import org.apache.flink.api.scala._
    val ds: DataStream[Temp] = dStream.map(perInfo => {
      val arr = perInfo.split(",")
      val uid: String = arr(0).trim
      val temperature: Double = arr(1).trim.toDouble
      val uname: String = arr(2).trim
      val timestamp: Long = arr(3).trim.toLong
      val location: String = arr(4).trim
      Temp(uid, temperature, uname, timestamp, location)
    })

    //初始化侧输标识
    val normalTag = OutputTag[String]("normal")  //正常标识
    val exceptionTag = OutputTag[String]("exception")  //正常标识

    //数据流处理
    val outputSideDS: DataStream[String] = ds.process(new ProcessFunction[Temp, String] {
      override def processElement(value: Temp,
                                  ctx: ProcessFunction[Temp, String]#Context,
                                  out: Collector[String]): Unit = {
        // emit data to regular output
        if (value.temp < 37.0 && value.temp > 35.3) {
          //out.collect(value.uid + "_" + value.temp)
          // emit data to side output
          ctx.output(normalTag, "normal-" + String.valueOf(value))
        } else {
          //out.collect(value.uid + "_" + value.temp)
          ctx.output(exceptionTag, "exception-" + String.valueOf(value))
        }

      }
    })

    //取出侧输流
    outputSideDS.getSideOutput(normalTag).print("normal->")
    outputSideDS.getSideOutput(exceptionTag).print("execption->")

    //启动env
    env.execute("sideoutput")
  }
}

```



#### split和select拆分流：

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.com.scala.bean.{Temp, WordCount}

/**
 * flink自定义
 *
 *   Select应用于Split的结果
 * * split:DataStream→ SplitStream
 * * 将指定的DataStream拆分成多个流用SplitStream来表示
 * *
 * * select:SplitStream→ DataStream
 * * 跟split搭配使用，从SplitStream中选择一个或多个流
*/
object Demo09_DataStream_SplitSelect {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //将温度正常和异常的旅客拆分
    //输入数据：uid temp uname timestamp location
    import org.apache.flink.api.scala._
    val ss: SplitStream[Temp] = dStream.map(perInfo => {
      val arr = perInfo.split(",")
      val uid: String = arr(0).trim
      val temperature: Double = arr(1).trim.toDouble
      val uname: String = arr(2).trim
      val timestamp: Long = arr(3).trim.toLong
      val location: String = arr(4).trim
      Temp(uid, temperature, uname, timestamp, location)
    })
      .split((temp: Temp) => {
        if (temp.temp >= 35.9 && temp.temp <= 37.5)
          Seq("正常")  //一个数据流的标识
        else
          Seq("异常")
      }
      )

    ss.print("split Stream->")

    //使用Select来选择流
    ss.select("正常").print("正常旅客->")
    ss.select("异常").print("异常旅客->")

    //启动env
    env.execute("Split和Select")
  }
}

```





#### Union和Connect合并流

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

import scala.com.scala.bean.Temp

/*
 * DataStream的算子：
 * union和connect：合并流
 *
 * union：DataStream* → DataStream
 * union 合并多个流，新的流包含所有流的数据；union的多个子流类型一致
 *
 * connect：* -> ConnectedStreams
 * connect只能连接两个流;onnect连接的两个流类型可以不一致;
 * 两个流之间可以共享状态(比如计数)。这在第一个流的输入会影响第二个流时, 会非常有用
 */
object Demo10_DataStream_Union {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)


    //将温度正常和异常的旅客拆分
    //输入数据：uid temp uname timestamp location
    import org.apache.flink.api.scala._
    val ss: SplitStream[Temp] = dStream.map(perInfo => {
      val arr = perInfo.split(",")
      val uid: String = arr(0).trim
      val temperature: Double = arr(1).trim.toDouble
      val uname: String = arr(2).trim
      val timestamp: Long = arr(3).trim.toLong
      val location: String = arr(4).trim
      Temp(uid, temperature, uname, timestamp, location)
    })
      .split((temp: Temp) => {
        if (temp.temp >= 35.9 && temp.temp <= 37.5)
          Seq("正常")  //一个数据流的标识
        else
        Seq("异常")
      }
      )

    /*//使用Select来选择流
    val commonDS: DataStream[Temp] = ss.select("正常")
    val execeptionDS: DataStream[Temp] = ss.select("异常")
    //union合并流
    val unionDS: DataStream[Temp] = commonDS.union(execeptionDS)
    unionDS.print("unionDS->")
    */

    val commonDS: DataStream[(String, Double)] = ss.select("正常").map(f => (f.uid, f.temp))
    val execeptionDS: DataStream[(String, Double, String)] = ss.select("异常").map(f => (f.uid, f.temp, f.uname))

    //union合并流
    //val unionDS: DataStream[Temp] = commonDS.union(execeptionDS)
    //unionDS.print("unionDS->")

    //connect合并流
    val connectDS: ConnectedStreams[(String, Double), (String, Double, String)] = commonDS.connect(execeptionDS)
    val connectMapDS: DataStream[String] = connectDS.map(
      common => "正常： " + common._1 + "_" + common._2,
      exception => "异常： " + exception._1 + "_" + exception._2 + "_" + exception._3)
    connectMapDS.print("connectDS->")

    env.execute("unionDS")
  }
}
```



#### KeyBy&Reduce操作

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 * keyBy和reduce合并：
 *
 * keyBy:DataStream→KeyedStream
 * 将具有相同Keys的所有记录都分配给同一分区。内部用散列分区实现
 * keyby类似于sql中的group by，将数据进行了分组。后面基于keyedSteam的操作，都是组内操作。
 *
 * reduce：聚合
 * reduce表示将数据合并成一个新的数据，返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。
 * reduce方法不能直接应用于SingleOutputStreamOperator对象，因为这个对象是个无限的流，对无限的数据做合并，没有任何意义。
 * reduce需要针对分组或者一个window(窗口)来执行，也就是分别对应于keyBy、window/timeWindow 处理后的数据，
 * 根据ReduceFunction将元素与上一个reduce后的结果合并，产出合并之后的结果。
 */
object Demo12_DataStream_KeyByReduce {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    /*env.fromElements(Tuple2(200, 33), Tuple2(100, 65), Tuple2(100, 56), Tuple2(200, 666), Tuple2(100, 678))
      .keyBy(0)
      .reduce((a,b)=>(a._1,a._2+b._2))
      .print("keyBy Reduce->")*/

    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    dStream.map(line=>{
      val fields: Array[String] = line.split(",")
      val flag: Int = fields(2).toInt
      val age: Int = fields(3).toInt
      val local: String = fields(4).toString.trim
      //封装返回
      (local,(flag,age))
    })
      .filter(_._2._1 == 1)
        .keyBy(0)
        /*.reduce((a,b)=>{
          //累加新增数
          val adds = a._2._1 + b._2._1
          val ages = a._2._2 + b._2._2
          (a._1,(adds,ages/adds))
        })*/
      .sum(1)
        .print("news 19->")

    env.execute("reduce")
  }
}
```



#### Aggregation操作

```java
package scala.com.scala.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/*
聚合：
 * 聚合算子：KeyedStream→DataStream
 * min
 * minBy
 * max
 * maxBy
 * sum
*/
object Demo13_DataStream_Aggregation {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    val ks: KeyedStream[(Int, Int), Tuple] =
      env.fromElements(Tuple2(200, 33), Tuple2(100, 65), Tuple2(100, 56), Tuple2(200, 666), Tuple2(100, 678))
      .keyBy(0)

    //聚合函数
    //ks.max(1).print("max->")
    //ks.maxBy(1).print("maxBy->")
    ks.min(1).print("min->")
    ks.minBy(1).print("minBy->")
    //ks.sum(1).print("sum->")

    env.execute("reduce")
  }
}
```



### 3.1.3 DataStream的sink

官网：https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html#data-sinks

```
writeAsText()

writeAsCsv(...) 

print()

writeUsingOutputFormat() 

writeToSocket

addSink 
```

#### 基础sink

```java
package scala.com.scala.stream

import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
flink 的sink
*/
object Demo14_DataStream_BasicSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    val res: DataStream[(String, Int)] = env.socketTextStream("hadoop01", 6666)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //sink输出
    //res.writeAsText("E:\\flinkdata\\out\\00\\")
    //res.writeAsText("hdfs://hadoop01:9000/out/10")
    //csv只能使用数据为tuple
    //res.writeAsCsv("E:\\flinkdata\\out\\01\\",WriteMode.OVERWRITE,"\n",",")
    //res.writeAsCsv("E:\\flinkdata\\out\\03\\").setParallelism(1)
    res.map(wc=>(wc._1,wc._2)).writeAsCsv("E:\\flinkdata\\out\\05\\").setParallelism(1)
    //打入socket
    //res.writeToSocket("hadoop01",9999,SerializableSerializer[(String,String)])

    env.execute("sink")
  }
}

```



#### 自定义mysql的sink

```java
package scala.com.scala.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 结果：
 * 2> (2020-5-13_beijing,(1,2))
 * 2> (2020-5-13_beijing,(2,3))
 * 4> (2020-5-13_shanghai,(1,0))
 * 4> (2020-5-13_shanghai,(2,1))
 *
 * 放到MySQL中:
 * mysql表结构：
 CREATE TABLE `yq_2002` (
 `dt` varchar(255) NOT NULL,
 `province` varchar(255) NOT NULL,
 `adds` int(10) DEFAULT '0',
 `possibles` int(10) DEFAULT '0',
 PRIMARY KEY (`dt`,`province`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
*/
object Demo15_DataStream_MysqlSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    //2020-5-13 beijing 1 2
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val sumed: DataStream[(String, (Int, Int))] = dStream.map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .reduce((a, b) => (a._1, (a._2._1 + b._2._1, a._2._2 + b._2._2)))

    sumed.print("adds&possibles->")

    //打入到mysql中
    sumed.addSink(new MyMySQLSink)

    env.execute("mysql sink")
  }
}

/*
自定义function
1、实现SinkFunction
2、实现RichSinkFunction
 */
class MyMySQLSink extends RichSinkFunction[(String,(Int,Int))]{

  //获取mysql的连接的
  var ps: PreparedStatement = _
  var connection: Connection = _
  var resultSet: ResultSet = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"  //sql连接不用ssl将会报警告
    //val url = "jdbc:mysql://hadoop01:3306/test?useSSL=true"  //用ssl需要配置
    val username = "root"
    val password = "root"
    Class.forName(driver)
    try {
      connection = DriverManager.getConnection(url, username, password)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  //每条数据执行一次  1、连接池   2、考虑批次(考虑时效性)  3、如果重跑实时任务是否需要累加以前
  override def invoke(value: (String, (Int, Int)), context: SinkFunction.Context[_]): Unit = {
    val sql = "replace into yq_2002(dt,province,adds,possibles) values(?,?,?,?) "
    ps = connection.prepareStatement(sql)
    //为ps赋值
    ps.setString(1,value._1.split("_")(0))
    ps.setString(2,value._1.split("_")(1))
    ps.setInt(3,value._2._1)
    ps.setInt(4,value._2._2)
    //批次提交
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (resultSet != null) {
      resultSet.close()
    }
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }
}

```





# 作业day03

思考如下的题目：

```
（1）档案数据（ArchiveData）结构[UserID, DeviceID]
    案例数据：
    U0000001,D0000001
    U0000001,D0000002
    U0000002,D0000003
（2）设备数据（DeviceData）结构[DeviceID, DataTime, DeviceData]
    案例数据：
    D0000001,2020040201,220
    D0000001,2020040202,220
    D0000001,2020040203,230
    D0000001,2020040204,220
    D0000001,2020040205,230 -
    D0000001,2020040206,230
    D0000001,2020040207,270
    D0000001,2020040208,270
    D0000001,2020040209,270
    D0000001,2020040210,270 -
    D0000001,2020040211,220
    D0000001,2020040212,
    ,,
    D0000001,2020040214,290
    D0000001,2020040215,220
    D0000002,2020040201,220
    D0000002,2020040202,220
    
3、需求说明
1. 设备DeviceData > 220即异常，异常持续时间 >= 3小时，则输出异常信息（AbnormalInfomation）报警
（1）时间计算：2020040202 - 2020040201 = 1小时；
（2）过滤异常数据，丢失数据按照正常计算；

2. 输出异常信息结构    [UserID,DeviceID,AbnormalStartTime,AbnormalStopTime,AbnormalDurationTime]    AbnormalStartTime：异常开始时间    
AbnormalStopTime：异常结束时间    
AbnormalDurationTime：异常持续时间

3. 异常信息输出至HDFS保存


4、结果
结果
Tmp:
D0000001,2020040204,220>>AbnormalInfomation(U0000001,D0000001,,,0)
D0000001,2020040205,230>>AbnormalInfomation(U0000001,D0000001,2020040205,,0)
D0000001,2020040206,230>>AbnormalInfomation(U0000001,D0000001,2020040205,,1)
D0000001,2020040207,270>>AbnormalInfomation(U0000001,D0000001,2020040205,,2)
D0000001,2020040208,270>>AbnormalInfomation(U0000001,D0000001,2020040205,,3)>>out
D0000001,2020040209,270>>AbnormalInfomation(U0000001,D0000001,2020040205,,4)>>out
D0000001,2020040210,270>>AbnormalInfomation(U0000001,D0000001,2020040205,,5)>>out
D0000001,2020040211,220>>AbnormalInfomation(U0000001,D0000001,2020040205,2020040210,5)>>out
最终输出结果(保存至HDFS)：
+--------+-----------------+----------------+--------------------+--------+
|deviceID|abnormalStartTime|abnormalStopTime|abnormalDurationTime|  userID|
+--------+-----------------+----------------+--------------------+--------+
|D0000001|       2020040205|                |                   3|U0000001|
|D0000001|       2020040205|                |                   4|U0000001|
|D0000001|       2020040205|                |                   5|U0000001|
|D0000001|       2020040205|      2020040210|                   5|U0000001|
+--------+-----------------+----------------+--------------------+--------+
```



2、提前预习kafka的sink、es的sink、redis的sink等



#### 自定义mysql的outputformat

```java
package scala.com.scala.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.{InputFormat, OutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.com.scala.bean.Yq

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 结果：
 * 2> (2020-5-13_beijing,(1,2))
 * 2> (2020-5-13_beijing,(2,3))
 * 4> (2020-5-13_shanghai,(1,0))
 * 4> (2020-5-13_shanghai,(2,1))
 *
 * 放到MySQL中:
 * mysql表结构：
 CREATE TABLE `yq_2002` (
 `dt` varchar(255) NOT NULL,
 `province` varchar(255) NOT NULL,
 `adds` int(10) DEFAULT '0',
 `possibles` int(10) DEFAULT '0',
 PRIMARY KEY (`dt`,`province`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
*/
object Demo16_DataStream_MysqlOutputFormat {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    //2020-5-13 beijing 1 2
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val sumed: DataStream[Yq] = dStream.map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .reduce((a, b) => (a._1, (a._2._1 + b._2._1, a._2._2 + b._2._2)))
        .map(f=>{
          Yq(f._1.split("_")(0),f._1.split("_")(1),f._2._1,f._2._2)
        })

    sumed.print("adds&possibles->")

    //打入到mysql中
    sumed.writeUsingOutputFormat(new MySQLOutputFormat)

    env.execute("mysql outputformat")
  }
}

/*
自定义outputformat
 */
class MySQLOutputFormat extends OutputFormat[Yq]{
  override def configure(parameters: Configuration): Unit = {}

  //获取mysql的连接的
  var ps: PreparedStatement = _
  var connection: Connection = _
  var resultSet: ResultSet = _
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"  //sql连接不用ssl将会报警告
    //val url = "jdbc:mysql://hadoop01:3306/test?useSSL=true"  //用ssl需要配置
    val username = "root"
    val password = "root"
    Class.forName(driver)
    try {
      connection = DriverManager.getConnection(url, username, password)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  //将每接收到的数据写出
  override def writeRecord(record: Yq): Unit = {
    val sql = "replace into yq_2002(dt,province,adds,possibles) values(?,?,?,?) "
    ps = connection.prepareStatement(sql)
    //为ps赋值
    ps.setString(1,record.dt)
    ps.setString(2,record.province)
    ps.setInt(3,record.adds)
    ps.setInt(4,record.possibles)
    //批次提交
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (resultSet != null) {
      resultSet.close()
    }
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }
}
```



#### 自定义kafka的sink

```java
package scala.com.scala.stream

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import org.apache.flink.api.common.io.{InputFormat, OutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.kafka.clients.producer.ProducerRecord

import scala.com.scala.bean.Yq

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 结果：
 * 2> (2020-5-13_beijing,(1,2))
 * 2> (2020-5-13_beijing,(2,3))
 * 4> (2020-5-13_shanghai,(1,0))
 * 4> (2020-5-13_shanghai,(2,1))
 *
 * 放到kafka中
 *
*/
object Demo17_DataStream_KafkaSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    //2020-5-13 beijing 1 2
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val sumed: DataStream[Yq] = dStream.map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .reduce((a, b) => (a._1, (a._2._1 + b._2._1, a._2._2 + b._2._2)))
        .map(f=>{
          Yq(f._1.split("_")(0),f._1.split("_")(1),f._2._1,f._2._2)
        })

    sumed.print("adds&possibles->")

    //打入到kafka中
    //定义生产相关信息
    val to_topic = "flink_test"
    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    val kafkaproducer: FlinkKafkaProducer[Yq] = new FlinkKafkaProducer[Yq](to_topic, new MySerializationSchema(to_topic), prop, Semantic.EXACTLY_ONCE)
    sumed.addSink(kafkaproducer)

    env.execute("mysql outputformat")
  }
}

/*
自定义kafka序列化
 */
class MySerializationSchema(to_topic:String) extends KafkaSerializationSchema[Yq]{
  //序列化方法
  override def serialize(element: Yq, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //输出到kafka中的key为dt+province   value为整行值
    //返回
    new ProducerRecord[Array[Byte], Array[Byte]](to_topic,(element.dt+"_"+element.province).getBytes,
      ("{"+"dt"+":"+element.dt+","+"provice"+":"+element.province+","+"adds"+":"+element.adds+","+"possibles"+":"+element.possibles+"}").getBytes
    )
  }
}
```

#### flink中kafka的二阶段提交

注:flink中kafka的二阶段提交是参考网络文章。

##### 2PC简介

​       两阶段提交（two-phase commit, 2PC）是最基础的分布式一致性协议，应用广泛。在分布式系统中，为了让每个节点都能够感知到其他节点的事务执行状况，需要引入一个中心节点来统一处理所有节点的执行逻辑，这个中心节点叫做协调者（coordinator），被中心节点调度的其他业务节点叫做参与者（participant）。

```
   2PC。简单说，2PC是将分布式事务分成了两个阶段，两个阶段分别为提交请求（准备）和提交（执行）。协调者根据参与者的响应来决定是否需要真正地执行事务，所有响应者都响应ok或者yes，就提交，否则终止。具体流程如下。
   
   ZooKeeper 中的数据一致性，也是用到了2PC协议。大家可以自行了解。
```

##### 2PC提交协议

两阶段提交指的是一种协议，经常用来实现分布式事务，可以简单理解为预提交+实际提交，一般分为协调器Coordinator(以下简称C)和若干事务参与者Participant(以下简称P)两种角色。

![1594229904780](Flink%E7%AC%94%E8%AE%B0.assets/1594229904780.png)

1、C先将prepare请求写入本地日志，然后发送一个prepare的请求给P
2、P收到prepare请求后，开始执行事务，如果执行成功返回一个Yes或OK状态给C，否则返回No，并将状态存到本地日志。
3、C收到P返回的状态，如果每个P的状态都是Yes，则开始执行事务Commit操作，发Commit请求给每个P，P收到Commit请求后各自执行Commit事务操作。如果至少一个P的状态为No，则会执行Abort操作，发Abort请求给每个P，P收到Abort请求后各自执行Abort事务操作。
注：C或P把发送或接收到的消息先写到日志里，主要是为了故障后恢复用，类似WAL

![1594229936345](Flink%E7%AC%94%E8%AE%B0.assets/1594229936345.png)

![1594229957512](Flink%E7%AC%94%E8%AE%B0.assets/1594229957512.png)



##### 2PC 在flink中应用

​       在 Flink 中2PC的实现目前有FlinkKafkaProducer。其它的关系型数据库、redis等都可以自行实现两阶段提交。

```
   Flink作为流式处理引擎，自然也提供了对 Exactly-Once 语义的保证。端到端的 Exactly-Once 语义，是输入、处理逻辑、输出三部分协同作用的结果。Flink内部依托检查点机制（CheckPoint）和轻量级分布式快照算法（SnapShot）来保证 Exactly-Once。而要实现精确一次的输出逻辑，则需要施加以下两种限制之一：幂等性写入（idempotent write）、事务性写入（transactional write）。

   在 Spark Streaming 中，要实现事务性写入完全靠用户自己，框架本身并没有提供任何实现。但是在 Flink 中提供了基于 2PC 的 SinkFunction ，名为 TwoPhaseCommitSinkFunction，帮助我们做了一些基础的工作。
```

##### FlinkKafkaProducer的二阶段提交流程

​	Flink在1.4.0版本引入了TwoPhaseCommitSinkFunction接口，封装了两阶段提交逻辑，并在Kafka Sink connector中实现了TwoPhaseCommitSinkFunction，依赖Kafka版本为0.11+，TwoPhaseCommitSinkFunction具体实现如下：

![1594230035967](Flink%E7%AC%94%E8%AE%B0.assets/1594230035967.png)



Flink Kafka Sink执行两阶段提交的流程图大致如下：

![1594230105456](Flink%E7%AC%94%E8%AE%B0.assets/1594230105456.png)

假设一种场景，从Kafka Source拉取数据，经过一次窗口聚合，最后将数据发送到Kafka Sink，如下图：

![1594230148028](Flink%E7%AC%94%E8%AE%B0.assets/1594230148028.png)

1、JobManager向Source发送Barrier，开始进入pre-Commit阶段，当只有内部状态时，pre-commit阶段无需执行额外的操作，仅仅是写入一些已定义的状态变量即可。当chckpoint成功时Flink负责提交这些写入，否则就终止取消掉它们。
2、当Source收到Barrier后，将自身的状态进行保存，后端可以根据配置进行选择，这里的状态是指消费的每个分区对应的offset。然后将Barrier发送给下一个Operator。
![1594230220744](Flink%E7%AC%94%E8%AE%B0.assets/1594230220744.png)
3、当Window这个Operator收到Barrier之后，对自己的状态进行保存，这里的状态是指聚合的结果(sum或count的结果)，然后将Barrier发送给Sink。Sink收到后也对自己的状态进行保存，之后会进行一次预提交。

![1594230273530](Flink%E7%AC%94%E8%AE%B0.assets/1594230273530.png)



4、预提交成功后，JobManager通知每个Operator，这一轮检查点已经完成，这个时候，Kafka Sink会向Kafka进行真正的事务Commit。

![1594230311278](Flink%E7%AC%94%E8%AE%B0.assets/1594230311278.png)



以上是两阶段的完整流程，提交过程中如果失败有以下两种情况：

1、Pre-commit失败，将恢复到最近一次CheckPoint位置
2、一旦pre-commit完成，必须要确保commit也要成功

因此，所有opeartor必须对checkpoint最终结果达成共识：即所有operator都必须认定数据提交要么成功执行，要么被终止然后回滚。





#### 自定义Redis的sink

添加依赖：

```xml
<!--flink-redis的依赖-->
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-connector-redis_2.11</artifactId>
 <version>1.1.5</version>
</dependency>
```



```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.com.scala.bean.Yq

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 结果：
 * 2> (2020-5-13_beijing,(1,2))
 * 2> (2020-5-13_beijing,(2,3))
 * 4> (2020-5-13_shanghai,(1,0))
 * 4> (2020-5-13_shanghai,(2,1))
 *
 * 放到redis中
 *
*/
object Demo18_DataStream_RedisSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10)

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    //2020-5-13 beijing 1 2
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val sumed: DataStream[Yq] = dStream.map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .reduce((a, b) => (a._1, (a._2._1 + b._2._1, a._2._2 + b._2._2)))
        .map(f=>{
          Yq(f._1.split("_")(0),f._1.split("_")(1),f._2._1,f._2._2)
        })

    sumed.print("adds&possibles->")

    //打入到redis中
    //定义生产相关信息
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setDatabase(1)
      .setHost("192.168.216.111")
      .setPort(6379)
      .setPassword("root")
      .setMaxIdle(10)
      .setMinIdle(3)
      .setTimeout(10000)
      .build()

    //构造Redis的sink
    sumed.addSink(new RedisSink(config,new MyRedisSink))

    env.execute("redis sink")
  }
}

/*
自定义redissink实现
 */
class MyRedisSink extends RedisMapper[Yq] {
  //获取插入redis的命令 ;;如果存储类型为HASH和SORTED_SET需要指定额外的key
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET,null)
  }
  //存储到redis中的key值
  override def getKeyFromData(data: Yq): String = data.dt+"_"+data.province
  //存储到redis中key所对应的value值
  override def getValueFromData(data: Yq): String = data.adds+"_"+data.possibles
}
```



#### 自定义ES的sink

添加依赖：

```xml
<!--flink-es6-->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-elasticsearch6_2.11</artifactId>
    <version>${flink.version}</version>
</dependency>
<!--flink-es整合需要-->
<dependency>
    <groupId>org.apache.httpcomponents</groupId>
    <artifactId>httpclient</artifactId>
    <version>4.5.8</version>
</dependency>
```



代码：

```java
package scala.com.scala.stream

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.com.scala.bean.Yq

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 结果：
 * 2> (2020-5-13_beijing,(1,2))
 * 2> (2020-5-13_beijing,(2,3))
 * 4> (2020-5-13_shanghai,(1,0))
 * 4> (2020-5-13_shanghai,(2,1))
 *
 * 放到ES中
 *
*/
object Demo19_DataStream_ESSink {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10)

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    //2020-5-13 beijing 1 2
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val sumed: DataStream[Yq] = dStream.map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .reduce((a, b) => (a._1, (a._2._1 + b._2._1, a._2._2 + b._2._2)))
      .map(f=>{
        Yq(f._1.split("_")(0),f._1.split("_")(1),f._2._1,f._2._2)
      })

    sumed.print("adds&possibles->")
    try{
      //打入到ES中
      //es的连接信息
      val httpHosts = new util.ArrayList[HttpHost]
      httpHosts.add(new HttpHost("192.168.216.111", 9200, "http"))
      httpHosts.add(new HttpHost("192.168.216.112", 9200, "http"))
      httpHosts.add(new HttpHost("192.168.216.113", 9200, "http"))
      //获取es的sink
      val esSinkBuilder: ElasticsearchSink.Builder[Yq] = new ElasticsearchSink.Builder[Yq](httpHosts, new MyESSink)
      esSinkBuilder.setBulkFlushMaxActions(1)  //设置每一条数据刷新一次
      //esSinkBuilder.setBulkFlushInterval(1000)  //刷新数据的间隔

      //将essink添加到sink中即可
      sumed.addSink(esSinkBuilder.build())

    env.execute("es sink")
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}

/*
自定义es sink实现
 */
class MyESSink extends ElasticsearchSinkFunction[Yq] {
  //将数据写入到es中即可
  override def process(element: Yq,
                       ctx: RuntimeContext,
                       indexer: RequestIndexer): Unit = {
    try{
      print(s"统计信息：$element ")

      //i)将当前的user实例中的信息封装到Map中
      //dt: String, province: String, adds: int, possibles:Int
      val javamap = new util.HashMap[String,String]()
      //javamap.put("uid",element.uid.trim) //uid用于作文档id
      javamap.put("dt",element.dt.trim)
      javamap.put("age",element.province.toString)
      javamap.put("adds",element.adds+"")
      javamap.put("possibles",element.possibles+"")

      //ii)构建indexRequest实例
      val es_id:String = element.dt+"_"+element.province
      val indexRequest:IndexRequest = Requests.indexRequest()
        .index("yq_report")  //索引名称
        .`type`("info")  //索引的类型
        .id(es_id)  //类似主键
        .source(javamap)  //key-value值

      //iii)往es中添加数据信息
      indexer.add(indexRequest)
    } catch {
      case e1:Exception => e1.printStackTrace()
    }
  }
}
```





#### 自定义FileSink

```xml
<!--如果执行落地parquet格式才需要，否则不需要-->
<!--flink落地parquet格式-->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-parquet_2.11</artifactId>
    <version>1.9.1</version>
</dependency>
<!--flink写parquet的其它依赖-->
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.9.0</version>
</dependency>
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-hadoop</artifactId>
    <version>1.10.1</version>
</dependency>
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-avro</artifactId>
    <version>1.10.1</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-hadoop-compatibility_2.11</artifactId>
    <version>1.9.1</version>
</dependency>


```



```java
package scala.com.scala.stream

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{BasePathBucketAssigner, DateTimeBucketAssigner}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.com.scala.bean.{YQDetail, Yq}

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 放到ES中
 *
*/
object Demo20_DataStream_StreamingFileSink {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)
    //实时落地一定需要开启检测点；；如果不开启，不会报错，但是数据永远不会回滚
    //env.enableCheckpointing(5*1000)

    try{
    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._


     //2020-5-13 beijing 1 2
   val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
      /*
      val ds: DataStream[String] = dStream.map(line => {
        val fields: Array[String] = line.split(" ")
        val dt: String = fields(0).toString.trim
        val province: String = fields(1).toString.trim
        val add: Int = fields(2).toInt
        val possible: Int = fields(3).toInt
        //封装返回
        (dt + "_" + province, (add, possible))
      })
        .filter(f => {
          f._2._1 != 0 && f._2._2 != 0 //过滤
        })
        .map(x => {
          x._1.split("_")(0) + "," + x._1.split("_")(1) + "," + x._2._1 + "," + x._2._2
        })

      ds.print("adds&possibles->")


      //定义文件实时落地
      //数据落地路径
      val outputPath :Path = new Path("hdfs://hadoop01:9000/out/flink/yq")
      //落地策略
      val rollingPolicy :DefaultRollingPolicy[String,String] = DefaultRollingPolicy.create()
        .withRolloverInterval(10*1000)  //回滚落地间隔，，单位毫秒
        .withInactivityInterval(5*1000)   //无数据时间间隔
        .withMaxPartSize(128*1024*1024)  //最大文件数量限制,,默认字节
        .build()

            //数据分桶分配器  trave/orders/dt=/2020061909  --- 如果数据目录不想有时间可以使用BasicPathBucketAssigner
            val bucketAssigner :BucketAssigner[String,String] = new DateTimeBucketAssigner("yyyyMMddHH")

           //输出sink
            val hdfsSink: StreamingFileSink[String] = StreamingFileSink
              //forRowFormat --- 行编码格式
              //forBulkFormat(outputBasePath, ParquetAvroWriters.forGenericRecord(schema))
              .forRowFormat(outputPath, new SimpleStringEncoder[String]("UTF-8"))
              .withBucketAssigner(bucketAssigner)  //设置桶分配器
              .withRollingPolicy(rollingPolicy)  //设置回滚策略
              .withBucketCheckInterval(5*1000)  //桶检测间隔
              .build()
            //添加sink
            ds.addSink(hdfsSink)*/

      //使用parquert格式落地
      val outputPath :Path = new Path("hdfs://hadoop01:9000/out/flink/yq_parquet")
      val ds1: DataStream[YQDetail] = dStream.map(line => {
        val fields: Array[String] = line.split(" ")
        val dt: String = fields(0).toString.trim
        val province: String = fields(1).toString.trim
        val add: Int = fields(2).toInt
        val possible: Int = fields(3).toInt
        //封装返回
        (dt + "_" + province, (add, possible))
      })
        .filter(f => {
          f._2._1 != 0 && f._2._2 != 0 //过滤
        })
        .map(x => {
          YQDetail(x._1.split("_")(0) , x._1.split("_")(1) , x._2._1 , x._2._2)
        })

      ds1.print()

      //数据分桶分配器
      val bucketAssigner :BucketAssigner[YQDetail,String] = new DateTimeBucketAssigner("yyyyMMddHH")
      //val bucketAssigner: BasePathBucketAssigner[YQDetail] = new BasePathBucketAssigner()
      //4 数据实时采集落地
      //需要引入Flink-parquet的依赖
      val hdfsParquetSink: StreamingFileSink[YQDetail] = StreamingFileSink

        //块编码
        .forBulkFormat(
          outputPath,
          ParquetAvroWriters.forReflectRecord(classOf[YQDetail])) //paquet序列化
        .withBucketAssigner(bucketAssigner)
        .withBucketCheckInterval(5*1000) //分桶器检测间隔
        .build()

      //添加sink
      ds1.addSink(hdfsParquetSink)

    env.execute("Streaming File sink")
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
```



# 作业day04

1、大家自行测试一下实时落地parquet格式

2、大家提前预习操作链概念和状态存储概念





```properties
有这么一个情况， 我用sparkstreaming消费kafka中的数据，将消费到的数据存储到mysql的表中，并且是手动维护offsets。
1、如果我将存储offsets放在刚消费到的数据的位置，存储完offsets然后在进行将数据存储mysql的操作，，如果刚存第一条数据报错了。就会出现一个批次的数据全部丢失，但是offsets确是存上的。
2、将所有数据存储到mysql中，然后再进行存储offsets就会出现当程序突然停止后offsets还没有来及存，导致重复消费。


解决方法：
1、两阶段提交实现(Flink有自带的接口)，而spark streaming的实现。
```





## 3.2 批次api

官网：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/

### 3.2.1 批次的source

```
基于文件：
readTextFile(path)
readTextFileWithValue(path)
readCsvFile(path)
readFileOfPrimitives(path, delimiter) 
readSequenceFile(Key, Value, path)

基于集合：
fromCollection(Iterable) 
fromCollection(Iterator) 
fromElements(elements: _*) 
fromParallelCollection(SplittableIterator)
generateSequence(from, to)

通用:
readFile(inputFormat, path)
createInput(inputFormat)
```



```java
package scala.com.scala.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/*
 * 和流式差不多，批次的分为3类：
 * 基于文件：
 * 基于集合：
 * 通用：
 */
object Demo02_basicsource {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    //读本地
    import org.apache.flink.api.scala._
    val localLines = env.readTextFile("E:\\flinkdata\\words.txt")
    localLines.print()

    //读hdfs
    val hdfsLines = env.readTextFile("hdfs://hadoop01:9000/words")
    hdfsLines.print()

    //读取csv文件
    val csvInput = env.readCsvFile[(String, Int)]("E:\\flinkdata\\test.csv")
    csvInput.print()

    //基于集合
    val colls = env.fromElements("Foo", "bar", "foobar", "fubar")
    colls.print()

    //生成数字序列
    val numbers = env.generateSequence(1, 100)
    numbers.print()

    //可以实现自定义输入的InputFormat，，然后使用readFile() | createInput()
    //env.createInput()
    //env.createInput()

    env.execute("batch source")
  }
}

```



#### 读取压缩文件

Flink 目前支持输入文件的透明解压缩，如果文件标有适当的文件扩展名。这意味着不需要进一步配置输入格式，并且任何 FileInputFormat 支持压缩，包括自定义输入格式。压缩文件可能无法并行读取，从而影响作业可伸缩性。

总结：

1、如果文件有明确的压缩后缀，可以不用配置文件输入格式相关参数。

2、读取压缩文件，不支持设置并行读取，有可能会影响性能

3、flink几乎支持任何文件格式，以及自定义的文件格式。

当前支持的压缩方法：

| 压缩方法 | 文件扩展名 | 可并行 |
| -------- | ---------- | ------ |
| DEFLATE  | .deflate   | no     |
| GZip     | .gz，.gzip | no     |
| Bzip2    | .bz2       | no     |
| XZ       | .xz        | no     |



### 3.2.2 批次操作

官网：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html

常见算子：

```
map
flatmap
mappartition
filter
Distinct
group by
reduce
max
min
sum
join
union
Rebalance
hashPartition
RangParttion
SortParttion
first-n
```



```java
package scala.com.scala.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment

/*
常见算子
 */
object Demo03_basicoperator {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    import org.apache.flink.api.scala._
    val text: DataSet[String] = env.fromElements("i like flink flink flink is nice nice")

    //map:一个数据生成一个新的数据
    text.map(x=>(x,1)).print()

    //flatmap:一个数据生成多个新的数据
    text.flatMap(x=>x.split(" ")).print()

    //mappartition:函数处理包含一个分区所有数据的“迭代器”，
    // 可以生成任意数量的结果值。每个分区中的元素数量取决于并行度和先前的算子操作。
    text.mapPartition(x=>x map((_,1))).print()

    //filter:执行布尔函数，只保存函数返回 true 的数据。
    text.filter(x=>x.length>20).print()

    //Distinct:对数据集中的元素除重并返回新的数据集。
    text.distinct().print()
    text.flatMap(x=>x.split(" ")).map((_,1)).distinct(0).print("dis2")
    //自定义类型，，根据字段去重,,如下有报错
     case class Word(word : String)
     text.flatMap(x=>x.split(" ")).map(x=>Word(x)).distinct("word").print("dis3")

    //Reduce:作用于整个 DataSet，合并该数据集的元素。
    val data = env.fromElements(11,22,33)
    data.reduce(_ + _).print()


    //Aggregate:聚合，对一组数据求聚合值，聚合可以应用于完整数据集或分组数据集。
    // 聚合转换只能应用于元组（Tuple）数据集，并且仅支持字段位置键进行分组。
    //常见聚合函数：min、max、sum
    val data1: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "zs", 16d), (1, "ls", 20d), (2, "goudan", 23d), (3, "mazi", 30d)
    )
    data1.aggregate(Aggregations.SUM, 0).print()
    data1.aggregate(Aggregations.SUM, 0).aggregate(Aggregations.MIN, 2).print()
    // 输出 (7,c,16.0)
    // 简化语法
    data1.sum(0).min(2).print()

    //MinBy / MaxBy:取元组数据集中指定一个或多个字段的值最小（最大）的元组，
    // 可以应用于完整数据集或分组数据集。用于比较的字段必须可比较的。
    // 如果多个元组具有最小（最大）字段值，则返回这些元组的任意元组。
    // 比较元组的第一个字段
    data1.minBy(0).print()
    // 输出 (1,b,20.0)
    // 比较元组的第一、三个字段
    data1.minBy(0,2).print()

    //groupBy:用来将数据分组
    // 根据元组的第一和第二个字段分组
    data1.groupBy(0, 1)
    data1.groupBy(0).sortGroup(1, Order.ASCENDING) //分组并排序
    data1.groupBy(1).sum(0).print("==")
    data1.groupBy(1).sum(0).max(2).print()  //.groupBy(0).max(2)

    //join:将两个 DataSet 连接生成一个新的 DataSet。默认是inner join
    val d1: DataSet[(String, Int)] = env.fromElements(("a", 11), ("b", 2), ("a", 33))
    val d2: DataSet[(String, Int)] = env.fromElements(("a", 12), ("b", 22), ("c", 56))
    d1.join(d2).where(0).equalTo(0).print()

    //Union:构建两个数据集的并集。
    d1.union(d2).print()

    //Cross:构建两个输入数据集的笛卡尔积。 非keyvalue类型需要自定义
    d1.cross(d2).print()

    //Rebalance:均匀地重新负载数据集的并行分区以消除数据偏差。后面只可以接类似 map 的算子操作。
    text.rebalance().map(x=>(x,1)).print()

    //hash-Partition :根据给定的 key 对数据集做 hash 分区。可以是 position keys，expression keys 或者 key selector functions。
    text.rebalance().map(x=>(x,1)).partitionByHash(0).print()

    //类似分区还有Range-Partition、Sort Partition和自定义分区---自行查看

    //First-n: 返回数据集的前n个元素。可以应用于任意数据集。类似topN
    //i like flink flink is nice
    /*
    i 1
    like 1
    flink 2
    is 1
    nice 1
     */
    text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).groupBy(0).sortGroup(1, Order.DESCENDING).first(3).print()
    env.execute("batch transformation")
  }
}

```





### 3.2.3 批次的sink

```
writeAsText()
writeAsCsv(...)
print()
write() : 需要outputFoamt，可以自定义，继承FileOutputFormat
output() ：需要outputFoamt，可以自定义，继承OutputFormat
```



```java
package scala.com.scala.batch

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.com.scala.bean.WordCount

/*
output
 */
object Demo05_Outputformat {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._
    val text: DataSet[String] = env.fromElements("i like flink flink is nice", "aa", "aa")

    val ds: DataSet[WordCount] = text
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .map(x => WordCount(x._1, x._2))

    ds.output(new MySQLOutputFormat1)

    env.execute("output")
  }
}


class MySQLOutputFormat1 extends OutputFormat[WordCount]{
  override def configure(parameters: Configuration): Unit = {}

  //获取mysql的连接的
  var ps: PreparedStatement = _
  var connection: Connection = _
  var resultSet: ResultSet = _
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"  //sql连接不用ssl将会报警告
    //val url = "jdbc:mysql://hadoop01:3306/test?useSSL=true"  //用ssl需要配置
    val username = "root"
    val password = "root"
    Class.forName(driver)
    try {
      connection = DriverManager.getConnection(url, username, password)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  //将每接收到的数据写出
  override def writeRecord(record: WordCount): Unit = {
    val sql = "replace into wc(word,count) values(?,?) "
    ps = connection.prepareStatement(sql)
    //为ps赋值
    ps.setString(1,record.word)
    ps.setInt(2,record.count)
    //批次提交
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (resultSet != null) {
      resultSet.close()
    }
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }
}
```



```properties
flink的批次也是懒加载执行。
触发执行方法如下：
'execute()', 'count()', 'collect()', or 'print()'

异常：
No new data sinks have been defined since the last execution. The last execution refers to the latest call to 
.
```



## 3.3 task&Slot

task: 任务，一个job或者作业有多个task。类似spark的taskSet。

subTask：最小的执行单元，放入到一个slot运行，但是一个slot可以运行同一个job的不同的subtask。

slot: 用于隔离资源(内存)的对象，1个taskmanager有多个slot时将会均分内存，类似于yarn的container。它是taskmanager的并行的体现。

官网地址：https://ci.apache.org/projects/flink/flink-docs-release-1.10/concepts/runtime.html

![1589216656335](Flink%E7%AC%94%E8%AE%B0.assets/1589216656335-1594919153887.png)

Flink 中每一个 worker(TaskManager)都是一个 JVM 进程，它可能会在独立的线程上执行一个或多个 subtask。

TaskManager为了对资源进行隔离和增加允许的task数，引入了slot的概念，这个slot对资源的隔离仅仅是对内存进行隔离，策略是均分。

为了控制一个 worker 能接收多少个 task，worker 通 过 task slot 来进行控制（一个 worker 至少有一个 task slot）。



![1589216717391](Flink%E7%AC%94%E8%AE%B0.assets/1589216717391-1594919153887.png)

默认情况下，flink允许如果任务是不同的task的时候，允许任务共享slot，当然，前提是必须在同一个job内部。

Task Slot 是静态的概念，是指 TaskManager 具有的并发执行能力。



举例说明：

![1589216809193](Flink%E7%AC%94%E8%AE%B0.assets/1589216809193-1594919153887.png)

![1589216821471](Flink%E7%AC%94%E8%AE%B0.assets/1589216821471-1594919153896.png)

可以通过参数taskmanager.numberOfTaskSlots进行配置，而并行度parallelism是动态概念，即TaskManager运行程序时实际使用的并发能力，可以通过参数parallelism.default进行配置。

Flink 集群所需的taskslots数与job中最高的并行度一致，不需要再去计算一个程序总共会起多少个task了。

假设一共有3个TaskManager，每一个TaskManager中的分配3个TaskSlot，也就是每个TaskManager可以接收3个task，一共9个TaskSlot，如果我们设置parallelism.default=1，即运行程序默认的并行度为1，9个TaskSlot只用了1个，有8个空闲，因此，设置合适的并行度才能提高效率。



#### 设置并行度考虑点

1、源的并行度，并行度设置数量和kafka的分区数一致，并行度多于分区没有意义，如果并行设置为分区数，感觉消费速度不高，可以提升kafka的分区数。

如果数据源是mysql，可以考虑分表来实现数据存储多地方，然后再设置并行度为mysql分表的数量。



2、操作的并行度，建议除源和sink之外的并行度尽量设置为相同，这样将会尽可能的将操作划分到一个操作链中，从而提高并行度，也减少数据跨节点传输和减少传输过程中的序列化和反序列化。



3、sink的并行，sink为kafka或者mysql或者es或者redis，考虑第三方系统接收数据能力，kafka任然还是一样和分区数量一致即可。对于mysql、es持久化，如果时效不强，可以设置批次。



### 并行度

task的parallelism可以在Flink的不同级别上指定。四种级别是：(操作链)算子级别、执行环境（ExecutionEnvironment）级别、客户端（命令行）级别、配置文件（flink-conf.yaml）级别

\* 每个operator、data source或者data sink都可以通过调用`setParallelism()`方法来指定

\* 运行环境的默认并发数可以通过调用`setParallelism()`方法来指定。`env.setParallelism(3);运行环境的并发数可以被每个算子确切的并发数配置所覆盖。`

```
* 对于CLI客户端，并发参数可以通过-p来指定
* 影响所有运行环境的系统级别的默认并发度可以在./conf/flink-conf.yaml的parallelism.defaul项中指定。不建议
```

```
当然，你也可以设置最大的并行度
* 你可以通过调用setMaxParallelism()方法来设置最大并发度。
```



分为4中级别：

算子(操作链)级别：

执行环节：evn.set

命令行(客户端)： -p

配置文件：defatult.par.=1   //建议不同的作业不同的并行度

优先级：算子--->执行环境--->命令行客户端--->配置文件



```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/*
 *并行度
 *
*/
object Demo21_DataStream_Parallel {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source
    import org.apache.flink.api.scala._
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、基于source的transformation
    //具体转换
    val maped: DataStream[(String, Int)] = dStream.flatMap(_.split(" "))
      .map(word => (word, 1))

    //基于算子设置并行--针对该操作链
    maped.setParallelism(8)

    val sumed: DataStream[(String, Int)] = maped.keyBy(0)
      .timeWindow(Time.seconds(5), Time.seconds(5))
      .sum(1)
    //基于算子设置并行--针对该操作链
    sumed.setParallelism(4)
    sumed.setMaxParallelism(5)  //设置最大并行度

    //4、结果sink --基于算子设置并行
    sumed.print().setParallelism(2)

    env.execute("parallel")
  }
}
```



### 作链

官网：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/

- [DataStream Transformations](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/#datastream-transformations)
- [Physical partitioning](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/#physical-partitioning)
- [Task chaining and resource groups](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/#task-chaining-and-resource-groups)

![1602648056682](Flink%E7%AC%94%E8%AE%B0.assets/1602648056682.png)



operator chain是指将满足一定条件的operator 链在一起，放在同一个task里面执行，是Flink任务优化的一种方式，在同一个task里面的operator的数据传输变成函数调用关系，这种方式减少数据传输过程。常见的chain例如：source->flatmap->map。source->fliter,source->fliter->flatmap

#### JobGraph生成

Flink中划分了四种图：StreamGraph、JobGraph、ExecutionGraph、物理执行图，前两种StreamGraph、JobGraph是在客户端生成，ExecutionGraph在jobmamanger中生成，最后一种物理执行图是一种虚拟的图，不存在的数据结构，运行在每一个TaskExecutor中。我们在Flink Web UI中看到的就是JobGraph，如下：
![1589536728285](Flink%E7%AC%94%E8%AE%B0.assets/1589536728285.png)

JobGraph相对于StreamGraph，可以理解为优化过后的StreamGraph，将能够chain一起的operator chain在一起，上图将source与filter两个operator chain在一起了，这个步骤在生成JobGraph过程中完成。其具体实现在StreamingJobGraphGenerator中：

```
private JobGraph createJobGraph() {
        .....
        Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();
        setChaining(hashes, legacyHashes, chainedOperatorHashes);
        setPhysicalEdges();
        setSlotSharingAndCoLocation();
        configureCheckpointing();
     ....
    }
```

重点就在setChaining方法中，在里面调用createChain方法，构造JobVertix的同时完成operator chain的操作，createChain方法：



......

#### OperatorChain生成

当Execution在deploy的过程中，也就是Task在TaskExecutor启动过程中， 会生成一个OperatorChain对象，在该OperatorChain对象中包含了所有的能够chain在一起的operator(source&flatmap)，其内部会生成一个名为chainEntryPoint的WatermarkGaugeExposingOutput对象，一个将数据输出的对象，其输出有两种形式：

```
1.函数调用，将数据推送给chain在一起的下一个operator节点(filter中)               
2.输出到下一个没有被chain的operator(process1)
```



#### 划分opreatorChain源码

```java
StreamingJobGraphGenerator类--->
	createJobGraph()--->
	setChaining(hashes, legacyHashes, chainedOperatorHashes);--->
	createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);(注意是有递归)--->
	isChainable(StreamEdge edge, StreamGraph streamGraph)--->
	
能否划分成一个任务链的规则如下：
return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& edge.getShuffleMode() != ShuffleMode.BATCH
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
```



```java
package scala.com.scala.stream

import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamingJobGraphGenerator}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/*
 *操作链
 *
*/
object Demo22_DataStream_chain {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //map操作符(调用startNewChain()操作符)不能往前链接，但是可能往后链接操作符----即map和print连接到一块
    import org.apache.flink.api.scala._
    env.fromElements("i like flink").map((_,1)).startNewChain().print("--startNewChain")

    //map操作符不能链接它的前面或者后面操作符---及禁止连接map操作
    env.fromElements("i like flink","i like flink").map((_,1)).disableChaining().print("--startNewChain")

    //将map操作放入指定slot组中共享slot，通常操作在默认slot中 ---即map共享default的slot
    env.fromElements("i like flink","i like flink","i like flink").map((_,1)).slotSharingGroup("default")
    //5、触发执行  流应用一定要触发执行
    env.execute("operter chain---")
  }
}
```

![1602664135573](Flink%E7%AC%94%E8%AE%B0.assets/1602664135573.png)



```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
 *操作链
 *
*/
object Demo23_DataStream_notchain {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //map操作符(调用startNewChain()操作符)不能往前链接，但是可能往后链接操作符----即map和print连接到一块
    /*
源码可知：
ChainingStrategy链策略有3种，分别如下：
ALWAYS:只要有可能，就迫不及待地进行链接。未提高性能，允许最大链接和提高操作并行度是一个较好选择(实践)。
NEVER:调用该策略的操作符不被允许向前或者向后操作进行链接。
HEAD:不能被链接到该操作的前一个操作，但可能链接他后面的操作。
    */
    import org.apache.flink.api.scala._
    env.fromElements("i like flink").map((_,1)).print("--startNewChain")

    //map操作符不能链接它的前面或者后面操作符---及禁止连接map操作
    env.fromElements("i like flink","i like flink").map((_,1)).print("--startNewChain")

    //将map操作放入指定slot组中共享slot，通常操作在默认slot中 ---即map共享default的slot
    env.fromElements("i like flink","i like flink","i like flink").map((_,1))
    //5、触发执行  流应用一定要触发执行
    env.execute("operter chain---")
  }
}
```

![1602664119421](Flink%E7%AC%94%E8%AE%B0.assets/1602664119421.png)





## 3.4 物理分区

#### 分区定义和分类

Spark的RDD有分区的概念，Flink的DataStream同样也有，只不过没有RDD那么显式而已。Flink通过流分区器StreamPartitioner来控制DataStream中的元素往下游的流向，以StreamPartitioner抽象类为中心的类图如下所示:

![1594576875638](Flink%E7%AC%94%E8%AE%B0.assets/1594576875638.png)

flink流分区器父类：StreamPartitioner，，，主要方法：

StreamPartitioner(),流分区器，需要自己实现；

selectChannel(),在父类中，为当前record选择一个通道索引。

常见8类分区器，大概如下：

GlobalPartitioner：默认选择了索引为0的channel进行输出。

ForwardPartitioner：该分区器将记录转发给在本地运行的下游的(归属于subtask)的operattion。

ShufflePartitioner：混洗分区器，该分区器会在所有output channel中选择一个随机的进行输出。

BroadcastPartitioner:广播分区器，是将数据发往下游所有节点

RescalPartitioner：可扩展分区器，是通过轮询的方式发往下游

KeyGroupStreamPartitioner：KeyGroupStreamPartitioner：通过记录的数据值获得分区key，通过如下公式

```javascript
keyGroupId * parallelism / maxParallelism   计算出最终的channel。
```

CustomPartitionerWrapper：是自定义分区器，通过Partitioner实例的partition方法(自定义的)将记录输出到下游

这些分区器都会被应用于API中来进行物理分区...



#### 分区实现及查看

​	StreamPartitioner继承自ChannelSelector接口。这里的Channel概念与Netty不同，只是Flink对于数据写入目的地的简单抽象，我们可以直接认为它就是下游算子的并发实例（即物理分区）。所有StreamPartitioner的子类都要实现selectChannel()方法，用来选择分区号。
![1594577403573](Flink%E7%AC%94%E8%AE%B0.assets/1594577403573.png)



KeyGroupStreamPartitioner分区补充说明：

上游操作所发送的元素被分区到下游操作的哪些子集，依赖于上游和下游操作的并行度。例如，如果上游操作的并行度为2，而下游操作的并行度为4，那么一个上游操作会分发元素给两个下游操作，同时另一个上游操作会分发给另两个下游操作。相反的，如果下游操作的并行度为2，而上游操作的并行度为4，那么两个上游操作会分发数据给一个下游操作，同时另两个上游操作会分发数据给另一个下游操作。在上下游的并行度不是呈倍数关系的情况下，下游操作会有数量不同的来自上游操作的输入  KeyGroupStreamPartitioner：通过记录的数据值获得分区key，通过如下公式

```javascript
keyGroupId * parallelism / maxParallelism
```

计算出最终的channel。

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 *分区器
 *
*/
object Demo24_DataStream_partitioner {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //shuffle ： 混乱分区，是随机发送
    dstream.shuffle.print("shuffle->").setParallelism(4)
    //reblanace ： 使用轮询发送下游
    dstream.rebalance.print("rebalance->").setParallelism(4)
    //resscala
    dstream.rescale.print("rescala->").setParallelism(4)

    //5、触发执行  流应用一定要触发执行
    env.execute("operter partitioner---")
  }
}
```



## 3.5 state、checkpoint、State Backend、savepoint

#### 什么是状态

state：flink中的functions和operator是有状态的，它们在处理数据的过程中存储的数据就是state。

#### 为什么要管理状态?

流式作业的特点是7*24小时运行，数据不重复消费，不丢失，保证只计算一次，数据实时产出不延迟，但是当状态很大，内存容量限制，或者实例运行奔溃，或需要扩展并发度等情况下，如何保证状态正确的管理，在任务重新执行的时候能正确执行，状态管理就显得尤为重要。

状态类型与使用示例
Flink中有两种基本的State: Keyed State 和 Managed State

#### Keyed State & Operator State

| 分类          | Keyed State                                                  | Operator State                                               |
| ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 适用场景      | 只能应用在KeyedSteam上                                       | 可以用于所有的算子                                           |
| state处理方式 | 每个key 对应一个 state，一个operator处理多个key ,会访问相应的多个state | 一个operator对应一个state                                    |
| 并发改变      | 并发改变时，state随着key在实例间迁移                         | 并发改变时需要你选择分配方式，内置：1.均匀分配 2.所有state合并后再分发给每个实例 |
| 访问方式      | 通过RuntimeContext访问，需要operator是一个richFunction       | 需要你实现CheckPointedFunction或ListCheckPointed接口         |
| 支持数据结构  | ValuedState,ListState,Reducing State,Aggregating State,MapState,FoldingState(1.4弃用) | 只支持 listState                                             |



#### Managed State & Raw State	

| 分类         | Managed State                                                | Raw State                                                    |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 状态管理方式 | Flink Runtime 管理，自动存储，自动恢复，内存管理方式上优化明显 | 用户自己管理，需要用户自己序列化                             |
| 状态数据结构 | 已知的数据结构 value , list ,map                             | flink不知道你存的是什么结构，都转换为二进制字节数据[]        |
| 使用场景     | 大多数场景适用                                               | 需要满足特殊业务，自定义operator时使用，flink满足不了你的需求时候，使用复杂 |



#### Keyed State的使用

| 接口             | 状态数据类型 | 访问接口                                                |
| ---------------- | ------------ | ------------------------------------------------------- |
| ValueState       | 单个值       | update/get                                              |
| MapState         | Map          | put/putAll/remove/contains/entries/iterator/keys/values |
| ListState        | List         | add/addAll/update/get                                   |
| ReducingState    | 单个值       | add/addAll/update/get                                   |
| AggregatingState | 单个值       | add IN类型，get Out 类型                                |



```java
package scala.com.scala.stream

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/*
 *valueState
 *
*/
object Demo25_DataStream_ValueState {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromElements((1L, 5L), (1L, 6L), (1L, 10L), (1L, 100L),(2L, 3L), (2L, 8L))
      .keyBy(0)
      .flatMap(new MyFlatMapFunction())
      .print()

    //5、触发执行  流应用一定要触发执行
    env.execute("value state---")
  }
}

class MyFlatMapFunction extends RichFlatMapFunction[(Long,Long),(Long,Long)]{
  /**
   * 初始化用于存储状态的熟悉，key：是count，value：是sum值
   */
  var sum:ValueState[(Long,Long)] = _

  //初始化
  override def open(parameters: Configuration): Unit = {
    //初始化valuestate描述器
    val descriptor: ValueStateDescriptor[(Long, Long)] = new ValueStateDescriptor[(Long, Long)](
      "average",  //描述器名称
      TypeInformation.of(new TypeHint[(Long, Long)] {}),  //状态中的数据类型的类
      (0l, 0l))  //状态中的初始值
    //获取其状态
    sum = getRuntimeContext().getState(descriptor)
  }

  //关闭初始化资源
  override def close(): Unit = super.close()

  //核心逻辑实现
  override def flatMap(in: (Long, Long), out: Collector[(Long, Long)]): Unit = {
    //获取当前状态
    var currentSum: (Long, Long) = sum.value()

    //count + 1
    val count = currentSum._1  + 1
    //求sum
    val sumed = currentSum._2 + in._2

    // 更新状态
    sum.update((count,sumed))

    //输出状态
    //out.collect(in._1,sumed)

    //状态输出：如果count到达2, 保存 count和平均值，清除之前的状态
    if (sum.value()._1 >= 2) {
      //out.collect((in._1, sum.value()._2 / sum.value()._1))
      out.collect((in._1, sum.value()._2))
      //状态清空
      sum.clear()
    }
  }
}
```



# day05作业

1、使用valuestate方式，来进行计算adds和possibles

2、理解任务链的划分条件

3、预习state状态后端存储





#### checkpoint

checkpoint是对job进行周期性的进行state快照，以便于作业恢复和稳定。



##### checkpint的流程

1. CheckpointCoordinator周期性的向该流应用的所有source算子发送barrier。
2. 当某个source算子收到一个barrier时，便暂停数据处理过程，然后将自己的当前状 态制作成快照，并保存到指定的持久化存储中，最后向CheckpointCoordinator报告 自己快照制作情况，同时向自身所有下游算子广播该barrier，恢复数据处理
3. 下游算子收到barrier之后，会暂停自己的数据处理过程，然后将自身的相关状态制作成快照，并保存到指定的持久化存储中，最后向CheckpointCoordinator报告自身 快照情况，同时向自身所有下游算子广播该barrier，恢复数据处理。
4. 每个算子按照步骤3不断制作快照并向下游广播，直到最后barrier传递到sink算子，快照制作完成。
5. 当CheckpointCoordinator收到所有算子的报告之后，认为该周期的快照制作成功; 否则，如果在规定的时间内没有收到所有算子的报告，则认为本周期快照制作失败 ;



##### checkpoint设置

1、checkpoint机制是定时对状态进行分布式快照备份，发生故障是任务可以从最后一个成功的checkpoint处重新开始处理。前提是数据源支持重读。checkpoint可以支持exactly-once和at-least-once语义。

2、默认情况下，checkpoint不会被保留，取消程序时即会删除它们，但是可以通过配置保留定期检查点。开启Checkpoint功能，有两种方式。其一是在conf/flink_conf.yaml中做系统设置；其二是针对任务再代码里灵活配置。推荐第二种方式，针对当前任务设置。



```java
package scala.com.scala.stream

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
 *checkpoint
 *
*/
object Demo27_DataStream_checkpoint {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置statebackend//设置statebackend
    env.setStateBackend(new MemoryStateBackend())    //状态数据存储于内存
    //env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/checkpoint")) //状态存储于hdfs中
    //需要引入RocksDB的依赖
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:9000/flink_rocksDB",true))

    //获取checkpoint配置
    val conf: CheckpointConfig = env.getCheckpointConfig

    // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    /*
    .RETAIN_ON_CANCELLATION： 取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
    .DELETE_ON_CANCELLATION： 取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
    */
    conf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的周期, 每隔2000 ms进行启动一个检查点
    conf.setCheckpointInterval(2000)
    // 设置模式为exactly-once
    conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少1000 ms的间隔【checkpoint最小间隔】
    conf.setMinPauseBetweenCheckpoints(1000)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    conf.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    conf.setMaxConcurrentCheckpoints(1)

    //job
    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute("checkpoint")
  }
}


```





##### State Backend(状态的后端存储)

- 默认情况下，state会保存在taskmanager的内存中，checkpoint会存储在JobManager的内存中。
- State Backend分为3类：
  - MemoryStateBackend
  - FsStateBackend
  - **RocksDBStateBackend**
  - 配置代码为：env.setStateBackend(…)
- MemoryStateBackend
  - state数据保存在java堆内存中，执行checkpoint的时候，会把state的快照数据保存到jobmanager的内存中
  - 基于内存的Memory state backend在生产环境下不建议使用
- FsStateBackend
  - state数据保存在taskmanager的内存中，执行checkpoint的时候，会把state的快照数据保存到配置的文件系统中
  - 可以使用hdfs等分布式文件系统
- RocksDBStateBackend
  - RocksDB跟上面的都略有不同，它会在本地文件系统中维护状态，state会直接写入本地rocksdb中。同时它需要配置一个远端的filesystem uri（一般是HDFS），在做checkpoint的时候，会把本地的数据直接复制到filesystem中。fail over的时候从filesystem中恢复到本地
  - RocksDB克服了state受内存限制的缺点，同时又能够持久化到远端文件系统中，比较适合在生产中使用



##### 状态存储的方式区别

| 可选方式           | 存储方式                                     | 容量限制                                                     | 使用场景                                           |
| ------------------ | -------------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| MemoryStateBackend | state存在tm内存中，checkpoint存在jm中        | 单个state默认5M,总大小不超过jm内存，可以在构造函数中传参调大 | 本地测试，不推荐生产                               |
| FsStateBackend     | state存内存，checkpoint存在外部存储如hdfs s3 | state总量不要超过tm内存，外存磁盘大小                        | 常规任务，分钟,join,需要开启ha的任务，可在生产使用 |
| RocksDbBackend     | state存在tm上的kv数据库中                    | 单个key最大2G                                                | 超大作业，天级窗口                                 |

```java
//设置statebackend//设置statebackend
    env.setStateBackend(new MemoryStateBackend())    //状态数据存储于内存
    //env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/checkpoint")) //状态存储于hdfs中
    //需要引入RocksDB的依赖
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:9000/flink_rocksDB",true))
```



##### checkpoint和savepoint的区别

1. 概念：Checkpoint 是 自动容错机制 ，Savepoint 程序全局状态镜像 。

2. 目的： Checkpoint 是程序自动容错，快速恢复 。Savepoint是 程序修改后继续从状态恢复，程序升级等。

3. 用户交互:Checkpoint 是 Flink 系统行为 。Savepoint是用户触发，保存点的生成和恢复成本可能更高。

   Checkpoint的生命周期由Flink管理，即Flink创建，拥有和发布Checkpoint，无需用户交互。轻量级和快速恢复。

   Savepoints由用户创建，拥有和删除。它们的用例是planned (计划) 的，manual backup( 手动备份 ) 和 resume（恢复）。

4. 状态文件保留策略：Checkpoint默认程序删除，可以设置CheckpointConfig中的参数进行保留 。Savepoint会一直保存，除非用户删除 。

5. checkpoint使用 state backend 特定的数据格式，可能以增量方式存储。而savepoint则可以实现。

6. checkpoint不支持 Flink 的特定功能，比如扩缩容。而savepoint则可以实现。



##### savepoint的操作介绍

官网地址：https://ci.apache.org/projects/flink/flink-docs-release-1.11/ops/state/savepoints.html

```properties
#命令行设置savepoint目录
-s,--fromSavepoint <savepointPath>
#配置文件设置savepoint目录
# Default savepoint target directory
state.savepoints.dir: hdfs:///flink/savepoints


#命令行触发
./bin/flink savepoint <jobId> [savepointDirectory]
#yarn-cluster运行的触发命令
./bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId

#重新存储savepoint
./bin/flink run -s <savepointPath> ...

#取消作业，停止savepoint --- 取消作业时友好停止savepoint
./bin/flink stop [-p targetDirectory] [-d] <jobID>

#取消作业时，不友好停止savepoint
./bin/flink cancel -s [:targetDirectory] :jobId

#恢复或者处理savepoint，，jar就是自己的恢复处理逻辑
./bin/flink savepoint -d <savepointPath> -j <jarFile>
```



#### 失败重启策略

任务重启策略

当一个task失败，flink需要重启失败的task和相关连的task的恢复。



#### 重启策略分类

- [Fixed Delay Restart Strategy](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/task_failure_recovery.html#fixed-delay-restart-strategy)
- [Failure Rate Restart Strategy](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/task_failure_recovery.html#failure-rate-restart-strategy)
- [No Restart Strategy](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/task_failure_recovery.html#no-restart-strategy)
- [Fallback Restart Strategy](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/task_failure_recovery.html#fallback-restart-strategy)

Fixed Delay Restart Strategy定期间隔重启侧率：

```properties
restart-strategy.fixed-delay.attempts: 3   默认1，，代表允许重启次数
restart-strategy.fixed-delay.delay: 10 s   延迟多久启动
```



Failure Rate Restart Strategy基于失败率的重启

```properties
restart-strategy.failure-rate.max-failures-per-interval: 3  每间隔最大失败数量(次数)
restart-strategy.failure-rate.failure-rate-interval: 5 min  失败率的间隔
restart-strategy.failure-rate.delay: 10 s  失败率的延迟启动
```



No Restart Strategy 没有重启策略

```properties
restart-strategy: none
```



Fallback Restart Strategy： 应用于集群的重启策略。

flink的配置文件中的属性：

| Failover Strategy        | Value for jobmanager.execution.failover-strategy |
| :----------------------- | :----------------------------------------------- |
| Restart all              | full   ---重启job的所有任务                      |
| Restart pipelined region | region  ---重启job的局部的任务                   |



重启策略代码

```java
 //配置重启策略
      //没有重启策略
      //env.setRestartStrategy(RestartStrategies.noRestart())

      //定期间隔重启
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(10)))

      //基于失败率的重启
      //env.setRestartStrategy(RestartStrategies.failureRateRestart(1,Time.seconds(10),Time.seconds(10)))

      //fallback重启策略
      //env.setRestartStrategy(RestartStrategies.fallBackRestart())
```





#### 广播变量

基本概念：

将某个变量分发给每个节点上，保持每个节点都保存一份只读的缓存变量，而不是传送变量的副本给tasks。
如果不使用 broadcast，则在每个节点中的每个 task 中都需要拷贝一份数据集，比较浪费内存(也就是一个节点中可能会存在多份数据)。

使用场景：

在计算过程中需要多次并行处理某个变量的值的情况可以使用广播变量。

广播步骤：

1、设置广播变量 
　　在某个需要用到该广播变量的算子后调用**withBroadcastSet**(var1, var2)进行设置，var1为需要广播变量的变量名，var2是自定义变量名，为String类型。注意，被广播的变量只能为DataSet类型，不能为List、Int、String等类型。
2、获取广播变量 
创建该算子对应的富函数类，例如map函数的富函数类是RichMapFunction，该类有两个构造参数，第一个参数为算子输入数据类型，第二个参数为算子输出数据类型。首先创建一个Traversable[_]接口用于接收广播变量并初始化为空，接收类型与算子输入数据类型相对应；然后重写open函数，通过getRuntimeContext.getBroadcastVariable[_](var)获取到广播变量，var即为设置广播变量时的自定义变量名，类型为String，open函数在算子生命周期的初始化阶段便会调用；最后在map方法中对获取到的广播变量进行访问及其它操作。



注意事项：

广播出去的变量存在于每个节点的内存中，所以这个数据集不能太大，否则会出现 OOM
广播变量在初始化广播出去以后不支持修改，这样才能保证每个节点的数据都是一致的



流式的广播变量步骤：

1、广播变量描述器

2、变量广播出去

3、主数据流连接广播变量流，然后实现broadcastProcessFunction



```java
package scala.com.scala.stream

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/*
 * 广播变量
 *
*/
object Demo29_DataStream_Broadcast {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._


    val ds1: DataStream[(Int, String)] = env.fromElements((1, "男"), (2, "女"))
    //输入数据 uid uname genderflag address
    val ds2: DataStream[(Int, String, Int, String)] = env.socketTextStream("hadoop01", 6666).map(perLine => {
      val arr = perLine.split(" ")
      val id = arr(0).trim.toInt
      val name = arr(1).trim
      val genderFlg = arr(2).trim.toInt
      val address = arr(3).trim
      (id, name, genderFlg, address)
    })

    //广播变量描述器
    val desc = new MapStateDescriptor("genderInfo",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    //广播出去
    val bc: BroadcastStream[(Int, String)] = ds1.broadcast(desc)

    //取出广播变量  输入数据的泛型 ,广播变量的类型 , 输出结果的类型
    val res: DataStream[(Int, String, String, String)] = ds2
      .connect(bc)
      .process(new BroadcastProcessFunction[(Int, String, Int, String), (Int, String), (Int, String, String, String)]() {

        //处理元素
        override def processElement(value: (Int, String, Int, String),
                                    ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, String), (Int, String, String, String)]#ReadOnlyContext,
                                    out: Collector[(Int, String, String, String)]): Unit = {
          //获取输入数据中genderFlag
          val genderFlg = value._3
          //使用上下文获取广播变量
          var gender = ctx.getBroadcastState(desc).get(genderFlg)
          //如果genderFlag取出来是Null，，需要单独处理一下
          if (gender == null) {
            gender = "妖"
          }
          //输出
          out.collect((value._1, value._2, gender, value._4))
        }

        override def processBroadcastElement(
                                              value: (Int, String),
                                              ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, String), (Int, String, String, String)]#Context,
                                              out: Collector[(Int, String, String, String)]): Unit = {
          ctx.getBroadcastState(desc).put(value._1, value._2)
        }
      })

    //打印
    res.print("broadcast->")

    env.execute("broad cast")
  }
}
```



#### 分布式缓存

基本概念：

```
可以在并行函数中很方便的共享包含静态外部数据的文件
类似于广播变量，而不同的是分布式缓存可以广播一个文件
可以在并行函数中很方便的读取本地文件，并把它放在taskmanager节点中，防止task重复拉取；当程序执行，Flink自动将文件复制到所有taskmanager节点的本地文件系统，仅会执行一次。
```

代码：

```java
package scala.com.scala.stream

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/*
 * 分布式缓存
 *
*/
object Demo30_DataStream_DistributedCache {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
        
        

    //1、分布式缓存文件到hdfs中  1,男
    env.registerCachedFile("hdfs://hadoop01:9000/sex.txt","gender")

    //输入数据 uid uname genderflag address
    val ds2: DataStream[(Int, String, Int, String)] = env.socketTextStream("hadoop01", 6666).map(perLine => {
      val arr = perLine.split(" ")
      val id = arr(0).trim.toInt
      val name = arr(1).trim
      val genderFlg = arr(2).trim.toInt
      val address = arr(3).trim
      (id, name, genderFlg, address)
    })

    //使用自定义函数获取分布式缓存文件即可
    val res: DataStream[(Int, String, String, String)] = ds2.map(new RichMapFunction[(Int, String, Int, String), (Int, String, String, String)] {

      //可变的Map集合，用于存放从分布式缓存中读取的信息
      val map: mutable.Map[Int, String] = mutable.HashMap()

      //存储分布式缓存中的数据流
      var bs: BufferedSource = _

      override def open(parameters: Configuration): Unit = {
        val genderFile: File = getRuntimeContext.getDistributedCache.getFile("gender")
        //将读取到的信息封装到Map实例中存储起来
        bs = Source.fromFile(genderFile)
        val lst = bs.getLines().toList
        for (perLine <- lst) {
          val arr = perLine.split(",")
          val genderFlg = arr(0).trim.toInt
          val genderName = arr(1).trim
          map.put(genderFlg, genderName)
        }
      }

      //处理每一行数据
      override def map(value: (Int, String, Int, String)): (Int, String, String, String) = {
        //从输入的value中获取genderFlag
        val genderFlag: Int = value._3
        //判断map中是否有存在
        val gender: String = map.getOrElse(genderFlag, "妖")
        (value._1, value._2, gender, value._4)
      }


      override def close(): Unit = {
        //关闭文件
        if (bs != null)
          bs.close()
      }
    })

    //打印
    res.print("distributed cache->")

    env.execute("distributed cache")
  }
}
```





#### 累加器

概念：

实现将变量的值进行分布式累加
job中的task只能操作累加器，但是只能在任务执行结束之后才能获得累加器的最终结果

步骤：

```
// 1：创建累加器
private IntCounter numLines = new IntCounter(); 
// 2：注册累加器
getRuntimeContext().addAccumulator("num-lines", this.numLines);
// 3：使用累加器
this.numLines.add(1); 
// 4：获取累加器的结果 （← 这个操作需要等待任务执行完成）
myJobExecutionResult.getAccumulatorResult("num-lines")
```

常用累加器：

```
IntCounter
LongCounter
DoubleCounter
Histogram（直方图）
自定义(实现SimpleAccumulator接口)
```



```java
package scala.com.scala.stream

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
 * 累加器
 *
*/
object Demo31_DataStream_Accumulator {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //1、分布式缓存文件到hdfs中  1,男
    env.registerCachedFile("hdfs://hadoop01:9000/sex.txt","gender")

    //输入数据 uid uname genderflag address
    env.socketTextStream("hadoop01", 6666)
    .map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, add, possible)
    })
      .keyBy(0)
      .map(new RichMapFunction[(String,Int,Int),(String,Int,Int)] {
        //定义累加器
        val adds = new IntCounter(0)  //0是初始值
        val possibles = new IntCounter(0)

        override def open(parameters: Configuration): Unit = {
          getRuntimeContext.addAccumulator("adds", adds)
          getRuntimeContext.addAccumulator("possibles", possibles)
        }

        //处理每一行输入数据
        override def map(value: (String, Int, Int)): (String, Int, Int) = {
          adds.add(value._2)  //累加新增
          possibles.add(value._3)
          //构造返回
          (value._1,adds.getLocalValue,possibles.getLocalValue)
        }

        override def close(): Unit = super.close()
      })
        .print("acc")
    //触发执行
    val result: JobExecutionResult = env.execute("distributed cache")
    val adds: AnyRef = result.getAllAccumulatorResults.get("adds")
    val possibles: AnyRef = result.getAllAccumulatorResults.get("possibles")
    println(s"累计新增:${adds},累计怀疑:${possibles}")
  }
}
```



#### 异步处理

官网：https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/asyncio.html

代码：

```java
/**
 * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
 */
class AsyncDatabaseRequest extends AsyncFunction[String, (String, String)] {

    /** The database specific client that can issue concurrent requests with callbacks */
    lazy val client: DatabaseClient = new DatabaseClient(host, post, credentials)

    /** The context used for the future callbacks */
    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())


    override def asyncInvoke(str: String, resultFuture: ResultFuture[(String, String)]): Unit = {

        // issue the asynchronous request, receive a future for the result
        val resultFutureRequested: Future[String] = client.query(str)

        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        resultFutureRequested.onSuccess {
            case result: String => resultFuture.complete(Iterable((str, result)))
        }
    }
}

// create the original stream
val stream: DataStream[String] = ...

// apply the async I/O transformation
val resultStream: DataStream[(String, String)] =
    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)
```



# 第四章 Window&Time&WaterMark

窗口是将无界流划分有界流的工具。

## 4.1 window

### 4.1.1 窗口场景

```
1、做最近一段时间、每隔一段时间、最近多少条数据等的统计
2、实时计算结果对实时性要求不高，就可以使用窗口
3、数据延迟处理使用窗口较好
```

### 4.1.2 窗口概念

将实时数据流拆分成有界的数据流的方式。

Flink的窗口支持时间驱动和数据驱动。



### 4.1.3 窗口分类 

```
1、时间窗口（Time Window）：按照时间生成Window
滚动时间窗口（Tumbling Window）
滑动时间窗口（Sliding Window）
会话窗口（Session Window）

2、计数窗口（Count Window）：按照指定的数据条数生成一个Window，与时间无关
滚动计数窗口
滑动计数窗口


一般来说  滚动窗口(一个参数)无重叠,滑动窗口(俩参数)有重叠(或者丢失)


总共2x2+2=6个
```

##### 滚动窗口 - 窗口大小属性

特点：
时间对齐
窗口长度固定
没有重叠

原理：
滚动窗口分配器将每个元素分配到一个指定窗口大小的窗口中
滚动窗口有一个固定的大小，并且不会出现重叠

 适用场景：
适合做BI统计等（做每个时间段的聚合计算）



##### 滑动窗口 - 窗口大小属性|滑动间隔

滑动窗口是固定窗口的更广义的一种形式，滑动窗口由固定的窗口长度和滑动间隔组成

特点：
窗口长度固定
可以有重叠

原理：
滑动窗口分配器将元素分配到固定长度的窗口中，与滚动窗口类似
窗口的大小由窗口大小参数来配置
窗口滑动参数控制滑动窗口开始的频率
滑动窗口如果滑动参数小于窗口大小的话，窗口是可以重叠的，在这种情况下元素会被分配到多个窗口中

  例如：
10分钟的窗口和5分钟的滑动
每个窗口中5分钟的窗口里包含着上个10分钟产生的数据

  适用场景：
对最近一个时间段内的统计（求某接口最近5min的失败率来决定是否要报警）



##### session会话窗口 - gap间隔

由一系列事件组合一个指定时间长度的gap间隙组成，类似于web应用的session，也就是一段时间没有接收到新数据就会生成新的窗口

特点：
时间无对齐

只基于时间



场景：

计算某时间段(gap间隔)指标量 



数据示例：

```
user1 click 2020-7-14 2:39:00
user1 click 2020-7-14 2:39:03
user1 click 2020-7-14 2:39:08
user1 click 2020-7-14 2:39:15
user1 click 2020-7-14 2:39:19
user1 click 2020-7-14 2:39:21
user1 click 2020-7-14 2:39:26

user2 click 2020-7-14 2:39:22
user2 click 2020-7-14 2:39:23
user2 click 2020-7-14 2:39:27
user2 click 2020-7-14 2:39:29
user2 click 2020-7-14 2:39:33
user2 click 2020-7-14 2:39:36

gap：5s ---> 相邻2条数据的间隙大于等于gap，就生成窗口和触发执行
user1 click的窗口数据：


user2 click的窗口数据：
```



session窗口起始时间：

```
窗口=[第一条数据时间/上一个窗口结束后的第一条数据时间，某条数据和相邻数据的时间差大于或者等于gap间隔的数据时间+gap时间)      --- 静态gap
```



#### 窗口基础代码

```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * 常见窗口操作
 *
*/
object Demo32_DataStream_Window {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      //.timeWindow(Time.seconds(5))  //基于时间滚动窗口
      //.timeWindow(Time.seconds(10),Time.seconds(5)) //基于时间的滑动窗口
      //.countWindow(3) //基于数据条数的滚动窗口
      //.countWindow(3,2) //基于数据条数的滑动窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
      .sum(1)
      .print("window")

    env.execute("window")
  }
}
```



# day06作业

1、将基于EventTimeSessionWindow测试通

2、预习Window的三种聚合方式



#### 窗口增量聚合-.reduce(ReduceFunction())

```java
package scala.com.scala.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * reducefunction
 *
*/
object Demo33_DataStream_ReduceFunction {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .map(x => {
        val fields: Array[String] = x.split(" ")
        val date = fields(0).trim
        val province = fields(1)
        val add = fields(2).trim.toInt
        (date+"_"+province, add)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))  //每隔10秒
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          //比较找最大值
          if(value1._2>value2._2){
            value1
          } else{
            value2
          }

          //聚合
          //(value1._1,value1._2+value2._2)
        }
      })
      .print()

    env.execute("window")
  }
}
```





#### 窗口增量聚合-aggregate()

```java
package scala.com.scala.stream

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * aggregatefunction
 *
*/
object Demo34_DataStream_AggFunction {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .map(x => {
        val fields: Array[String] = x.split(" ")
        val date = fields(0).trim
        val province = fields(1)
        val add = fields(2).trim.toInt
        (date+"_"+province, add)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))  //每隔10秒
      // 输入数据类型  累加器的类型  返回类型
      .aggregate(new AggregateFunction[(String,Int),(String,Int,Int),(String,Int)] {
        //初始化
        override def createAccumulator(): (String, Int, Int) = ("",0,0)
        //单个增加
        override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
          val cnt = accumulator._2 + 1
          val adds = accumulator._3 + value._2
          (value._1, cnt, adds)
        }

        //多个分区中的进行合并
        override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = {
          val mergeCnt = a._2 + b._2
          val mergeAdds = a._3 + b._3
          (a._1, mergeCnt, mergeAdds)
        }

        //获取结果
        override def getResult(accumulator: (String, Int, Int)): (String, Int) = {
          (accumulator._1, accumulator._3 / accumulator._2)
        }
      })
      .print()

    env.execute("window")
  }
}
```



#### 窗口全量聚合-process()

```java
package scala.com.scala.stream

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * aggregatefunction
 *
*/
object Demo35_DataStream_ProcessFunction {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .map(x => {
        val fields: Array[String] = x.split(" ")
        val date = fields(0).trim
        val province = fields(1)
        val add = fields(2).trim.toInt
        (date+"_"+province, add)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))  //每隔10秒
      //输出数据类型    构造器中的泛型：输入类型   输出类型  key的类型  窗口类型
      .process[(String, Double)](new ProcessWindowFunction[(String,Int), (String, Double), Tuple, TimeWindow] {
        //处理窗口中的每一行元素  --- 输入数据是一个迭代器
        override def process(key: Tuple,
                             context: Context,
                             elements: Iterable[(String,Int)],
                             out: Collector[(String, Double)]): Unit = {
          //计算平均值
          var cnt = 0
          var totalAdds = 0.0
          //循环累加
          elements.foreach(perRecord => {
            cnt = cnt + 1
            totalAdds = totalAdds + perRecord._2
          })
          //输出
          out.collect((key.getField(0), totalAdds / cnt))
        }
      }).print()

    env.execute("window")
  }
}
```

reduce&aggregate&process区别：

共性：增对窗口里面的数据做处理。

```
reduce：聚合，通用性较强，可以使用多个function包括ReduceFunction, AggregateFunction, FoldFunction or ProcessWindowFunction)；；；增量计算
aggregate:是一种reduce的通用特例，，有累加器；；；增量计算
process：比reduce和aggregate灵活，除聚合外可以有更多操作；；全量窗口计算


```





#### 触发器

.trigger()——触发器：触发窗口执行操作。
定义window什么时候关闭，触发计算并输出结果
每一个Window分配器都会有一个默认的Trigger(触发器)
若默认的Trigger（触发器）不满足需要，可以自定义触发器

常见的Trigger：

```
EventTimeTrigger：基于EventTime Window的默认触发器
ProcessingTimeTrigger：基于ProcessingTime Window的默认触发器
CountTrigger：基于Count Window的默认触发器
PurgingTrigger：内部使用，用于清除窗口内容
NeverTrigger：永不触发的触发器
```



如果用户不设置Trigger类，Flink将会调用默认的trigger，例如对于时间属性为 EventTime 的窗口，Flink 默认会使用`EventTimeTrigger`类；时间属性为 ProcessingTime 的窗口，Flink 默认使用 `ProcessingTimeTrigger`类，如果用户指定了要使用的 trigger，默认的 trigger 将会被覆盖，不会起作用。



触发器代码：

`TriggerResult`中包含四个枚举值：

- `CONTINUE`表示对窗口不执行任何操作。
- `FIRE`表示对窗口中的数据按照窗口函数中的逻辑进行计算，并将结果输出。注意计算完成后，**窗口中的数据并不会被清除，将会被保留**。
- `PURGE`表示将窗口中的数据和窗口清除。All elements in the window are cleared and the window is discarded, without evaluating the window function or emitting any elements.
- `FIRE_AND_PURGE`表示先将数据进行计算，输出结果，然后将窗口中的数据和窗口进行清除。



```java
package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/*
 * aggregatefunction
 *
*/
object Demo36_DataStream_Trigger {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val res: DataStream[(String, Int)] = env.socketTextStream("hadoop01", 6666)
      .map(x => {
        val fields: Array[String] = x.split(" ")
        val date = fields(0).trim
        val province = fields(1)
        val add = fields(2).trim.toInt
        (date + "_" + province, add)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(20)) //每隔10秒
      //输出数据类型    构造器中的泛型：输入类型   输出类型  key的类型  窗口类型
      .trigger(new MyTrigger)
      .sum(1)

      res.print("trigger->")

    env.execute("window")
  }
}

//自定义触发器
class MyTrigger extends Trigger[(String,Int),TimeWindow]{

  var cnt = 0
  //每一行数据的处理逻辑
  override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //注册一个时间触发器;;;当当前时间超过window.maxTimestamp()就调用onProcessingTime()方法
    ctx.registerProcessingTimeTimer(window.maxTimestamp())  //window.maxTimestamp()：当前窗口最大值，窗口大小不一样，最大值不一样
    //ctx.registerEventTimeTimer()
    println("指定时间:"+window.maxTimestamp())
    //判断数据条数是否大于5条，大于就触发，然后cnt重置0
    if (cnt > 5) {
      println("通过计数触发窗口...")
      cnt = 0
      TriggerResult.FIRE  //触发窗口执行
    } else {
      cnt = cnt + 1
      TriggerResult.CONTINUE //什么都不做。继续
    }
  }


  //基于处理时间触发操作
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("通过处理时间触发窗口...")
    TriggerResult.FIRE   //基于处理时间触发
  }

  //基于事件时间触发的操作
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = ???

  //清空窗口数据操作
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteProcessingTimeTimer(window.maxTimestamp()) //删除基于处理时间的触发器
  }
}
```



#### 窗口api的顺序

![1602820141203](Flink%E7%AC%94%E8%AE%B0.assets/1602820141203.png)



## 4.2 时间 (Time)

flink中时间分类

```
Event Time：事件发生的时间   --- 最重要
Processing Time：处理消息的时间  --- 次之
Ingestion Time：进入到系统的时间  --- 次次之
```

### 4.2.1 哪种时间语义更重要

1、不同的时间语义有不同的应用场合
2、往往更关心事件时间（Event Time）
3、Event Time可以从日志数据的时间戳中提取
4、在Flink的流式处理中，绝大部分的业务都会使用eventTime，一般只在eventTime无法使用时，才会被迫使用ProcessingTime或者IngestionTime
5、要使用EventTime，需要引入EventTime的时间属性，引入的方式是：

```
//从调用时刻开始给env创建的每一个stream追加时间特征
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
```

### 4.2.2 乱序数据的影响

![1589899051918](Flink%E7%AC%94%E8%AE%B0.assets/1589899051918.png)

1、流处理从事件产生，到流经source，再到operator，中间是有一个过程和时间的

2、当Flink以Event Time模式处理数据流时，它会根据数据里的时间戳来处理基于时间的算子

3、大部分情况下，流到operator的数据都是按照事件产生的时间顺序来的，但是也不排除由于网络、分布式等原因，会导致乱序数据的产生

4、所谓乱序，就是指Flink接收到的事件的先后顺序不是严格按照事件的EventTime顺序排列的

5、乱序数据会让窗口计算不准确



## 4.3 水位线（Watermark）

怎样避免乱序数据带来的不正确？？？

```
1、一旦出现乱序，如果只根据eventTime决定window的运行，就不能明确数据是否全部到位，但又不能无限期的等下去，此时必须要有个机制来保证一个特定的时间后，必须触发window去进行计算了，这个特别的机制，就是Watermark

2、Watermark是一种衡量EventTime进展的机制

3、Watermark是用于处理乱序事件的，而正确的处理乱序事件，通常用Watermark机制结合window来实现

4、数据流中的Watermark用于表示timestamp小于Watermark的数据，都已经到达了，因此，window的执行也是由Watermark触发的

5、Watermark可以理解成一个延迟触发机制，可以设置Watermark的延时时长t，每次系统会校验已经到达的数据中最大的maxEventTime，然后认定eventTime小于maxEventTime-t的所有数据都已经到达，如果有窗口的停止时间等于maxEventTime–t，那么这个窗口被触发执行
```



### 4.3.1 Watermark的特点

1、Watermark是一条特殊的数据记录
2、Watermark必须递增，以确保任务的事件时间时钟在向前推进，而不是在后退
3、Watermark与数据的时间戳相关

![1589900185422](Flink%E7%AC%94%E8%AE%B0.assets/1589900185422.png)



### 4.3.2 有序流的Watermarks

window的触发执行要符合以下条件：

```
1、watermark 时间 >= window_end_time
2、在[window_start_time,window_end_time)区间中有数据存在，注意是左闭右开的区间，同时满足了以上 2 个条件，window 才会触发
```

window 的触发机制，是先按照自然时间将 window 划分，如果 window 大小是 3 秒，那么 1分钟内会把 window 划分为如下的形式【左闭右开】

![1589900531104](Flink%E7%AC%94%E8%AE%B0.assets/1589900531104.png)

有序watermark代码：

```java
package scala.com.scala.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * 水印
 *
*/
object Demo37_DataStream_WaterMark {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度为1
    env.setParallelism(1)

    import org.apache.flink.api.scala._
    
    //输入数据两列  name 时间戳
    env.socketTextStream("hadoop01", 6666)
      .filter(_.nonEmpty)  //过滤掉空数据
      .map(line => {
        val arr = line.split(" ")
        var uname: String = arr(0).trim
        var timestamp: Long = arr(1).trim.toLong
        (uname: String, timestamp: Long)
      })
      //分配时间戳和水位
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
        var maxTimestamp = 0L  //迄今为止窗口中最大的时间戳
        val lateness = 10000L  //最大允许乱序数据延迟时间
        //为咯查看方便，加入格式
        val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        //获取当前水印----也是在分配水印
        override def getCurrentWatermark: Watermark = new Watermark(maxTimestamp - lateness)

        //分配时间戳---从输入数据中提取事件时间
        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          val now_time = element._2 //当前数据时间戳
          //获取最大的时间戳
          maxTimestamp = Math.max(now_time, maxTimestamp)
          //获取当前水印的时间戳
          val nowWaterMark:Long = getCurrentWatermark.getTimestamp
          println(s"Event时间→$now_time | ${fmt.format(now_time)}，" +
            s"本窗口迄今为止最大的时间→$maxTimestamp | ${fmt.format(maxTimestamp)}，" +
            s"当前WaterMark→$nowWaterMark | ${fmt.format(nowWaterMark)}")
          now_time  //返回当前数据的时间
        }
      }
      )
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .apply(new RichWindowFunction[(String, Long), String, Tuple, TimeWindow] {
        private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        override def apply(key: Tuple,
                           window: TimeWindow,
                           input: Iterable[(String, Long)],
                           out: Collector[String]): Unit = {
          //获取整个窗口输入
          val lst = input.iterator.toList.sortBy(_._2)
          val startTime = fmt.format(window.getStart)  //该窗口的开始时间
          val endTime = fmt.format(window.getEnd)     //取窗口的结束时间

          val result = s"key→${key.getField(0)}，" +
            s"开始EventTime→${fmt.format(lst.head._2)}，" +
            s"结束EventTime→${fmt.format(lst.last._2)}，" +
            s"窗口开始时间→${startTime}，" +
            s"窗口结束时间→${endTime}"
          out.collect(result)
        }
      })
      .print()

    env.execute("window watermark")
  }
}
```



问题1：

```
1、watermark能不能不减10？？？可以的，但是一定要给最大值

2、输入的数据记录中的eventtime能不能不是时间戳？？？能
```





### 4.3.3 延迟数据处理

对于“迟到(late element)”太多的数据，Flink的处理方案是：

```
方案1：丢弃(默认)
方案2：allowedLateness 指定允许数据延迟的时间
方案3：sideOutputLateData 收集迟到的数据
```

代码：

```java
package scala.com.scala.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * 水印+延迟
 *
*/
object Demo37_DataStream_WaterMark {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度为1
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    //输入数据两列  name 时间戳
    env.socketTextStream("hadoop01", 6666)
      .filter(_.nonEmpty)  //过滤掉空数据
      .map(line => {
        val arr = line.split(" ")
        var uname: String = arr(0).trim
        var timestamp: Long = arr(1).trim.toLong
        (uname: String, timestamp: Long)
      })
      //分配时间戳和水位
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
        var maxTimestamp = 0L  //迄今为止窗口中最大的时间戳
        val lateness = 10000L  //最大允许乱序数据延迟时间
        //为咯查看方便，加入格式
        val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        //获取当前水印----也是在分配水印
        override def getCurrentWatermark: Watermark = new Watermark(maxTimestamp - lateness)

        //分配时间戳---从输入数据中提取事件时间
        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          val now_time = element._2 //当前数据时间戳
          //获取最大的时间戳
          maxTimestamp = Math.max(now_time, maxTimestamp)
          //获取当前水印的时间戳
          val nowWaterMark:Long = getCurrentWatermark.getTimestamp
          println(s"Event时间→$now_time | ${fmt.format(now_time)}，" +
            s"本窗口迄今为止最大的时间→$maxTimestamp | ${fmt.format(maxTimestamp)}，" +
            s"当前WaterMark→$nowWaterMark | ${fmt.format(nowWaterMark)}")
          now_time  //返回当前数据的时间
        }
      }
      )
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .allowedLateness(Time.seconds(2))  //允许延迟2秒
      .apply(new RichWindowFunction[(String, Long), String, Tuple, TimeWindow] {
        private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        override def apply(key: Tuple,
                           window: TimeWindow,
                           input: Iterable[(String, Long)],
                           out: Collector[String]): Unit = {
          //获取整个窗口输入
          val lst = input.iterator.toList.sortBy(_._2)
          val startTime = fmt.format(window.getStart)  //该窗口的开始时间
          val endTime = fmt.format(window.getEnd)     //取窗口的结束时间

          val result = s"key→${key.getField(0)}，" +
            s"开始EventTime→${fmt.format(lst.head._2)}，" +
            s"结束EventTime→${fmt.format(lst.last._2)}，" +
            s"窗口开始时间→${startTime}，" +
            s"窗口结束时间→${endTime}"
          out.collect(result)
        }
      })
      .print()

    env.execute("window watermark")
  }
}
```

```
测试数据：
zs 1602830553808
zs 1602830555000
zs 1602830556000
zs 1602830557000
zs 1602830558000
zs 1602830559000
zs 1602830566000
zs 1602830555666
```



### 4.3.4 延迟数据侧输流



```java
package scala.com.scala.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * 水印+延迟+延迟侧输
 *
*/
object Demo37_DataStream_WaterMark {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度为1
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    //定义侧输出流  该实例就是给DataStream中延迟的数据添加一个标签
    val outputTag = new OutputTag[(String, Long)]("side_data")

    //输入数据两列  name 时间戳
    val res: DataStream[String] = env.socketTextStream("hadoop01", 6666)
      .filter(_.nonEmpty) //过滤掉空数据
      .map(line => {
        val arr = line.split(" ")
        var uname: String = arr(0).trim
        var timestamp: Long = arr(1).trim.toLong
        (uname: String, timestamp: Long)
      })
      //分配时间戳和水位
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
        var maxTimestamp = 0L //迄今为止窗口中最大的时间戳
        val lateness = 10000L //最大允许乱序数据延迟时间
        //为咯查看方便，加入格式
        val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        //获取当前水印----也是在分配水印
        override def getCurrentWatermark: Watermark = new Watermark(maxTimestamp - lateness)

        //分配时间戳---从输入数据中提取事件时间
        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          val now_time = element._2 //当前数据时间戳
          //获取最大的时间戳
          maxTimestamp = Math.max(now_time, maxTimestamp)
          //获取当前水印的时间戳
          val nowWaterMark: Long = getCurrentWatermark.getTimestamp
          println(s"Event时间→$now_time | ${fmt.format(now_time)}，" +
            s"本窗口迄今为止最大的时间→$maxTimestamp | ${fmt.format(maxTimestamp)}，" +
            s"当前WaterMark→$nowWaterMark | ${fmt.format(nowWaterMark)}")
          now_time //返回当前数据的时间
        }
      }
      )
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      //.allowedLateness(Time.seconds(2))  //允许延迟2秒
      .sideOutputLateData(outputTag)
      .apply(new RichWindowFunction[(String, Long), String, Tuple, TimeWindow] {
        private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        override def apply(key: Tuple,
                           window: TimeWindow,
                           input: Iterable[(String, Long)],
                           out: Collector[String]): Unit = {
          //获取整个窗口输入
          val lst = input.iterator.toList.sortBy(_._2)
          val startTime = fmt.format(window.getStart) //该窗口的开始时间
          val endTime = fmt.format(window.getEnd) //取窗口的结束时间

          val result = s"key→${key.getField(0)}，" +
            s"开始EventTime→${fmt.format(lst.head._2)}，" +
            s"结束EventTime→${fmt.format(lst.last._2)}，" +
            s"窗口开始时间→${startTime}，" +
            s"窗口结束时间→${endTime}"
          out.collect(result)
        }
      })

    res.print()
    res.getSideOutput(outputTag).print("侧输流->")

    env.execute("window watermark")
  }
}
```



### 4.3.5 多并行度下的水印

获取所有并行度中水印时间最小的一个来作为最终水印。

```java
package scala.com.scala.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * 多并行度下的水印
 *
*/
object Demo38_DataStream_MultiWaterMark {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    //输入数据两列  name 时间戳
    val res: DataStream[String] = env.socketTextStream("hadoop01", 6666)
      .filter(_.nonEmpty) //过滤掉空数据
      .map(line => {
        val arr = line.split(" ")
        var uname: String = arr(0).trim
        var timestamp: Long = arr(1).trim.toLong
        (uname: String, timestamp: Long)
      })
      //分配时间戳和水位
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
        var maxTimestamp = 0L //迄今为止窗口中最大的时间戳
        val lateness = 10000L //最大允许乱序数据延迟时间
        //为咯查看方便，加入格式
        val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        //获取当前水印----也是在分配水印
        override def getCurrentWatermark: Watermark = new Watermark(maxTimestamp - lateness)

        //分配时间戳---从输入数据中提取事件时间
        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          val now_time = element._2 //当前数据时间戳
          //获取最大的时间戳
          maxTimestamp = Math.max(now_time, maxTimestamp)
          //获取当前水印的时间戳
          val nowWaterMark: Long = getCurrentWatermark.getTimestamp
          val threadId = Thread.currentThread().getId
          println(s"线程ID->${threadId} , Event时间→$now_time | ${fmt.format(now_time)}，" +
            s"本窗口迄今为止最大的时间→$maxTimestamp | ${fmt.format(maxTimestamp)}，" +
            s"当前WaterMark→$nowWaterMark | ${fmt.format(nowWaterMark)}")
          now_time //返回当前数据的时间
        }
      }
      )
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .apply(new RichWindowFunction[(String, Long), String, Tuple, TimeWindow] {
        private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        override def apply(key: Tuple,
                           window: TimeWindow,
                           input: Iterable[(String, Long)],
                           out: Collector[String]): Unit = {
          //获取整个窗口输入
          val lst = input.iterator.toList.sortBy(_._2)
          val startTime = fmt.format(window.getStart) //该窗口的开始时间
          val endTime = fmt.format(window.getEnd) //取窗口的结束时间

          val result = s"key→${key.getField(0)}，" +
            s"开始EventTime→${fmt.format(lst.head._2)}，" +
            s"结束EventTime→${fmt.format(lst.last._2)}，" +
            s"窗口开始时间→${startTime}，" +
            s"窗口结束时间→${endTime}"
          out.collect(result)
        }
      })

    res.print()

    env.execute("window watermark")
  }
}
```



总结：

With Periodic Watermarks【较常用】
周期性的触发watermark的生成和发送，默认是100ms
每隔N秒自动向流里注入一个watermark时间间隔由ExecutionConfig.setAutoWatermarkInterval 决定，每次调用getCurrentWatermark 方法，如果得到的watermark不为空并且比之前的大就注入流中 。



Flink应该如何设置最大乱序时间？？
这个要结合自己的业务以及数据情况去设置
如果maxOutOfOrderness设置的太小，而自身数据发送时由于网络等原因导致乱序或者late太多，那么最终的结果就是会有很多单条的数据在window中被触发
对于严重乱序的数据，需要严格统计数据最大延迟时间，才能保证计算的数据准确。







# 第五章 Table和SQL





## 5.1 table的api

sql的api

```java
package scala.com.scala.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * scala版本
 * 1、获取表执行环境
 * 2、输入数据源
 * 3、转换成table
 * 4、对table进行操作
 * 5、触发执行
 */
object Demo01_Table01 {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //table环境
    val tenv = StreamTableEnvironment.create(env)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[(String, String, Int, Int)] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        val date: String = arr(0).trim
        val province: String = arr(1).trim
        val add: Int = arr(2).trim.toInt
        val possible: Int = arr(2).trim.toInt
        (date, province, add, possible)
      })

    //基于DataStream生成一张Table
    var table:Table = tenv.fromDataStream(ds)

    //查询table中特定的字段  ,,,如果是样例类，可以使用字段:table.select("date,province,add")
    //table = table.select("_1,_2,_3")
    table = table
      .select("_1,_2,_3")
      .where("_3>5")

    //将table中的数据拿到新的DataStream中，然后输出
    tenv.toAppendStream[Row](table)
      .print("表中输出输出后的结果是 →")

    //启动
    env.execute("table api")
  }
}
```





# day06作业

1、基于我们的第一个table的api求一下add的sum

2、提前查看flink中的cep概念





表达式类取值

```java
package scala.com.scala.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * 别名
 */
object Demo02_alias {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //table环境
    val tenv = StreamTableEnvironment.create(env)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[(String, String, Int, Int)] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        val date: String = arr(0).trim
        val province: String = arr(1).trim
        val add: Int = arr(2).trim.toInt
        val possible: Int = arr(2).trim.toInt
        (date, province, add, possible)
      })

    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    var table: Table = tenv.fromDataStream(ds, 'd,'p,'a)

    //查询table中特定的字段  ,,,如果是样例类，可以使用字段:table.select("date,province,add")
    //table = table.select("_1,_2,_3")
    table = table
      .select('d,'p,'a)
      //  .select("d,p,a")  //等价于上边
      .where("a>5")

    //将table中的数据拿到新的DataStream中，然后输出
    tenv.toAppendStream[Row](table)
      .print("表中输出输出后的结果是 →")

    //启动
    env.execute("table api")
  }
}

```



窗口和时间水印分配

```java
package scala.com.scala.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.com.scala.bean.Yq

/**
 * 窗口和水印
 */
object Demo03_WindowAndWaterMark {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //table环境
    val tenv = StreamTableEnvironment.create(env)


    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[Yq] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        val date: String = arr(0).trim
        val province: String = arr(1).trim
        val add: Int = arr(2).trim.toInt
        val possible: Int = arr(2).trim.toInt
        Yq(date, province, add, possible)
      }) //下边是分配水印和时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Yq](Time.seconds(2)) {
        override def extractTimestamp(element: Yq): Long = element.dt.toLong * 1000
      })

    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    var table: Table = tenv.fromDataStream(ds, 'd,'p,'a)

    //查询table中特定的字段  ,,,如果是样例类，可以使用字段:table.select("date,province,add")
    //table = table.select("_1,_2,_3")
    table = tenv.fromDataStream(ds, 'dt, 'ts.rowtime)
      .window(Tumble over 5.second on 'ts as 'tt1)
      .groupBy('dt, 'tt1)
      .select('dt, 'dt.count)

    //将table中的数据拿到新的DataStream中，然后输出
    tenv.toAppendStream[Row](table)
      .print("表中输出输出后的结果是 →")

    //启动
    env.execute("table api")
  }
}

```







批次的api：

```java
package scala.com.scala.sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
 * 批次
 */
object Demo04_batch {
  def main(args: Array[String]): Unit = {
    //步骤：
    //批环境
    val benv = ExecutionEnvironment.getExecutionEnvironment

    //table环境
    val tenv = BatchTableEnvironment.create(benv)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataSet[(String,Int)] = benv.readTextFile("E:\\flinkdata\\test.csv")
      .map(line=>{
        val fileds: Array[String] = line.split(",")
        (fileds(0).trim,fileds(1).toInt)
      })

    //基于DataSet生成一张Table
    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataSet(ds, 'name, 'age)

    //查询操作
    table
        .groupBy('name)
        .select('name,'age.sum as 'sum_age)
      .toDataSet[Row]
      .print()
  }
}

```



## 5.2 sql的api

```java
package scala.com.scala.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.com.scala.bean.Yq

/**
 * sql
 */
object Demo05_sql {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //table环境
    val tenv = StreamTableEnvironment.create(env)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[Yq] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        val date: String = arr(0).trim
        val province: String = arr(1).trim
        val add: Int = arr(2).trim.toInt
        val possible: Int = arr(3).trim.toInt
        Yq(date, province, add, possible)
      })

    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataStream(ds)

    //duisql进行查询
   /* tenv.sqlQuery(
      s"""
        |select
        |*
        |from $table
        |where adds > 5
        |""".stripMargin)
        .toAppendStream[Row]
        .print("sql->")*/

    tenv.sqlQuery(
      s"""
         |select
         |dt,
         |sum(adds) adds,
         |sum(possibles) possibles
         |from $table
         |group by dt
         |""".stripMargin)
      //.toAppendStream[Row]
      .toRetractStream[Row]
      .print("sql->")

    //启动
    env.execute("sql api")
  }
}

```





sql实时统计词频

```java
package scala.com.scala.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.com.scala.bean.Yq

/**
 * sql统计
 */
object Demo07_wordcount {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //table环境
    val tenv = StreamTableEnvironment.create(env)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[(String, Int)] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))


    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataStream(ds, 'word,'cnt)

    //使用sql操作
    tenv.sqlQuery(
      s"""
        |select
        |word,
        |sum(cnt)
        |from $table
        |group by word
        |""".stripMargin)
        .toRetractStream[Row]
        .print("sql")

    //启动
    env.execute("sql wordcount")
  }
}

```



## 5.3 Blink的sql

kafka2mysql ---> sql：

```java
package scala.com.scala.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/*
blink的sql
kafka2mysql
 */
object Demo08_kafka2sql_blink {
  def main(args: Array[String]): Unit = {
    //设置执行环境为blink解析器
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    //获取Blink的table执行环境
    val tenv: TableEnvironment = TableEnvironment.create(settings)

    //Aflink执行环境 --- aflink的执行环境不能使用使用如下的操作形式
    /*val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv: TableEnvironment = StreamTableEnvironment.create(env)*/

    //执行create table语句
    tenv.sqlUpdate(
      s"""
         |CREATE TABLE user_log (
         |    user_id VARCHAR,
         |    item_id VARCHAR,
         |    category_id VARCHAR,
         |    action VARCHAR,
         |    ts TIMESTAMP
         |) WITH (
         |    'connector.type' = 'kafka', -- 使用 kafka connector
         |    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本
         |    'connector.topic' = 'test',  -- kafka topic
         |    'connector.properties.0.key' = 'zookeeper.connect',  -- 连接信息
         |    'connector.properties.0.value' = 'hadoop01:2181,hadoop02:2181,hadoop03:2181',
         |    'connector.properties.1.key' = 'bootstrap.servers',
         |    'connector.properties.1.value' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
         |    'update-mode' = 'append',  --更新模式，，追加形式
         |    'connector.startup-mode' = 'latest-offset',  -- earliest-offset:从起始 offset 开始读取
         |    'format.type' = 'json',  -- 数据源格式为 json
         |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
         |)
         |""".stripMargin)

    //使用sql执行---transformation
    //执行查询语句 replace 和 ON DUPLICATE KEY UPDATE都不支持,,因为这两是mysql的语法，非标准通用语法
    tenv.sqlUpdate(
      s"""
         |INSERT into pvuv_sink
         |SELECT
         |  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,
         |  COUNT(*) AS pv,
         |  COUNT(DISTINCT user_id) AS uv
         |FROM user_log
         |GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')
         |""".stripMargin)

    //执行创建表语句
    tenv.sqlUpdate(
      s"""
         |CREATE TABLE pvuv_sink (
         |    dt VARCHAR,
         |    pv BIGINT,
         |    uv BIGINT
         |) WITH (
         |    'connector.type' = 'jdbc', -- 使用 jdbc connector
         |    'connector.url' = 'jdbc:mysql://hadoop01:3306/test', -- jdbc url
         |    'connector.table' = 'pvuv_sink', -- 表名
         |    'connector.username' = 'root', -- 用户名
         |    'connector.password' = 'root', -- 密码
         |    'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
         |)
         |""".stripMargin)

    tenv.execute("blink sql")
  }
}

```



错误2：

```
Caused by: org.apache.flink.table.api.NoMatchingTableFactoryException: Could not find a suitable table factory for 'org.apache.flink.table.factories.DeserializationSchemaFactory' in
the classpath.

Reason: No factory implements 'org.apache.flink.table.factories.DeserializationSchemaFactory'.

解决办法：
将flink-kafka-connector和flink-json的包放到flink的lib目录，并将其添加到项目中。

flink-json的依赖：
  <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>1.9.1</version>
        </dependency>
```



错误3：

```
org.apache.flink.table.api.ValidationException: Unsupported SQL query! sqlQuery() only accepts a single SQL query of type SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.

解决方法：
insert into ... 必须使用tenv.sqlUpudate()
```



错误4：

```
Exception in thread "main" org.apache.flink.table.api.NoMatchingTableFactoryException: Could not find a suitable table factory for 'org.apache.flink.table.factories.TableSinkFactory' in
the classpath.

解决办法：
引入flink-jdbc的依赖即可
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-jdbc_2.11</artifactId>
            <version>1.9.1</version>
        </dependency>
```

错误5：

```
Caused by: java.io.IOException: Failed to deserialize JSON object.
解决办法：
1、注意生产的数据格式是否为json
2、注意kafka的消费位置前是否有非json格式
3、flink-json包是否放到classpath目录
```



测试：

```shell
[root@hadoop01 ~]# nohup kafka-server-start.sh /usr/local/kafka_2.11-1.1.1/config/server.properties > /var/log/kafka.log 2>&1 &
列出主题
[root@hadoop01 ~]# kafka-topics.sh --list --zookeeper hadoop02:2181/kafka
生产数据
[root@hadoop01 ~]# kafka-console-producer.sh --broker-list hadoop01:9092,hadoop02:9092,hadoop03:9092 --topic test
消费数据
[root@hadoop02 ~]# kafka-console-consumer.sh --bootstrap-server hadoop01:9092,hadoop02:9092,hadoop03:9092 --topic test
测试自定义source即可？？？
```



创建mysql的结果表：

```sql
CREATE TABLE `pvuv_sink` (
  `dt` varchar(255) DEFAULT NULL,
  `pv` bigint(11) DEFAULT '0',
  `uv` bigint(11) DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```



往test的topic中生产数据：

```json
{"user_id": "543462", "item_id":"1715", "category_id": "1464116", "behavior": "pv", "ts": "2020-10-17T11:38:00Z"}
{"user_id": "662867", "item_id":"2244074", "category_id": "1575622", "behavior": "pv", "ts": "2020-10-17T11:38:00Z"}
```





# 第六章 CEP

Complex Event Processing (CEP) 复杂事件处理。

Flink CEP是在Flink中实现的复杂事件处理库
CEP允许在无休止的时间流中检测事件模式，有机会掌握数据中重要的部分
一个或多个由简单事件构成的事件流通过一定的规则匹配，然后输出用户想得到的数据，满足规则的复杂事件

复杂事件举例：

```
123 zs 789 click
123 zs 789 start
123 zs 789 stop
123 zs 789 start
123 zs 789 end
123 zs 888 view

根据如上数据，找出有效观看、深度观看等用户。
```



特征：

```
目标：从有序的简单事件流中发现一些高阶特征
输入：一个或多个由简单事件构成的事件流
处理：识别简单事件之间的内在联系，多个符合一定规则的简单事件构成复杂事件
输出：满足规则的复杂事件
```

 三种状态：

```
其中边StateTransition（状态转换）分为三种
take: 状态满足跳变条件后直接跳变到B状态
ignore: 状态满足跳变条件以后又回到原来状态，状态保持不变
process: 这条边可以忽略也可以不忽略
```

三种模式序列：

```
严格近邻（Strict Contiguity）
所有事件按照严格的顺序出现，中间没有任务不匹配的事件，由.next()指定
例如对于模式“a next b”，事件序列[a,c,b1,b2]没有匹配的

宽松近邻（Ralaxed Contiguity）
允许中间出现不匹配的事件，由followedBy指定
例如对于模式“a followedBy b”，事件序列[a,c,b,b2]匹配为{a,b1}

非确定性宽松近邻（Non-Deterministic Ralaxed Contiguity）
进一步放宽条件，之前匹配过的事件可以再次匹配，由.followedByAny指定
例如对于模式“a followedByAny B”，事件序列[a,c,b1,b2]匹配为{a,b1}，{a，b2}
```

非确定：

```
可以定义“不希望出现某种近邻关系”
notNext——不希望让某个事件严格紧邻前一个事件发生
notFollowedBy——不想让某个事件在两个事件之间发生

需要注意：
—— 所有模式序列必须以.begin()开始
—— 模式序列不能以.notFollowedBy()结束
—— not类型的模式不能被optional所修饰
—— 可以为模式指定时间约束，用来要求在多长时间内匹配有效，如：next.within(Time.seconds(8))
```



引入依赖：

引入依赖：

```xml
<!--flink-cep的依赖-->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep-scala_2.11</artifactId>
    <version>${flink.version}</version>
</dependency>
```

事件实体代码：

```java
package com.qf.entry;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Objects;
//事件封装
public class Event {
    private int id;
    private String name;
    private double price;

    public Event(int id, String name, double price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public double getPrice() {
        return price;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Event(编号→" + id + ",名称→ " + name + ", 旅游景点的价格→" + price + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) {
            Event other = (Event) obj;

            return name.equals(other.name) && price == other.price && id == other.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, price, id);
    }
}
```

子事件实体代码：

```java
package com.qf.entry;

public class SubEvent extends Event {
	/**
	 * 业务场景：旅游经典的容量
	 */
	private final double volume;

	public SubEvent(int id, String name, double price, double volume) {
		super(id, name, price);
		this.volume = volume;
	}

	public double getVolume() {
		return volume;
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof SubEvent &&
				super.equals(obj) &&
				((SubEvent) obj).volume == volume;
	}

	@Override
	public int hashCode() {
		return super.hashCode() + (int) volume;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("SubEvent(编号→")
			.append(getId())
			.append(", 名称→")
			.append(getName())
			.append(", 旅游景点的价格→")
			.append(getPrice())
			.append(", 旅游景点的容纳量→")
			.append(getVolume())
			.append(")");

		return builder.toString();
	}
}
```



cep代码：

```java
package com.qianfeng.stream

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
cep案例：
1、过滤事件流规则为：start开始--->任意--->middle --->任意--->最后end
2、过滤事件流规则为：start开始--->middle --->任意--->最后end
3、过滤事件流规则为：start开始--->任意--->middle(3次) --->任意--->最后end
 */
object Demo39_stream_cep {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //通过fromElements的方式准备一个DataStream
    import org.apache.flink.api.scala._
    val ds: DataStream[Event] = env.fromElements[Event](
      new Event(1, "start", 1.0), //理解为→ 登陆旅游网站
      new Event(2, "middle", 2.0), //理解为→ 浏览旅游网站上的旅游景点的信息
      new Event(3, "foobar", 3.0),
      new SubEvent(4, "foo", 4.0, 1.0),
      new Event(5, "middle", 5.0),
      new SubEvent(6, "middle", 6.0, 2.0),
      new SubEvent(7, "bar", 3.0, 3.0),
      new Event(42, "42", 42.0),
      new Event(8, "end", 1.0) //理解为→ 退出旅游网站
    )

    //定制匹配模式规则 --- scala包
    /*
    //宽松近邻
    val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .followedByAny("middle")
      .where(_.getName.equals("middle")) //浏览
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))*/

    //严格近邻
    /*val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .next("middle")
      .where(_.getName.equals("middle")) //浏览
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))*/

    val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .followedBy("middle")
      .where(_.getName.equals("middle"))
      .followedBy("end") //退出
      .where(_.getName.equals("end"))

    /*val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .followedByAny("middle")
      .where(_.getName.equals("middle"))
      .where(_.getPrice > 5.0)
      //.times(4)  //匹配某一个出现的次数
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))
      .within(Time.seconds(10))  //指定窗口，，希望10s中内配如上的规则，如果超过10s将不会进行匹配
*/
    //将匹配模式应用于当前的DataStream中，从DataStream中筛选出满足条件的元素，放到匹配模式流中存储起来
    val ps: PatternStream[Event] = CEP.pattern(ds, patternRule)

    //从匹配模式流中取出数据，并予以显示
    ps.flatSelect[String]((ele: scala.collection.Map[String, Iterable[Event]], out: Collector[String]) => {
      //步骤：
      //准备一个容器，如：StringBuilder，用于存放结果
      val builder = new StringBuilder

      //从匹配模式流中取出对应的元素,并追加到容器中 --- get(name)需要和followedBy(name)一致
      val startEvent = ele.get("start").get.toList.head.toString
      val middleEvent = ele.get("middle").get.toList.head.toString
      val endEvent = ele.get("end").get.toList.head.toString

      builder.append(startEvent).append("\t|\t")
        .append(middleEvent).append("\t|\t")
        .append(endEvent)

      //将结果通过Collector发送到新的DataStream中存储起来
      out.collect(builder.toString)

    }).print("CEP 运行结果 ： ")


    //启动应用
    env.execute("cep")
  }
}
```



# day07作业

1、思考spark和flink区别

2、编写简历(职责、业务、思考实现)




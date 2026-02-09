flink的所有的connectors


数据流
离线datax+hive
1. datax抽一次 flink到hive（只会新增(I D U全算新增)需要merge）
oracle可以写一个触发器记录删除和更新 datax再抽取 然后merge
2. flink直接到paimon(可以修改数据)


维表join
用java集合map


数据入湖+元数据应用(自动获取字段等)
主要是flink sql
有mysql/postgresql+cdc+kafka+flink+paimon+redis+es+mysql+doris
或者日志数据、爬虫数据、埋点数据、接口数据、数据库数据
工具：flume+kafka(多个flume可以hdfs和kafka都写)、IoT 设备数据(直接到kafka)、
接口数据(Flask)、用户行为埋点(nginx+lua、spring boot、Flask(我主要用这个))、python爬虫
PLC(各种)传感器、扫码枪记录
另外如果老项目没有把日志写到本地文件并且不想改代码的时候就可以用flask接口代码写到kafka

做过数据迁移项目
oracle等存储过程改hive sql
通过mysql等数据库的元数据拼接cdc+paimon的代码并运行
生产还是cdc+kafka+paimon模块

对于原表DDL变更(使用paimon)
新增字段=支持(不需重启)、改变字段类型=支持(但varchar改int会报错)、删除字段、新建表=(如果用正则则无需重启、例如分表增加)
改主键=不支持(删除重建需追数)


flink风控

flink sql窗口函数
SELECT
window_start,
window_end,
COUNT(*) AS order_count,
SUM(price) AS total_amount
FROM TABLE(
TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end;
时间和计数
滚动
滑动
会话
全局窗口=自己写触发




flink状态

flink 2pc

flink 双流join

主要技术点flink cdc、kafka、paimon、redis、elasticsearch、doris


场景1:查询加密后的手机号
方案:切片


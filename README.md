flink的所有的connectors


数据流
oracle+ogg+kafka+flink+paimon+hive+ads
1. datax抽一次 flink到hive（只会新增(I D U全算新增)需要merge）
2. flink直接到paimon(可以修改数据)

mysql+flink-cdc+kafka+paimon
postgresql+flink-cdc+kafka+paimon




package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.bean.CdcTable;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

public class OracleJdbcSource extends RichParallelSourceFunction<CdcTable> {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;
    private final String primaryKey; // 用于分片和排序
    private final int fetchSize;      // 每批读取行数
    private final boolean useOffsetFetch; // true: Oracle 12c+, false: Oracle 11g

    private transient HikariDataSource dataSource;
    private volatile boolean isRunning = true;

    // 构造函数（推荐使用）
    public OracleJdbcSource(String jdbcUrl, String username, String password,
                            String tableName, String primaryKey) {
        this(jdbcUrl, username, password, tableName, primaryKey, 10_000, true);
    }

    public OracleJdbcSource(String jdbcUrl, String username, String password,
                            String tableName, String primaryKey,
                            int fetchSize, boolean useOffsetFetch) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.fetchSize = fetchSize;
        this.useOffsetFetch = useOffsetFetch;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(1); // 每个 subtask 一个连接足够
        config.setConnectionTimeout(30_000);
        config.setIdleTimeout(60_000);
        dataSource = new HikariDataSource(config);
    }

    @Override
    public void run(SourceContext<CdcTable> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalParallelism = getRuntimeContext().getNumberOfParallelSubtasks();

        // 【关键】计算当前 subtask 负责的 ID 范围（需提前知道 min/max）
        // 简化版：假设你知道全局 minId=1, maxId=1_000_000
        // 生产中可通过预查询获取，或传入参数
        long minId = 1L;
        long maxId = 1_000_000L;

        long range = (maxId - minId + 1) / totalParallelism;
        long startId = minId + range * subtaskIndex;
        long endId = (subtaskIndex == totalParallelism - 1) ? maxId : startId + range - 1;

        if (useOffsetFetch) {
            readWithOffsetFetch(ctx, startId, endId);
        } else {
            readWithRowNum(ctx, startId, endId);
        }
    }

    // Oracle 12c+ 分页
    private void readWithOffsetFetch(SourceContext<CdcTable> ctx, long startId, long endId) throws Exception {
        long currentId = startId;
        while (currentId <= endId && isRunning) {
            String sql = String.format(
                    "SELECT ID, NAME, CREATE_TIME, STATUS FROM %s " +
                            "WHERE ID >= ? AND ID <= ? " +
                            "ORDER BY %s " +
                            "OFFSET 0 ROWS FETCH NEXT %d ROWS ONLY",
                    tableName, primaryKey, fetchSize
            );

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                stmt.setLong(1, currentId);
                stmt.setLong(2, endId);

                try (ResultSet rs = stmt.executeQuery()) {
                    boolean hasData = false;
                    while (rs.next()) {
                        hasData = true;
                        emitRecord(ctx, rs);
                        currentId = rs.getLong("ID") + 1;
                    }
                    if (!hasData) break; // 无更多数据
                }
            }
        }
    }

    // Oracle 11g 分页（ROWNUM）
    private void readWithRowNum(SourceContext<CdcTable> ctx, long startId, long endId) throws Exception {
        long offset = 0;
        while (isRunning) {
            String innerSql = String.format(
                    "SELECT ID, NAME, CREATE_TIME, STATUS FROM %s " +
                            "WHERE ID >= %d AND ID <= %d ORDER BY %s",
                    tableName, startId, endId, primaryKey
            );

            String sql = String.format(
                    "SELECT * FROM (" +
                            "  SELECT a.*, ROWNUM rn FROM (%s) a " +
                            "  WHERE ROWNUM <= %d" +
                            ") WHERE rn > %d",
                    innerSql, offset + fetchSize, offset
            );

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {

                boolean hasData = false;
                while (rs.next()) {
                    hasData = true;
                    emitRecord(ctx, rs);
                }
                if (!hasData) break;
                offset += fetchSize;
            }
        }
    }

    private void emitRecord(SourceContext<CdcTable> ctx, ResultSet rs) throws Exception {
        CdcTable record = new CdcTable();
        record.setId(rs.getInt("ID"));
        record.setName(rs.getString("NAME"));
        if (rs.getTimestamp("CREATE_TIME") != null) {
            record.setCreateTime(rs.getTimestamp("CREATE_TIME").toLocalDateTime());
        }
        record.setStatus(rs.getInt("STATUS"));
        ctx.collect(record);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        super.close();
    }
}

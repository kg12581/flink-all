SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.optimize.skewjoin = true;
SET mapred.reduce.tasks = 50;

WITH
-- 步骤1：增量表预处理 - 去重，取每个工单最新的一条增量记录
latest_incr AS (
    -- 外层查询：筛选出子查询中行号为1的记录（即每个订单最新的那条）
    SELECT
        order_id,
        product_line,
        machine_id,
        order_status,
        plan_qty,
        actual_qty,
        op_time,
        op_type,
        dt
    FROM (
        -- 内层子查询：先给每条记录按订单分组、按操作时间降序标行号
        SELECT
            order_id,
            product_line,
            machine_id,
            order_status,
            plan_qty,
            actual_qty,
            op_time,
            op_type,
            dt,
            -- 核心窗口函数：按order_id分组，op_time降序，生成行号
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY op_time DESC) AS rn
        FROM ods_work_order_incr
        WHERE dt = '${biz_date}'
    ) t  -- 给子查询起别名t，方便外层引用
    WHERE t.rn = 1  -- 筛选出每个分组内行号为1的记录（最新操作时间）
),

-- 步骤2：全量表与预处理后的增量表 FULL JOIN 关联
full_join_data AS (
    SELECT
        f.order_id AS full_order_id,
        f.product_line AS full_product_line,
        f.machine_id AS full_machine_id,
        f.order_status AS full_order_status,
        f.plan_qty AS full_plan_qty,
        f.actual_qty AS full_actual_qty,
        f.op_time AS full_op_time,
        i.order_id AS incr_order_id,
        i.product_line AS incr_product_line,
        i.machine_id AS incr_machine_id,
        i.order_status AS incr_order_status,
        i.plan_qty AS incr_plan_qty,
        i.actual_qty AS incr_actual_qty,
        i.op_time AS incr_op_time,
        i.op_type AS incr_op_type,
        f.dt
    FROM dwd_work_order_full f
    FULL JOIN latest_incr i
        ON f.order_id = i.order_id
        AND f.dt = i.dt
    WHERE f.dt = '${biz_date}'
),

-- 步骤3：字段合并 + 状态判断，生成最终结果
final_merge AS (
    SELECT
        -- 主键：优先取增量，无增量则取全量
        COALESCE(incr_order_id, full_order_id) AS order_id,
        -- 业务字段：增量非空则覆盖全量
        COALESCE(incr_product_line, full_product_line) AS product_line,
        COALESCE(incr_machine_id, full_machine_id) AS machine_id,
        COALESCE(incr_order_status, full_order_status) AS order_status,
        COALESCE(incr_plan_qty, full_plan_qty) AS plan_qty,
        COALESCE(incr_actual_qty, full_actual_qty) AS actual_qty,
        -- 最新操作时间：取增量和全量的最大值
        GREATEST(COALESCE(incr_op_time, '1970-01-01 00:00:00'), full_op_time) AS latest_op_time,
        -- 数据来源标记
        CASE WHEN incr_order_id IS NOT NULL THEN 'INCR' ELSE 'FULL' END AS data_source,
        dt
    FROM full_join_data
    -- 过滤增量表中标记为删除的记录
    WHERE incr_op_type != 'D' OR incr_op_type IS NULL
)

-- 步骤4：写入目标全量表（覆盖对应分区）
INSERT OVERWRITE TABLE dwd_work_order_full PARTITION (dt = '${biz_date}')
SELECT * FROM final_merge;
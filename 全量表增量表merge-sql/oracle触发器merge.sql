--oracle 触发器==>编写一个oracle触发器把表信息的修改和删除记录进去

CREATE OR REPLACE TRIGGER trg_employees_audit
    BEFORE UPDATE OR DELETE ON employees
    FOR EACH ROW
DECLARE
    v_operation VARCHAR2(10);
BEGIN
    -- 判断操作类型
    IF UPDATING THEN
        v_operation := 'UPDATE';
    ELSIF DELETING THEN
        v_operation := 'DELETE';
    END IF;

    -- 插入审计记录
    INSERT INTO employees_audit (
        operation,
        emp_id,
        old_name,
        new_name,
        old_salary,
        new_salary,
        old_department,
        new_department,
        changed_by,
        changed_time
    ) VALUES (
        v_operation,
        :OLD.emp_id,
        :OLD.name,
        CASE WHEN UPDATING THEN :NEW.name ELSE NULL END,
        :OLD.salary,
        CASE WHEN UPDATING THEN :NEW.salary ELSE NULL END,
        :OLD.department,
        CASE WHEN UPDATING THEN :NEW.department ELSE NULL END,
        USER,                     -- 当前数据库用户
        SYSTIMESTAMP
    );
END;
/

--抽取删除和修改表信息和每天增量表和全量表merge
-- 设置动态分区（如果用分区表）
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 创建临时表存储 T 日新全量（也可直接 INSERT OVERWRITE 到原表）
CREATE TABLE IF NOT EXISTS full_table_new
LIKE full_table;  -- 保持相同结构

-- 插入 T 日最新全量数据
INSERT OVERWRITE TABLE full_table_new
SELECT
  COALESCE(upd.id, old.id) AS id,
  COALESCE(upd.name, old.name) AS name,
  COALESCE(upd.status, old.status) AS status
  -- 其他字段类似...
FROM (
  -- 保留 T-1 日中未被删除、也未被更新的记录
  SELECT *
  FROM full_table
  WHERE id NOT IN (
    SELECT id FROM daily_delete_ids
    UNION ALL
    SELECT id FROM daily_increment
  )
) old

FULL OUTER JOIN (
  -- T 日新增或更新的记录（覆盖旧值）
  SELECT * FROM daily_increment
) upd
ON old.id = upd.id

-- 注意：已删除的记录不会出现在 old（因过滤）也不会出现在 upd，故自然消失
;
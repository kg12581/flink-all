--保险经验 财产险

--制造业经验

--逐级汇总 上卷和下钻
INSERT OVERWRITE TABLE agg_order
SELECT
    -- 维度（对汇总 NULL 做兜底）
    COALESCE(dt, DATE '9999-12-31')        AS dt,
    COALESCE(city, 'ALL')                  AS city,
    COALESCE(channel, 'ALL')               AS channel,

    -- 层级标识（核心）
    CASE
        WHEN GROUPING(dt)=1
         AND GROUPING(city)=1
         AND GROUPING(channel)=1 THEN 'ALL'
        WHEN GROUPING(city)=1
         AND GROUPING(channel)=1 THEN 'DT'
        WHEN GROUPING(channel)=1 THEN 'DT_CITY'
        ELSE 'DT_CITY_CHANNEL'
    END AS agg_level,

    -- 指标
    COUNT(*)        AS order_cnt,
    SUM(amount)     AS total_amount

FROM fact_order
WHERE dt >= '${start_dt}'
  AND dt <= '${end_dt}'

GROUP BY GROUPING SETS (
    (dt, city, channel),
    (dt, city),
    (dt),
    ()
);

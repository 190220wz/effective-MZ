--------------------------------------------------------------------------------
-- 1. 商品维度表（DIM层）
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS dim.dim_goods_info;
CREATE TABLE dim_goods_info (
    goods_id          STRING COMMENT '商品ID',
    goods_name        STRING COMMENT '商品名称（空值填充为“未知商品”）',
    category_id       STRING COMMENT '品类ID',
    category_name     STRING COMMENT '品类名称（空值填充为“未知品类”）',
    is_drainage       INT    COMMENT '是否引流款（1=是，0=否；异常值修正为0）',
    is_hot            INT    COMMENT '是否热销款（1=是，0=否；异常值修正为0）',
    create_time       STRING COMMENT '商品创建时间（标准化为 yyyy-MM-dd HH:mm:ss）',
    etl_time          STRING COMMENT 'ETL处理时间（记录数据清洗时间）'
)
COMMENT '商品维度表（DIM层，清洗后数据，支撑关联分析）'
STORED AS ORC;

INSERT OVERWRITE TABLE dim_goods_info
SELECT
    goods_id,
    -- 1. 商品名称空值填充
    COALESCE(goods_name, '未知商品') AS goods_name,
    category_id,
    -- 2. 品类名称空值填充
    COALESCE(category_name, '未知品类') AS category_name,
    -- 3. 引流款/热销款标识异常值修正（非0/1转为0）
    CASE WHEN is_drainage NOT IN (0, 1) THEN 0 ELSE is_drainage END AS is_drainage,
    CASE WHEN is_hot NOT IN (0, 1) THEN 0 ELSE is_hot END AS is_hot,
    -- 4. 时间标准化（假设原始格式为 yyyyMMddHHmmss）
    FROM_UNIXTIME(UNIX_TIMESTAMP(create_time, 'yyyyMMddHHmmss')) AS create_time,
    -- 5. 记录ETL时间
    CURRENT_TIMESTAMP() AS etl_time
FROM day85.ods_dim_goods_info  -- 修正后ODS表，无字段级dt冲突
WHERE ds = '20250805';  -- 按需替换分区日期


--------------------------------------------------------------------------------
-- 2. 监控商品配置维度表（DIM层）
--------------------------------------------------------------------------------
-- 1. 建表（指定数据库，修正时间字段类型）
-- 1. 建表（指定数据库，修正时间字段类型）
DROP TABLE IF EXISTS dim_monitor_goods_setting;
CREATE TABLE dim_monitor_goods_setting (
    monitor_id        STRING COMMENT '监控规则ID',
    main_goods_id     STRING COMMENT '主商品ID',
    related_goods_ids STRING COMMENT '关联商品ID列表（清洗后）',
    monitor_type      STRING COMMENT '监控类型（标准化：连带购买/浏览）',
    start_date        TIMESTAMP COMMENT '生效日期（格式标准化）',  -- 修正为TIMESTAMP
    etl_time          TIMESTAMP COMMENT 'ETL处理时间'             -- 修正为TIMESTAMP
)
COMMENT '监控商品配置维度表（DIM层，定义分析规则）'
STORED AS ORC;

-- 2. 数据清洗与加载（补充数据库前缀，修正分区字段）
INSERT OVERWRITE TABLE dim_monitor_goods_setting
SELECT
    monitor_id,
    main_goods_id,
    COALESCE(related_goods_ids, '-') AS related_goods_ids,
    CASE monitor_type
        WHEN 'browse' THEN '连带浏览'
        WHEN 'purchase' THEN '连带购买'
        ELSE '未知类型'
    END AS monitor_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP(start_date, 'yyyyMMdd')) AS start_date,  -- 格式匹配
    CURRENT_TIMESTAMP() AS etl_time
FROM day85.ods_dim_monitor_goods_setting  -- 补充ODS层数据库前缀
WHERE dt = '20250805';  -- 修正为ODS层分区字段 `dt`
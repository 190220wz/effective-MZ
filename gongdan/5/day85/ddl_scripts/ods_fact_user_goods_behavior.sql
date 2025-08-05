-- 自动生成的Hive DDL脚本
use day85;
-- 源表: day85.fact_user_goods_behavior
-- 目标表: ods_fact_user_goods_behavior
-- 生成时间: 2025-08-05 15:32:16

CREATE TABLE IF NOT EXISTS ods_fact_user_goods_behavior (
    behavior_id INT,
    user_id INT,
    goods_id INT,
    behavior_type TINYINT,
    behavior_time DATE,
    behavior_date DATE
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day85/ods_fact_user_goods_behavior'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
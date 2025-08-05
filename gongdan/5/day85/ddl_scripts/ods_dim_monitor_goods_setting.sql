-- 自动生成的Hive DDL脚本
use day85;
-- 源表: day85.dim_monitor_goods_setting
-- 目标表: ods_dim_monitor_goods_setting
-- 生成时间: 2025-08-05 15:32:15

CREATE TABLE IF NOT EXISTS ods_dim_monitor_goods_setting (
    monitor_id INT,
    goods_id INT,
    is_default TINYINT,
    monitor_start_time DATE,
    monitor_status TINYINT,
    update_time DATE
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day85/ods_dim_monitor_goods_setting'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
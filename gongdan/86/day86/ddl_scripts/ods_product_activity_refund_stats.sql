-- 自动生成的Hive DDL脚本
use day86;
-- 源表: day86.product_activity_refund_stats
-- 目标表: ods_product_activity_refund_stats
-- 生成时间: 2025-08-06 15:09:07

CREATE TABLE IF NOT EXISTS ods_product_activity_refund_stats (
    id INT,
    product_id INT,
    stats_date DATE,
    stats_period STRING,
    juhuasuan_pay_amount DECIMAL(12,2),
    refund_amount DECIMAL(12,2),
    yearly_pay_amount DECIMAL(15,2),
    competitiveness_score INT
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day86/ods_product_activity_refund_stats'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
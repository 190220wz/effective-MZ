-- �Զ����ɵ�Hive DDL�ű�
use day86;
-- Դ��: day86.product_traffic_stats
-- Ŀ���: ods_product_traffic_stats
-- ����ʱ��: 2025-08-06 15:09:07

CREATE TABLE IF NOT EXISTS ods_product_traffic_stats (
    id INT,
    product_id INT,
    stats_date DATE,
    stats_period STRING,
    visitor_count INT,
    pc_visitor_count INT,
    wireless_visitor_count INT,
    view_count INT,
    avg_stay_time DECIMAL(6,2),
    bounce_rate DECIMAL(5,2),
    micro_view_count INT
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day86/ods_product_traffic_stats'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
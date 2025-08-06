-- �Զ����ɵ�Hive DDL�ű�
use day85;
-- Դ��: day85.dim_monitor_goods_setting
-- Ŀ���: ods_dim_monitor_goods_setting
-- ����ʱ��: 2025-08-05 15:32:15

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
    
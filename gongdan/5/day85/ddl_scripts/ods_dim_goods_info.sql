-- �Զ����ɵ�Hive DDL�ű�
use day85;
-- Դ��: day85.dim_goods_info
-- Ŀ���: ods_dim_goods_info
-- ����ʱ��: 2025-08-05 15:32:15

CREATE TABLE IF NOT EXISTS ods_dim_goods_info (
    goods_id INT,
    goods_name STRING,
    category_id INT,
    category_name STRING,
    is_drainage TINYINT,
    is_hot TINYINT,
    create_time DATE,
    update_time DATE
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day85/ods_dim_goods_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
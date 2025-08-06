-- �Զ����ɵ�Hive DDL�ű�
use day86;
-- Դ��: day86.product_base
-- Ŀ���: ods_product_base
-- ����ʱ��: 2025-08-06 15:09:07

CREATE TABLE IF NOT EXISTS ods_product_base (
    product_id INT,
    product_name STRING,
    category_id INT,
    category_name STRING,
    price DECIMAL(10,2),
    create_time DATE
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day86/ods_product_base'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
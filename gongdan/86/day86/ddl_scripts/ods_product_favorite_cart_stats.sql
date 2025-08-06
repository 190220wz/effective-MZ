-- �Զ����ɵ�Hive DDL�ű�
use day86;
-- Դ��: day86.product_favorite_cart_stats
-- Ŀ���: ods_product_favorite_cart_stats
-- ����ʱ��: 2025-08-06 15:09:07

CREATE TABLE IF NOT EXISTS ods_product_favorite_cart_stats (
    id INT,
    product_id INT,
    stats_date DATE,
    stats_period STRING,
    favorite_count INT,
    cart_item_count INT,
    cart_user_count INT,
    favorite_convert_rate DECIMAL(5,2),
    cart_convert_rate DECIMAL(5,2)
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day86/ods_product_favorite_cart_stats'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
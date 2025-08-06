-- �Զ����ɵ�Hive DDL�ű�
use day86;
-- Դ��: day86.product_user_stratify_stats
-- Ŀ���: ods_product_user_stratify_stats
-- ����ʱ��: 2025-08-06 15:09:07

CREATE TABLE IF NOT EXISTS ods_product_user_stratify_stats (
    id INT,
    product_id INT,
    stats_date DATE,
    stats_period STRING,
    new_pay_user_count INT,
    old_pay_user_count INT,
    old_pay_amount DECIMAL(12,2),
    customer_price DECIMAL(10,2),
    visitor_avg_value DECIMAL(10,2)
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day86/ods_product_user_stratify_stats'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
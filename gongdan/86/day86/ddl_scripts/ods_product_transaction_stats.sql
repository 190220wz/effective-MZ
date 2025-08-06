-- �Զ����ɵ�Hive DDL�ű�
use day86;
-- Դ��: day86.product_transaction_stats
-- Ŀ���: ods_product_transaction_stats
-- ����ʱ��: 2025-08-06 15:09:07

CREATE TABLE IF NOT EXISTS ods_product_transaction_stats (
    id INT,
    product_id INT,
    stats_date DATE,
    stats_period STRING,
    order_user_count INT,
    order_item_count INT,
    order_amount DECIMAL(12,2),
    pay_user_count INT,
    pay_item_count INT,
    pay_amount DECIMAL(12,2),
    prepay_amount DECIMAL(12,2),
    cod_pay_amount DECIMAL(12,2),
    order_convert_rate DECIMAL(5,2),
    pay_convert_rate DECIMAL(5,2)
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day86/ods_product_transaction_stats'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
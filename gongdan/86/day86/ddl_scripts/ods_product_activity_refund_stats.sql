-- �Զ����ɵ�Hive DDL�ű�
use day86;
-- Դ��: day86.product_activity_refund_stats
-- Ŀ���: ods_product_activity_refund_stats
-- ����ʱ��: 2025-08-06 15:09:07

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
    
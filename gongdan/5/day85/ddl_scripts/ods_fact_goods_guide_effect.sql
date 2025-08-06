-- �Զ����ɵ�Hive DDL�ű�
use day85;
-- Դ��: day85.fact_goods_guide_effect
-- Ŀ���: ods_fact_goods_guide_effect
-- ����ʱ��: 2025-08-05 15:32:16

CREATE TABLE IF NOT EXISTS ods_fact_goods_guide_effect (
    guide_id INT,
    main_goods_id INT,
    guided_goods_id INT,
    guide_visitor_count INT,
    guide_collect_addcart_count INT,
    guide_pay_count INT,
    stat_date DATE,
    update_time DATE
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day85/ods_fact_goods_guide_effect'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
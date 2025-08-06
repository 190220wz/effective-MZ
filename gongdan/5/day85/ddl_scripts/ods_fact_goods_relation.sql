-- �Զ����ɵ�Hive DDL�ű�
use day85;
-- Դ��: day85.fact_goods_relation
-- Ŀ���: ods_fact_goods_relation
-- ����ʱ��: 2025-08-05 15:32:16

CREATE TABLE IF NOT EXISTS ods_fact_goods_relation (
    relation_id INT,
    main_goods_id INT,
    related_goods_id INT,
    relation_type TINYINT,
    relation_count INT,
    stat_period_start DATE,
    stat_period_end DATE,
    update_time DATE
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/day85/ods_fact_goods_relation'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    
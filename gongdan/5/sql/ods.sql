SET hive.exec.dynamic.partition.mode = nonstrict;
USE day85;

--------------------------------------------------------------------------------
-- 1. 商品维度信息表（ODS层）
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_dim_goods_info;
CREATE EXTERNAL TABLE day85.ods_dim_goods_info (
    goods_id          STRING COMMENT '商品ID',
    goods_name        STRING COMMENT '商品名称',
    category_id       STRING COMMENT '品类ID',
    category_name     STRING COMMENT '品类名称',
    is_drainage       INT    COMMENT '是否引流款（1=是，0=否）',
    is_hot            INT    COMMENT '是否热销款（1=是，0=否）',
    create_time       STRING COMMENT '商品创建时间'  -- 移除字段列表中的dt
)
COMMENT '业务库商品维度原始表（ODS层）'
PARTITIONED BY (dt STRING COMMENT '分区日期')  -- 仅在此处定义分区列
STORED AS ORC
LOCATION '/warehouse/day85/ods/ods_dim_goods_info/';

-- 加载示例（同步当日数据）
ALTER TABLE day85.ods_dim_goods_info ADD IF NOT EXISTS PARTITION (dt='20250805')
LOCATION '/warehouse/day85/ods/ods_dim_goods_info/dt=20250805';


--------------------------------------------------------------------------------
-- 2. 监控商品配置表（ODS层）
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS day85.ods_dim_monitor_goods_setting;
CREATE EXTERNAL TABLE day85.ods_dim_monitor_goods_setting (
    monitor_id        STRING COMMENT '监控规则ID',
    main_goods_id     STRING COMMENT '主商品ID（引流/热销款）',
    related_goods_ids STRING COMMENT '关联商品ID列表（逗号分隔）',
    monitor_type      STRING COMMENT '监控类型（连带购买/浏览）',
    start_date        STRING COMMENT '生效日期'  -- 移除字段列表中的 `dt`
)
COMMENT '业务库监控商品配置原始表（ODS层）'
PARTITIONED BY (dt STRING COMMENT '分区日期')  -- 仅在此处定义分区列
STORED AS ORC
LOCATION '/warehouse/day85/ods/ods_dim_monitor_goods_setting/';
-- 加载示例
ALTER TABLE day85.ods_dim_monitor_goods_setting ADD IF NOT EXISTS PARTITION (dt='20250805')
LOCATION '/warehouse/day85/ods/ods_dim_monitor_goods_setting/dt=20250805';


--------------------------------------------------------------------------------
-- 3. 商品引流效果事实表（ODS层）
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS day85.ods_fact_goods_guide_effect;
CREATE EXTERNAL TABLE day85.ods_fact_goods_guide_effect (
    goods_id          STRING COMMENT '商品ID',
    uv                BIGINT COMMENT '访客数',
    pv                BIGINT COMMENT '访问次数',
    add_cart_num      BIGINT COMMENT '加购数',
    pay_num           BIGINT COMMENT '支付数',
    stat_date         STRING COMMENT '统计日期'  -- 移除字段列表中的 `dt`
)
COMMENT '业务库商品引流效果原始表（ODS层）'
PARTITIONED BY (dt STRING COMMENT '分区日期')  -- 仅在此处定义分区列
STORED AS ORC
LOCATION '/warehouse/day85/ods/ods_fact_goods_guide_effect/';

-- 加载示例
ALTER TABLE day85.ods_fact_goods_guide_effect ADD IF NOT EXISTS PARTITION (dt='20250805')
LOCATION '/warehouse/day85/ods/ods_fact_goods_guide_effect/dt=20250805';


--------------------------------------------------------------------------------
-- 4. 商品关联关系事实表（ODS层）
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS day85.ods_fact_goods_relation
;
CREATE EXTERNAL TABLE day85.ods_fact_goods_relation (
    main_goods_id     STRING COMMENT '主商品ID',
    related_goods_id  STRING COMMENT '关联商品ID',
    relation_type     STRING COMMENT '关联类型（同时浏览/加购/支付）',
    relation_count    BIGINT COMMENT '关联次数',
    stat_date         STRING COMMENT '统计日期'  -- 移除字段列表中的 `dt`
)
COMMENT '业务库商品关联关系原始表（ODS层）'
PARTITIONED BY (dt STRING COMMENT '分区日期')  -- 仅在此处定义分区列
STORED AS ORC
LOCATION '/warehouse/day85/ods/ods_fact_goods_relation/';
-- 加载示例
ALTER TABLE day85.ods_fact_goods_relation ADD IF NOT EXISTS PARTITION (dt='20250805')
LOCATION '/warehouse/day85/ods/ods_fact_goods_relation/dt=20250805';


--------------------------------------------------------------------------------
-- 5. 用户商品行为事实表（ODS层）
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS day85.ods_fact_user_goods_behavior;
CREATE EXTERNAL TABLE day85.ods_fact_user_goods_behavior (
    user_id           STRING COMMENT '用户ID',
    goods_id          STRING COMMENT '商品ID',
    behavior_type     INT    COMMENT '行为类型（1=浏览，2=加购，3=支付）',
    behavior_time     STRING COMMENT '行为时间',
    dt                STRING COMMENT '分区日期'
)
COMMENT '业务库用户商品行为原始表（ODS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/warehouse/day85/ods/ods_fact_user_goods_behavior/';

-- 加载示例
ALTER TABLE day85.ods_fact_user_goods_behavior ADD IF NOT EXISTS PARTITION (dt='20250805')
LOCATION '/warehouse/day85/ods/ods_fact_user_goods_behavior/dt=20250805';
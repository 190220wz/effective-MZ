-- 商品维度表：存储基础属性
DROP TABLE IF EXISTS dim_goods;
CREATE TABLE dim_goods (
    goods_id BIGINT COMMENT '商品ID（对应ods的product_id）',
    goods_name STRING COMMENT '商品名称',
    category_id BIGINT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    price DECIMAL(10,2) COMMENT '商品价格',
    create_time STRING COMMENT '商品创建时间',
    dt STRING COMMENT '数据日期'
) COMMENT '商品维度表（源自ods_product_base）'
STORED AS ORC;

-- 插入数据（假设ODS表已通过Sqoop同步到Hive）
INSERT OVERWRITE TABLE dim_goods
SELECT
    product_id AS goods_id,
    product_name,
    category_id,
    category_name,
    price,
    create_time,  -- ODS中需为字符串格式（如'2025-01-01 12:00:00'）
    '20250806' AS ds  -- 示例日期，实际建议分区存储
FROM ods_product_base
WHERE ds = '20250806';  -- 与ODS表分区一致
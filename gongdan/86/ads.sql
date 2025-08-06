-- 与示例完全对齐的表结构和逻辑
DROP TABLE IF EXISTS ads_goods_sales_analysis;
CREATE TABLE ads_goods_sales_analysis (
    goods_id BIGINT COMMENT '商品ID',
    goods_name STRING COMMENT '商品名称',
    dt STRING COMMENT '数据日期',
    pv BIGINT COMMENT '浏览次数',
    uv BIGINT COMMENT '独立访客数',
    pay_rate DOUBLE COMMENT '支付率（支付/加购）',
    total_pay BIGINT COMMENT '总支付数'
) COMMENT '商品销售分析'
STORED AS ORC;

INSERT OVERWRITE TABLE ads_goods_sales_analysis
SELECT
    s.goods_id,
    g.goods_name,  -- 关联维度表获取商品名称
    s.dt,
    s.pv,
    s.uv,
    -- 支付率计算：加购为0则置为0，否则保留2位小数
    CASE
        WHEN s.add_cart = 0 THEN 0.0
        ELSE ROUND(s.pay / s.add_cart, 2)
    END AS pay_rate,
    s.pay AS total_pay
FROM dws_goods_behavior_stats s
LEFT JOIN dim_goods g
    ON s.goods_id = g.goods_id AND s.dt = g.dt
WHERE s.dt = '2025-01-01';


select * from ads_goods_sales_analysis;
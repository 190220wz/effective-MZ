DROP TABLE IF EXISTS dws_goods_behavior_stats;
CREATE TABLE dws_goods_behavior_stats (
    goods_id BIGINT COMMENT '商品ID',
    dt STRING COMMENT '数据日期',
    pv BIGINT COMMENT '浏览次数（PV）',
    uv BIGINT COMMENT '独立访客数',
    add_cart BIGINT COMMENT '加购人数',  -- 来自收藏加购表
    pay BIGINT COMMENT '支付人数',      -- 来自交易表
    order_amount DECIMAL(12,2) COMMENT '下单金额',
    pay_amount DECIMAL(12,2) COMMENT '支付金额'
) COMMENT '商品行为汇总表（整合多表指标）'
STORED AS ORC;

INSERT OVERWRITE TABLE dws_goods_behavior_stats
SELECT
    t.goods_id,
    t.dt,
    t.view_count AS pv,              -- 流量表的浏览次数
    t.visitor_count AS uv,           -- 流量表的访客数
    f.cart_user_count AS add_cart,   -- 收藏加购表的加购人数
    tr.pay_user_count AS pay,        -- 交易表的支付人数
    tr.order_amount,                 -- 下单金额
    tr.pay_amount                    -- 支付金额
FROM dwd_product_traffic_stats t
LEFT JOIN dwd_product_favorite_cart_stats f
    ON t.goods_id = f.goods_id AND t.dt = f.dt
LEFT JOIN dwd_product_transaction_stats tr
    ON t.goods_id = tr.goods_id AND t.dt = tr.dt
WHERE t.dt = '2025-01-01';


DROP TABLE IF EXISTS dws_category_stats;
CREATE TABLE dws_category_stats (
    category_id BIGINT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    dt STRING COMMENT '数据日期',
    total_goods_num BIGINT COMMENT '类目商品总数',
    total_pv BIGINT COMMENT '总浏览量',
    total_uv BIGINT COMMENT '总访客数',
    total_add_cart BIGINT COMMENT '总加购人数',
    total_pay BIGINT COMMENT '总支付人数',
    total_pay_amount DECIMAL(12,2) COMMENT '总支付金额',
    avg_pay_rate DOUBLE COMMENT '类目平均支付率'
) COMMENT '类目维度指标汇总'
STORED AS ORC;

INSERT OVERWRITE TABLE dws_category_stats
SELECT
    g.category_id,
    g.category_name,
    s.dt,
    COUNT(DISTINCT s.goods_id) AS total_goods_num,
    SUM(s.pv) AS total_pv,
    SUM(s.uv) AS total_uv,
    SUM(s.add_cart) AS total_add_cart,
    SUM(s.pay) AS total_pay,
    SUM(s.pay_amount) AS total_pay_amount,
    -- 类目平均支付率（总支付/总加购）
    CASE
        WHEN SUM(s.add_cart) = 0 THEN 0.0
        ELSE ROUND(SUM(s.pay) / SUM(s.add_cart), 4)
    END AS avg_pay_rate
FROM dws_goods_behavior_stats s
LEFT JOIN dim_goods g
    ON s.goods_id = g.goods_id AND s.dt = g.dt
WHERE s.dt = '2025-01-01'
GROUP BY g.category_id, g.category_name, s.dt;


DROP TABLE IF EXISTS dws_user_behavior_stats;
CREATE TABLE dws_user_behavior_stats (
    user_type STRING COMMENT '用户类型（新用户/老用户）',
    dt STRING COMMENT '数据日期',
    total_visitor BIGINT COMMENT '总访客数',
    total_pay_user BIGINT COMMENT '总支付用户数',
    total_pay_amount DECIMAL(12,2) COMMENT '总支付金额',
    avg_customer_price DECIMAL(10,2) COMMENT '平均客单价',
    pay_convert_rate DOUBLE COMMENT '支付转化率（支付用户/访客）'
) COMMENT '新老用户行为汇总'
STORED AS ORC;

INSERT OVERWRITE TABLE dws_user_behavior_stats
SELECT
    '新用户' AS user_type,
    u.dt,
    SUM(t.visitor_count) AS total_visitor,  -- 新用户对应访客数（假设流量表可关联）
    SUM(u.new_pay_user_count) AS total_pay_user,
    SUM(u.old_pay_amount + (u.customer_price * u.new_pay_user_count)) AS total_pay_amount,  -- 新用户支付金额=客单价*新用户数
    ROUND(AVG(u.customer_price), 2) AS avg_customer_price,
    ROUND(SUM(u.new_pay_user_count) / SUM(t.visitor_count), 4) AS pay_convert_rate
FROM dwd_product_user_stratify_stats u
LEFT JOIN dwd_product_traffic_stats t
    ON u.goods_id = t.goods_id AND u.dt = t.dt
WHERE u.dt = '2025-01-01'
GROUP BY u.dt

UNION ALL

SELECT
    '老用户' AS user_type,
    u.dt,
    SUM(t.visitor_count) AS total_visitor,
    SUM(u.old_pay_user_count) AS total_pay_user,
    SUM(u.old_pay_amount) AS total_pay_amount,
    ROUND(AVG(u.customer_price), 2) AS avg_customer_price,
    ROUND(SUM(u.old_pay_user_count) / SUM(t.visitor_count), 4) AS pay_convert_rate
FROM dwd_product_user_stratify_stats u
LEFT JOIN dwd_product_traffic_stats t
    ON u.goods_id = t.goods_id AND u.dt = t.dt
WHERE u.dt = '2025-01-01'
GROUP BY u.dt;


DROP TABLE IF EXISTS dws_activity_effect_stats;
CREATE TABLE dws_activity_effect_stats (
    activity_type STRING COMMENT '活动类型（如聚划算）',
    dt STRING COMMENT '数据日期',
    total_goods_num BIGINT COMMENT '参与活动商品数',
    total_pay_amount DECIMAL(12,2) COMMENT '活动总支付金额',
    total_refund_amount DECIMAL(12,2) COMMENT '活动总退款金额',
    activity_pay_rate DOUBLE COMMENT '活动支付率（活动支付/活动加购）',
    avg_competitiveness_score INT COMMENT '平均竞争力评分'
) COMMENT '活动效果指标汇总'
STORED AS ORC;

INSERT OVERWRITE TABLE dws_activity_effect_stats
SELECT
    '聚划算' AS activity_type,
    a.dt,
    COUNT(DISTINCT a.goods_id) AS total_goods_num,
    SUM(a.juhuasuan_pay_amount) AS total_pay_amount,
    SUM(a.refund_amount) AS total_refund_amount,
    -- 活动支付率（活动商品支付数/加购数）
    CASE
        WHEN SUM(f.cart_user_count) = 0 THEN 0.0
        ELSE ROUND(SUM(tr.pay_user_count) / SUM(f.cart_user_count), 4)
    END AS activity_pay_rate,
    AVG(a.competitiveness_score) AS avg_competitiveness_score
FROM dwd_product_activity_refund_stats a
LEFT JOIN dwd_product_favorite_cart_stats f
    ON a.goods_id = f.goods_id AND a.dt = f.dt
LEFT JOIN dwd_product_transaction_stats tr
    ON a.goods_id = tr.goods_id AND a.dt = tr.dt
WHERE a.dt = '2025-01-01'
    AND a.juhuasuan_pay_amount > 0  -- 筛选参与聚划算的商品
GROUP BY a.dt;

select *
from dws_activity_effect_stats;


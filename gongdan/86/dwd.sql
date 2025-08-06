DROP TABLE IF EXISTS dwd_product_traffic_stats;
CREATE TABLE dwd_product_traffic_stats (
    goods_id BIGINT COMMENT '商品ID',
    dt STRING COMMENT '数据日期（stats_date转换）',
    visitor_count BIGINT COMMENT '访客数',
    pc_visitor_count BIGINT COMMENT 'PC端访客数',
    wireless_visitor_count BIGINT COMMENT '无线端访客数',
    view_count BIGINT COMMENT '浏览次数（PV）',
    avg_stay_time DOUBLE COMMENT '平均停留时间（秒）',
    bounce_rate DOUBLE COMMENT '跳出率',
    micro_view_count BIGINT COMMENT '微详情浏览数'
) COMMENT '商品流量明细'
STORED AS ORC;

INSERT OVERWRITE TABLE dwd_product_traffic_stats
SELECT
    product_id AS goods_id,      -- ODS 表实际列名是 product_id
    stats_date AS dt,            -- ODS 表的 stats_date 字段
    visitor_count,               -- 直接引用 ODS 列
    pc_visitor_count,
    wireless_visitor_count,
    view_count,
    avg_stay_time,
    bounce_rate,
    micro_view_count
FROM ods_product_traffic_stats
WHERE ds = '20250806';          -- 必须与 ODS 分区列一致（如 ds）

DROP TABLE IF EXISTS dwd_product_favorite_cart_stats;
CREATE TABLE dwd_product_favorite_cart_stats (
    goods_id BIGINT COMMENT '商品ID',
    dt STRING COMMENT '数据日期',
    favorite_count BIGINT COMMENT '收藏数',
    cart_user_count BIGINT COMMENT '加购人数',
    cart_item_count BIGINT COMMENT '加购件数',
    favorite_convert_rate DOUBLE COMMENT '收藏转化率',
    cart_convert_rate DOUBLE COMMENT '加购转化率'
) COMMENT '商品收藏加购明细'
STORED AS ORC;

INSERT OVERWRITE TABLE dwd_product_favorite_cart_stats
SELECT
    product_id AS goods_id,
    stats_date AS ds,
    favorite_count,
    cart_user_count,
    cart_item_count,
    favorite_convert_rate,
    cart_convert_rate
FROM ods_product_favorite_cart_stats
WHERE ds = '20250806';


DROP TABLE IF EXISTS dwd_product_transaction_stats;
CREATE TABLE dwd_product_transaction_stats (
    goods_id BIGINT COMMENT '商品ID',
    dt STRING COMMENT '数据日期',
    order_user_count BIGINT COMMENT '下单人数',
    order_item_count BIGINT COMMENT '下单件数',
    order_amount DECIMAL(12,2) COMMENT '下单金额',
    pay_user_count BIGINT COMMENT '支付人数',
    pay_item_count BIGINT COMMENT '支付件数',
    pay_amount DECIMAL(12,2) COMMENT '支付金额',
    prepay_amount DECIMAL(12,2) COMMENT '预支付金额',
    cod_pay_amount DECIMAL(12,2) COMMENT '货到付款金额',
    order_convert_rate DOUBLE COMMENT '下单转化率',
    pay_convert_rate DOUBLE COMMENT '支付转化率'
) COMMENT '商品交易明细'
STORED AS ORC;

INSERT OVERWRITE TABLE dwd_product_transaction_stats
SELECT
    product_id AS goods_id,
    stats_date AS ds,
    order_user_count,
    order_item_count,
    order_amount,
    pay_user_count,
    pay_item_count,
    pay_amount,
    prepay_amount,
    cod_pay_amount,
    order_convert_rate,
    pay_convert_rate
FROM ods_product_transaction_stats
WHERE ds = '20250806';

DROP TABLE IF EXISTS dwd_product_user_stratify_stats;
CREATE TABLE dwd_product_user_stratify_stats (
    goods_id BIGINT COMMENT '商品ID',
    dt STRING COMMENT '数据日期',
    new_pay_user_count BIGINT COMMENT '新支付用户数',
    old_pay_user_count BIGINT COMMENT '老支付用户数',
    old_pay_amount DECIMAL(12,2) COMMENT '老用户支付金额',
    customer_price DECIMAL(10,2) COMMENT '客单价',
    visitor_avg_value DECIMAL(10,2) COMMENT '访客价值'
) COMMENT '商品用户分层明细'
STORED AS ORC;

INSERT OVERWRITE TABLE dwd_product_user_stratify_stats
SELECT
    product_id AS goods_id,
    stats_date AS ds,
    new_pay_user_count,
    old_pay_user_count,
    old_pay_amount,
    customer_price,
    visitor_avg_value
FROM ods_product_user_stratify_stats
WHERE ds = '20250806';

DROP TABLE IF EXISTS dwd_product_activity_refund_stats;
CREATE TABLE dwd_product_activity_refund_stats (
    goods_id BIGINT COMMENT '商品ID',
    dt STRING COMMENT '数据日期',
    juhuasuan_pay_amount DECIMAL(12,2) COMMENT '聚划算支付金额',
    refund_amount DECIMAL(12,2) COMMENT '退款金额',
    yearly_pay_amount DECIMAL(12,2) COMMENT '年度支付金额',
    competitiveness_score INT COMMENT '竞争力评分'
) COMMENT '商品活动退款明细'
STORED AS ORC;

INSERT OVERWRITE TABLE dwd_product_activity_refund_stats
SELECT
    product_id AS goods_id,
    stats_date AS ds,
    juhuasuan_pay_amount,
    refund_amount,
    yearly_pay_amount,
    competitiveness_score
FROM ods_product_activity_refund_stats
WHERE ds = '20250806';
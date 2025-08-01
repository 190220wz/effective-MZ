CREATE EXTERNAL TABLE ads_entry_analysis (
    entry_id BIGINT,
    stat_date DATE,
    entry_page STRING,
    entry_count INT,
    avg_stay_duration DECIMAL(10,2),
    conversion_rate DECIMAL(5,2),
    terminal_type STRING,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ads_entry_analysis';

CREATE EXTERNAL TABLE ads_page_ranking (
    rank_id BIGINT,
    rank_date DATE,
    page_id STRING,
    page_type STRING,
    visit_count INT,
    user_count INT,
    avg_duration DECIMAL(10,2),
    ranking INT,
    terminal_type STRING,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ads_page_ranking';

CREATE EXTERNAL TABLE ads_path_conversion (
    conversion_id BIGINT,
    stat_date DATE,
    path_pattern STRING,
    conversion_rate DECIMAL(5,2),
    avg_step INT,
    terminal_type STRING,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ads_path_conversion';

CREATE EXTERNAL TABLE ads_shop_path_analysis (
    analysis_id BIGINT,
    analysis_date DATE,
    source_page STRING,
    target_page STRING,
    jump_count INT,
    jump_rate DECIMAL(5,2),
    terminal_type STRING,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ads_shop_path_analysis';

CREATE EXTERNAL TABLE ads_traffic_inlet (
    inlet_id BIGINT,
    stat_date DATE,
    source_page STRING,
    source_count INT,
    source_rate DECIMAL(5,2),
    terminal_type STRING,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ads_traffic_inlet';











INSERT INTO dws_user_path_summary PARTITION(dt='2025-01-26') VALUES
(5, '2025-01-26', 1005, 'sess_1005_1', 'shop_category_1', 'product_list_1', array('shop_category_1', 'product_list_1'), 2, 'ios', 1003),
(6, '2025-01-26', 1006, 'sess_1006_1', 'shop_home_1', 'search_result_1', array('shop_home_1', 'search_result_1'), 2, 'android', 1004),
(7, '2025-01-26', 1007, 'sess_1007_1', 'product_detail_1', 'cart_page_1', array('product_detail_1', 'cart_page_1'), 2, 'pc', 1001),
(8, '2025-01-26', 1008, 'sess_1008_1', 'cart_page_1', 'checkout_page_1', array('cart_page_1', 'checkout_page_1'), 2, 'mac', 1001),
(9, '2025-01-26', 1009, 'sess_1009_1', 'user_center_1', 'user_center_1', array('user_center_1'), 1, 'windows', 1002),
(10, '2025-01-26', 1010, 'sess_1010_1', 'user_center_1', 'order_list_1', array('user_center_1', 'order_list_1'), 2, 'linux', 1002),
(11, '2025-01-26', 1011, 'sess_1011_1', 'checkout_page_1', 'payment_page_1', array('checkout_page_1', 'payment_page_1'), 2, 'smart_tv', 1003),
(12, '2025-01-26', 1012, 'sess_1012_1', 'shop_home_1', 'promotion_page_1', array('shop_home_1', 'promotion_page_1'), 2, 'wearable', 1004),
(13, '2025-01-26', 1013, 'sess_1013_1', 'help_center_1', 'help_center_1', array('help_center_1'), 1, 'smart_watch', 1005),
(14, '2025-01-26', 1014, 'sess_1014_1', 'help_center_1', 'about_us_1', array('help_center_1', 'about_us_1'), 2, 'tablet', 1005),
(15, '2025-01-26', 1015, 'sess_1015_1', 'about_us_1', 'contact_us_1', array('about_us_1', 'contact_us_1'), 2, 'chrome_os', 1006),
(16, '2025-01-26', 1016, 'sess_1016_1', 'shop_category_1', 'product_list_2', array('shop_category_1', 'product_list_2'), 2, 'firefox_os', 1006),
(17, '2025-01-26', 1017, 'sess_1017_1', 'shop_home_1', 'search_result_2', array('shop_home_1', 'search_result_2'), 2, 'safari', 1007),
(18, '2025-01-26', 1018, 'sess_1018_1', 'product_detail_1', 'cart_page_2', array('product_detail_1', 'cart_page_2'), 2, 'chrome', 1007),
(19, '2025-01-26', 1019, 'sess_1019_1', 'cart_page_2', 'checkout_page_2', array('cart_page_2', 'checkout_page_2'), 2, 'firefox', 1008),
(20, '2025-01-26', 1020, 'sess_1020_1', 'user_center_2', 'user_center_2', array('user_center_2'), 1, 'edge', 1008),
(21, '2025-01-26', 1021, 'sess_1021_1', 'user_center_2', 'order_list_2', array('user_center_2', 'order_list_2'), 2, 'opera', 1009),
(22, '2025-01-26', 1022, 'sess_1022_1', 'checkout_page_2', 'payment_page_2', array('checkout_page_2', 'payment_page_2'), 2, 'uc', 1009),
(23, '2025-01-26', 1023, 'sess_1023_1', 'shop_home_1', 'promotion_page_2', array('shop_home_1', 'promotion_page_2'), 2, 'qq_browser', 1010),
(24, '2025-01-26', 1024, 'sess_1024_1', 'help_center_2', 'help_center_2', array('help_center_2'), 1, 'baidu_browser', 1010);

-- dws_page_visit_stats新增20条
INSERT INTO dws_page_visit_stats PARTITION(dt='2025-01-26') VALUES
(6, '2025-01-26', 'cart_page_1', 'cart', 130, 110, 45.5, 12.0, 1001),
(7, '2025-01-26', 'checkout_page_1', 'checkout', 95, 85, 75.2, 8.0, 1001),
(8, '2025-01-26', 'user_center_1', 'center', 180, 160, 120.3, 15.0, 1002),
(9, '2025-01-26', 'order_list_1', 'list', 120, 105, 60.1, 10.0, 1002),
(10, '2025-01-26', 'payment_page_1', 'payment', 105, 95, 90.4, 7.0, 1003),
(11, '2025-01-26', 'promotion_page_1', 'promotion', 80, 70, 30.2, 5.0, 1004),
(12, '2025-01-26', 'help_center_1', 'help', 90, 80, 60.3, 8.0, 1005),
(13, '2025-01-26', 'about_us_1', 'about', 75, 65, 45.1, 6.0, 1005),
(14, '2025-01-26', 'contact_us_1', 'contact', 60, 50, 30.0, 4.0, 1006),
(15, '2025-01-26', 'product_list_2', 'list', 140, 120, 60.5, 12.0, 1006),
(16, '2025-01-26', 'search_result_2', 'result', 110, 95, 90.3, 9.0, 1007),
(17, '2025-01-26', 'cart_page_2', 'cart', 125, 110, 45.4, 11.0, 1007),
(18, '2025-01-26', 'checkout_page_2', 'checkout', 100, 90, 75.1, 8.0, 1008),
(19, '2025-01-26', 'user_center_2', 'center', 170, 150, 120.2, 14.0, 1008),
(20, '2025-01-26', 'order_list_2', 'list', 115, 100, 60.0, 9.0, 1009),
(21, '2025-01-26', 'payment_page_2', 'payment', 100, 90, 90.3, 6.0, 1009),
(22, '2025-01-26', 'promotion_page_2', 'promotion', 75, 65, 30.1, 4.0, 1010),
(23, '2025-01-26', 'help_center_2', 'help', 85, 75, 60.2, 7.0, 1010),
(24, '2025-01-26', 'about_us_2', 'about', 70, 60, 45.0, 5.0, 1011),
(25, '2025-01-26', 'contact_us_2', 'contact', 55, 45, 30.0, 3.0, 1011);

-- dws_daily_visit新增20条
INSERT INTO dws_daily_visit PARTITION(dt='2025-01-26') VALUES
(3, '2025-01-26', 1003, 210, 180, 85.7, 45, 165),
(4, '2025-01-26', 1004, 190, 160, 84.2, 40, 150),
(5, '2025-01-26', 1005, 120, 100, 83.3, 30, 90),
(6, '2025-01-26', 1006, 160, 140, 87.5, 35, 125),
(7, '2025-01-26', 1007, 240, 210, 87.5, 55, 195),
(8, '2025-01-26', 1008, 200, 175, 87.5, 45, 160),
(9, '2025-01-26', 1009, 280, 250, 89.3, 60, 220),
(10, '2025-01-26', 1010, 150, 130, 86.7, 30, 110),
(11, '2025-01-26', 1011, 180, 155, 86.1, 40, 140),
(12, '2025-01-26', 1012, 220, 190, 86.4, 50, 170),
(13, '2025-01-26', 1013, 140, 120, 85.7, 30, 105),
(14, '2025-01-26', 1014, 160, 135, 84.4, 35, 120),
(15, '2025-01-26', 1015, 210, 185, 88.1, 45, 165),
(16, '2025-01-26', 1016, 230, 200, 87.0, 50, 180),
(17, '2025-01-26', 1017, 190, 165, 86.8, 40, 150),
(18, '2025-01-26', 1018, 250, 220, 88.0, 55, 195),
(19, '2025-01-26', 1019, 170, 145, 85.3, 35, 130),
(20, '2025-01-26', 1020, 200, 170, 85.0, 45, 155),
(21, '2025-01-26', 1021, 220, 190, 86.4, 50, 170),
(22, '2025-01-26', 1022, 180, 155, 86.1, 40, 140);

-- dws_user_retention新增20条
INSERT INTO dws_user_retention PARTITION(dt='2025-01-26') VALUES
(3, '2025-01-26', 1003, 30, 25, 22, 18),
(4, '2025-01-26', 1004, 25, 21, 19, 16),
(5, '2025-01-26', 1005, 15, 12, 10, 8),
(6, '2025-01-26', 1006, 20, 17, 15, 13),
(7, '2025-01-26', 1007, 35, 30, 27, 23),
(8, '2025-01-26', 1008, 25, 22, 19, 16),
(9, '2025-01-26', 1009, 40, 35, 32, 28),
(10, '2025-01-26', 1010, 18, 15, 13, 11),
(11, '2025-01-26', 1011, 22, 19, 17, 14),
(12, '2025-01-26', 1012, 28, 24, 21, 18),
(13, '2025-01-26', 1013, 17, 14, 12, 10),
(14, '2025-01-26', 1014, 20, 17, 15, 13),
(15, '2025-01-26', 1015, 32, 27, 24, 20),
(16, '2025-01-26', 1016, 30, 26, 23, 20),
(17, '2025-01-26', 1017, 24, 20, 18, 15),
(18, '2025-01-26', 1018, 35, 30, 27, 23),
(19, '2025-01-26', 1019, 21, 18, 16, 13),
(20, '2025-01-26', 1020, 25, 21, 19, 16),
(21, '2025-01-26', 1021, 28, 24, 21, 18),
(22, '2025-01-26', 1022, 22, 19, 17, 14);

-- dws_conversion_rate新增20条
INSERT INTO dws_conversion_rate PARTITION(dt='2025-01-26') VALUES
(3, '2025-01-26', 1003, 14.8, 8.5, 5.1, 'ios'),
(4, '2025-01-26', 1004, 13.2, 7.9, 4.6, 'android'),
(5, '2025-01-26', 1005, 11.5, 6.8, 3.9, 'smart_watch'),
(6, '2025-01-26', 1006, 12.8, 7.5, 4.2, 'firefox_os'),
(7, '2025-01-26', 1007, 15.5, 9.2, 5.5, 'safari'),
(8, '2025-01-26', 1008, 14.2, 8.7, 5.0, 'firefox'),
(9, '2025-01-26', 1009, 16.1, 9.8, 5.9, 'opera'),
(10, '2025-01-26', 1010, 12.3, 7.2, 4.0, 'qq_browser'),
(11, '2025-01-26', 1011, 13.5, 8.0, 4.5, 'smart_tv'),
(12, '2025-01-26', 1012, 11.8, 6.5, 3.7, 'wearable'),
(13, '2025-01-26', 1013, 10.5, 5.9, 3.2, 'smart_watch'),
(14, '2025-01-26', 1014, 11.2, 6.2, 3.4, 'tablet'),
(15, '2025-01-26', 1015, 12.0, 6.8, 3.8, 'chrome_os'),
(16, '2025-01-26', 1016, 13.8, 8.1, 4.7, 'firefox_os'),
(17, '2025-01-26', 1017, 14.5, 8.5, 4.9, 'safari'),
(18, '2025-01-26', 1018, 13.2, 7.6, 4.3, 'chrome'),
(19, '2025-01-26', 1019, 12.7, 7.3, 4.1, 'firefox'),
(20, '2025-01-26', 1020, 14.0, 8.3, 4.8, 'edge'),
(21, '2025-01-26', 1021, 15.2, 8.9, 5.3, 'opera'),
(22, '2025-01-26', 1022, 13.0, 7.5, 4.2, 'uc');


-- ADS层各表新增20条数据

-- ads_entry_analysis新增20条
INSERT INTO ads_entry_analysis PARTITION(dt='2025-01-26') VALUES
(3, '2025-01-26', 'cart_page_1', 130, 45.5, 5.1, 'pc', 1001),
(4, '2025-01-26', 'checkout_page_1', 95, 75.2, 5.3, 'mac', 1001),
(5, '2025-01-26', 'user_center_1', 180, 120.3, 4.8, 'windows', 1002),
(6, '2025-01-26', 'order_list_1', 120, 60.1, 4.5, 'linux', 1002),
(7, '2025-01-26', 'payment_page_1', 105, 90.4, 5.0, 'smart_tv', 1003),
(8, '2025-01-26', 'promotion_page_1', 80, 30.2, 4.3, 'wearable', 1004),
(9, '2025-01-26', 'help_center_1', 90, 60.3, 3.8, 'smart_watch', 1005),
(10, '2025-01-26', 'about_us_1', 75, 45.1, 3.5, 'tablet', 1005),
(11, '2025-01-26', 'contact_us_1', 60, 30.0, 3.2, 'chrome_os', 1006),
(12, '2025-01-26', 'product_list_2', 140, 60.5, 4.6, 'firefox_os', 1006),
(13, '2025-01-26', 'search_result_2', 110, 90.3, 4.9, 'safari', 1007),
(14, '2025-01-26', 'cart_page_2', 125, 45.4, 4.7, 'chrome', 1007),
(15, '2025-01-26', 'checkout_page_2', 100, 75.1, 5.2, 'firefox', 1008),
(16, '2025-01-26', 'user_center_2', 170, 120.2, 4.6, 'edge', 1008),
(17, '2025-01-26', 'order_list_2', 115, 60.0, 4.2, 'opera', 1009),
(18, '2025-01-26', 'payment_page_2', 100, 90.3, 4.4, 'uc', 1009),
(19, '2025-01-26', 'promotion_page_2', 75, 30.1, 4.0, 'qq_browser', 1010),
(20, '2025-01-26', 'help_center_2', 85, 60.2, 3.7, 'baidu_browser', 1010),
(21, '2025-01-26', 'about_us_2', 70, 45.0, 3.4, 'ios', 1011),
(22, '2025-01-26', 'contact_us_2', 55, 30.0, 3.1, 'android', 1011);

-- ads_page_ranking新增20条
INSERT INTO ads_page_ranking PARTITION(dt='2025-01-26') VALUES
(4, '2025-01-26', 'cart_page_1', 'cart', 130, 110, 45.5, 4, 'pc', 1001),
(5, '2025-01-26', 'checkout_page_1', 'checkout', 95, 85, 75.2, 5, 'mac', 1001),
(6, '2025-01-26', 'user_center_1', 'center', 180, 160, 120.3, 6, 'windows', 1002),
(7, '2025-01-26', 'order_list_1', 'list', 120, 105, 60.1, 7, 'linux', 1002),
(8, '2025-01-26', 'payment_page_1', 'payment', 105, 95, 90.4, 8, 'smart_tv', 1003),
(9, '2025-01-26', 'promotion_page_1', 'promotion', 80, 70, 30.2, 9, 'wearable', 1004),
(10, '2025-01-26', 'help_center_1', 'help', 90, 80, 60.3, 10, 'smart_watch', 1005),
(11, '2025-01-26', 'about_us_1', 'about', 75, 65, 45.1, 11, 'tablet', 1005),
(12, '2025-01-26', 'contact_us_1', 'contact', 60, 50, 30.0, 12, 'chrome_os', 1006),
(13, '2025-01-26', 'product_list_2', 'list', 140, 120, 60.5, 13, 'firefox_os', 1006),
(14, '2025-01-26', 'search_result_2', 'result', 110, 95, 90.3, 14, 'safari', 1007),
(15, '2025-01-26', 'cart_page_2', 'cart', 125, 110, 45.4, 15, 'chrome', 1007),
(16, '2025-01-26', 'checkout_page_2', 'checkout', 100, 90, 75.1, 16, 'firefox', 1008),
(17, '2025-01-26', 'user_center_2', 'center', 170, 150, 120.2, 17, 'edge', 1008),
(18, '2025-01-26', 'order_list_2', 'list', 115, 100, 60.0, 18, 'opera', 1009),
(19, '2025-01-26', 'payment_page_2', 'payment', 100, 90, 90.3, 19, 'uc', 1009),
(20, '2025-01-26', 'promotion_page_2', 'promotion', 75, 65, 30.1, 20, 'qq_browser', 1010),
(21, '2025-01-26', 'help_center_2', 'help', 85, 75, 60.2, 21, 'baidu_browser', 1010),
(22, '2025-01-26', 'about_us_2', 'about', 70, 60, 45.0, 22, 'ios', 1011),
(23, '2025-01-26', 'contact_us_2', 'contact', 55, 45, 30.0, 23, 'android', 1011);

-- ads_path_conversion新增20条
INSERT INTO ads_path_conversion PARTITION(dt='2025-01-26') VALUES
(3, '2025-01-26', 'detail->cart', 4.8, 5, 'pc', 1001),
(4, '2025-01-26', 'cart->checkout', 5.3, 4, 'mac', 1001),
(5, '2025-01-26', 'home->user_center', 4.2, 6, 'windows', 1002),
(6, '2025-01-26', 'user_center->order_list', 4.5, 3, 'linux', 1002),
(7, '2025-01-26', 'checkout->payment', 5.0, 4, 'smart_tv', 1003),
(8, '2025-01-26', 'home->promotion', 3.9, 2, 'wearable', 1004),
(9, '2025-01-26', 'home->help', 3.2, 3, 'smart_watch', 1005),
(10, '2025-01-26', 'help->about', 3.5, 2, 'tablet', 1005),
(11, '2025-01-26', 'about->contact', 3.1, 2, 'chrome_os', 1006),
(12, '2025-01-26', 'category->product_list_2', 4.6, 5, 'firefox_os', 1006),
(13, '2025-01-26', 'home->search_result_2', 4.9, 6, 'safari', 1007),
(14, '2025-01-26', 'detail->cart_2', 4.7, 4, 'chrome', 1007),
(15, '2025-01-26', 'cart_2->checkout_2', 5.2, 3, 'firefox', 1008),
(16, '2025-01-26', 'home->user_center_2', 4.3, 5, 'edge', 1008),
(17, '2025-01-26', 'user_center_2->order_list_2', 4.2, 3, 'opera', 1009),
(18, '2025-01-26', 'checkout_2->payment_2', 4.4, 4, 'uc', 1009),
(19, '2025-01-26', 'home->promotion_2', 3.8, 2, 'qq_browser', 1010),
(20, '2025-01-26', 'home->help_2', 3.5, 3, 'baidu_browser', 1010),
(21, '2025-01-26', 'home->about_2', 3.2, 2, 'ios', 1011),
(22, '2025-01-26', 'about_2->contact_2', 3.0, 2, 'android', 1011);

-- ads_shop_path_analysis新增20条
INSERT INTO ads_shop_path_analysis PARTITION(dt='2025-01-26') VALUES
(3, '2025-01-26', 'product_detail_1', 'cart_page_1', 130, 78.0, 'pc', 1001),
(4, '2025-01-26', 'cart_page_1', 'checkout_page_1', 95, 65.0, 'mac', 1001),
(5, '2025-01-26', 'shop_home_1', 'user_center_1', 180, 62.0, 'windows', 1002),
(6, '2025-01-26', 'user_center_1', 'order_list_1', 120, 75.0, 'linux', 1002),
(7, '2025-01-26', 'checkout_page_1', 'payment_page_1', 105, 82.0, 'smart_tv', 1003),
(8, '2025-01-26', 'shop_home_1', 'promotion_page_1', 80, 55.0, 'wearable', 1004),
(9, '2025-01-26', 'shop_home_1', 'help_center_1', 90, 48.0, 'smart_watch', 1005),
(10, '2025-01-26', 'help_center_1', 'about_us_1', 75, 60.0, 'tablet', 1005),
(11, '2025-01-26', 'about_us_1', 'contact_us_1', 60, 52.0, 'chrome_os', 1006),
(12, '2025-01-26', 'shop_category_1', 'product_list_2', 140, 72.0, 'firefox_os', 1006),
(13, '2025-01-26', 'shop_home_1', 'search_result_2', 110, 68.0, 'safari', 1007),
(14, '2025-01-26', 'product_detail_1', 'cart_page_2', 125, 70.0, 'chrome', 1007),
(15, '2025-01-26', 'cart_page_2', 'checkout_page_2', 100, 65.0, 'firefox', 1008),
(16, '2025-01-26', 'shop_home_1', 'user_center_2', 170, 60.0, 'edge', 1008),
(17, '2025-01-26', 'user_center_2', 'order_list_2', 115, 72.0, 'opera', 1009),
(18, '2025-01-26', 'checkout_page_2', 'payment_page_2', 100, 78.0, 'uc', 1009),
(19, '2025-01-26', 'shop_home_1', 'promotion_page_2', 75, 58.0, 'qq_browser', 1010),
(20, '2025-01-26', 'shop_home_1', 'help_center_2', 85, 52.0, 'baidu_browser', 1010),
(21, '2025-01-26', 'shop_home_1', 'about_us_2', 70, 55.0, 'ios', 1011),
(22, '2025-01-26', 'about_us_2', 'contact_us_2', 55, 48.0, 'android', 1011);

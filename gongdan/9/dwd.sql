CREATE EXTERNAL TABLE dwd_page_visit_detail (
    detail_id BIGINT,
    log_id BIGINT,
    user_id BIGINT,
    session_id STRING,
    page_id STRING,
    page_type STRING,
    terminal_type STRING,
    refer_page STRING,
    event_time TIMESTAMP,
    stay_duration INT,
    shop_id INT,
    visit_date DATE
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dwd_page_visit_detail';

CREATE EXTERNAL TABLE dwd_user_behavior (
    behavior_id BIGINT,
    user_id BIGINT,
    session_id STRING,
    page_id STRING,
    action_type STRING,
    action_time TIMESTAMP,
    product_id BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dwd_user_behavior';

CREATE EXTERNAL TABLE dwd_visit_session (
    session_id STRING,
    user_id BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    page_count INT,
    shop_id INT,
    terminal_type STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dwd_visit_session';

CREATE EXTERNAL TABLE dwd_order_detail (
    order_detail_id BIGINT,
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    shop_id INT,
    order_time TIMESTAMP,
    quantity INT,
    price DECIMAL(10,2),
    payment_status TINYINT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dwd_order_detail';

CREATE EXTERNAL TABLE dwd_product_view (
    view_id BIGINT,
    product_id BIGINT,
    user_id BIGINT,
    view_time TIMESTAMP,
    duration INT,
    terminal_type STRING,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dwd_product_view';







INSERT INTO dwd_page_visit_detail PARTITION(dt='2025-01-26') VALUES
(6, 6, 1005, 'sess_1005_1', 'product_list_1', 'list', 'ios', 'shop_category_1', '2025-01-26 09:30:20', 60, 1003, '2025-01-26'),
(7, 7, 1006, 'sess_1006_1', 'search_result_1', 'result', 'android', 'shop_home_1', '2025-01-26 09:45:10', 90, 1004, '2025-01-26'),
(8, 8, 1007, 'sess_1007_1', 'cart_page_1', 'cart', 'pc', 'product_detail_1', '2025-01-26 10:00:05', 45, 1001, '2025-01-26'),
(9, 9, 1008, 'sess_1008_1', 'checkout_page_1', 'checkout', 'mac', 'cart_page_1', '2025-01-26 10:15:30', 75, 1001, '2025-01-26'),
(10, 10, 1009, 'sess_1009_1', 'user_center_1', 'center', 'windows', '', '2025-01-26 10:30:45', 120, 1002, '2025-01-26'),
(11, 11, 1010, 'sess_1010_1', 'order_list_1', 'list', 'linux', 'user_center_1', '2025-01-26 10:45:20', 60, 1002, '2025-01-26'),
(12, 12, 1011, 'sess_1011_1', 'payment_page_1', 'payment', 'smart_tv', 'checkout_page_1', '2025-01-26 11:00:10', 90, 1003, '2025-01-26'),
(13, 13, 1012, 'sess_1012_1', 'promotion_page_1', 'promotion', 'wearable', 'shop_home_1', '2025-01-26 11:15:30', 30, 1004, '2025-01-26'),
(14, 14, 1013, 'sess_1013_1', 'help_center_1', 'help', 'smart_watch', '', '2025-01-26 11:30:45', 60, 1005, '2025-01-26'),
(15, 15, 1014, 'sess_1014_1', 'about_us_1', 'about', 'tablet', 'help_center_1', '2025-01-26 11:45:20', 45, 1005, '2025-01-26'),
(16, 16, 1015, 'sess_1015_1', 'contact_us_1', 'contact', 'chrome_os', 'about_us_1', '2025-01-26 12:00:10', 30, 1006, '2025-01-26'),
(17, 17, 1016, 'sess_1016_1', 'product_list_2', 'list', 'firefox_os', 'shop_category_1', '2025-01-26 12:15:30', 60, 1006, '2025-01-26'),
(18, 18, 1017, 'sess_1017_1', 'search_result_2', 'result', 'safari', 'shop_home_1', '2025-01-26 12:30:45', 90, 1007, '2025-01-26'),
(19, 19, 1018, 'sess_1018_1', 'cart_page_2', 'cart', 'chrome', 'product_detail_1', '2025-01-26 12:45:20', 45, 1007, '2025-01-26'),
(20, 20, 1019, 'sess_1019_1', 'checkout_page_2', 'checkout', 'firefox', 'cart_page_2', '2025-01-26 13:00:10', 75, 1008, '2025-01-26'),
(21, 21, 1020, 'sess_1020_1', 'user_center_2', 'center', 'edge', '', '2025-01-26 13:15:30', 120, 1008, '2025-01-26'),
(22, 22, 1021, 'sess_1021_1', 'order_list_2', 'list', 'opera', 'user_center_2', '2025-01-26 13:30:45', 60, 1009, '2025-01-26'),
(23, 23, 1022, 'sess_1022_1', 'payment_page_2', 'payment', 'uc', 'checkout_page_2', '2025-01-26 13:45:20', 90, 1009, '2025-01-26'),
(24, 24, 1023, 'sess_1023_1', 'promotion_page_2', 'promotion', 'qq_browser', 'shop_home_1', '2025-01-26 14:00:10', 30, 1010, '2025-01-26'),
(25, 25, 1024, 'sess_1024_1', 'help_center_2', 'help', 'baidu_browser', '', '2025-01-26 14:15:30', 60, 1010, '2025-01-26');

-- dwd_user_behavior新增20条
INSERT INTO dwd_user_behavior PARTITION(dt='2025-01-26') VALUES
(6, 1005, 'sess_1005_1', 'product_list_1', 'view', '2025-01-26 09:30:20', 5003),
(7, 1005, 'sess_1005_1', 'product_list_1', 'add_cart', '2025-01-26 09:32:10', 5003),
(8, 1006, 'sess_1006_1', 'search_result_1', 'view', '2025-01-26 09:45:10', 5004),
(9, 1006, 'sess_1006_1', 'search_result_1', 'click', '2025-01-26 09:47:30', 5004),
(10, 1007, 'sess_1007_1', 'cart_page_1', 'add_cart', '2025-01-26 10:00:05', 5001),
(11, 1008, 'sess_1008_1', 'checkout_page_1', 'purchase', '2025-01-26 10:15:30', 5001),
(12, 1009, 'sess_1009_1', 'user_center_1', 'login', '2025-01-26 10:30:45', NULL),
(13, 1010, 'sess_1010_1', 'order_list_1', 'view', '2025-01-26 10:45:20', 9002),
(14, 1011, 'sess_1011_1', 'payment_page_1', 'pay', '2025-01-26 11:00:10', 9003),
(15, 1012, 'sess_1012_1', 'promotion_page_1', 'view', '2025-01-26 11:15:30', NULL),
(16, 1013, 'sess_1013_1', 'help_center_1', 'view', '2025-01-26 11:30:45', NULL),
(17, 1014, 'sess_1014_1', 'about_us_1', 'view', '2025-01-26 11:45:20', NULL),
(18, 1015, 'sess_1015_1', 'contact_us_1', 'submit', '2025-01-26 12:00:10', NULL),
(19, 1016, 'sess_1016_1', 'product_list_2', 'view', '2025-01-26 12:15:30', 5006),
(20, 1016, 'sess_1016_1', 'product_list_2', 'add_cart', '2025-01-26 12:17:30', 5006),
(21, 1017, 'sess_1017_1', 'search_result_2', 'view', '2025-01-26 12:30:45', 5007),
(22, 1018, 'sess_1018_1', 'cart_page_2', 'add_cart', '2025-01-26 12:45:20', 5007),
(23, 1019, 'sess_1019_1', 'checkout_page_2', 'purchase', '2025-01-26 13:00:10', 5008),
(24, 1020, 'sess_1020_1', 'user_center_2', 'login', '2025-01-26 13:15:30', NULL),
(25, 1021, 'sess_1021_1', 'order_list_2', 'view', '2025-01-26 13:30:45', 9008);

-- dwd_visit_session新增20条
INSERT INTO dwd_visit_session PARTITION(dt='2025-01-26') VALUES
('sess_1005_1', 1005, '2025-01-26 09:30:20', '2025-01-26 09:45:30', 6, 1003, 'ios'),
('sess_1006_1', 1006, '2025-01-26 09:45:10', '2025-01-26 10:00:45', 9, 1004, 'android'),
('sess_1007_1', 1007, '2025-01-26 10:00:05', '2025-01-26 10:15:30', 5, 1001, 'pc'),
('sess_1008_1', 1008, '2025-01-26 10:15:30', '2025-01-26 10:30:45', 7, 1001, 'mac'),
('sess_1009_1', 1009, '2025-01-26 10:30:45', '2025-01-26 10:45:30', 8, 1002, 'windows'),
('sess_1010_1', 1010, '2025-01-26 10:45:20', '2025-01-26 11:00:45', 6, 1002, 'linux'),
('sess_1011_1', 1011, '2025-01-26 11:00:10', '2025-01-26 11:15:30', 9, 1003, 'smart_tv'),
('sess_1012_1', 1012, '2025-01-26 11:15:30', '2025-01-26 11:30:45', 4, 1004, 'wearable'),
('sess_1013_1', 1013, '2025-01-26 11:30:45', '2025-01-26 11:45:30', 6, 1005, 'smart_watch'),
('sess_1014_1', 1014, '2025-01-26 11:45:20', '2025-01-26 12:00:45', 5, 1005, 'tablet'),
('sess_1015_1', 1015, '2025-01-26 12:00:10', '2025-01-26 12:15:30', 4, 1006, 'chrome_os'),
('sess_1016_1', 1016, '2025-01-26 12:15:30', '2025-01-26 12:30:45', 6, 1006, 'firefox_os'),
('sess_1017_1', 1017, '2025-01-26 12:30:45', '2025-01-26 12:45:30', 9, 1007, 'safari'),
('sess_1018_1', 1018, '2025-01-26 12:45:20', '2025-01-26 13:00:45', 5, 1007, 'chrome'),
('sess_1019_1', 1019, '2025-01-26 13:00:10', '2025-01-26 13:15:30', 7, 1008, 'firefox'),
('sess_1020_1', 1020, '2025-01-26 13:15:30', '2025-01-26 13:30:45', 8, 1008, 'edge'),
('sess_1021_1', 1021, '2025-01-26 13:30:45', '2025-01-26 13:45:30', 6, 1009, 'opera'),
('sess_1022_1', 1022, '2025-01-26 13:45:20', '2025-01-26 14:00:45', 9, 1009, 'uc'),
('sess_1023_1', 1023, '2025-01-26 14:00:10', '2025-01-26 14:15:30', 4, 1010, 'qq_browser'),
('sess_1024_1', 1024, '2025-01-26 14:15:30', '2025-01-26 14:30:45', 6, 1010, 'baidu_browser');

-- dwd_order_detail新增20条
INSERT INTO dwd_order_detail PARTITION(dt='2025-01-26') VALUES
(5, 9005, 1005, 5003, 1003, '2025-01-26 09:45:30', 1, 129.99, 1),
(6, 9006, 1006, 5004, 1004, '2025-01-26 10:00:45', 1, 89.99, 1),
(7, 9007, 1007, 5001, 1001, '2025-01-26 10:15:30', 1, 249.99, 1),
(8, 9008, 1008, 5001, 1001, '2025-01-26 10:30:45', 1, 349.99, 1),
(9, 9009, 1009, 5002, 1002, '2025-01-26 10:45:30', 1, 799.99, 1),
(10, 9010, 1010, 5002, 1002, '2025-01-26 11:00:45', 1, 699.99, 1),
(11, 9011, 1011, 5003, 1003, '2025-01-26 11:15:30', 1, 159.99, 1),
(12, 9012, 1012, 5004, 1004, '2025-01-26 11:30:45', 1, 49.99, 1),
(13, 9013, 1013, 5005, 1005, '2025-01-26 11:45:30', 1, 29.99, 1),
(14, 9014, 1014, 5005, 1005, '2025-01-26 12:00:45', 1, 19.99, 1),
(15, 9015, 1015, 5006, 1006, '2025-01-26 12:15:30', 1, 39.99, 1),
(16, 9016, 1016, 5006, 1006, '2025-01-26 12:30:45', 1, 119.99, 1),
(17, 9017, 1017, 5007, 1007, '2025-01-26 12:45:30', 1, 249.99, 1),
(18, 9018, 1018, 5007, 1007, '2025-01-26 13:00:45', 1, 89.99, 1),
(19, 9019, 1019, 5008, 1008, '2025-01-26 13:15:30', 1, 59.99, 1),
(20, 9020, 1020, 5008, 1008, '2025-01-26 13:30:45', 1, 179.99, 1),
(21, 9021, 1021, 5009, 1009, '2025-01-26 13:45:30', 1, 499.99, 1),
(22, 9022, 1022, 5009, 1009, '2025-01-26 14:00:45', 1, 349.99, 1),
(23, 9023, 1023, 5010, 1010, '2025-01-26 14:15:30', 1, 69.99, 1),
(24, 9024, 1024, 5010, 1010, '2025-01-26 14:30:45', 1, 89.99, 1);

-- dwd_product_view新增20条
INSERT INTO dwd_product_view PARTITION(dt='2025-01-26') VALUES
(3, 5003, 1005, '2025-01-26 09:30:20', 120, 'ios', 1003),
(4, 5004, 1006, '2025-01-26 09:45:10', 180, 'android', 1004),
(5, 5001, 1007, '2025-01-26 10:00:05', 90, 'pc', 1001),
(6, 5001, 1008, '2025-01-26 10:15:30', 150, 'mac', 1001),
(7, 5002, 1009, '2025-01-26 10:30:45', 210, 'windows', 1002),
(8, 5002, 1010, '2025-01-26 10:45:20', 180, 'linux', 1002),
(9, 5003, 1011, '2025-01-26 11:00:10', 120, 'smart_tv', 1003),
(10, 5004, 1012, '2025-01-26 11:15:30', 60, 'wearable', 1004),
(11, 5005, 1013, '2025-01-26 11:30:45', 120, 'smart_watch', 1005),
(12, 5005, 1014, '2025-01-26 11:45:20', 90, 'tablet', 1005),
(13, 5006, 1015, '2025-01-26 12:00:10', 60, 'chrome_os', 1006),
(14, 5006, 1016, '2025-01-26 12:15:30', 120, 'firefox_os', 1006),
(15, 5007, 1017, '2025-01-26 12:30:45', 180, 'safari', 1007),
(16, 5007, 1018, '2025-01-26 12:45:20', 90, 'chrome', 1007),
(17, 5008, 1019, '2025-01-26 13:00:10', 150, 'firefox', 1008),
(18, 5008, 1020, '2025-01-26 13:30:45', 210, 'edge', 1008),
(19, 5009, 1021, '2025-01-26 13:45:30', 180, 'opera', 1009),
(20, 5009, 1022, '2025-01-26 14:00:45', 150, 'uc', 1009),
(21, 5010, 1023, '2025-01-26 14:15:30', 60, 'qq_browser', 1010),
(22, 5010, 1024, '2025-01-26 14:30:45', 120, 'baidu_browser', 1010);


-- DWS层各表新增20条数据
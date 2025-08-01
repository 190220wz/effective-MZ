
CREATE EXTERNAL TABLE ods_page_log (
    log_id BIGINT,
    user_id BIGINT,
    session_id STRING,
    page_id STRING,
    page_type STRING,
    terminal_type STRING,
    refer_page STRING,
    event_time TIMESTAMP,
    stay_duration INT,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ods_page_log';

CREATE EXTERNAL TABLE ods_shop_visit (
    visit_id BIGINT,
    shop_id INT,
    user_id BIGINT,
    entry_time TIMESTAMP,
    exit_time TIMESTAMP,
    total_pages INT,
    terminal_type STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ods_shop_visit';

CREATE EXTERNAL TABLE ods_user_info (
    user_id BIGINT,
    user_name STRING,
    reg_date DATE,
    last_login TIMESTAMP,
    device_type STRING
)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ods_user_info';

CREATE EXTERNAL TABLE ods_product_info (
    product_id BIGINT,
    product_name STRING,
    category_id INT,
    shop_id INT,
    online_date DATE
)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ods_product_info';

CREATE EXTERNAL TABLE ods_order_info (
    order_id BIGINT,
    user_id BIGINT,
    shop_id INT,
    order_time TIMESTAMP,
    order_amount DECIMAL(10,2),
    payment_status TINYINT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/ods_order_info';

INSERT INTO ods_page_log PARTITION(dt='2025-01-26') VALUES
(6, 1005, 'sess_1005_1', 'product_list_1', 'list', 'ios', 'shop_category_1', '2025-01-26 09:30:20', 60, 1003),
(7, 1006, 'sess_1006_1', 'search_result_1', 'result', 'android', 'shop_home_1', '2025-01-26 09:45:10', 90, 1004),
(8, 1007, 'sess_1007_1', 'cart_page_1', 'cart', 'pc', 'product_detail_1', '2025-01-26 10:00:05', 45, 1001),
(9, 1008, 'sess_1008_1', 'checkout_page_1', 'checkout', 'mac', 'cart_page_1', '2025-01-26 10:15:30', 75, 1001),
(10, 1009, 'sess_1009_1', 'user_center_1', 'center', 'windows', '', '2025-01-26 10:30:45', 120, 1002),
(11, 1010, 'sess_1010_1', 'order_list_1', 'list', 'linux', 'user_center_1', '2025-01-26 10:45:20', 60, 1002),
(12, 1011, 'sess_1011_1', 'payment_page_1', 'payment', 'smart_tv', 'checkout_page_1', '2025-01-26 11:00:10', 90, 1003),
(13, 1012, 'sess_1012_1', 'promotion_page_1', 'promotion', 'wearable', 'shop_home_1', '2025-01-26 11:15:30', 30, 1004),
(14, 1013, 'sess_1013_1', 'help_center_1', 'help', 'smart_watch', '', '2025-01-26 11:30:45', 60, 1005),
(15, 1014, 'sess_1014_1', 'about_us_1', 'about', 'tablet', 'help_center_1', '2025-01-26 11:45:20', 45, 1005),
(16, 1015, 'sess_1015_1', 'contact_us_1', 'contact', 'chrome_os', 'about_us_1', '2025-01-26 12:00:10', 30, 1006),
(17, 1016, 'sess_1016_1', 'product_list_2', 'list', 'firefox_os', 'shop_category_1', '2025-01-26 12:15:30', 60, 1006),
(18, 1017, 'sess_1017_1', 'search_result_2', 'result', 'safari', 'shop_home_1', '2025-01-26 12:30:45', 90, 1007),
(19, 1018, 'sess_1018_1', 'cart_page_2', 'cart', 'chrome', 'product_detail_1', '2025-01-26 12:45:20', 45, 1007),
(20, 1019, 'sess_1019_1', 'checkout_page_2', 'checkout', 'firefox', 'cart_page_2', '2025-01-26 13:00:10', 75, 1008),
(21, 1020, 'sess_1020_1', 'user_center_2', 'center', 'edge', '', '2025-01-26 13:15:30', 120, 1008),
(22, 1021, 'sess_1021_1', 'order_list_2', 'list', 'opera', 'user_center_2', '2025-01-26 13:30:45', 60, 1009),
(23, 1022, 'sess_1022_1', 'payment_page_2', 'payment', 'uc', 'checkout_page_2', '2025-01-26 13:45:20', 90, 1009),
(24, 1023, 'sess_1023_1', 'promotion_page_2', 'promotion', 'qq_browser', 'shop_home_1', '2025-01-26 14:00:10', 30, 1010),
(25, 1024, 'sess_1024_1', 'help_center_2', 'help', 'baidu_browser', '', '2025-01-26 14:15:30', 60, 1010);

-- ods_shop_visit新增20条
INSERT INTO ods_shop_visit PARTITION(dt='2025-01-26') VALUES
(5, 1005, 1003, '2025-01-26 09:25:00', '2025-01-26 09:50:00', 6, 'ios'),
(6, 1006, 1004, '2025-01-26 09:40:00', '2025-01-26 10:05:00', 9, 'android'),
(7, 1007, 1001, '2025-01-26 09:55:00', '2025-01-26 10:20:00', 5, 'pc'),
(8, 1008, 1001, '2025-01-26 10:10:00', '2025-01-26 10:35:00', 7, 'mac'),
(9, 1009, 1002, '2025-01-26 10:25:00', '2025-01-26 10:50:00', 8, 'windows'),
(10, 1010, 1002, '2025-01-26 10:40:00', '2025-01-26 11:05:00', 6, 'linux'),
(11, 1011, 1003, '2025-01-26 10:55:00', '2025-01-26 11:20:00', 9, 'smart_tv'),
(12, 1012, 1004, '2025-01-26 11:10:00', '2025-01-26 11:35:00', 4, 'wearable'),
(13, 1013, 1005, '2025-01-26 11:25:00', '2025-01-26 11:50:00', 6, 'smart_watch'),
(14, 1014, 1005, '2025-01-26 11:40:00', '2025-01-26 12:05:00', 5, 'tablet'),
(15, 1015, 1006, '2025-01-26 11:55:00', '2025-01-26 12:20:00', 4, 'chrome_os'),
(16, 1016, 1006, '2025-01-26 12:10:00', '2025-01-26 12:35:00', 6, 'firefox_os'),
(17, 1017, 1007, '2025-01-26 12:25:00', '2025-01-26 12:50:00', 9, 'safari'),
(18, 1018, 1007, '2025-01-26 12:40:00', '2025-01-26 13:05:00', 5, 'chrome'),
(19, 1019, 1008, '2025-01-26 12:55:00', '2025-01-26 13:20:00', 7, 'firefox'),
(20, 1020, 1008, '2025-01-26 13:10:00', '2025-01-26 13:35:00', 8, 'edge'),
(21, 1021, 1009, '2025-01-26 13:25:00', '2025-01-26 13:50:00', 6, 'opera'),
(22, 1022, 1009, '2025-01-26 13:40:00', '2025-01-26 14:05:00', 9, 'uc'),
(23, 1023, 1010, '2025-01-26 13:55:00', '2025-01-26 14:20:00', 4, 'qq_browser'),
(24, 1024, 1010, '2025-01-26 14:10:00', '2025-01-26 14:35:00', 6, 'baidu_browser');

-- ods_user_info新增20条
INSERT INTO ods_user_info VALUES
(1006, 'user_1006', '2024-12-10', '2025-01-26 09:30:00', 'Android'),
(1007, 'user_1007', '2024-12-15', '2025-01-26 09:45:00', 'iOS'),
(1008, 'user_1008', '2024-12-20', '2025-01-26 10:00:00', 'Windows'),
(1009, 'user_1009', '2024-12-25', '2025-01-26 10:15:00', 'macOS'),
(1010, 'user_1010', '2024-12-30', '2025-01-26 10:30:00', 'Linux'),
(1011, 'user_1011', '2025-01-01', '2025-01-26 10:45:00', 'Android'),
(1012, 'user_1012', '2025-01-05', '2025-01-26 11:00:00', 'iOS'),
(1013, 'user_1013', '2025-01-10', '2025-01-26 11:15:00', 'Windows'),
(1014, 'user_1014', '2025-01-15', '2025-01-26 11:30:00', 'macOS'),
(1015, 'user_1015', '2025-01-20', '2025-01-26 11:45:00', 'Linux'),
(1016, 'user_1016', '2024-11-05', '2025-01-25 12:00:00', 'Android'),
(1017, 'user_1017', '2024-11-10', '2025-01-25 12:15:00', 'iOS'),
(1018, 'user_1018', '2024-11-15', '2025-01-25 12:30:00', 'Windows'),
(1019, 'user_1019', '2024-11-20', '2025-01-25 12:45:00', 'macOS'),
(1020, 'user_1020', '2024-11-25', '2025-01-25 13:00:00', 'Linux'),
(1021, 'user_1021', '2024-10-05', '2025-01-24 13:15:00', 'Android'),
(1022, 'user_1022', '2024-10-10', '2025-01-24 13:30:00', 'iOS'),
(1023, 'user_1023', '2024-10-15', '2025-01-24 13:45:00', 'Windows'),
(1024, 'user_1024', '2024-10-20', '2025-01-24 14:00:00', 'macOS'),
(1025, 'user_1025', '2024-10-25', '2025-01-24 14:15:00', 'Linux');

-- ods_product_info新增20条
INSERT INTO ods_product_info VALUES
(5006, 'Novel Collection', 601, 1006, '2024-12-25'),
(5007, 'Educational Toys', 701, 1007, '2025-01-01'),
(5008, 'Organic Snacks', 801, 1008, '2025-01-05'),
(5009, 'Dog Food', 901, 1009, '2025-01-10'),
(5010, 'Gold Necklace', 1001, 1010, '2025-01-15'),
(5011, 'Sofa Set', 1101, 1011, '2025-01-20'),
(5012, 'Running Shoes', 1201, 1012, '2025-01-25'),
(5013, 'Leather Bag', 1301, 1013, '2024-12-10'),
(5014, 'Lipstick Set', 1401, 1014, '2024-12-15'),
(5015, 'Tennis Racket', 1501, 1015, '2024-12-20'),
(5016, 'Digital Camera', 1601, 1016, '2024-12-25'),
(5017, 'Wireless Headphones', 1701, 1017, '2025-01-01'),
(5018, 'Oil Painting', 1801, 1018, '2025-01-05'),
(5019, 'Roses Bouquet', 1901, 1019, '2025-01-10'),
(5020, 'Notebook Set', 2001, 1020, '2025-01-15'),
(5021, 'Luxury Watch', 2101, 1021, '2025-01-20'),
(5022, 'Travel Luggage', 2201, 1022, '2025-01-25'),
(5023, 'Perfume Gift Set', 2301, 1023, '2024-12-10'),
(5024, 'Designer Sunglasses', 2401, 1024, '2024-12-15'),
(5025, 'Woolen Hat', 2501, 1025, '2024-12-20');

-- ods_order_info新增20条
INSERT INTO ods_order_info PARTITION(dt='2025-01-26') VALUES
(9005, 1005, 1003, '2025-01-26 09:45:30', 129.99, 1),
(9006, 1006, 1004, '2025-01-26 10:00:45', 89.99, 1),
(9007, 1007, 1001, '2025-01-26 10:15:30', 249.99, 1),
(9008, 1008, 1001, '2025-01-26 10:30:45', 349.99, 1),
(9009, 1009, 1002, '2025-01-26 10:45:30', 799.99, 1),
(9010, 1010, 1002, '2025-01-26 11:00:45', 699.99, 1),
(9011, 1011, 1003, '2025-01-26 11:15:30', 159.99, 1),
(9012, 1012, 1004, '2025-01-26 11:30:45', 49.99, 1),
(9013, 1013, 1005, '2025-01-26 11:45:30', 29.99, 1),
(9014, 1014, 1005, '2025-01-26 12:00:45', 19.99, 1),
(9015, 1015, 1006, '2025-01-26 12:15:30', 39.99, 1),
(9016, 1016, 1006, '2025-01-26 12:30:45', 119.99, 1),
(9017, 1017, 1007, '2025-01-26 12:45:30', 249.99, 1),
(9018, 1018, 1007, '2025-01-26 13:00:45', 89.99, 1),
(9019, 1019, 1008, '2025-01-26 13:15:30', 59.99, 1),
(9020, 1020, 1008, '2025-01-26 13:30:45', 179.99, 1),
(9021, 1021, 1009, '2025-01-26 13:45:30', 499.99, 1),
(9022, 1022, 1009, '2025-01-26 14:00:45', 349.99, 1),
(9023, 1023, 1010, '2025-01-26 14:15:30', 69.99, 1),
(9024, 1024, 1010, '2025-01-26 14:30:45', 89.99, 1);


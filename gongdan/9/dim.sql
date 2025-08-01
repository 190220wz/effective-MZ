
CREATE EXTERNAL TABLE dim_page (
    page_id STRING,
    page_name STRING,
    page_category STRING,
    page_type STRING,
    is_shop_page TINYINT
)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dim_page';

CREATE EXTERNAL TABLE dim_terminal (
    terminal_id STRING,
    terminal_name STRING,
    is_mobile TINYINT
)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dim_terminal';

CREATE EXTERNAL TABLE dim_date (
    date_id DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    week INT,
    is_weekend TINYINT
)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dim_date';

CREATE EXTERNAL TABLE dim_user (
    user_id BIGINT,
    user_name STRING,
    reg_date DATE,
    user_level TINYINT,
    last_active DATE
)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dim_user';

CREATE EXTERNAL TABLE dim_shop (
    shop_id INT,
    shop_name STRING,
    category STRING,
    create_date DATE,
    status TINYINT
)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dim_shop';

-- 1. 页面维度表（dim_page）
INSERT INTO dim_page VALUES
('shop_home_1', 'Home Page 1', 'shop', 'home', 1),
('shop_home_2', 'Home Page 2', 'shop', 'home', 1),
('shop_home_3', 'Home Page 3', 'shop', 'home', 1),
('product_detail_1', 'Product Detail 1', 'product', 'detail', 0),
('product_detail_2', 'Product Detail 2', 'product', 'detail', 0),
('product_detail_3', 'Product Detail 3', 'product', 'detail', 0),
('product_detail_4', 'Product Detail 4', 'product', 'detail', 0),
('product_detail_5', 'Product Detail 5', 'product', 'detail', 0),
('other_live_1', 'Live Page 1', 'other', 'live', 0),
('other_live_2', 'Live Page 2', 'other', 'live', 0),
('other_live_3', 'Live Page 3', 'other', 'live', 0),
('shop_category_1', 'Category Page 1', 'shop', 'category', 1),
('shop_category_2', 'Category Page 2', 'shop', 'category', 1),
('shop_category_3', 'Category Page 3', 'shop', 'category', 1),
('shop_category_4', 'Category Page 4', 'shop', 'category', 1),
('other_subscribe_1', 'Subscribe Page 1', 'other', 'subscribe', 0),
('other_subscribe_2', 'Subscribe Page 2', 'other', 'subscribe', 0),
('shop_activity_1', 'Activity Page 1', 'shop', 'activity', 1),
('shop_activity_2', 'Activity Page 2', 'shop', 'activity', 1),
('shop_new_1', 'New Arrivals Page 1', 'shop', 'new', 1);

-- 2. 终端维度表（dim_terminal）
INSERT INTO dim_terminal VALUES
('pc', 'PC', 0),
('mobile', 'Mobile', 1),
('tablet', 'Tablet', 1),
('pc_mac', 'Mac PC', 0),
('mobile_ios', 'iOS Mobile', 1),
('mobile_android', 'Android Mobile', 1),
('pad_ios', 'iOS Pad', 1),
('pad_android', 'Android Pad', 1);

-- 3. 日期维度表（dim_date）
INSERT INTO dim_date VALUES
('2025-01-01', 2025, 1, 1, 1, 1, 0),
('2025-01-02', 2025, 1, 2, 1, 2, 0),
('2025-01-03', 2025, 1, 3, 1, 3, 0),
('2025-01-04', 2025, 1, 4, 1, 4, 0),
('2025-01-05', 2025, 1, 5, 1, 5, 0),
('2025-01-06', 2025, 1, 6, 1, 6, 0),
('2025-01-07', 2025, 1, 7, 1, 7, 0),
('2025-01-08', 2025, 1, 8, 1, 1, 0),
('2025-01-09', 2025, 1, 9, 1, 2, 0),
('2025-01-10', 2025, 1, 10, 1, 3, 0),
('2025-01-11', 2025, 1, 11, 1, 4, 0),
('2025-01-12', 2025, 1, 12, 1, 5, 0),
('2025-01-13', 2025, 1, 13, 1, 6, 0),
('2025-01-14', 2025, 1, 14, 1, 7, 0),
('2025-01-15', 2025, 1, 15, 1, 1, 0),
('2025-01-16', 2025, 1, 16, 1, 2, 0),
('2025-01-17', 2025, 1, 17, 1, 3, 0),
('2025-01-18', 2025, 1, 18, 1, 4, 0),
('2025-01-19', 2025, 1, 19, 1, 5, 0),
('2025-01-26', 2025, 1, 26, 1, 4, 0);

-- 4. 用户维度表（dim_user）
INSERT INTO dim_user VALUES
(1001, 'user_1001', '2024-03-15', 3, '2025-01-20'),
(1002, 'user_1002', '2024-05-22', 2, '2025-01-26'),
(1003, 'user_1003', '2024-11-30', 4, '2025-01-25'),
(1004, 'user_1004', '2024-01-10', 1, '2025-01-26'),
(1005, 'user_1005', '2024-02-18', 2, '2025-01-25'),
(1006, 'user_1006', '2024-04-05', 3, '2025-01-24'),
(1007, 'user_1007', '2024-06-12', 2, '2025-01-26'),
(1008, 'user_1008', '2024-07-20', 4, '2025-01-23'),
(1009, 'user_1009', '2024-08-08', 1, '2025-01-26'),
(1010, 'user_1010', '2024-09-15', 3, '2025-01-22'),
(1011, 'user_1011', '2024-10-22', 2, '2025-01-26'),
(1012, 'user_1012', '2024-12-05', 4, '2025-01-21'),
(1013, 'user_1013', '2025-01-10', 1, '2025-01-26'),
(1014, 'user_1014', '2025-01-15', 2, '2025-01-26'),
(1015, 'user_1015', '2024-03-20', 3, '2025-01-20'),
(1016, 'user_1016', '2024-05-28', 2, '2025-01-25'),
(1017, 'user_1017', '2024-08-30', 4, '2025-01-24'),
(1018, 'user_1018', '2024-11-15', 1, '2025-01-26'),
(1019, 'user_1019', '2024-12-20', 2, '2025-01-23'),
(1020, 'user_1020', '2025-01-05', 3, '2025-01-26');

-- 5. 店铺维度表（dim_shop）
INSERT INTO dim_shop VALUES
(1001, 'Fashion Shop', 'Fashion', '2023-01-15', 1),
(1002, 'Electronics Store', 'Electronics', '2023-03-20', 1),
(1003, 'Home Decor', 'Home', '2023-06-10', 1),
(1004, 'Sports Gear', 'Sports', '2023-02-25', 1),
(1005, 'Beauty Store', 'Beauty', '2023-04-15', 1),
(1006, 'Book Shop', 'Books', '2023-05-30', 1),
(1007, 'Toy Store', 'Toys', '2023-07-05', 1),
(1008, 'Grocery Mart', 'Grocery', '2023-08-20', 1),
(1009, 'Jewelry Shop', 'Jewelry', '2023-09-10', 1),
(1010, 'Pet Store', 'Pets', '2023-10-15', 1),
(1011, 'Furniture Shop', 'Furniture', '2023-11-20', 1),
(1012, 'Shoe Store', 'Footwear', '2023-12-05', 1),
(1013, 'Bag Shop', 'Accessories', '2024-01-15', 1),
(1014, 'Watch Store', 'Watches', '2024-02-20', 1),
(1015, 'Plant Shop', 'Plants', '2024-03-10', 1),
(1016, 'Camera Store', 'Electronics', '2024-04-25', 1),
(1017, 'Music Shop', 'Music', '2024-05-15', 1),
(1018, 'Art Gallery', 'Art', '2024-06-30', 1),
(1019, 'Stationery Shop', 'Stationery', '2024-07-20', 1),
(1020, 'Car Accessories', 'Automotive', '2024-08-10', 1);

INSERT INTO dim_page VALUES
('product_list_2', 'Product List 2', 'product', 'list', 1),
('product_list_3', 'Product List 3', 'product', 'list', 1),
('search_result_2', 'Search Result 2', 'search', 'result', 0),
('search_result_3', 'Search Result 3', 'search', 'result', 0),
('cart_page_1', 'Cart Page 1', 'order', 'cart', 0),
('cart_page_2', 'Cart Page 2', 'order', 'cart', 0),
('checkout_page_1', 'Checkout Page 1', 'order', 'checkout', 0),
('checkout_page_2', 'Checkout Page 2', 'order', 'checkout', 0),
('user_center_1', 'User Center 1', 'user', 'center', 1),
('user_center_2', 'User Center 2', 'user', 'center', 1),
('order_list_1', 'Order List 1', 'order', 'list', 1),
('order_list_2', 'Order List 2', 'order', 'list', 1),
('payment_page_1', 'Payment Page 1', 'order', 'payment', 0),
('payment_page_2', 'Payment Page 2', 'order', 'payment', 0),
('promotion_page_1', 'Promotion Page 1', 'marketing', 'promotion', 1),
('promotion_page_2', 'Promotion Page 2', 'marketing', 'promotion', 1),
('help_center_1', 'Help Center 1', 'service', 'help', 1),
('help_center_2', 'Help Center 2', 'service', 'help', 1),
('about_us_1', 'About Us 1', 'company', 'about', 1),
('contact_us_1', 'Contact Us 1', 'company', 'contact', 1);

-- dim_terminal新增20条
INSERT INTO dim_terminal VALUES
('ios', 'iOS', 1),
('android', 'Android', 1),
('ipad', 'iPad', 1),
('mac', 'Mac', 0),
('windows', 'Windows', 0),
('linux', 'Linux', 0),
('smart_tv', 'Smart TV', 0),
('wearable', 'Wearable Device', 1),
('smart_watch', 'Smart Watch', 1),
('tablet', 'Tablet', 1),
('chrome_os', 'Chrome OS', 0),
('firefox_os', 'Firefox OS', 0),
('safari', 'Safari Browser', 0),
('chrome', 'Chrome Browser', 0),
('firefox', 'Firefox Browser', 0),
('edge', 'Edge Browser', 0),
('opera', 'Opera Browser', 0),
('uc', 'UC Browser', 0),
('qq_browser', 'QQ Browser', 0),
('baidu_browser', 'Baidu Browser', 0);

-- dim_date新增20条
INSERT INTO dim_date VALUES
('2025-01-02', 2025, 1, 2, 4, 1, 0),
('2025-01-03', 2025, 1, 3, 5, 1, 0),
('2025-01-04', 2025, 1, 4, 6, 1, 0),
('2025-01-05', 2025, 1, 5, 0, 1, 0),
('2025-01-06', 2025, 1, 6, 1, 1, 0),
('2025-01-07', 2025, 1, 7, 2, 1, 0),
('2025-01-08', 2025, 1, 8, 3, 1, 0),
('2025-01-09', 2025, 1, 9, 4, 1, 0),
('2025-01-10', 2025, 1, 10, 5, 1, 0),
('2025-01-11', 2025, 1, 11, 6, 1, 0),
('2025-01-12', 2025, 1, 12, 0, 1, 0),
('2025-01-13', 2025, 1, 13, 1, 1, 0),
('2025-01-14', 2025, 1, 14, 2, 1, 0),
('2025-01-15', 2025, 1, 15, 3, 1, 0),
('2025-01-16', 2025, 1, 16, 4, 1, 0),
('2025-01-17', 2025, 1, 17, 5, 1, 0),
('2025-01-18', 2025, 1, 18, 6, 1, 0),
('2025-01-19', 2025, 1, 19, 0, 1, 0),
('2025-01-20', 2025, 1, 20, 1, 1, 0),
('2025-01-21', 2025, 1, 21, 2, 1, 0);

-- dim_user新增20条
INSERT INTO dim_user VALUES
(1006, 'user_1006', '2024-12-10', 2, '2025-01-26'),
(1007, 'user_1007', '2024-12-15', 3, '2025-01-26'),
(1008, 'user_1008', '2024-12-20', 4, '2025-01-26'),
(1009, 'user_1009', '2024-12-25', 5, '2025-01-26'),
(1010, 'user_1010', '2024-12-30', 1, '2025-01-26'),
(1011, 'user_1011', '2025-01-01', 2, '2025-01-26'),
(1012, 'user_1012', '2025-01-05', 3, '2025-01-26'),
(1013, 'user_1013', '2025-01-10', 4, '2025-01-26'),
(1014, 'user_1014', '2025-01-15', 5, '2025-01-26'),
(1015, 'user_1015', '2025-01-20', 1, '2025-01-26'),
(1016, 'user_1016', '2024-11-05', 2, '2025-01-25'),
(1017, 'user_1017', '2024-11-10', 3, '2025-01-25'),
(1018, 'user_1018', '2024-11-15', 4, '2025-01-25'),
(1019, 'user_1019', '2024-11-20', 5, '2025-01-25'),
(1020, 'user_1020', '2024-11-25', 1, '2025-01-25'),
(1021, 'user_1021', '2024-10-05', 2, '2025-01-24'),
(1022, 'user_1022', '2024-10-10', 3, '2025-01-24'),
(1023, 'user_1023', '2024-10-15', 4, '2025-01-24'),
(1024, 'user_1024', '2024-10-20', 5, '2025-01-24'),
(1025, 'user_1025', '2024-10-25', 1, '2025-01-24');

-- dim_shop新增20条
INSERT INTO dim_shop VALUES
(1006, 'Book Store', 'Books', '2023-08-10', 1),
(1007, 'Toy Shop', 'Toys', '2023-09-15', 1),
(1008, 'Grocery Store', 'Food', '2023-10-20', 1),
(1009, 'Pet Shop', 'Pets', '2023-11-25', 1),
(1010, 'Jewelry Store', 'Jewelry', '2023-12-01', 1),
(1011, 'Furniture Store', 'Furniture', '2023-12-10', 1),
(1012, 'Shoe Store', 'Footwear', '2023-12-15', 1),
(1013, 'Bag Store', 'Accessories', '2024-01-05', 1),
(1014, 'Cosmetics Store', 'Beauty', '2024-01-15', 1),
(1015, 'Sports Equipment', 'Sports', '2024-02-01', 1),
(1016, 'Camera Shop', 'Electronics', '2024-02-10', 1),
(1017, 'Music Store', 'Entertainment', '2024-02-15', 1),
(1018, 'Art Gallery', 'Art', '2024-03-01', 1),
(1019, 'Flower Shop', 'Gifts', '2024-03-10', 1),
(1020, 'Stationery Store', 'Office', '2024-03-15', 1),
(1021, 'Watch Store', 'Accessories', '2024-03-20', 1),
(1022, 'Luggage Store', 'Travel', '2024-04-01', 1),
(1023, 'Perfume Shop', 'Beauty', '2024-04-10', 1),
(1024, 'Sunglasses Store', 'Fashion', '2024-04-15', 1),
(1025, 'Hat Shop', 'Fashion', '2024-04-20', 1);
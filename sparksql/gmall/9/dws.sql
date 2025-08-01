CREATE EXTERNAL TABLE dws_user_path_summary (
    summary_id BIGINT,
    summary_date DATE,
    user_id BIGINT,
    session_id STRING,
    entry_page STRING,
    exit_page STRING,
    path_sequence ARRAY<STRING>,
    total_steps INT,
    terminal_type STRING,
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dws_user_path_summary';

CREATE EXTERNAL TABLE dws_page_visit_stats (
    stat_id BIGINT,
    stat_date DATE,
    page_id STRING,
    page_type STRING,
    visit_count INT,
    unique_visitor INT,
    avg_stay_duration DECIMAL(10,2),
    bounce_rate DECIMAL(5,2),
    shop_id INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dws_page_visit_stats';

CREATE EXTERNAL TABLE dws_daily_visit (
    daily_id BIGINT,
    visit_date DATE,
    shop_id INT,
    total_visits INT,
    unique_visitors INT,
    avg_duration DECIMAL(10,2),
    pc_visits INT,
    mobile_visits INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dws_daily_visit';

CREATE EXTERNAL TABLE dws_user_retention (
    retention_id BIGINT,
    calc_date DATE,
    shop_id INT,
    new_users INT,
    d1_retention INT,
    d7_retention INT,
    d30_retention INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dws_user_retention';

CREATE EXTERNAL TABLE dws_conversion_rate (
    conversion_id BIGINT,
    stat_date DATE,
    shop_id INT,
    view_to_cart_rate DECIMAL(5,2),
    cart_to_order_rate DECIMAL(5,2),
    view_to_order_rate DECIMAL(5,2),
    terminal_type STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/ecommerce.db/dws_conversion_rate';

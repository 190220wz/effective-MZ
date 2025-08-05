import argparse
import pymysql
from pymysql import Error
from datetime import datetime
import os


def get_all_tables(host, port, user, password, database):
    """获取MySQL day85数据库中商品主题相关表名（适配连带分析看板需求）"""
    try:
        connection = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # 获取数据库中所有表，筛选商品主题核心表（匹配文档中"商品""行为""关联"等分析对象）
        cursor.execute(f"SHOW TABLES")
        all_tables = [row[0] for row in cursor.fetchall()]
        # 筛选与商品连带分析相关的表（如商品信息、用户行为、关联关系表等）
        topic_tables = [
            table for table in all_tables
            if any(key in table.lower() for key in ['goods', 'behavior', 'relation', 'monitor'])
        ]
        return topic_tables

    except Error as e:
        print(f"获取day85数据库商品主题表列表失败: {e}")
        exit(1)
    finally:
        if 'connection' in locals() and connection:
            connection.close()


def get_table_columns(host, port, user, password, database, table):
    """获取MySQL表字段结构（支撑商品主题数据类型映射）"""
    try:
        connection = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # 获取表结构信息（用于生成Hive表结构）
        cursor.execute(f"DESCRIBE `{table}`")
        columns = []
        for row in cursor.fetchall():
            column_info = {
                'name': row[0],
                'type': row[1],
                'nullable': 'YES' in row[2].upper(),
                'key': row[3],
                'default': row[4],
                'extra': row[5]
            }
            columns.append(column_info)
        return columns

    except Error as e:
        print(f"获取表 {table} 字段信息失败: {e}")
        return []
    finally:
        if 'connection' in locals() and connection:
            connection.close()


def map_mysql_to_hive_type(mysql_type):
    """MySQL到Hive数据类型映射（适配商品主题数据存储）"""
    mysql_type = mysql_type.lower()

    # 数值类型（商品ID、行为次数等）
    if 'tinyint' in mysql_type:
        return 'TINYINT'
    elif 'smallint' in mysql_type:
        return 'SMALLINT'
    elif 'int' in mysql_type:
        return 'INT'
    elif 'bigint' in mysql_type:
        return 'BIGINT'  # 商品ID、用户ID等大整数用BIGINT
    elif 'float' in mysql_type:
        return 'FLOAT'
    elif 'double' in mysql_type:
        return 'DOUBLE'
    elif 'decimal' in mysql_type:
        return f'DECIMAL{mysql_type[mysql_type.find("("):]}'

    # 日期时间类型（行为时间、统计周期等，文档需近7天行为分析）
    elif 'date' in mysql_type or 'time' in mysql_type or 'timestamp' in mysql_type:
        return 'TIMESTAMP'
    elif 'year' in mysql_type:
        return 'INT'

    # 字符串类型（商品名称、类别等描述信息）
    elif 'char' in mysql_type or 'text' in mysql_type or 'enum' in mysql_type:
        return 'STRING'
    elif 'binary' in mysql_type or 'blob' in mysql_type:
        return 'BINARY'
    else:
        return 'STRING'  # 默认兼容类型


def generate_hive_ddl(table, columns, host, port, output_dir="ddl_scripts"):
    """生成Hive ODS层DDL（支撑商品主题原始数据存储）"""
    os.makedirs(output_dir, exist_ok=True)
    ddl_file = os.path.join(output_dir, f"ods_{table}.sql")

    # 构建DDL内容（匹配文档数仓分层中的ODS层需求）
    hive_ddl = f"-- 商品主题连带分析看板 - Hive ODS层表\n"
    hive_ddl += f"-- 文档来源: 大数据-电商数仓-05-商品主题连带分析看板V1.2-20250122.pdf\n"
    hive_ddl += f"-- 源表: day85.{table}\n"
    hive_ddl += f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    hive_ddl += f"CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_{table} (\n"

    # 添加字段定义
    for col in columns:
        hive_type = map_mysql_to_hive_type(col['type'])
        hive_ddl += f"    {col['name']} {hive_type} COMMENT '{col['name']}（商品主题分析字段）',\n"

    hive_ddl = hive_ddl.rstrip(",\n") + "\n)\n"

    # 分区及存储配置（按日期分区，适配近7天数据统计需求）
    hive_ddl += "PARTITIONED BY (dt STRING COMMENT '分区日期yyyy-MM-dd，用于近7天行为分析')\n"
    hive_ddl += f"LOCATION 'hdfs://{host}:{port}/user/hive/warehouse/ods/ods_{table}/'\n"
    hive_ddl += """TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true',
    'comment' = '商品主题连带分析ODS层原始表'
);
"""

    with open(ddl_file, "w", encoding="utf-8") as f:
        f.write(hive_ddl)
    print(f"  → 生成Hive ODS层DDL: {ddl_file}")
    return hive_ddl


def has_create_time_column(columns):
    """检查是否含create_time字段（支撑增量同步，适配文档动态数据更新需求）"""
    return any(col['name'].lower() == 'create_time' for col in columns)


def generate_seatunnel_config(mysql_host, mysql_port, mysql_user, mysql_password,
                              hive_metastore_uri, hdfs_namenode, table, columns,
                              database="day85", custom_dt=None):
    """生成SeaTunnel同步配置（MySQL day85到Hive ODS层，支撑看板数据接入）"""
    os.makedirs("conf", exist_ok=True)
    config_file = os.path.join("conf", f"{table}_to_hive.conf")

    # 字段列表构建
    column_names = [col['name'] for col in columns]
    column_list_sql = ", ".join([f"`{col}`" for col in column_names])

    # 同步查询语句（适配增量/全量同步，支撑关联行为数据更新）
    has_create_time = has_create_time_column(columns)
    if custom_dt:
        query = f"SELECT {column_list_sql}, '{custom_dt}' AS dt FROM `{database}`.`{table}`"
    else:
        if has_create_time:
            # 用create_time生成dt分区，适配近7天增量数据
            query = f"SELECT {column_list_sql}, date_format(create_time, '%Y-%m-%d') AS dt FROM `{database}`.`{table}`"
        else:
            query = f"SELECT {column_list_sql}, date_format(now(), '%Y-%m-%d') AS dt FROM `{database}`.`{table}`"

    # 配置内容（匹配文档数据同步需求）
    config_content = f"""# 商品主题连带分析数据同步配置
# 文档来源: 大数据-电商数仓-05-商品主题连带分析看板V1.2-20250122.pdf
# 源表: {database}.{table} → 目标表: ods.ods_{table}
# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

env {{
  execution.parallelism = 1
  job.mode = "BATCH"
}}

source {{
  Jdbc {{
    url = "jdbc:mysql://{mysql_host}:{mysql_port}/{database}"
    driver = "com.mysql.jdbc.Driver"
    user = "{mysql_user}"
    password = "{mysql_password}"
    query = "{query}"  # 按dt分区同步，支撑近7天关联分析
    connection_check_timeout_sec = 100
  }}
}}

transform {{
  # 预留数据清洗节点（适配商品主题数据质量要求）
}}

sink {{
  Hive {{
    table_name = "ods.ods_{table}"
    metastore_uri = "{hive_metastore_uri}"
    hive.hadoop.conf-path = "/etc/hadoop/conf"
    save_mode = "overwrite"
    partition_by = ["dt"]  # 按日期分区，匹配文档统计周期
    dynamic_partition = true
    file_format = "orc"
    orc_compress = "SNAPPY"
    fields = {column_names + ["dt"]}
    tbl_properties = {{
      "external.table.purge" = "true"
    }}
  }}
}}
"""

    with open(config_file, "w", encoding="utf-8") as f:
        f.write(config_content)
    print(f"  → 生成同步配置: {config_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='商品主题连带分析看板数据同步配置生成工具')
    parser.add_argument('--mysql_user', default='root', help='MySQL用户名')
    parser.add_argument('--ddl_dir', default='ddl_scripts', help='Hive DDL输出目录')
    parser.add_argument('--dt', default=None, help='自定义分区日期(格式:yyyy-MM-dd，适配近7天分析)')
    args = parser.parse_args()

    # 环境配置（适配文档数仓环境）
    mysql_host = "cdh01"
    mysql_port = "3306"
    mysql_password = "123456"
    database = "day85"  # 目标数据库
    hdfs_port = "8020"
    hive_metastore_uri = "thrift://cdh01:9083"
    hdfs_nn = f"hdfs://{mysql_host}:{hdfs_port}"

    # 获取day85数据库商品主题表
    print("基于文档需求，获取day85数据库商品主题表列表...")
    tables = get_all_tables(
        host=mysql_host,
        port=mysql_port,
        user=args.mysql_user,
        password=mysql_password,
        database=database
    )
    print(f"找到{len(tables)}张商品主题核心表: {', '.join(tables)}")

    # 生成DDL和同步配置
    for table in tables:
        print(f"\n处理表: {table}")
        columns = get_table_columns(
            host=mysql_host,
            port=mysql_port,
            user=args.mysql_user,
            password=mysql_password,
            database=database,
            table=table
        )
        if not columns:
            print(f"  → 字段信息为空，跳过表")
            continue

        # 生成Hive ODS层DDL
        generate_hive_ddl(
            table=table,
            columns=columns,
            host=mysql_host,
            port=hdfs_port,
            output_dir=args.ddl_dir
        )

        # 生成SeaTunnel同步配置
        generate_seatunnel_config(
            mysql_host=mysql_host,
            mysql_port=mysql_port,
            mysql_user=args.mysql_user,
            mysql_password=mysql_password,
            hive_metastore_uri=hive_metastore_uri,
            hdfs_namenode=hdfs_nn,
            table=table,
            columns=columns,
            database=database,
            custom_dt=args.dt
        )

    print("\n所有配置生成完成!")
    print(f"Hive ODS层DDL路径: {args.ddl_dir}")
    print(f"SeaTunnel同步配置路径: conf/")
    print("配置适配《大数据-电商数仓-05-商品主题连带分析看板V1.2-20250122.pdf》数据需求")
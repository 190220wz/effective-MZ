import pymysql
import random
from faker import Faker
from datetime import datetime, timedelta
import time

# 初始化Faker生成虚拟数据
fake = Faker('zh_CN')
Faker.seed(42)  # 固定随机种子，保证数据可复现

# 数据库连接配置（请替换为实际环境信息）
DB_CONFIG = {
    'host': 'cdh01',
    'user': 'root',
    'password': '123456',
    'db': 'day85',
    'port': 3306,
    'charset': 'utf8mb4'
}

# 基础参数配置
TOTAL_GOODS = 10000  # 商品总数
TOTAL_USERS = 50000  # 用户总数
TOTAL_BEHAVIORS = 10000  # 用户行为记录数
BATCH_SIZE = 10000  # 批量插入批次大小
STAT_START_DATE = (datetime.now() - timedelta(days=7)).date()  # 近7天统计起始日
STAT_END_DATE = datetime.now().date()  # 统计结束日


def create_db_connection():
    """创建数据库连接"""
    return pymysql.connect(**DB_CONFIG)


def truncate_table(table_name):
    """清空表数据"""
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        print(f"表 {table_name} 已清空")
    except Exception as e:
        print(f"清空表 {table_name} 失败: {e}")
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()


def generate_dim_goods_info():
    """生成商品基本信息表数据"""
    truncate_table("dim_goods_info")
    conn = create_db_connection()
    cursor = conn.cursor()
    print("开始插入商品基本信息表数据...")

    for i in range(1, TOTAL_GOODS + 1):
        goods_id = i
        goods_name = f"商品_{fake.word()}_{i}"
        category_id = random.randint(1, 20)
        category_name = f"类别_{category_id}"
        is_drainage = 1 if random.random() < 0.1 else 0
        is_hot = 1 if random.random() < 0.15 else 0
        create_time = fake.date_time_between(start_date='-1y', end_date='now')

        sql = """
        INSERT INTO dim_goods_info 
        (goods_id, goods_name, category_id, category_name, is_drainage, is_hot, create_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(sql, (goods_id, goods_name, category_id, category_name,
                                 is_drainage, is_hot, create_time))
            if i % BATCH_SIZE == 0:
                conn.commit()
                print(f"已插入 {i}/{TOTAL_GOODS} 条商品数据")
        except pymysql.err.IntegrityError as e:
            if "Duplicate entry" in str(e):
                print(f"商品ID {goods_id} 已存在，跳过...")
                continue
            else:
                raise e

    conn.commit()
    cursor.close()
    conn.close()
    print("商品基本信息表数据插入完成！")


def generate_fact_user_goods_behavior():
    """生成用户商品行为表数据（一万条）"""
    truncate_table("fact_user_goods_behavior")
    conn = create_db_connection()
    cursor = conn.cursor()
    print("开始插入用户商品行为表数据（一万条）...")

    for i in range(1, TOTAL_BEHAVIORS + 1):
        behavior_id = i
        user_id = random.randint(1, TOTAL_USERS)
        goods_id = random.randint(1, TOTAL_GOODS)
        behavior_type = random.randint(1, 3)
        behavior_time = fake.date_time_between(start_date=STAT_START_DATE, end_date=STAT_END_DATE)
        behavior_date = behavior_time.date()

        sql = """
        INSERT INTO fact_user_goods_behavior 
        (behavior_id, user_id, goods_id, behavior_type, behavior_time, behavior_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(sql, (behavior_id, user_id, goods_id, behavior_type,
                                 behavior_time, behavior_date))
            if i % (BATCH_SIZE // 10) == 0:
                conn.commit()
                print(f"已插入 {i}/{TOTAL_BEHAVIORS} 条行为数据")
        except pymysql.err.IntegrityError as e:
            if "Duplicate entry" in str(e):
                print(f"行为ID {behavior_id} 已存在，跳过...")
                continue
            else:
                raise e

    conn.commit()
    cursor.close()
    conn.close()
    print("用户商品行为表数据（一万条）插入完成！")


def generate_fact_goods_relation():
    """生成商品关联关系表数据（基于近7天行为）"""
    truncate_table("fact_goods_relation")
    conn = create_db_connection()
    cursor = conn.cursor()
    print("开始插入商品关联关系表数据...")

    cursor.execute("SELECT goods_id FROM dim_goods_info WHERE is_drainage=1 OR is_hot=1")
    main_goods_list = [row[0] for row in cursor.fetchall()]
    relation_id = 1

    for main_goods_id in main_goods_list:
        existing_relations = set()
        related_count = random.randint(10, 30)
        current_count = 0

        while current_count < related_count:
            related_goods_id = random.randint(1, TOTAL_GOODS)
            if related_goods_id == main_goods_id:
                continue
            relation_type = random.randint(1, 3)
            relation_key = (related_goods_id, relation_type)
            if relation_key in existing_relations:
                continue

            relation_count = random.randint(10, 500)
            sql = """
            INSERT INTO fact_goods_relation 
            (relation_id, main_goods_id, related_goods_id, relation_type, relation_count,
             stat_period_start, stat_period_end)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            try:
                cursor.execute(sql, (relation_id, main_goods_id, related_goods_id, relation_type,
                                     relation_count, STAT_START_DATE, STAT_END_DATE))
                existing_relations.add(relation_key)
                relation_id += 1
                current_count += 1
            except pymysql.err.IntegrityError as e:
                if "Duplicate entry" in str(e):
                    print(f"关联ID {relation_id} 已存在，跳过...")
                    relation_id += 1
                    continue
                else:
                    raise e

        if relation_id % BATCH_SIZE == 0:
            conn.commit()
            print(f"已插入 {relation_id} 条商品关联数据")
    conn.commit()
    cursor.close()
    conn.close()
    print("商品关联关系表数据插入完成！")


def generate_fact_goods_guide_effect():
    """生成商品详情页引导效果表数据（修复随机数范围问题）"""
    truncate_table("fact_goods_guide_effect")
    conn = create_db_connection()
    cursor = conn.cursor()
    print("开始插入商品详情页引导效果表数据...")

    cursor.execute("SELECT goods_id FROM dim_goods_info LIMIT 10")
    top_guide_goods = [row[0] for row in cursor.fetchall()]
    guide_id = 1

    for main_goods_id in top_guide_goods:
        guided_count = random.randint(5, 15)
        for _ in range(guided_count):
            guided_goods_id = random.randint(1, TOTAL_GOODS)
            if guided_goods_id == main_goods_id:
                continue
            guide_visitor_count = random.randint(50, 2000)

            # 修复收藏加购数可能过小的问题：确保至少有10个
            max_collect = int(guide_visitor_count * 0.3)
            guide_collect_addcart_count = random.randint(10, max_collect) if max_collect >= 10 else 10

            # 修复支付数范围问题：确保结束值不小于起始值
            max_pay = int(guide_collect_addcart_count * 0.2)
            min_pay = 5
            # 如果最大支付数小于最小支付数，则强制设置为最小支付数
            if max_pay < min_pay:
                guide_pay_count = min_pay
            else:
                guide_pay_count = random.randint(min_pay, max_pay)

            stat_date = fake.date_between(start_date=STAT_START_DATE, end_date=STAT_END_DATE)

            sql = """
            INSERT INTO fact_goods_guide_effect 
            (guide_id, main_goods_id, guided_goods_id, guide_visitor_count, 
             guide_collect_addcart_count, guide_pay_count, stat_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            try:
                cursor.execute(sql, (guide_id, main_goods_id, guided_goods_id, guide_visitor_count,
                                     guide_collect_addcart_count, guide_pay_count, stat_date))
                guide_id += 1
            except pymysql.err.IntegrityError as e:
                if "Duplicate entry" in str(e):
                    print(f"引导ID {guide_id} 已存在，跳过...")
                    guide_id += 1
                    continue
                else:
                    raise e

        if guide_id % BATCH_SIZE == 0:
            conn.commit()
            print(f"已插入 {guide_id} 条引导效果数据")
    conn.commit()
    cursor.close()
    conn.close()
    print("商品详情页引导效果表数据插入完成！")


def generate_dim_monitor_goods_setting():
    """生成监控商品设置表数据"""
    truncate_table("dim_monitor_goods_setting")
    conn = create_db_connection()
    cursor = conn.cursor()
    print("开始插入监控商品设置表数据...")

    cursor.execute("SELECT goods_id FROM dim_goods_info LIMIT 10")
    default_monitor_goods = [row[0] for row in cursor.fetchall()]
    monitor_id = 1

    for goods_id in default_monitor_goods:
        is_default = 1
        monitor_start_time = STAT_START_DATE
        monitor_status = 1

        sql = """
        INSERT INTO dim_monitor_goods_setting 
        (monitor_id, goods_id, is_default, monitor_start_time, monitor_status)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(sql, (monitor_id, goods_id, is_default, monitor_start_time, monitor_status))
            monitor_id += 1
        except pymysql.err.IntegrityError as e:
            if "Duplicate entry" in str(e):
                print(f"监控ID {monitor_id} 已存在，跳过...")
                monitor_id += 1
                continue
            else:
                raise e

    conn.commit()
    cursor.close()
    conn.close()
    print("监控商品设置表数据插入完成！")


if __name__ == "__main__":
    start_time = time.time()
    generate_dim_goods_info()
    generate_fact_user_goods_behavior()
    generate_fact_goods_relation()
    generate_fact_goods_guide_effect()
    generate_dim_monitor_goods_setting()
    end_time = time.time()
    print(f"所有表数据生成完成！总耗时：{round(end_time - start_time, 2)}秒")
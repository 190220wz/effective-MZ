import random
import datetime
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text

# 初始化Faker生成假数据
fake = Faker('zh_CN')

# 数据库连接配置
DB_CONFIG = {
    'host': 'cdh01',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'day86'
}

# 生成日期范围（近7天）
START_DATE = datetime.date(2025, 1, 1)
END_DATE = datetime.date(2025, 1, 7)
DATE_LIST = [START_DATE + datetime.timedelta(days=i) for i in range((END_DATE - START_DATE).days + 1)]

# 基础配置参数（控制总数据量在千级）
TOTAL_PRODUCTS = 50  # 商品总数
TOTAL_CATEGORIES = 10  # 叶子类目总数
BATCH_SIZE = 500  # 批量插入大小

# 初始化数据库连接
engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
    f"{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
)


def clear_existing_data():
    """清空现有表数据，解决重复插入问题"""
    tables = [
        'product_activity_refund_stats',
        'product_user_stratify_stats',
        'product_transaction_stats',
        'product_favorite_cart_stats',
        'product_traffic_stats',
        'product_base'
    ]

    try:
        with engine.connect() as conn:
            trans = conn.begin()
            for table in tables:
                conn.execute(text(f"TRUNCATE TABLE {table}"))
                print(f"已清空表 {table} 的数据")
            trans.commit()
    except Exception as e:
        print(f"清空表数据失败: {str(e)}")
        if 'trans' in locals():
            trans.rollback()


def generate_product_base_data():
    """生成商品基础信息数据"""
    data = []
    for product_id in range(1, TOTAL_PRODUCTS + 1):
        category_id = random.randint(1, TOTAL_CATEGORIES)
        # 随机价格
        price = round(random.uniform(10, 1000), 2)
        create_time = fake.date_time_between(start_date='-1y', end_date='now')
        data.append({
            'product_id': product_id,
            'product_name': fake.word() + str(product_id),
            'category_id': category_id,
            'category_name': f'叶子类目_{category_id}',
            'price': price,
            'create_time': create_time
        })
    return pd.DataFrame(data)


def generate_daily_stats(product_id, stats_date):
    """为单个商品生成单日统计数据"""
    # 基础流量数据
    visitor_count = random.randint(20, 2000)  # 商品访客数（去重）
    pc_ratio = random.uniform(0.1, 0.4)  # PC端占比10%-40%
    pc_visitor = int(visitor_count * pc_ratio)
    wireless_visitor = visitor_count - pc_visitor

    view_count = random.randint(visitor_count, visitor_count * 5)  # 商品浏览量
    avg_stay_time = round(random.uniform(10, 150), 2)  # 平均停留时长（秒）
    bounce_rate = round(random.uniform(0.2, 0.7), 4)  # 详情页跳出率
    micro_view_count = int(visitor_count * random.uniform(0.3, 0.6))  # 微详情访客数

    # 收藏加购数据
    favorite_rate = round(random.uniform(0.02, 0.12), 4)  # 访问收藏转化率
    favorite_count = int(visitor_count * favorite_rate)

    cart_rate = round(random.uniform(0.03, 0.18), 4)  # 访问加购转化率
    cart_user_count = int(visitor_count * cart_rate)
    cart_item_count = cart_user_count * random.randint(1, 3)  # 加购件数

    # 交易转化数据
    order_rate = round(random.uniform(0.02, 0.08), 4)  # 下单转化率
    order_user_count = int(visitor_count * order_rate)
    order_item_count = order_user_count * random.randint(1, 2)
    order_amount = round(order_item_count * random.uniform(80, 400), 2)

    pay_rate = round(random.uniform(0.75, 0.95), 4)  # 支付转化率
    pay_user_count = int(order_user_count * pay_rate)
    pay_item_count = int(order_item_count * pay_rate)
    pay_amount = round(order_amount * pay_rate, 2)

    # 新老用户数据
    new_user_ratio = round(random.uniform(0.3, 0.6), 4)  # 新用户占比30%-60%
    new_pay_user = int(pay_user_count * new_user_ratio)
    old_pay_user = pay_user_count - new_pay_user
    old_pay_amount = round(pay_amount * (old_pay_user / pay_user_count) if pay_user_count > 0 else 0, 2)

    # 活动与退款数据
    juhuasuan_amount = round(pay_amount * random.uniform(0.2, 0.7), 2) if random.random() < 0.2 else 0
    refund_amount = round(pay_amount * random.uniform(0, 0.04), 2)  # 退款金额
    competitiveness_score = random.randint(65, 92)  # 竞争力评分

    return {
        'product_id': product_id,
        'stats_date': stats_date,
        'stats_period': 'day',  # 支持日维度分析

        # 流量数据
        'traffic': {
            'visitor_count': visitor_count,
            'pc_visitor_count': pc_visitor,
            'wireless_visitor_count': wireless_visitor,
            'view_count': view_count,
            'avg_stay_time': avg_stay_time,
            'bounce_rate': bounce_rate,
            'micro_view_count': micro_view_count
        },

        # 收藏加购数据
        'favorite_cart': {
            'favorite_count': favorite_count,
            'cart_item_count': cart_item_count,
            'cart_user_count': cart_user_count,
            'favorite_convert_rate': favorite_rate,
            'cart_convert_rate': cart_rate
        },

        # 交易转化数据
        'transaction': {
            'order_user_count': order_user_count,
            'order_item_count': order_item_count,
            'order_amount': order_amount,
            'pay_user_count': pay_user_count,
            'pay_item_count': pay_item_count,
            'pay_amount': pay_amount,
            'prepay_amount': round(pay_amount * random.uniform(0, 0.25), 2),
            'cod_pay_amount': round(pay_amount * random.uniform(0, 0.15), 2),
            'order_convert_rate': order_rate,
            'pay_convert_rate': pay_rate
        },

        # 用户分层数据
        'user_stratify': {
            'new_pay_user_count': new_pay_user,
            'old_pay_user_count': old_pay_user,
            'old_pay_amount': old_pay_amount,
            'customer_price': round(pay_amount / pay_user_count, 2) if pay_user_count > 0 else 0,
            'visitor_avg_value': round(pay_amount / visitor_count, 2) if visitor_count > 0 else 0
        },

        # 活动与退款数据
        'activity_refund': {
            'juhuasuan_pay_amount': juhuasuan_amount,
            'refund_amount': refund_amount,
            'yearly_pay_amount': round(pay_amount * random.uniform(15, 25), 2),
            'competitiveness_score': competitiveness_score
        }
    }


def insert_batch_data(table_name, data_df):
    """批量插入数据到数据库"""
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            data_df.to_sql(
                name=table_name,
                con=conn,
                if_exists='append',
                index=False,
                chunksize=BATCH_SIZE
            )
            trans.commit()
        print(f"成功插入 {len(data_df)} 条数据到 {table_name}")
    except Exception as e:
        print(f"插入数据失败: {str(e)}")
        if 'trans' in locals():
            trans.rollback()


def main():
    # 新增：先清空现有数据，解决重复插入问题
    print("正在清空现有表数据...")
    clear_existing_data()

    # 1. 插入商品基础信息表数据
    print("生成商品基础信息数据...")
    product_base_df = generate_product_base_data()
    insert_batch_data('product_base', product_base_df)

    # 2. 生成并插入各统计表格数据
    traffic_data = []
    favorite_cart_data = []
    transaction_data = []
    user_stratify_data = []
    activity_refund_data = []

    print(f"生成 {TOTAL_PRODUCTS} 个商品 {len(DATE_LIST)} 天的监控数据...")
    for product_id in range(1, TOTAL_PRODUCTS + 1):
        for stats_date in DATE_LIST:
            stats = generate_daily_stats(product_id, stats_date)

            # 流量表数据
            traffic = stats['traffic']
            traffic_data.append({
                'product_id': stats['product_id'],
                'stats_date': stats['stats_date'],
                'stats_period': stats['stats_period'],
                'visitor_count': traffic['visitor_count'],
                'pc_visitor_count': traffic['pc_visitor_count'],
                'wireless_visitor_count': traffic['wireless_visitor_count'],
                'view_count': traffic['view_count'],
                'avg_stay_time': traffic['avg_stay_time'],
                'bounce_rate': traffic['bounce_rate'],
                'micro_view_count': traffic['micro_view_count']
            })

            # 收藏加购表数据
            favorite_cart = stats['favorite_cart']
            favorite_cart_data.append({
                'product_id': stats['product_id'],
                'stats_date': stats['stats_date'],
                'stats_period': stats['stats_period'],
                'favorite_count': favorite_cart['favorite_count'],
                'cart_item_count': favorite_cart['cart_item_count'],
                'cart_user_count': favorite_cart['cart_user_count'],
                'favorite_convert_rate': favorite_cart['favorite_convert_rate'],
                'cart_convert_rate': favorite_cart['cart_convert_rate']
            })

            # 交易转化表数据
            transaction = stats['transaction']
            transaction_data.append({
                'product_id': stats['product_id'],
                'stats_date': stats['stats_date'],
                'stats_period': stats['stats_period'],
                'order_user_count': transaction['order_user_count'],
                'order_item_count': transaction['order_item_count'],
                'order_amount': transaction['order_amount'],
                'pay_user_count': transaction['pay_user_count'],
                'pay_item_count': transaction['pay_item_count'],
                'pay_amount': transaction['pay_amount'],
                'prepay_amount': transaction['prepay_amount'],
                'cod_pay_amount': transaction['cod_pay_amount'],
                'order_convert_rate': transaction['order_convert_rate'],
                'pay_convert_rate': transaction['pay_convert_rate']
            })

            # 用户分层表数据
            user_stratify = stats['user_stratify']
            user_stratify_data.append({
                'product_id': stats['product_id'],
                'stats_date': stats['stats_date'],
                'stats_period': stats['stats_period'],
                'new_pay_user_count': user_stratify['new_pay_user_count'],
                'old_pay_user_count': user_stratify['old_pay_user_count'],
                'old_pay_amount': user_stratify['old_pay_amount'],
                'customer_price': user_stratify['customer_price'],
                'visitor_avg_value': user_stratify['visitor_avg_value']
            })

            # 活动与退款表数据
            activity_refund = stats['activity_refund']
            activity_refund_data.append({
                'product_id': stats['product_id'],
                'stats_date': stats['stats_date'],
                'stats_period': stats['stats_period'],
                'juhuasuan_pay_amount': activity_refund['juhuasuan_pay_amount'],
                'refund_amount': activity_refund['refund_amount'],
                'yearly_pay_amount': activity_refund['yearly_pay_amount'],
                'competitiveness_score': activity_refund['competitiveness_score']
            })

    # 批量插入数据
    insert_batch_data('product_traffic_stats', pd.DataFrame(traffic_data))
    insert_batch_data('product_favorite_cart_stats', pd.DataFrame(favorite_cart_data))
    insert_batch_data('product_transaction_stats', pd.DataFrame(transaction_data))
    insert_batch_data('product_user_stratify_stats', pd.DataFrame(user_stratify_data))
    insert_batch_data('product_activity_refund_stats', pd.DataFrame(activity_refund_data))

    print("所有千级数据生成和插入完成！")


if __name__ == "__main__":
    main()

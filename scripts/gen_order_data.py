# scripts/gen_order_data.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import logging
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================================================
# 项目路径配置
# ==================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_PATH = f"{BASE_DIR}/warehouse"
# Hive 数据库和表名
ODS_DATABASE = "ods"
ODS_TABLE = "ods_order_daily"

etl_date = datetime.now().strftime("%Y-%m-%d")


# ==================================================
# 订单状态枚举
# ==================================================
ORDER_STATUS = ["待支付", "已支付", "已取消", "已发货", "已完成"]


# ==================================================
# Spark 会话（启用 Hive 支持）
# ==================================================
def get_spark():
    spark = SparkSession.builder \
        .appName("user_etl") \
        .master("local[*]") \
        .enableHiveSupport() \
        .config("spark.sql.warehouse.dir", "/Users/ok/Desktop/data_warehouse_etl/warehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .config("hive.metastore.warehouse.dir", "/Users/ok/Desktop/data_warehouse_etl/warehouse") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/Users/ok/Desktop/data_warehouse_etl/metastore_db;create=true") \
        .config("datanucleus.schema.autoCreateAll", "true") \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .getOrCreate()
    return spark


def create_ods_table(spark):
    """创建 ODS 订单日表（如果不存在）"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ODS_DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ODS_DATABASE}.{ODS_TABLE} (
            order_id STRING,
            user_id STRING,
            goods_id STRING,
            order_amount DOUBLE,
            order_status STRING,
            order_time STRING
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    logger.info(f"表 {ODS_DATABASE}.{ODS_TABLE} 已就绪")


def generate_order_time(base_date):
    """生成随机订单时间（分布在当天 0-23 小时）"""
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{base_date} {hour:02d}:{minute:02d}:{second:02d}"


def gen_order_data(spark, num_orders=150, num_users=70, num_goods=50):
    """
    生成订单数据并写入 Hive 表分区
    
    Args:
        spark: SparkSession
        num_orders: 订单数量（默认 150）
        num_users: 用户数量（默认 70，与用户数据对应）
        num_goods: 商品数量（默认 50，与商品数据对应）
    """
    logger.info(f"开始生成订单数据，订单数={num_orders}")
    
    rows = []
    for i in range(1, num_orders + 1):
        order_id = f"o{i:04d}"
        user_id = f"u{random.randint(1, num_users):03d}"
        goods_id = f"g{random.randint(1, num_goods):03d}"
        order_amount = round(random.uniform(10.0, 5000.0), 2)
        # 订单状态加权分布
        status_weights = ["待支付", "已支付", "已支付", "已支付", "已支付", "已支付", 
                          "已取消", "已发货", "已完成", "已完成"]
        order_status = random.choice(status_weights)
        order_time = generate_order_time(etl_date)
        
        rows.append((
            order_id,
            user_id,
            goods_id,
            order_amount,
            order_status,
            order_time
        ))
    
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("user_id", StringType()),
        StructField("goods_id", StringType()),
        StructField("order_amount", DoubleType()),
        StructField("order_status", StringType()),
        StructField("order_time", StringType()),
    ])
    
    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("dt", F.lit(etl_date))
    
    # 写入 Hive 表
    df.write.mode("overwrite") \
        .insertInto(f"{ODS_DATABASE}.{ODS_TABLE}")
    
    count = df.count()
    logger.info(f"订单数据写入完成，分区 dt={etl_date}，共 {count} 条订单")
    
    return count


if __name__ == "__main__":
    spark = get_spark()
    create_ods_table(spark)
    count = gen_order_data(spark, num_orders=200)
    logger.info(f"共生成 {count} 条订单数据")
    spark.stop()
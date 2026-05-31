# scripts/gen_user_data.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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
ODS_TABLE = "ods_user_daily"

etl_date = datetime.now().strftime("%Y-%m-%d")


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
    """创建 ODS 用户日表（如果不存在）"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ODS_DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ODS_DATABASE}.{ODS_TABLE} (
            user_id STRING,
            name STRING,
            gender STRING,
            age INT,
            phone STRING,
            email STRING
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    logger.info(f"表 {ODS_DATABASE}.{ODS_TABLE} 已就绪")


def gen_user_data(spark, num=70):
    """生成用户数据并写入 Hive 表分区"""
    rows = []
    for i in range(1, num + 1):
        rows.append((
            f"u{i:03d}",
            f"user_{i}",
            random.choice(["男", "女"]),
            random.randint(18, 60),
            f"138{random.randint(10000000, 99999999)}",
            f"user{i}@test.com"
        ))

    schema = StructType([
        StructField("user_id", StringType()),
        StructField("name", StringType()),
        StructField("gender", StringType()),
        StructField("age", IntegerType()),
        StructField("phone", StringType()),
        StructField("email", StringType()),
    ])

    df = spark.createDataFrame(rows, schema)
    # 添加分区列
    df = df.withColumn("dt", F.lit(etl_date))

    # 写入 Hive 表（覆盖当天分区）
    df.write.mode("overwrite") \
        .insertInto(f"{ODS_DATABASE}.{ODS_TABLE}")

    logger.info(f"用户数据写入完成，分区 dt={etl_date}")
    return df.count()


if __name__ == "__main__":
    spark = get_spark()
    create_ods_table(spark)
    count = gen_user_data(spark)
    logger.info(f"共生成 {count} 条用户数据")
    spark.stop()
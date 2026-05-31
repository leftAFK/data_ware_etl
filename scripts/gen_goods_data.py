# scripts/fix_and_recreate_goods.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import logging
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_PATH = f"{BASE_DIR}/warehouse"
ODS_DATABASE = "ods"
ODS_TABLE = "ods_goods_daily"
etl_date = datetime.now().strftime("%Y-%m-%d")

# 创建 Spark 会话
spark = SparkSession.builder \
    .appName("fix_and_recreate_goods") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", WAREHOUSE_PATH) \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # 1. 删除旧表
    logger.info("删除旧表...")
    spark.sql(f"DROP TABLE IF EXISTS {ODS_DATABASE}.{ODS_TABLE}")
    
    # 2. 创建数据库（如果不存在）
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ODS_DATABASE}")
    
    # 3. 创建新表
    logger.info("创建新表...")
    spark.sql(f"""
        CREATE TABLE {ODS_DATABASE}.{ODS_TABLE} (
            goods_id STRING,
            goods_name STRING,
            category STRING,
            price DOUBLE,
            stock INT,
            brand STRING
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    logger.info(f"表 {ODS_DATABASE}.{ODS_TABLE} 创建成功")
    
    # 4. 生成数据
    logger.info("生成商品数据...")
    categories = ["电子产品", "服装", "家居用品", "图书", "美妆", "食品"]
    brands = ["华为", "小米", "苹果", "耐克", "宜家", "当当", "雅诗兰黛", "三只松鼠"]
    
    rows = []
    for i in range(1, 51):
        rows.append((
            f"g{i:03d}",
            f"商品_{i}",
            random.choice(categories),
            round(random.uniform(10.0, 5000.0), 2),
            random.randint(0, 500),
            random.choice(brands)
        ))
    
    schema = StructType([
        StructField("goods_id", StringType()),
        StructField("goods_name", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("stock", IntegerType()),
        StructField("brand", StringType()),
    ])
    
    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("dt", F.lit(etl_date))
    
    # 5. 写入数据
    logger.info("写入数据...")
    df.write.mode("overwrite").insertInto(f"{ODS_DATABASE}.{ODS_TABLE}")
    
    # 6. 验证
    count = spark.sql(f"SELECT COUNT(*) FROM {ODS_DATABASE}.{ODS_TABLE} WHERE dt='{etl_date}'").collect()[0][0]
    logger.info(f"✓ 成功写入 {count} 条商品数据")
    
    # 7. 显示样例数据
    logger.info("样例数据:")
    spark.sql(f"SELECT * FROM {ODS_DATABASE}.{ODS_TABLE} WHERE dt='{etl_date}' LIMIT 5").show()
    
except Exception as e:
    logger.error(f"错误: {e}")
finally:
    spark.stop()
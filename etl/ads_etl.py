# etl/ads_etl.py

import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)    


# ==================================================
# Spark 配置（与 user_etl.py 保持一致）
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


def create_ads_tables(spark):
    """创建 ADS 层表"""
    
    spark.sql("CREATE DATABASE IF NOT EXISTS ads")
    
    # 1. 用户画像表
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads.ads_user_portrait (
            user_id STRING,
            user_name STRING,
            age_group STRING,
            gender STRING,
            vip_level STRING,
            favorite_category STRING,
            avg_basket_price DOUBLE,
            shopping_frequency STRING,
            user_value STRING
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    
    # 2. 销售日报表
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads.ads_sales_daily (
            gmv DOUBLE,
            order_count BIGINT,
            user_count BIGINT,
            avg_order_value DOUBLE,
            top_category STRING
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    
    logger.info("ADS 层表创建完成")


def run_ads_user_portrait(spark, etl_date):
    """运行用户画像 ETL"""
    logger.info(f"开始执行用户画像 ETL，日期: {etl_date}")
    
    result_df = spark.sql(f"""
        SELECT 
            u.user_id,
            u.user_name,
            u.age_group,
            u.gender,
            u.vip_level,
            COALESCE(g.favorite_category, '未知') as favorite_category,
            COALESCE(o.avg_amount, 0) as avg_basket_price,
            CASE 
                WHEN u.total_order_count >= 10 THEN '高频'
                WHEN u.total_order_count >= 3 THEN '中频'
                WHEN u.total_order_count >= 1 THEN '低频'
                ELSE '无购买'
            END as shopping_frequency,
            CASE 
                WHEN u.total_order_amount >= 5000 THEN '高'
                WHEN u.total_order_amount >= 1000 THEN '中'
                ELSE '低'
            END as user_value,
            '{etl_date}' as dt
        FROM dws.dws_user_summary u
        LEFT JOIN (
            SELECT 
                o.user_id,
                g.category as favorite_category
            FROM dwd.dwd_order_zip o
            JOIN dwd.dwd_goods_zip g ON o.goods_id = g.goods_id
            WHERE o.end_date = '9999-12-31'
            GROUP BY o.user_id, g.category
        ) g ON u.user_id = g.user_id
        LEFT JOIN (
            SELECT 
                user_id,
                AVG(order_amount) as avg_amount
            FROM dwd.dwd_order_zip
            WHERE end_date = '9999-12-31'
            GROUP BY user_id
        ) o ON u.user_id = o.user_id
        WHERE u.dt = '{etl_date}'
    """)
    
    result_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .format("parquet") \
        .saveAsTable("ads.ads_user_portrait")
    
    count = result_df.count()
    logger.info(f"用户画像表写入完成，记录数: {count}")
    
    return {"status": "success", "count": count}


def run_ads_sales_daily(spark, etl_date):
    """运行销售日报 ETL"""
    logger.info(f"开始执行销售日报 ETL，日期: {etl_date}")
    
    result_df = spark.sql(f"""
        SELECT 
            SUM(order_amount) as gmv,
            COUNT(*) as order_count,
            COUNT(DISTINCT user_id) as user_count,
            AVG(order_amount) as avg_order_value,
            (
                SELECT category 
                FROM dwd.dwd_order_zip o
                JOIN dwd.dwd_goods_zip g ON o.goods_id = g.goods_id
                WHERE o.start_date <= '{etl_date}' AND o.end_date >= '{etl_date}'
                GROUP BY g.category
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) as top_category,
            '{etl_date}' as dt
        FROM dwd.dwd_order_zip
        WHERE start_date <= '{etl_date}' 
          AND end_date >= '{etl_date}'
          AND order_status = '已支付'
    """)
    
    result_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .format("parquet") \
        .saveAsTable("ads.ads_sales_daily")
    
    count = result_df.count()
    logger.info(f"销售日报表写入完成，记录数: {count}")
    
    return {"status": "success", "count": count}


def run_ads_etl(spark, etl_date):
    """运行所有 ADS ETL"""
    logger.info(f"========== 开始 ADS 层 ETL，日期: {etl_date} ==========")
    
    results = {}
    
    # 先创建数据库和表
    create_ads_tables(spark)
    
    # 1. 用户画像
    results['user_portrait'] = run_ads_user_portrait(spark, etl_date)
    
    # 2. 销售日报
    results['sales_daily'] = run_ads_sales_daily(spark, etl_date)
    
    logger.info(f"ADS 层 ETL 完成")
    return results


if __name__ == "__main__":
    etl_date = datetime.now().strftime("%Y-%m-%d")
    spark = get_spark()
    run_ads_etl(spark, etl_date)
    spark.stop()
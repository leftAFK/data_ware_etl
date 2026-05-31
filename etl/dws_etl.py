# etl/dws_etl.py

import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from etl.data_quality import run_data_quality_checks

import sys
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================================================
# Spark 配置（与 user_etl.py、order_etl.py 保持一致）
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


def create_dws_tables(spark):
    """创建 DWS 层表"""
    
    # 1. 用户汇总表（每日用户状态汇总）
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS dws
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws.dws_user_summary (
            user_id STRING COMMENT '用户ID',
            user_name STRING COMMENT '用户姓名',
            gender STRING COMMENT '性别',
            age INT COMMENT '年龄',
            age_group STRING COMMENT '年龄段',
            vip_level STRING COMMENT '会员等级',
            total_order_count BIGINT COMMENT '历史总订单数',
            total_order_amount DOUBLE COMMENT '历史总消费金额',
            avg_order_amount DOUBLE COMMENT '平均订单金额',
            last_order_date STRING COMMENT '最后下单日期',
            user_status STRING COMMENT '用户状态：活跃/沉默/流失'
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    
    # 2. 订单统计表（每日订单汇总）
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws.dws_order_stats (
            total_orders BIGINT COMMENT '总订单数',
            total_users BIGINT COMMENT '下单用户数',
            total_amount DOUBLE COMMENT '总金额',
            avg_order_amount DOUBLE COMMENT '平均订单金额',
            max_order_amount DOUBLE COMMENT '最大订单金额',
            min_order_amount DOUBLE COMMENT '最小订单金额'
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    
    # 3. 商品分析表（每日商品汇总）
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws.dws_goods_analysis (
            goods_id STRING COMMENT '商品ID',
            goods_name STRING COMMENT '商品名称',
            category STRING COMMENT '品类',
            brand STRING COMMENT '品牌',
            price DOUBLE COMMENT '当前价格',
            total_sales BIGINT COMMENT '历史总销量',
            total_revenue DOUBLE COMMENT '历史总收入',
            avg_daily_sales DOUBLE COMMENT '日均销量'
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
    """)
    
    logger.info("DWS 层表创建完成")


def run_dws_user_summary(spark, etl_date):
    """运行用户汇总 ETL"""
    logger.info(f"开始执行用户汇总 ETL，日期: {etl_date}")
    
    result_df = spark.sql(f"""
        SELECT 
            u.user_id,
            u.name as user_name,
            u.gender,
            u.age,
            CASE 
                WHEN u.age < 18 THEN '未成年'
                WHEN u.age < 30 THEN '青年'
                WHEN u.age < 45 THEN '中年'
                ELSE '老年'
            END as age_group,
            CASE 
                WHEN COALESCE(o.total_amount, 0) > 10000 THEN '钻石'
                WHEN COALESCE(o.total_amount, 0) > 5000 THEN '黄金'
                WHEN COALESCE(o.total_amount, 0) > 1000 THEN '白银'
                ELSE '青铜'
            END as vip_level,
            COALESCE(o.order_count, 0) as total_order_count,
            COALESCE(o.total_amount, 0) as total_order_amount,
            COALESCE(o.avg_amount, 0) as avg_order_amount,
            o.last_order_date,
            CASE 
                WHEN o.last_order_date >= DATE_SUB('{etl_date}', 7) THEN '活跃'
                WHEN o.last_order_date >= DATE_SUB('{etl_date}', 30) THEN '沉默'
                ELSE '流失'
            END as user_status,
            '{etl_date}' as dt
        FROM dwd.dwd_user_zip u
        LEFT JOIN (
            SELECT 
                user_id,
                COUNT(*) as order_count,
                SUM(order_amount) as total_amount,
                AVG(order_amount) as avg_amount,
                MAX(order_time) as last_order_date
            FROM dwd.dwd_order_zip
            WHERE end_date = '9999-12-31'
            GROUP BY user_id
        ) o ON u.user_id = o.user_id
        WHERE u.end_date = '9999-12-31'
    """)
    
    # 写入 DWS 表
    result_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .format("parquet") \
        .saveAsTable("dws.dws_user_summary")
    
    count = result_df.count()
    logger.info(f"用户汇总表写入完成，记录数: {count}")
    
    # 数据质量检查
    rules = [
        {"type": "not_null", "column": "user_id", "severity": "ERROR"},
        {"type": "volume", "min_count": 1, "severity": "WARNING"},
    ]
    quality_report = run_data_quality_checks(spark, result_df, "dws.dws_user_summary", etl_date, rules)
    
    return {"status": "success", "count": count, "quality_report": quality_report}


def run_dws_order_stats(spark, etl_date):
    """运行订单统计 ETL"""
    logger.info(f"开始执行订单统计 ETL，日期: {etl_date}")
    
    result_df = spark.sql(f"""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT user_id) as total_users,
            SUM(order_amount) as total_amount,
            AVG(order_amount) as avg_order_amount,
            MAX(order_amount) as max_order_amount,
            MIN(order_amount) as min_order_amount,
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
        .saveAsTable("dws.dws_order_stats")
    
    count = result_df.count()
    logger.info(f"订单统计表写入完成，记录数: {count}")
    
    return {"status": "success", "count": count}


def run_dws_goods_analysis(spark, etl_date):
    """运行商品分析 ETL"""
    logger.info(f"开始执行商品分析 ETL，日期: {etl_date}")
    
    result_df = spark.sql(f"""
        SELECT 
            g.goods_id,
            g.goods_name,
            g.category,
            g.brand,
            g.price,
            COALESCE(o.sales_count, 0) as total_sales,
            COALESCE(o.revenue, 0) as total_revenue,
            COALESCE(ROUND(o.sales_count / 30.0, 2), 0) as avg_daily_sales,
            '{etl_date}' as dt
        FROM dwd.dwd_goods_zip g
        LEFT JOIN (
            SELECT 
                goods_id,
                COUNT(*) as sales_count,
                SUM(order_amount) as revenue
            FROM dwd.dwd_order_zip
            WHERE end_date = '9999-12-31'
              AND order_status = '已支付'
            GROUP BY goods_id
        ) o ON g.goods_id = o.goods_id
        WHERE g.end_date = '9999-12-31'
    """)
    
    result_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .format("parquet") \
        .saveAsTable("dws.dws_goods_analysis")
    
    count = result_df.count()
    logger.info(f"商品分析表写入完成，记录数: {count}")
    
    return {"status": "success", "count": count}


def run_dws_etl(spark, etl_date):
    """运行所有 DWS ETL"""
    logger.info(f"========== 开始 DWS 层 ETL，日期: {etl_date} ==========")
    
    results = {}
    
    # 先创建数据库和表
    create_dws_tables(spark)
    
    # 1. 用户汇总
    results['user_summary'] = run_dws_user_summary(spark, etl_date)
    
    # 2. 订单统计
    results['order_stats'] = run_dws_order_stats(spark, etl_date)
    
    # 3. 商品分析
    results['goods_analysis'] = run_dws_goods_analysis(spark, etl_date)
    
    logger.info(f"DWS 层 ETL 完成")
    return results


if __name__ == "__main__":
    etl_date = datetime.now().strftime("%Y-%m-%d")
    spark = get_spark()
    run_dws_etl(spark, etl_date)
    spark.stop()
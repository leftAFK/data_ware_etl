# dwd/goods_etl.py
import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ========== 引入数据质量检查模块 ==========
from etl.data_quality import DataQualityChecker, DataMonitor, run_data_quality_checks

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================================================
# Spark 配置
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


# ==================================================
# 数据清洗
# ==================================================
def clean_goods_data(df):
    """清洗商品数据"""
    # 去重
    df = df.dropDuplicates(["goods_id"])
    
    # 空值处理
    df = df.filter(F.col("goods_id").isNotNull())
    df = df.filter(F.col("goods_name").isNotNull())
    
    # 价格清洗（负数转0，空值转0）
    df = df.withColumn("price", F.col("price").cast("double"))
    df = df.withColumn(
        "price",
        F.when(F.col("price") < 0, 0.0)
         .when(F.col("price").isNull(), 0.0)
         .otherwise(F.col("price"))
    )
    
    # 库存清洗（负数转0，空值转0）
    df = df.withColumn("stock", F.col("stock").cast("int"))
    df = df.withColumn(
        "stock",
        F.when(F.col("stock") < 0, 0)
         .when(F.col("stock").isNull(), 0)
         .otherwise(F.col("stock"))
    )
    
    # 品类和品牌清洗（去除空格）
    df = df.withColumn("category", F.trim(F.col("category")))
    df = df.withColumn("brand", F.trim(F.col("brand")))
    
    return df


# ==================================================
# 初始化拉链表
# ==================================================
def init_goods_zipper(spark, new_df, etl_date):
    """初始化商品拉链表"""
    logger.info("开始初始化商品拉链表")
    
    new_df = new_df.withColumn("salt", F.floor(F.rand() * 10))
    new_df.createOrReplaceTempView("tmp_goods_new")
    
    result_df = spark.sql(f"""
        SELECT
            goods_id,
            goods_name,
            category,
            price,
            stock,
            brand,
            '{etl_date}' AS start_date,
            '9999-12-31' AS end_date
        FROM tmp_goods_new
    """)
    
    spark.sql("CREATE DATABASE IF NOT EXISTS dwd")
    
    result_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .bucketBy(8, "goods_id") \
        .sortBy("goods_id") \
        .saveAsTable("dwd.dwd_goods_zip")
    logger.info("商品拉链表初始化完成")


# ==================================================
# 数据质量检查规则配置
# ==================================================
def get_goods_quality_rules():
    """定义商品数据质量检查规则"""
    return [
        {"type": "not_null", "column": "goods_id", "severity": "ERROR"},
        {"type": "not_null", "column": "goods_name", "severity": "ERROR"},
        {"type": "unique", "column": "goods_id", "severity": "ERROR"},
        {"type": "range", "column": "price", "min": 0, "severity": "WARNING"},
        {"type": "range", "column": "stock", "min": 0, "severity": "WARNING"},
        {"type": "completeness", "columns": ["goods_id", "goods_name", "category", "price", "stock", "brand"], "severity": "ERROR"},
        {"type": "volume", "min_count": 1, "severity": "ERROR"}
    ]


# ==================================================
# 商品拉链ETL
# ==================================================
def run_goods_etl(spark, etl_date):
    """商品拉链表 ETL 主流程"""
    ##############################
    #【探测代码：直接定位问题】
    ##############################
    logger.info("=" * 60)
    logger.info(f"当前 Spark 仓库路径：{spark.conf.get('spark.sql.warehouse.dir')}")
    logger.info("📋 列出所有数据库：")
    spark.sql("SHOW DATABASES").show()

    logger.info("📋 列出 ods 库下的所有表：")
    spark.sql("SHOW TABLES IN ods").show()

    logger.info(f"📋 检查表是否存在：{spark.catalog.tableExists('ods.ods_goods_daily')}")
    logger.info("=" * 60)
    ##############################
    #探测代码结束
    ##############################
    
    # ========== 1. 读取 ODS 数据 ==========
    new_df = spark.sql(f"SELECT * FROM ods.ods_goods_daily WHERE dt = '{etl_date}'")
    
    # ========== 2. 数据质量检查（清洗前） ==========
    logger.info("=" * 50)
    logger.info("开始执行数据质量检查（清洗前）")
    logger.info("=" * 50)
    
    quality_rules = get_goods_quality_rules()
    pre_clean_report = run_data_quality_checks(spark, new_df, "ods.ods_goods_daily", etl_date, quality_rules)
    
    logger.info(f"清洗前质量报告: {pre_clean_report}")
    
    # 如果有 ERROR 级别的问题，可以决定是否终止流程
    error_issues = [i for i in pre_clean_report["issues"] if i["severity"] == "ERROR"]
    if error_issues:
        logger.error(f"发现 {len(error_issues)} 个 ERROR 级别问题，请人工确认是否继续")
        # 这里可以选择抛出异常终止，或者继续执行
        # raise Exception("数据质量检查未通过")
    
    # ========== 3. 数据清洗 ==========
    clean_df = clean_goods_data(new_df)
    
    # ========== 4. 数据质量检查（清洗后） ==========
    logger.info("=" * 50)
    logger.info("开始执行数据质量检查（清洗后）")
    logger.info("=" * 50)
    
    post_clean_report = run_data_quality_checks(spark, clean_df, "dwd.dwd_goods_zip", etl_date, quality_rules)
    logger.info(f"清洗后质量报告: {post_clean_report}")
    
    # ========== 5. 数据监控指标收集 ==========
    monitor = DataMonitor(spark)
    monitor.collect_metrics(clean_df, "dwd_goods_zip", etl_date)
    logger.info(f"监控指标: {monitor.get_metrics_report()}")
    
    prev_date = (datetime.strptime(etl_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # ==================================================
    # 判断是否需要初始化
    # ==================================================
    try:
        spark.sql("SELECT 1 FROM dwd.dwd_goods_zip LIMIT 1").collect()
        active_count = spark.sql(
            "SELECT COUNT(*) FROM dwd.dwd_goods_zip WHERE end_date = '9999-12-31'"
        ).collect()[0][0]
        
        if active_count == 0:
            logger.info("商品拉链表无有效记录，需要初始化")
            init_goods_zipper(spark, clean_df, etl_date)
            return
        else:
            logger.info(f"商品拉链表已有 {active_count} 条有效记录，执行增量更新")
            
    except Exception:
        logger.info("商品拉链表不存在，需要初始化")
        init_goods_zipper(spark, clean_df, etl_date)
        return
    
    old_df = spark.sql("SELECT * FROM dwd.dwd_goods_zip WHERE end_date = '9999-12-31'")
    
    # 加盐
    clean_df_salted = clean_df.withColumn("salt", F.floor(F.rand() * 10))
    old_df_salted = old_df.withColumn("salt", F.floor(F.rand() * 10))
    
    clean_df_salted.createOrReplaceTempView("tmp_goods_daily")
    old_df_salted.createOrReplaceTempView("tmp_goods_zip")
    
    # 启用 AQE
    spark.sql("SET spark.sql.adaptive.enabled=true")
    spark.sql("SET spark.sql.adaptive.skewJoin.enabled=true")
    
    logger.info("开始更新商品拉链表")
    
    result_df = spark.sql(f"""
        WITH 
        history AS (
            SELECT * FROM dwd.dwd_goods_zip WHERE end_date < '9999-12-31'
        ),
        old AS (
            SELECT * FROM tmp_goods_zip WHERE end_date = '9999-12-31'
        ),
        new AS (
            SELECT * FROM tmp_goods_daily
        ),
        changed AS (
            SELECT DISTINCT old.goods_id
            FROM old
            JOIN new ON old.goods_id = new.goods_id AND old.salt = new.salt
            WHERE old.price != new.price
               OR old.stock != new.stock
               OR old.goods_name != new.goods_name
               OR old.category != new.category
               OR old.brand != new.brand
        ),
        closed AS (
            SELECT
                old.goods_id,
                old.goods_name,
                old.category,
                old.price,
                old.stock,
                old.brand,
                old.start_date,
                '{prev_date}' AS end_date
            FROM old
            JOIN changed ON old.goods_id = changed.goods_id
        ),
        new_state AS (
            SELECT
                new.goods_id,
                new.goods_name,
                new.category,
                new.price,
                new.stock,
                new.brand,
                '{etl_date}' AS start_date,
                '9999-12-31' AS end_date
            FROM new
            JOIN changed ON new.goods_id = changed.goods_id
        ),
        unchanged AS (
            SELECT
                old.goods_id,
                old.goods_name,
                old.category,
                old.price,
                old.stock,
                old.brand,
                old.start_date,
                old.end_date
            FROM old
            LEFT ANTI JOIN changed ON old.goods_id = changed.goods_id
        ),
        added AS (
            SELECT
                new.goods_id,
                new.goods_name,
                new.category,
                new.price,
                new.stock,
                new.brand,
                '{etl_date}' AS start_date,
                '9999-12-31' AS end_date
            FROM new
            LEFT ANTI JOIN old ON new.goods_id = old.goods_id
        )
        SELECT * FROM history
        UNION ALL
        SELECT * FROM closed
        UNION ALL
        SELECT * FROM new_state
        UNION ALL
        SELECT * FROM unchanged
        UNION ALL
        SELECT * FROM added
    """)
    
    # ==================================================
    # 覆盖写入 Hive
    # ==================================================
    spark.sql("DROP TABLE IF EXISTS dwd.tmp_goods_zip")
    
    result_df.drop("salt").write \
        .mode("overwrite") \
        .format("parquet") \
        .bucketBy(8, "goods_id") \
        .sortBy("goods_id") \
        .saveAsTable("dwd.tmp_goods_zip")
    
    spark.sql("DROP TABLE IF EXISTS dwd.dwd_goods_zip")
    spark.sql("ALTER TABLE dwd.tmp_goods_zip RENAME TO dwd.dwd_goods_zip")
    
    # ========== 6. 最终质量检查 ==========
    final_df = spark.table("dwd.dwd_goods_zip")
    final_report = run_data_quality_checks(spark, final_df, "dwd.dwd_goods_zip", etl_date, quality_rules)
    logger.info(f"最终质量报告: {final_report}")
    
    # 统计信息
    total = spark.table("dwd.dwd_goods_zip").count()
    active = spark.sql("SELECT COUNT(*) FROM dwd.dwd_goods_zip WHERE end_date = '9999-12-31'").collect()[0][0]
    logger.info(f"商品拉链表更新完成，总记录={total}，有效记录={active}")


# ==================================================
# Main
# ==================================================
if __name__ == "__main__":
    etl_date = datetime.now().strftime("%Y-%m-%d")
    spark = get_spark()
    
    run_goods_etl(spark, etl_date)
    
    spark.stop()
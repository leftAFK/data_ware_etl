# dwd/user_etl.py
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
def clean_user_data(df):
    """清洗用户数据"""
    # 去重
    df = df.dropDuplicates(["user_id"])
    
    # 空值处理
    df = df.filter(F.col("user_id").isNotNull())
    df = df.filter(F.col("name").isNotNull())
    
    # 年龄清洗（范围 0-120，空值默认为0）
    df = df.withColumn("age", F.col("age").cast("int"))
    df = df.withColumn(
        "age",
        F.when((F.col("age") < 0) | (F.col("age") > 120), 0)
         .when(F.col("age").isNull(), 0)
         .otherwise(F.col("age"))
    )
    
    # 性别清洗（只保留男/女）
    df = df.withColumn("gender", F.trim(F.col("gender")))
    df = df.withColumn(
        "gender",
        F.when(~F.col("gender").isin(["男", "女"]), "未知")
         .otherwise(F.col("gender"))
    )
    
    # 手机号清洗（去除空格）
    df = df.withColumn("phone", F.trim(F.col("phone")))
    
    # 邮箱清洗（转小写，去除空格）
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    
    return df


# ==================================================
# 初始化拉链表
# ==================================================
def init_user_zipper(spark, new_df, etl_date):
    """初始化用户拉链表"""
    logger.info("开始初始化用户拉链表")
    
    new_df = new_df.withColumn("salt", F.floor(F.rand() * 10))
    new_df.createOrReplaceTempView("tmp_user_new")
    
    result_df = spark.sql(f"""
        SELECT
            user_id,
            name,
            gender,
            age,
            phone,
            email,
            '{etl_date}' AS start_date,
            '9999-12-31' AS end_date
        FROM tmp_user_new
    """)
    
    spark.sql("CREATE DATABASE IF NOT EXISTS dwd")
    
    result_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .bucketBy(8, "user_id") \
        .sortBy("user_id") \
        .saveAsTable("dwd.dwd_user_zip")
    logger.info("用户拉链表初始化完成")


# ==================================================
# 数据质量检查规则配置
# ==================================================
def get_user_quality_rules():
    """定义用户数据质量检查规则"""
    return [
        {"type": "not_null", "column": "user_id", "severity": "ERROR"},
        {"type": "not_null", "column": "name", "severity": "ERROR"},
        {"type": "unique", "column": "user_id", "severity": "ERROR"},
        {"type": "range", "column": "age", "min": 0, "max": 120, "severity": "WARNING"},
        {"type": "pattern", "column": "phone", "pattern": r"^1[3-9]\d{9}$", "severity": "WARNING"},
        {"type": "pattern", "column": "email", "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", "severity": "WARNING"},
        {"type": "completeness", "columns": ["user_id", "name", "gender", "age", "phone", "email"], "severity": "ERROR"},
        {"type": "volume", "min_count": 1, "severity": "ERROR"}
    ]


# ==================================================
# 用户拉链ETL
# ==================================================
def run_user_etl(spark, etl_date):
    """用户拉链表 ETL 主流程"""
    
    # ========== 1. 读取 ODS 数据 ==========
    new_df = spark.sql(f"SELECT * FROM ods.ods_user_daily WHERE dt = '{etl_date}'")
    
    # ========== 2. 数据质量检查（清洗前） ==========
    logger.info("=" * 50)
    logger.info("开始执行数据质量检查（清洗前）")
    logger.info("=" * 50)
    
    quality_rules = get_user_quality_rules()
    pre_clean_report = run_data_quality_checks(spark, new_df, "ods.ods_user_daily", etl_date, quality_rules)
    
    logger.info(f"清洗前质量报告: {pre_clean_report}")
    
    # 如果有 ERROR 级别的问题，可以决定是否终止流程
    error_issues = [i for i in pre_clean_report["issues"] if i["severity"] == "ERROR"]
    if error_issues:
        logger.error(f"发现 {len(error_issues)} 个 ERROR 级别问题，请人工确认是否继续")
        # 这里可以选择抛出异常终止，或者继续执行
        # raise Exception("数据质量检查未通过")
    
    # ========== 3. 数据清洗 ==========
    clean_df = clean_user_data(new_df)
    
    # ========== 4. 数据质量检查（清洗后） ==========
    logger.info("=" * 50)
    logger.info("开始执行数据质量检查（清洗后）")
    logger.info("=" * 50)
    
    post_clean_report = run_data_quality_checks(spark, clean_df, "dwd.dwd_user_zip", etl_date, quality_rules)
    logger.info(f"清洗后质量报告: {post_clean_report}")
    
    # ========== 5. 数据监控指标收集 ==========
    monitor = DataMonitor(spark)
    monitor.collect_metrics(clean_df, "dwd_user_zip", etl_date)
    logger.info(f"监控指标: {monitor.get_metrics_report()}")
    
    # ========== 6. 拉链表逻辑 ==========
    prev_date = (datetime.strptime(etl_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 判断是否需要初始化
    try:
        spark.sql("SELECT 1 FROM dwd.dwd_user_zip LIMIT 1").collect()
        active_count = spark.sql(
            "SELECT COUNT(*) FROM dwd.dwd_user_zip WHERE end_date = '9999-12-31'"
        ).collect()[0][0]
        
        if active_count == 0:
            logger.info("用户拉链表无有效记录，需要初始化")
            init_user_zipper(spark, clean_df, etl_date)
            return
        else:
            logger.info(f"用户拉链表已有 {active_count} 条有效记录，执行增量更新")
            
    except Exception:
        logger.info("用户拉链表不存在，需要初始化")
        init_user_zipper(spark, clean_df, etl_date)
        return
    
    old_df = spark.sql("SELECT * FROM dwd.dwd_user_zip WHERE end_date = '9999-12-31'")
    
    # 加盐
    clean_df_salted = clean_df.withColumn("salt", F.floor(F.rand() * 10))
    old_df_salted = old_df.withColumn("salt", F.floor(F.rand() * 10))
    
    clean_df_salted.createOrReplaceTempView("tmp_user_daily")
    old_df_salted.createOrReplaceTempView("tmp_user_zip")
    
    # 启用 AQE
    spark.sql("SET spark.sql.adaptive.enabled=true")
    spark.sql("SET spark.sql.adaptive.skewJoin.enabled=true")
    
    logger.info("开始更新用户拉链表")
    
    result_df = spark.sql(f"""
        WITH 
        history AS (
            SELECT * FROM dwd.dwd_user_zip WHERE end_date < '9999-12-31'
        ),
        old AS (
            SELECT * FROM tmp_user_zip WHERE end_date = '9999-12-31'
        ),
        new AS (
            SELECT * FROM tmp_user_daily
        ),
        changed AS (
            SELECT DISTINCT old.user_id
            FROM old
            JOIN new ON old.user_id = new.user_id AND old.salt = new.salt
            WHERE old.name != new.name
               OR old.gender != new.gender
               OR old.age != new.age
               OR old.phone != new.phone
               OR old.email != new.email
        ),
        closed AS (
            SELECT
                old.user_id,
                old.name,
                old.gender,
                old.age,
                old.phone,
                old.email,
                old.start_date,
                '{prev_date}' AS end_date
            FROM old
            JOIN changed ON old.user_id = changed.user_id
        ),
        new_state AS (
            SELECT
                new.user_id,
                new.name,
                new.gender,
                new.age,
                new.phone,
                new.email,
                '{etl_date}' AS start_date,
                '9999-12-31' AS end_date
            FROM new
            JOIN changed ON new.user_id = changed.user_id
        ),
        unchanged AS (
            SELECT
                old.user_id,
                old.name,
                old.gender,
                old.age,
                old.phone,
                old.email,
                old.start_date,
                old.end_date
            FROM old
            LEFT ANTI JOIN changed ON old.user_id = changed.user_id
        ),
        added AS (
            SELECT
                new.user_id,
                new.name,
                new.gender,
                new.age,
                new.phone,
                new.email,
                '{etl_date}' AS start_date,
                '9999-12-31' AS end_date
            FROM new
            LEFT ANTI JOIN old ON new.user_id = old.user_id
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
    
    # ========== 7. 覆盖写入 Hive ==========
    spark.sql("DROP TABLE IF EXISTS dwd.tmp_user_zip")
    
    result_df.drop("salt").write \
        .mode("overwrite") \
        .format("parquet") \
        .bucketBy(8, "user_id") \
        .sortBy("user_id") \
        .saveAsTable("dwd.tmp_user_zip")
    
    spark.sql("DROP TABLE IF EXISTS dwd.dwd_user_zip")
    spark.sql("ALTER TABLE dwd.tmp_user_zip RENAME TO dwd.dwd_user_zip")
    
    # ========== 8. 最终质量检查 ==========
    final_df = spark.table("dwd.dwd_user_zip")
    final_report = run_data_quality_checks(spark, final_df, "dwd.dwd_user_zip", etl_date, quality_rules)
    logger.info(f"最终质量报告: {final_report}")
    
    # 统计信息
    total = spark.table("dwd.dwd_user_zip").count()
    active = spark.sql("SELECT COUNT(*) FROM dwd.dwd_user_zip WHERE end_date = '9999-12-31'").collect()[0][0]
    logger.info(f"用户拉链表更新完成，总记录={total}，有效记录={active}")


# ==================================================
# Main
# ==================================================
if __name__ == "__main__":
    etl_date = datetime.now().strftime("%Y-%m-%d")
    spark = get_spark()
    
    run_user_etl(spark, etl_date)
    
    spark.stop()
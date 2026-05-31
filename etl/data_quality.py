# etl/data_quality.py

import logging
from datetime import datetime
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """数据质量检查类"""
    
    def __init__(self, spark, table_name, etl_date):
        self.spark = spark
        self.table_name = table_name
        self.etl_date = etl_date
        self.issues = []
    
    def check_not_null(self, df, column, severity="WARNING"):
        """检查非空约束"""
        null_count = df.filter(F.col(column).isNull()).count()
        if null_count > 0:
            issue = {
                "table": self.table_name,
                "column": column,
                "issue": f"发现 {null_count} 条空值",
                "severity": severity,
                "date": self.etl_date
            }
            self.issues.append(issue)
            logger.warning(f"[{severity}] {self.table_name}.{column} 有 {null_count} 条空值")
        return null_count == 0
    
    def check_unique(self, df, column, severity="ERROR"):
        """检查唯一性约束"""
        total_count = df.count()
        distinct_count = df.select(column).distinct().count()
        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            issue = {
                "table": self.table_name,
                "column": column,
                "issue": f"发现 {duplicate_count} 条重复数据",
                "severity": severity,
                "date": self.etl_date
            }
            self.issues.append(issue)
            logger.error(f"[{severity}] {self.table_name}.{column} 有 {duplicate_count} 条重复数据")
        return total_count == distinct_count
    
    def check_value_range(self, df, column, min_val=None, max_val=None, severity="WARNING"):
        """检查值范围"""
        if min_val is not None:
            out_of_range = df.filter(F.col(column) < min_val).count()
            if out_of_range > 0:
                issue = {
                    "table": self.table_name,
                    "column": column,
                    "issue": f"发现 {out_of_range} 条小于 {min_val} 的值",
                    "severity": severity,
                    "date": self.etl_date
                }
                self.issues.append(issue)
                logger.warning(f"[{severity}] {self.table_name}.{column} 有 {out_of_range} 条小于 {min_val}")
        
        if max_val is not None:
            out_of_range = df.filter(F.col(column) > max_val).count()
            if out_of_range > 0:
                issue = {
                    "table": self.table_name,
                    "column": column,
                    "issue": f"发现 {out_of_range} 条大于 {max_val} 的值",
                    "severity": severity,
                    "date": self.etl_date
                }
                self.issues.append(issue)
                logger.warning(f"[{severity}] {self.table_name}.{column} 有 {out_of_range} 条大于 {max_val}")
    
    def check_pattern(self, df, column, pattern, severity="WARNING"):
        """检查正则表达式模式"""

        mismatched = df.filter(~F.col(column).rlike(pattern)).count()
        if mismatched > 0:
            issue = {
                "table": self.table_name,
                "column": column,
                "issue": f"发现 {mismatched} 条不符合格式的数据",
                "severity": severity,
                "date": self.etl_date
            }
            self.issues.append(issue)
            logger.warning(f"[{severity}] {self.table_name}.{column} 有 {mismatched} 条格式错误")
        return mismatched == 0
    
    def check_data_volume(self, df, min_count=0, max_count=None, severity="WARNING"):
        """检查数据量"""
        count = df.count()
        if count < min_count:
            issue = {
                "table": self.table_name,
                "issue": f"数据量不足: {count} < {min_count}",
                "severity": severity,
                "date": self.etl_date
            }
            self.issues.append(issue)
            logger.warning(f"[{severity}] {self.table_name} 数据量 {count} 小于最小值 {min_count}")
        
        if max_count is not None and count > max_count:
            issue = {
                "table": self.table_name,
                "issue": f"数据量过大: {count} > {max_count}",
                "severity": severity,
                "date": self.etl_date
            }
            self.issues.append(issue)
            logger.warning(f"[{severity}] {self.table_name} 数据量 {count} 大于最大值 {max_count}")
        
        return count
    
    def check_completeness(self, df, expected_columns, severity="ERROR"):
        """检查列完整性"""
        #expected_columns: 期望的列
        actual_columns = set(df.columns)
        missing_columns = set(expected_columns) - actual_columns
        if missing_columns:
            issue = {
                "table": self.table_name,
                "issue": f"缺少列: {missing_columns}",
                "severity": severity,
                "date": self.etl_date
            }
            self.issues.append(issue)
            logger.error(f"[{severity}] {self.table_name} 缺少列: {missing_columns}")
        return len(missing_columns) == 0
    
    def get_report(self):
        """获取检测报告"""
        return {
            "table": self.table_name,
            "date": self.etl_date,
            "total_issues": len(self.issues),
            "issues": self.issues,
            "status": "PASS" if len(self.issues) == 0 else "FAIL"
        }


class DataMonitor:
    """数据监控类"""
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = {}
    
    def collect_metrics(self, df, table_name, etl_date):
        """收集数据指标"""
        total_count = df.count()
        
        metrics = {
            "table": table_name,
            "date": etl_date,
            "timestamp": datetime.now().isoformat(),
            "total_count": total_count,
            "column_count": len(df.columns),
            "null_counts": {},
            "distinct_counts": {}
        }
        
        # 统计每列的空值数量
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            metrics["null_counts"][col_name] = null_count
            metrics["distinct_counts"][col_name] = df.select(col_name).distinct().count()
        
        self.metrics[table_name] = metrics
        logger.info(f"收集指标完成: {table_name} - 总记录数={total_count}")
        return metrics
    
    def compare_with_previous(self, current_table, previous_table, join_key):
        """对比两个表的数据"""
        current_df = self.spark.table(current_table)
        previous_df = self.spark.table(previous_table)
        
        # 新增记录
        added = current_df.join(previous_df, join_key, "left_anti").count()
        
        # 删除记录
        deleted = previous_df.join(current_df, join_key, "left_anti").count()
        
        # 变更记录（需要比较全部字段）
        # 这里简化处理，实际可以比较更多字段
        
        comparison = {
            "current_table": current_table,
            "previous_table": previous_table,
            "added_count": added,
            "deleted_count": deleted,
            "change_rate": (added + deleted) / max(current_df.count(), 1)
        }
        
        logger.info(f"数据对比: 新增={added}, 删除={deleted}")
        return comparison
    
    def get_metrics_report(self):
        """获取指标报告"""
        return self.metrics


def run_data_quality_checks(spark, df, table_name, etl_date, rules):
    """
    运行数据质量检查
    
    Args:
        spark: SparkSession
        df: 要检查的 DataFrame
        table_name: 表名
        etl_date: ETL 日期
        rules: 检查规则字典
    
    Returns:
        检查报告
    """
    checker = DataQualityChecker(spark, table_name, etl_date)
    
    for rule in rules:
        rule_type = rule.get("type")
        column = rule.get("column")
        
        if rule_type == "not_null":
            checker.check_not_null(df, column, rule.get("severity", "WARNING"))
        
        elif rule_type == "unique":
            checker.check_unique(df, column, rule.get("severity", "ERROR"))
        
        elif rule_type == "range":
            checker.check_value_range(
                df, column, 
                rule.get("min"), rule.get("max"), 
                rule.get("severity", "WARNING")
            )
        
        elif rule_type == "pattern":
            checker.check_pattern(df, column, rule.get("pattern"), rule.get("severity", "WARNING"))
        
        elif rule_type == "volume":
            checker.check_data_volume(
                df, 
                rule.get("min_count", 0), 
                rule.get("max_count"), 
                rule.get("severity", "WARNING")
            )
        
        elif rule_type == "completeness":
            checker.check_completeness(df, rule.get("columns", []), rule.get("severity", "ERROR"))
    
    return checker.get_report()
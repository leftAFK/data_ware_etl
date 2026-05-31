[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/)
[![Airflow 3.2.0](https://img.shields.io/badge/airflow-3.0.0-green.svg)](https://airflow.apache.org/)
[![Spark 3.5.1](https://img.shields.io/badge/spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![License MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
## 项目简介

基于 Apache Airflow 3 和 PySpark 的数据仓库 ETL 项目，实现用户、订单、商品三个维度的拉链表处理，包含完整的数据质量检测和数据监控功能。

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Apache Airflow | 3.x | 工作流调度 |
| PySpark | 3.5+ | 大数据处理 |
| Hive | 2.3+ | 数据仓库 |
| Parquet | - | 列式存储 |

## 项目结构
data_warehouse_etl/
├── dags/ # Airflow DAG 文件
│ └── full_warehouse_etl.py # 主 DAG：ODS→DWD→DWS→ADS
├── etl/ # ETL 处理逻辑
│ ├── init.py
│ ├── data_quality.py # 数据质量检测模块
│ ├── user_etl.py # 用户拉链表 ETL
│ ├── order_etl.py # 订单拉链表 ETL
│ ├── goods_etl.py # 商品拉链表 ETL
│ ├── dws_etl.py # DWS 层汇总
│ └── ads_etl.py # ADS 层应用
├── scripts/ # 辅助脚本
│ ├── gen_user_data.py # 生成用户测试数据
│ ├── gen_order_data.py # 生成订单测试数据
│ └── gen_goods_data.py # 生成商品测试数据
├── warehouse/ # Hive 数据仓库目录
├── .gitignore
└── README.md

## 数仓分层架构
ODS (操作数据层) DWD (明细数据层) DWS (汇总数据层) ADS (应用数据层)
─────────────────────────────────────────────────────────────────────────────────────
ods_user_daily → dwd_user_zip → dws_user_summary → ads_user_portrait
ods_order_daily → dwd_order_zip → dws_order_stats → ads_sales_daily
ods_goods_daily → dwd_goods_zip → dws_goods_analysis

## 功能特性

### 1. 拉链表设计
- 支持历史数据追溯（通过 start_date/end_date）
- 使用加盐技术解决数据倾斜
- 启用 AQE 自适应查询优化
- 支持初始化和增量更新

### 2. 数据质量检测
| 检测类型 | 说明 | 严重级别 |
|---------|------|---------|
| not_null | 非空检查 | ERROR/WARNING |
| unique | 唯一性检查 | ERROR |
| range | 值范围检查 | WARNING |
| pattern | 格式检查 | WARNING |
| volume | 数据量检查 | WARNING |
| completeness | 列完整性检查 | ERROR |

### 3. 数据监控
- 自动收集数据指标（空值率、唯一值数等）
- 数据质量报告生成
- 异常告警机制

## 快速开始

### 环境要求

- Python 3.12+
- Apache Airflow 3.x
- PySpark 3.5+
- Java 11+ (Spark 需要)

### 安装步骤

**1. 克隆仓库**
```bash
git clone https://github.com/leftAFK/data_warehouse_etl.git
cd data_warehouse_etl

2. 创建虚拟环境

bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows
3. 安装依赖

bash
pip install apache-airflow==3.2.0 pyspark==3.5.0 pandas
4. 生成测试数据

bash
# 生成用户、订单、商品测试数据
python scripts/gen_user_data.py
python scripts/gen_order_data.py
python scripts/gen_goods_data.py

5. 配置 Airflow

bash
export AIRFLOW_HOME=/path/to/airflow
export PYTHONPATH=/path/to/data_warehouse_etl:$PYTHONPATH

# 初始化数据库
airflow db migrate

# 启动 Airflow
airflow standalone

6. 访问 Web UI

地址: http://localhost:8080

用户名: admin

密码: 查看终端输出的密码

DAG 调度配置
参数	            值	                     说明
DAG_ID	   data_warehouse_etl_clean	DAG    唯一标识
调度时间	        0 1 * * *	            每天凌晨 1:00
最大并发	          1	                    同时只运行一个实例
重试次数	          2	                    失败后重试 2 次

依赖关系
start
  ├── dwd_goods_etl ──┐
  ├── dwd_user_etl  ──┼── dwd_complete ── dws_etl ── dws_complete ── ads_etl ── end
  └── dwd_order_etl ──┘

数据质量报告示例
{
  "table": "dwd.dwd_order_zip",
  "date": "2026-05-30",
  "status": "PASS",
  "check_stats": {
      "total_checks": 7,
      "passed_checks": 6,
      "failed_checks": 1,
      "pass_rate": 85.71
  },
  "total_issues": 1
}

常用命令
# 查看 DAG 列表
airflow dags list

# 手动触发 DAG
airflow dags trigger data_warehouse_etl_clean

# 查看 DAG 运行状态
airflow dags list-runs --dag-id data_warehouse_etl_clean

# 测试单个 ETL
python etl/user_etl.py
python etl/order_etl.py
python etl/goods_etl.py

项目亮点
✅ 完整的数仓分层设计（ODS → DWD → DWS → ADS）
✅ 拉链表实现，支持历史数据追溯
✅ 数据质量检测与监控告警
✅ 解决数据倾斜的加盐技术
✅ AQE 自适应查询优化
✅ Airflow 3 任务编排

许可证
MIT License

联系方式
作者: [leftAFK] 
邮箱: [3432221344@qq.com]

## 3. 检查并清理不必要的文件

```bash
cd /Users/ok/Desktop/data_warehouse_etl

# 删除不需要的文件
rm -rf __pycache__
rm -rf etl/__pycache__
rm -rf dags/__pycache__
rm -rf scripts/__pycache__
rm -f .DS_Store
rm -f etl/.DS_Store
rm -f dags/.DS_Store

4. 更新 .gitignore
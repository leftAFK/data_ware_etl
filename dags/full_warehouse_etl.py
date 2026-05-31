from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
import pendulum
import os

PROJECT_ROOT = "/Users/ok/Desktop/data_warehouse_etl"
VENV_PYTHON = f"{PROJECT_ROOT}/.venv/bin/python"

DWD_GOODS_ETL = f"{PROJECT_ROOT}/etl/goods_etl.py"
DWD_USER_ETL = f"{PROJECT_ROOT}/etl/user_etl.py"
DWD_ORDER_ETL = f"{PROJECT_ROOT}/etl/order_etl.py"
DWS_ETL = f"{PROJECT_ROOT}/etl/dws_etl.py"
ADS_ETL = f"{PROJECT_ROOT}/etl/ads_etl.py"

EMAIL_RECIPIENTS = ['3432221344@qq.com']

# 成功通知（使用默认 SMTP 配置，不指定 conn_id）
success_notifier = SmtpNotifier(
    to=EMAIL_RECIPIENTS,
    subject='✅ Airflow 数据仓库 ETL 执行成功',
    html_content='''
    <h2>✅ 数据仓库 ETL 执行成功</h2>
    <p><b>DAG ID:</b> {{ dag.dag_id }}</p>
    <p><b>执行时间:</b> {{ execution_date }}</p>
    <p>所有 ETL 任务已成功完成！</p>
    <hr>
    <ul>
        <li>✅ DWD 用户拉链表</li>
        <li>✅ DWD 订单拉链表</li>
        <li>✅ DWD 商品拉链表</li>
        <li>✅ DWS 汇总层</li>
        <li>✅ ADS 应用层</li>
    </ul>
    '''
)

# 失败通知
failure_notifier = SmtpNotifier(
    to=EMAIL_RECIPIENTS,
    subject='❌ Airflow 数据仓库 ETL 执行失败',
    html_content='''
    <h2>❌ Airflow 任务执行失败</h2>
    <p><b>DAG 名称:</b> {{ dag.dag_id }}</p>
    <p><b>任务名称:</b> {{ task.task_id }}</p>
    <p><b>执行时间:</b> {{ execution_date }}</p>
    <p><b>失败原因:</b> {{ exception }}</p>
    <hr>
    <p>请检查 Airflow 日志获取详细信息。</p>
    '''
)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz='Asia/Shanghai'),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': failure_notifier,
}

with DAG(
    dag_id='data_warehouse_etl_clean',
    default_args=default_args,
    description='数据仓库ETL - 支持邮件告警',
    schedule='0 1 * * *',
    catchup=False,
    max_active_runs=1,
    on_success_callback=success_notifier,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    env = {
        'PYTHONPATH': PROJECT_ROOT,
        'PATH': os.environ.get('PATH', ''),
        'HOME': os.environ.get('HOME', ''),
        'SPARK_LOCAL_IP': '127.0.0.1',
    }
    
    cmd_prefix = f'cd {PROJECT_ROOT} && source .venv/bin/activate && '
    
    dwd_goods = BashOperator(
        task_id='dwd_goods_etl',
        bash_command=cmd_prefix + f'python {DWD_GOODS_ETL}',
        env=env,
    )
    
    dwd_user = BashOperator(
        task_id='dwd_user_etl',
        bash_command=cmd_prefix + f'python {DWD_USER_ETL}',
        env=env,
    )
    
    dwd_order = BashOperator(
        task_id='dwd_order_etl',
        bash_command=cmd_prefix + f'python {DWD_ORDER_ETL}',
        env=env,
    )
    
    dwd_complete = EmptyOperator(task_id='dwd_complete')
    
    dws = BashOperator(
        task_id='dws_etl',
        bash_command=cmd_prefix + f'python {DWS_ETL}',
        env=env,
    )
    
    dws_complete = EmptyOperator(task_id='dws_complete')
    
    ads = BashOperator(
        task_id='ads_etl',
        bash_command=cmd_prefix + f'python {ADS_ETL}',
        env=env,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [dwd_goods, dwd_user, dwd_order] >> dwd_complete
    dwd_complete >> dws >> dws_complete
    dws_complete >> ads >> end
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {'owner': 'airflow',
                'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
                        dag_id = "social_network_aus_data",
                        default_args=default_args,
                        schedule_interval=None,
                        )

partition_overwrite = SparkSubmitOperator(
                        task_id='partition_overwrite',
                        dag=dag_spark,
                        application ='/lessons/partition_overwrite.py' ,
                        conn_id= 'yarn_spark',
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

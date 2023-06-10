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
                        dag_id = "aus_social_network_result",
                        default_args=default_args,
                        start_date=datetime(2023, 6, 10),
                        schedule_interval='@daily'
                        )


user_data = SparkSubmitOperator(
                        task_id='user_data',
                        dag=dag_spark,
                        application ='/lessons/user_data.py' ,
                        conn_id= 'yarn_spark',
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

city_data = SparkSubmitOperator(
                        task_id='city_data',
                        dag=dag_spark,
                        application ='/lessons/city_data.py' ,
                        conn_id= 'yarn_spark',
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

friends_reco = SparkSubmitOperator(
                        task_id='friends_reco',
                        dag=dag_spark,
                        application ='/lessons/friends_reco.py' ,
                        conn_id= 'yarn_spark',
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksCreateJobsOperator, DatabricksRunNowOperator
from airflow.utils.dates import days_ago


default_args = {
  'owner': 'airflow'
}

with DAG(
    dag_id="databricks_create_jobs",
    schedule=None,
    start_date=days_ago(2),
    tags=["DatabricksCreateJobsOperator"],
    default_args = default_args,
    catchup=False,
    ) as dag:

    name = "DatabricksCreateJobExample"
    
    tasks = [
        {
            "task_key": "task1",
            "job_cluster_key": "first_cluster",
            "notebook_task": {
                "notebook_path": "/Users/andrew.lucius@databricks.com/Airflow Run Now",
            } 
        },
        {
            "task_key": "task2",
            "job_cluster_key": "first_cluster",
            "notebook_task": {
                "notebook_path": "/Users/andrew.lucius@databricks.com/Airflow Run Now Task 2",
            },
        },
        {
            "task_key": "task3",
            "job_cluster_key": "second_cluster",
            "notebook_task": {
                "notebook_path": "/Users/andrew.lucius@databricks.com/Airflow Run Now Task 2",
            },
        },
    ]
    job_clusters = [  
        {
            "job_cluster_key": "first_cluster",
            "new_cluster": {    
                "spark_version": "11.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "aws_attributes": {"availability": "ON_DEMAND"},
                "num_workers": 0,
                },
        },
        {
            "job_cluster_key": "second_cluster",
            "new_cluster": {    
                "spark_version": "11.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "aws_attributes": {"availability": "ON_DEMAND"},
                "num_workers": 0,
                },
        }
    ]
    
    jobs_create_named = DatabricksCreateJobsOperator(
        task_id="DatabricksJobsCreate", 
        tasks=tasks, 
        job_clusters=job_clusters,
        name=name
    )

    run_now = DatabricksRunNowOperator(
        task_id="run_now", 
        job_id="{{ ti.xcom_pull(task_ids='DatabricksJobsCreate') }}",
        databricks_conn_id = 'databricks_default'
        )
    jobs_create_named >> run_now
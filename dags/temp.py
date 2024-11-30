from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def print_hello_world():
    print("Hello, World!")


def choose_branch(**kwargs):
    if kwargs['logical_date'].weekday() < 5:
        return 'weekday_task'
    else:
        return 'weekend_task'

with DAG(
    'complex_dag_example',
    default_args=default_args,
    description='A complex DAG example',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    list_files = BashOperator(
        task_id='list_files',
        bash_command='ls -l',
    )

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=print_hello_world,
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch,
        provide_context=True,
    )

    weekday_task = BashOperator(
        task_id='weekday_task',
        bash_command='echo "Today is a weekday!"',
    )

    weekend_task = BashOperator(
        task_id='weekend_task',
        bash_command='echo "Today is a weekend!"',
    )

    weekday_weekend_task = BashOperator(
        task_id='weekday_weekend_task',
        bash_command='echo "Today is not weekend and weekday!"',
    )

    final_task = BashOperator(
        task_id='final_task',
        bash_command='echo "This is the final task"',
        trigger_rule='none_failed_or_skipped',
    )

    list_files >> hello_world >> branching
    branching >> weekday_task >> final_task
    branching >> weekend_task >> final_task
    branching >> weekday_weekend_task >> final_task

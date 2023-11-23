from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import get_data_lib as lib

# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла
bash_command_tmpl = """
head {{ params.files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_data():
    bucket_files = ['groups.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']

    groups = PythonOperator(
        task_id=f'fetch_groups',
        python_callable=lib.load_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups'},
    )

    users = PythonOperator(
        task_id=f'fetch_users',
        python_callable=lib.load_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'users', 'type_override': {'age': 'Int64'}},
    )
    
    dialogs = PythonOperator(
        task_id=f'fetch_dialogs',
        python_callable=lib.load_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs', 'type_override': {'message_group': 'Int64'}},
    )

    dialogs = PythonOperator(
        task_id=f'fetch_group_logs',
        python_callable=lib.load_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log', 'type_override': {'user_id_from': 'Int64'}},
    )
    
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': ' '.join([f'/data/{f}' for f in bucket_files])}
    )

    [groups, users, dialogs, dialogs] >> print_10_lines_of_each

sprint6_dag_get_data = sprint6_dag_get_data()
from airflow import DAG
from airflow.operators.python import PythonOperator
# import pendulum
from datetime import datetime
import papermill as pm

def run_notebook():
    pm.execute_notebook(
        '/home/iceberg/notebooks/Untitled.ipynb',
        '/home/iceberg/notebooks/output.ipynb'
    )

with DAG(
    dag_id="run_spark_notebook_python",
    # start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    run_notebook_task = PythonOperator(
        task_id='run_notebook_task',
        python_callable=run_notebook
    )

####### OLD 
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# with DAG(
#     dag_id="run_spark_notebook",
#     start_date=datetime(2025, 9, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     run_notebook = BashOperator(
#         task_id="run_notebook",
#         # bash_command="papermill /home/iceberg/notebooks/Untitled.ipynb /home/iceberg/notebooks/output.ipynb"
#         # bash_command="""
#         #             export PATH=$PATH:/home/airflow/.local/bin
#         #             papermill /home/iceberg/notebooks/Untitled.ipynb /home/iceberg/notebooks/output.ipynb
#         #             """
#         # bash_command="""
#         #                 python -m papermill /home/iceberg/notebooks/Untitled.ipynb /home/iceberg/notebooks/output.ipynb
#         #              """,        
#         # bash_command="""
#         # export PATH=$PATH:/home/airflow/.local/bin
#         # python -m papermill /home/iceberg/notebooks/Untitled.ipynb /home/iceberg/notebooks/output.ipynb
#         # """
#         bash_command="""
#             # Make sure user-local bin is on PATH
#             export PATH=$PATH:/home/airflow/.local/bin
#             python -m papermill \
#                 /home/iceberg/notebooks/Untitled.ipynb \
#                 /home/iceberg/notebooks/output_$(date +%Y%m%d%H%M%S).ipynb
#         """,
#     )
    
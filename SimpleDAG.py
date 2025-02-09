# To initiate the DAG Object
from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator


# Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12)
}

# Creating DAG Object
dag = DAG(dag_id='DAG-1',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

# Creating first task
 start = DummyOperator(task_id = 'start', dag = dag)

# Creating second task
 run = DummyOperator(task_id = 'run', dag = dag)

# Creating Third task
 end = DummyOperator(task_id = 'end', dag = dag)

# Setting up dependencies 
start >> run >> end 

# We can also write it as 
end.set_upstream(run)
run.set_upstream(start)

# or We can also write it as
start.set_downstream(run).set_downstream(end)


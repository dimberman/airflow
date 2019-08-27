import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from tests.models import DEFAULT_DATE
from airflow.models import DAG, TaskFail, TaskInstance as TI, TaskReschedule


dag_id = 'test_knative_worker'
dag = DAG(dag_id, start_date=DEFAULT_DATE,
          end_date=DEFAULT_DATE + datetime.timedelta(days=10))
task = DummyOperator(task_id='op', email='test@test.test', dag=dag)
tis = []
for i in range(0, 100):
    task = DummyOperator(task_id='op' + str(i), email='test@test.test', dag=dag)
    tis.append(TI(task=task, execution_date=datetime.datetime.now()))

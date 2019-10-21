import unittest

from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.models import taskinstance
from airflow.utils.state import State
from tests.models import DEFAULT_DATE
from airflow.worker import task_runner_worker
import time
import asyncio

class TestTaskRunnerWorker(unittest.TestCase):
    def test_heartbeating(self):
        dag = DAG(dag_id='test_requeue_over_dag_concurrency', start_date=DEFAULT_DATE,
                  max_active_runs=1, concurrency=2)
        task = DummyOperator(task_id='test_requeue_over_dag_concurrency_op', dag=dag)

        ti = TI(task=task, execution_date=timezone.utcnow(), state=State.RUNNING)
        # TI.run() will sync from DB before validating deps.
        with create_session() as session:
            session.add(ti)
            session.commit()
        ti.heartbeat(session=session, time=timezone.utcnow())
        ti.refresh_from_db()
        a = session.query(TI).all()
        time.sleep(5)
        stale = taskinstance.get_stale_running_task_instances(session, stale_tolerance=3)
        self.assertIsNone(stale)

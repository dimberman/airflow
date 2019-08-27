import asyncio
import logging
import os
from datetime import datetime
from typing import Any

from flask import Blueprint
from flask import Flask
from flask import request
import base64
from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.models import (
    DagBag, TaskInstance
)
from airflow.utils.log.logging_mixin import (LoggingMixin)
from airflow.utils.net import get_hostname
from airflow.utils.state import State
from concurrent.futures import ProcessPoolExecutor

app = None  # type: Any
loop: asyncio.AbstractEventLoop = None
executor = None
DAGS_FOLDER = settings.DAGS_FOLDER

from flask import jsonify

from flask_api import FlaskAPI, status, exceptions

class AirflowTaskFailedException(Exception):
    status_code = 500

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

async def abar(a):
    print(a)


def create_app():
    global loop, app, executor
    loop = asyncio.get_event_loop()
    executor = ProcessPoolExecutor()
    app = FlaskAPI(__name__)
    app.register_blueprint(routes)
    return app


routes = Blueprint('routes', __name__)


@routes.route("/health")
def health():
    return "I am healthy"


@routes.route("/run")
def run_task():
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')
    subdir = request.args.get('subdir')
    execution_date = datetime.fromtimestamp(int(request.args.get("execution_date")))
    log = LoggingMixin().log
    #
    log.info("running dag {} for task {} on date {} in subdir {}".format(dag_id,task_id,execution_date, subdir))
    logging.shutdown()
    #
    try:
        out = run(dag_id, task_id, execution_date, subdir)
        # run(dag_id=dag_id, task_id=task_id, subdir=subdir, execution_date=execution_date)
        # loop.run_until_complete(run(dag_id=dag_id, task_id=task_id, execution_date=datetime.now()))
        if out.state == 'success':
            return "successfully ran dag {} for task {} on date {}".format(dag_id, task_id, execution_date), status.HTTP_200_OK
        else:
            return "task failed", status.HTTP_500_INTERNAL_SERVER_ERROR
    except ValueError as e:
        import traceback
        tb = traceback.format_exc()
        return "failed {} {}".format(e, tb), status.HTTP_500_INTERNAL_SERVER_ERROR


def process_subdir(subdir):
    if subdir:
        subdir = subdir.replace('DAGS_FOLDER', DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
        return subdir


def get_dag(dag_id: str, subdir: str) -> DAG:
    dagbag = DagBag(process_subdir(subdir))
    if dag_id not in dagbag.dags:
        raise AirflowException(
            'dag_id could not be found: {}. Either the dag did not exist or it failed to '
            'parse.'.format(dag_id))
    return dagbag.dags[dag_id]


def get_task_instance(
    dag_id: str,
    task_id: str,
    subdir: str,
    execution_date: datetime,
):
    dag = get_dag(dag_id, subdir)

    task = dag.get_task(task_id=task_id)
    ti = TaskInstance(task, execution_date)
    return ti


def run(dag_id: str,
        task_id: str,
        execution_date: datetime,
        subdir: str = None,
        ):
    log = LoggingMixin().log

    # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    settings.configure_orm(disable_connection_pool=True)
    ti = get_task_instance(dag_id=dag_id,
                           task_id=task_id,
                           subdir=subdir,
                           execution_date=execution_date)
    run_task_instance(ti, log)
    logging.shutdown()
    return ti


def run_task_instance(ti: TaskInstance, log):
    ti.refresh_from_db()
    set_task_instance_to_running(ti)
    ti.init_run_context()
    hostname = get_hostname()
    log.info("Running %s on host %s", ti, hostname)
    ti._run_raw_task()


def set_task_instance_to_running(ti):
    ti.state = State.RUNNING
    session = settings.Session()
    session.merge(ti)
    session.commit()

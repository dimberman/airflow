import asyncio
import logging
import os
from datetime import datetime
from typing import Any

# from flask import Blueprint
# from flask import Flask
# from flask import request
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
pool = None
executor = None
DAGS_FOLDER = settings.DAGS_FOLDER
# from multiprocessing import Pool
from functools import partial
# from flask import jsonify

import asyncio
from aiohttp import web


async def health(request):
    name =request.rel_url.query["name"]
    return web.Response(text="Hello, {}".format(name))


async def run_task(request):
    dag_id = request.rel_url.query['dag_id']
    task_id = request.rel_url.query['task_id']
    subdir = request.rel_url.query['subdir']
    execution_date = datetime.fromtimestamp(int(request.rel_url.query["execution_date"]))
    log = LoggingMixin().log
    #
    log.info("running dag {} for task {} on date {} in subdir {}".format(dag_id, task_id, execution_date, subdir))
    logging.shutdown()
    try:
        out = run(dag_id, task_id, execution_date, subdir)
        if out.state == 'success':
            return web.Response(body="successfully ran dag {} for task {} on date {}".format(dag_id, task_id, execution_date),
                         status=200)
        else:
            return web.Response(body="task failed", status=500)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return web.Response(body="failed {} {}".format(e, tb), status=500)


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


async def create_app():
    global loop, app, executor
    loop = asyncio.get_event_loop()
    executor = ProcessPoolExecutor()
    loop.set_default_executor(executor)
    app = web.Application()
    app.add_routes([web.get('/health', health), web.get('/run', run_task)])
    return app

import importlib
import logging

import os
import subprocess
import textwrap
import random
import string
from importlib import import_module
import functools

import getpass
import reprlib
import argparse
from argparse import RawTextHelpFormatter
from datetime import datetime
from airflow.utils.timezone import parse as parsedate
import json
from tabulate import tabulate

import daemon
from daemon.pidfile import TimeoutPIDLockFile
import signal
import sys
import threading
import traceback
import time
import psutil
import re
from urllib.parse import urlunparse
from typing import Any

import airflow
from airflow import api
from airflow import jobs, settings
from airflow import configuration as conf
from airflow.exceptions import AirflowException, AirflowWebServerTimeout
from airflow.executors import get_default_executor
from airflow.models import (
    Connection, DagModel, DagBag, DagPickle, TaskInstance, DagRun, Variable, DAG
)

from parameterized import parameterized, param
from sqlalchemy.orm.session import Session
from airflow import models, settings, configuration
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import DAG, TaskFail, TaskInstance as TI, TaskReschedule, DagRun
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.state import State
from tests.models import DEFAULT_DATE

from airflow.ti_deps.dep_context import (DepContext, SCHEDULER_DEPS)
from airflow.utils import cli as cli_utils, db
from airflow.utils.net import get_hostname
from airflow.utils.log.logging_mixin import (LoggingMixin, redirect_stderr,
                                             redirect_stdout)
from airflow.www.app import cached_app, create_app, cached_appbuilder

from sqlalchemy.orm import exc
from flask import request

import asyncio
from flask import Flask


async def abar(a):
    print(a)

def create_app():
    loop = asyncio.get_event_loop()
    app = Flask(__name__)
    DAGS_FOLDER = settings.DAGS_FOLDER
    return app


@app.route("/run")
def run_task():
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')
    print("running dag {} for task {}".format(dag_id,task_id))
    # loop.run_until_complete(run(dag_id=dag_id, task_id=task_id, execution_date=datetime.now()))
    return "OK"


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


async def run(dag_id: str,
              task_id: str,
              execution_date: datetime):
    log = LoggingMixin().log

    # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    settings.configure_orm(disable_connection_pool=True)
    dag = get_dag(dag_id, "")

    task = dag.get_task(task_id=task_id)
    ti = TaskInstance(task, execution_date)
    ti.refresh_from_db()
    set_task_instance_to_running(ti)

    ti.init_run_context()

    hostname = get_hostname()
    log.info("Running %s on host %s", ti, hostname)
    ti._run_raw_task()
    logging.shutdown()


def set_task_instance_to_running(ti):
    ti.state = State.RUNNING
    session = settings.Session()
    session.merge(ti)
    session.commit()



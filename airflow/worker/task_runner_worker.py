# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import logging
import os
from datetime import datetime
from typing import Any

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.models import (
    DagBag, TaskInstance
)
from airflow.utils.log.logging_mixin import (LoggingMixin)
from airflow.jobs import LocalTaskJob

import pendulum
from flask import request

from flask_api import FlaskAPI

app = None  # type: Any
DAGS_FOLDER = settings.DAGS_FOLDER
app = FlaskAPI(__name__)


@app.route("/health")
def health():
    name = request.args.get("name")
    return "Hello, {}..".format(name)


@app.route("/run")
def run_task():
    dag_id = request.args.get('dag_id')
    task_id = request.args.get('task_id')
    # subdir = request.rel_url.query['subdir']
    # subdir = "/root/airflow/dags"
    subdir = None
    execution_date = pendulum.fromtimestamp(int(request.args.get("execution_date")))
    log = LoggingMixin().log
    log.info("running dag {} for task {} on date {} in subdir {}"
             .format(dag_id, task_id, execution_date, subdir))
    logging.shutdown()
    try:
        # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
        # behind multiple open sleeping connections while heartbeating, which could
        # easily exceed the database connection limit when
        # processing hundreds of simultaneous tasks.
        settings.configure_orm(disable_connection_pool=True)

        ti = get_task_instance(dag_id=dag_id,
                               task_id=task_id,
                               subdir=subdir,
                               execution_date=execution_date)
        ti.refresh_from_db()
        local_job = LocalTaskJob(
            task_instance=ti,
            ignore_ti_state=True,
        )
        local_job.run()
        return "task passed or failed successfully", 200
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return "failed {} {}".format(e, tb), 500


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


def create_app():
    return app

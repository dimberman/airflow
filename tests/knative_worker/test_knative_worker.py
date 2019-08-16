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

import datetime
import unittest

from airflow import models
from airflow.knative_worker import knative_worker
from airflow.models import DAG, TaskFail, TaskInstance as TI, TaskReschedule
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import create_session
from airflow.utils.log.logging_mixin import (LoggingMixin)
from airflow.utils.state import State
from tests.models import DEFAULT_DATE


class TestKnativeWorker(unittest.TestCase):

    def tearDown(self):
        with create_session() as session:
            session.query(TaskFail).delete()
            session.query(TaskReschedule).delete()
            session.query(models.TaskInstance).delete()
            session.query(models.DagRun).delete()

    def test_knative_run_task(self):
        dag = DAG('test_success_callbak_no_race_condition', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task = DummyOperator(task_id='op', email='test@test.test', dag=dag)
        ti = TI(task=task, execution_date=datetime.datetime.now())
        log = LoggingMixin().log

        knative_worker.run_task_instance(ti, log)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

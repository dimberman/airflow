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
import asyncio
import requests
from requests import Response
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import aiohttp

loop: asyncio.AbstractEventLoop = None
executor = None

def make_request(task_id):
    req = 'http://35.245.62.83/run'
    date = int(datetime.datetime.timestamp(datetime.datetime.now()))

    params = {
        "task_id": task_id,
        "dag_id": 'my_dag',
        "execution_date": date,
        "subdir": "/root/airflow/dags"
    }

    return requests.get(req, params, headers={"Host": "airflow-knative.default.example.com"})


async def make_request_async(task_id) -> aiohttp.ClientResponse:
    req = 'http://35.245.62.83/run'
    date = int(datetime.datetime.timestamp(datetime.datetime.now()))
    params = {
        "task_id": task_id,
        "dag_id": 'my_dag',
        "execution_date": date,
        "subdir": "/root/airflow/dags"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url=req, params=params, headers={"Host": "airflow-knative.default.example.com"}) as resp:
            print(resp.status)
            print(await resp.text())
            return resp

async def request(dag_id, task_id, execution_date, app):
    query_string = "dag_id={}&task_id={}&execution_date={}".format(dag_id, task_id, execution_date)
    single_task = app.get("/run", query_string=query_string)


class TestKnativeWorker(unittest.TestCase):

    def setUp(self):
        global loop, executor
        executor = ThreadPoolExecutor
        loop = asyncio.get_event_loop()
        self.app = knative_worker.create_app()
        # self.assertEqual(app.debug, False)

    # executed after each test
    def tearDown(self):
        with create_session() as session:
            session.query(TaskFail).delete()
            session.query(TaskReschedule).delete()
            session.query(models.TaskInstance).delete()
            session.query(models.DagRun).delete()

    def test_basic_health(self):
        health_check = self.app.get("/health")
        self.assertEqual(200, health_check.status_code)

    def test_knative_run_task(self):
        date = int(datetime.datetime.timestamp(datetime.datetime.now()))
        dag_id = "test_knative_worker"
        task_id = "op0"
        query_string = "dag_id={}&task_id={}&execution_date={}".format(dag_id, task_id, date)
        single_task = self.app.get("/run", query_string=query_string)
        self.assertEqual(200, single_task.status_code)
        dag = knative_worker.get_dag(dag_id=dag_id, subdir=None)
        tis = dag.get_task_instances()
        self.assertEqual(1, len(tis))
        self.assertEqual(State.SUCCESS, tis[0].state)

    def test_knative_run_multiple_tasks(self):
        requests = []
        dag_id = "test_knative_worker"
        date = int(datetime.datetime.timestamp(datetime.datetime.now()))

        for t in range(0, 100):
            # task_ids.append("op" + str(t))
            requests.append(
                asyncio.ensure_future(request(dag_id=dag_id, task_id="op" + str(t), execution_date=date, app=self.app)))
        self.loop.run_until_complete(asyncio.gather(*requests))

    def test_execute_work(self):
        resp = make_request("runme_0")
        self.assertEqual(200, resp.status_code)

    def test_execute_lots_of_work(self):
        tasks=[]
        for i in range(0, 20):
            tasks.append(make_request_async("runme_" + str(i)))
        results = loop.run_until_complete(asyncio.gather(*tasks))
        for result in results:
            self.assertEqual(200, result.status)

    def test_execute_lots_of_work_async(self):
        tasks = []
        for j in range(0,10):
            for i in range(0, 20):
                tasks.append(asyncio.ensure_future(make_request_async("runme_"+str(i))))
        results = loop.run_until_complete(asyncio.gather(*tasks))
        status_codes = [result.status_code for result in results]
        correct = status_codes.count(200)
        incorrect = status_codes.count(500)
        for result in results:
            self.assertEqual(200, result.status_code)
        print(results)

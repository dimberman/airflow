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
"""
LocalExecutor runs tasks by spawning processes in a controlled fashion in different
modes. Given that BaseExecutor has the option to receive a `parallelism` parameter to
limit the number of process spawned, when this parameter is `0` the number of processes
that LocalExecutor can spawn is unlimited.

The following strategies are implemented:
1. Unlimited Parallelism (self.parallelism == 0): In this strategy, LocalExecutor will
spawn a process every time `execute_async` is called, that is, every task submitted to the
LocalExecutor will be executed in its own process. Once the task is executed and the
result stored in the `result_queue`, the process terminates. There is no need for a
`task_queue` in this approach, since as soon as a task is received a new process will be
allocated to the task. Processes used in this strategy are of class LocalWorker.

2. Limited Parallelism (self.parallelism > 0): In this strategy, the LocalExecutor spawns
the number of processes equal to the value of `self.parallelism` at `start` time,
using a `task_queue` to coordinate the ingestion of tasks and the work distribution among
the workers, which will take a task as soon as they are ready. During the lifecycle of
the LocalExecutor, the worker processes are running waiting for tasks, once the
LocalExecutor receives the call to shutdown the executor a poison token is sent to the
workers to terminate them. Processes used in this strategy are of class QueuedLocalWorker.

Arguably, `SequentialExecutor` could be thought as a LocalExecutor with limited
parallelism of just 1 worker, i.e. `self.parallelism = 1`.
This option could lead to the unification of the executor implementations, running
locally, into just one `LocalExecutor` with multiple modes.
"""

import multiprocessing
import subprocess

from queue import Empty

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
import asyncio
import datetime
from concurrent.futures import ProcessPoolExecutor
import requests
from airflow.utils.dag_processing import SimpleTaskInstance
from asyncio.futures import Future
from functools import partial
from requests import Response
import aiohttp



async def make_request_async(task_id, dag_id, ) -> aiohttp.ClientResponse:
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


class KnativeExecutor(BaseExecutor):
    """
    KnativeExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """
    task_queue: multiprocessing.Queue = None
    result_queue: multiprocessing.Queue = None
    def terminate(self):
        pass

    def __init__(self, loop):
        self.manager = multiprocessing.Manager()
        self.result_queue = self.manager.Queue()
        self.loop = loop
        self.pool = multiprocessing.Pool(processes=10)

        super().__init__()

     def execute_work(self, key, task_instance: SimpleTaskInstance = None):
        """
        Executes command received and stores result state in queue.
        :param task_instance:
        :param key: the key to identify the TI
        :type key: tuple(dag_id, task_id, execution_date)
        :param command: the command to execute
        :type command: str
        """
        if key is None:
            return
        self.log.info("%s running %s", self.__class__.__name__, task_instance)
        try:
            date = int(datetime.datetime.timestamp(task_instance.execution_date))
            req = 'http://airflow-knative.default/run'
            params = {
                "task_id": task_instance.task_id,
                "dag_id": task_instance.dag_id,
                "execution_date": date,
                "subdir": "/root/airflow/dags"
            }
            self.log.info(
                "expected request {}/run?task_id={}&dag_id={}&execution_date={}".format(req, task_instance.task_id,
                                                                                        task_instance.dag_id, date))
            future = self.make_request_async(task_instance.task_id)
            resp: requests.Response = await future
            if resp.status_code != 200:
                state = State.FAILED
                self.log.error("Failed to execute task %s.", str(resp.text))
                self.result_queue.put((key, state))
            else:
                self.log.info("assuming task success")
                # self.result_queue.put((key, None))
        except asyncio.InvalidStateError as e:
            state = State.FAILED
            self.log.error("Failed to execute task %s.", str(e))
            self.result_queue.put((key, state))

    def start(self):
        self.manager = multiprocessing.Manager()
        self.result_queue = self.manager.Queue()
        self.task_queue:multiprocessing.Queue = self.manager.Queue()
        self.workers = []

    async def execute_group_async(self,
                            task_instances:multiprocessing.Queue=None):
        tasks_to_run = []
        while not task_instances.empty():
            tasks_to_run.append(task_instances.get())
        for t in tasks_to_run:
            await make_request_async(t.task_id, t.dag_id)
            (key, ti) = t
            tasks.append(asyncio.ensure_future(self.execute_work(key=key, task_instance=ti)))
        self.loop.run_until_complete(asyncio.gather(*tasks))

    def execute_async(self, key, command, queue=None, executor_config=None, task_instance=None):
        self.task_queue.put(task_instance)
        # self.loop.run_until_complete(
        pass

    def sync(self):
        self.execute_group_async(self.task_queue)
        while not self.result_queue.empty():
            try:
                results = self.result_queue.get_nowait()
                self.change_state(*results)
            except Empty:
                break

    def end(self):
        self.sync()
        self.manager.shutdown()

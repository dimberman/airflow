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

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
import asyncio
import datetime
from airflow.utils.dag_processing import SimpleTaskInstance
import aiohttp
import functools


async def make_request_async(task_id, dag_id, execution_date) -> aiohttp.ClientResponse:
    # req = 'http://35.245.62.83/run'
    req = "http://localhost:8082/run"
    date = int(datetime.datetime.timestamp(execution_date))
    params = {
        "task_id": task_id,
        "dag_id": dag_id,
        "execution_date": date,
    }
    timeout = aiohttp.ClientTimeout(total=60000)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url=req, params=params, ) as resp:
            print(resp.status)
            print(await resp.text())
            return resp


class LocalExecutorLoop(multiprocessing.Process, LoggingMixin):
    def __init__(self,
                 task_pipe: multiprocessing.Pipe,
                 result_pipe: multiprocessing.Pipe,
                 pop_running_pipe: multiprocessing.Pipe
                 ):

        super().__init__()
        self.task_pipe = task_pipe
        self.result_pipe = result_pipe
        self.pop_running_pipe = pop_running_pipe

    def recieve_and_execute(self, loop):
        current_task = self.task_pipe.recv()
        key, task_instance = current_task
        loop.create_task(self.execute_work(key=key, task_instance=task_instance))

    def run(self):
        loop = asyncio.get_event_loop()
        loop.add_reader(fd=self.task_pipe, callback=functools.partial(self.recieve_and_execute, loop))
        loop.run_forever()

    async def execute_work(self, key, task_instance: SimpleTaskInstance = None):
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
            future = make_request_async(task_instance.task_id, task_instance.dag_id, task_instance.execution_date)
            resp: aiohttp.ClientResponse = await future
            if resp.status != 200:
                state = State.FAILED
                self.log.error("Failed to execute task %s.", str(resp.text))
                self.result_pipe.send((key, state))
            else:
                self.log.info("assuming task success")
                # self.result_queue.put((key, None))
                self.pop_running_pipe.send(key)
                import sys


        except asyncio.InvalidStateError as e:
            state = State.FAILED
            self.log.error("Failed to execute task %s.", str(e))
            import sys
            self.result_pipe.send((key, state))



class TaskMessage:
    def __init__(self, key, ti):
        self.key = key
        self.ti = ti


class LocalExecutor(BaseExecutor):
    """
    KnativeExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """
    task_queue: multiprocessing.Queue = None
    result_queue: multiprocessing.Queue = None
    result_pipe: multiprocessing.Pipe = None
    pop_running_queue: multiprocessing.Pipe = None

    def terminate(self):
        pass

    def __init__(self):
        super().__init__()

    def start(self):
        self.result_pipe, child_result_pipe = multiprocessing.Pipe()
        self.task_pipe, child_task_pipe = multiprocessing.Pipe()
        self.pop_running_pipe, child_pop_running_pipe = multiprocessing.Pipe()

        self.local_loop = LocalExecutorLoop(
            task_pipe=child_task_pipe,
            result_pipe=child_result_pipe,
            pop_running_pipe=child_pop_running_pipe
        )
        self.local_loop.start()

    def execute_async(self, key, command, queue=None, executor_config=None, task_instance=None):
        self.task_pipe.send((key, task_instance))

    def sync(self):
        while self.result_pipe.poll():
            results = self.result_pipe.recv()
            (_, state) = results
            if state == state.FAILED:
                import sys
                sys.exit(0)
            self.change_state(*results)
        while self.pop_running_pipe.poll():
            self.set_not_running(self.pop_running_pipe.recv())

    def end(self):
        self.sync()

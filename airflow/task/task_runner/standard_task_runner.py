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

import os

import psutil
from setproctitle import setproctitle

from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import reap_process_group


class StandardTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task by invoking through the Bash shell.
    """
    def __init__(self, local_task_job):
        super().__init__(local_task_job)
        self._rc = None

    def start(self):
        pid = os.fork()
        if pid:
            self.log.info("Started process %d to run task", pid)
            self.process = psutil.Process(pid)
            return
        else:
            from airflow.bin.cli import CLIFactory
            parser = CLIFactory.get_parser()
            args = parser.parse_args(self._command[1:])
            setproctitle("airflow task runner {0.dag_id} {0.task_id} {0.execution_date}".format(args))
            args.func(args)
            os._exit(0)

    def return_code(self, timeout=0):
        # We call this multiple times, but we can only wait on the process once.
        if self._rc is not None:
            return self._rc
        try:
            self._rc = self.process.wait(timeout=timeout)
            self.process = None
        except psutil.TimeoutExpired:
            pass
        self.log.debug("RC %r", self._rc)
        return self._rc

    def terminate(self):
        if self.process and psutil.pid_exists(self.process.pid):
            reap_process_group(self.process.pid, self.log)

    def on_finish(self):
        super().on_finish()

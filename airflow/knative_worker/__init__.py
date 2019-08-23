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


from airflow.knative_worker import *

import asyncio
import smtplib
from threading import Thread
def send_notification(email):
    """Generate and send the notification email"""  # Do some work to get email body
    message = ...

    # Connect to the server
    server = smtplib.SMTP("smtp.gmail.com:587")
    server.ehlo()
    server.starttls()
    server.login(username, password)  # Send the email
    server.sendmail(from_addr, email, message)
    server.quit()


def start_email_worker(loop):
    """Switch to new event loop and run forever"""
    asyncio.set_event_loop(loop)
    loop.run_forever()  # Create the new loop and worker thread


worker_loop = asyncio.new_event_loop()
worker = Thread(target=start_email_worker, args=(worker_loop,))  # Start the thread
worker.start()  # Assume a Flask restful interface endpoint


@app.route("/notify")
def notify(email):
    """Request notification email"""
    worker_loop.call_soon_threadsafe(send_notification, email)

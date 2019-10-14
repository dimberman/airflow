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

"""add heartbeat to taskinstance

Revision ID: 6564bf8cb1cc
Revises: eb30addb1400
Create Date: 2019-10-14 12:31:03.344840

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6564bf8cb1cc'
down_revision = 'eb30addb1400'
branch_labels = None
depends_on = None



def upgrade():
    """Apply add heartbeat to taskinstance"""
    op.add_column('task_instance',
                  sa.Column('last_heartbeat', sa.DateTime, nullable=True))



def downgrade():
    """Unapply add heartbeat to taskinstance"""
    op.drop_column('task_instance',
                  sa.Column('last_heartbeat', sa.DateTime, nullable=True))

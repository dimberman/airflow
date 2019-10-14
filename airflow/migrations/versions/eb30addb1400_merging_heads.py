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

"""merging heads

Revision ID: eb30addb1400
Revises: 74effc47d867, 004c1210f153
Create Date: 2019-10-14 12:29:57.650657

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eb30addb1400'
down_revision = ('74effc47d867', '004c1210f153')
branch_labels = None
depends_on = None


def upgrade():
    """Apply merging heads"""
    pass


def downgrade():
    """Unapply merging heads"""
    pass

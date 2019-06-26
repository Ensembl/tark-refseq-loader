"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
from datetime import datetime
from luigi.contrib.mysqldb import CopyToTable


class LoadSession(CopyToTable):

    host = "localhost"
    database = "tark_luigi"
    user = "prem"
    password = "prem"
    table = "session"

    columns = (
        ('client_id', 'varchar(128)'),
        ('start_date', 'datetime'),
        ('status', 'varchar(45)')
    )

    def rows(self):
        today = datetime.now().date()
        yield ("Test session", today, '1')

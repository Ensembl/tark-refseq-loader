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

from handlers.refseq.confighandler import ConfigHandler
from handlers.refseq.databasehandler import DatabaseHandler
from handlers.utr.utr_handler import UtrHandler

# This script loads calculates utr information for all transcripts and loads it into the transcripts table.
# Run this command to execute using LSF from the tark-refseq-loader directory:
# PYTHONPATH='.' bsub -o loading_log.out -e loading_log.err python scripts/run_utr_handler.py
if __name__ == "__main__":
    mydb_config = ConfigHandler().getInstance().get_section_config(section_name="DATABASE")
    dbh = DatabaseHandler(db_config=mydb_config,
                          mypool_name="test_pool")
    dbc = dbh.get_connection()
    utr_handler = UtrHandler(dbc)
    utr_handler.populate_utr_info()

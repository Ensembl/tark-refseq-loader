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

import argparse

from handlers.refseq.confighandler import ConfigHandler
from handlers.refseq.databasehandler import DatabaseHandler
from handlers.utr.utr_handler import UtrHandler

def main():
    parser = argparse.ArgumentParser(description="Calculate Tark transcript UTRs.")
    parser.add_argument("--config", help="Path to the loader INI file.")
    parser.add_argument("--transcript-id", dest="transcript_ids", type=int, action="append", default=[],
                        help="Limit the calculation to this internal transcript ID. Repeat for multiple IDs.")
    parser.add_argument("--dry-run", action="store_true", help="Report calculated coordinates and lengths without writing.")
    args = parser.parse_args()

    mydb_config = ConfigHandler(args.config).get_section_config(section_name="DATABASE")
    dbh = DatabaseHandler(db_config=mydb_config, mypool_name="utr_pool", pool_size=1)
    dbc = dbh.get_connection()
    try:
        utr_handler = UtrHandler(dbc)
        if args.dry_run:
            for utr_info in utr_handler.calculate_utr_info(args.transcript_ids):
                ends = []
                for end, label in (("five", "5′"), ("three", "3′")):
                    sequence = utr_info[f"{end}_prime_utr_seq"]
                    start = utr_info[f"{end}_prime_utr_start"]
                    stop = utr_info[f"{end}_prime_utr_end"]
                    interval = "unavailable" if sequence is None else f"{start}–{stop} ({len(sequence)} bp)"
                    ends.append(f"{label} UTR {interval}")
                print(f"{utr_info['transcript_id']}  " + "  ".join(ends))
        else:
            utr_handler.populate_utr_info(args.transcript_ids)
    finally:
        dbc.close()


if __name__ == "__main__":
    main()

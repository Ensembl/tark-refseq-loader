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

from handlers.utr.utr_transcript import UtrTranscript


class UtrHandler:
    """Handler for calculating and populating UTR information"""
    def __init__(self, dbc):
        self.dbc = dbc

    def populate_utr_info(self):
        """This method populates the UTR information for ALL transcripts, whether they come from RefSeq, Ensembl or
        anywhere else"""
        connection_pool = self.dbc
        cursor = connection_pool.cursor(dictionary=True)
        select_sql = ("""SELECT t.transcript_id, tl.translation_id, tl.loc_start as translation_start, 
        tl.loc_end as translation_end, t.loc_start as transcript_start, t.loc_end as transcript_end, 
        t.loc_strand as transcript_strand, e.exon_id, e.loc_start as exon_start, e.loc_end as exon_end, 
        et.exon_order, s.sequence as exon_sequence
    FROM transcript t INNER JOIN translation_transcript tt ON tt.transcript_id = t.transcript_id
    INNER JOIN translation tl on tl.translation_id = tt.translation_id
    INNER JOIN exon_transcript et on et.transcript_id = t.transcript_id
    INNER JOIN exon e on e.exon_id = et.exon_id
    INNER JOIN sequence s on s.seq_checksum = e.seq_checksum
    INNER JOIN transcript_release_tag trg ON trg.feature_id = t.transcript_id
    INNER JOIN translation_release_tag tlrg ON tlrg.feature_id = tl.translation_id
    WHERE trg.release_id = tlrg.release_id
    """)
        cursor.execute(select_sql)

        aggregated_results = self.aggregate_results(cursor)

        utr_infos = self.map_to_utrtranscipts(aggregated_results)
        print("Finished calculating utr information.  Dumping the utr information...")

        self.update_transcripts(utr_infos)
        print("Finished dumping utr information.")

    @staticmethod
    def aggregate_results(cursor):
        """Aggregates query results by transcript id.
        It would be better to aggregate inside the query, but MySQL 5.6 doesn't offer a way to do that.
        MySQL 8.0 does have JSON_ARRAYAGG, which we should consider using if we upgrade MySQL
        :arg cursor, an iterable which is expected to be the result of a SQL query
        """
        aggregated_results = {}
        for i, row in enumerate(cursor):
            if i % 10000 == 0:
                print(f"Processed {i} rows")
            if row["transcript_id"] in aggregated_results:
                if row not in aggregated_results[row["transcript_id"]]:
                    aggregated_results[row["transcript_id"]].append(row)
            else:
                aggregated_results[row["transcript_id"]] = [row]
        return aggregated_results

    @staticmethod
    def map_to_utrtranscipts(aggregated_results):
        utr_infos = []
        num_results = len(aggregated_results)
        for i, transcript_id in enumerate(aggregated_results):
            if i % 1000 == 0:
                print(f"Processed {i} transcripts of {num_results}")
            try:
                utr_transcript = UtrTranscript(aggregated_results[transcript_id])
                utr_info = utr_transcript.get_utr_info()
                utr_infos.append(utr_info)
            except ValueError as ve:
                print(
                    f"Failed to create UTR information for transcript with id {transcript_id} due to the following validation error: {ve}")
            except Exception as e:
                print(f"Encountered exception with transcript with id {transcript_id}")
                raise e
        return utr_infos

    def update_transcripts(self, utr_infos):
        """Write the utr info into the transcript table
        :arg utr_infos, a list containing all the utr information for all the transcripts
        """
        connection_pool = self.dbc
        cursor = connection_pool.cursor(dictionary=True)
        for utr_info in utr_infos:
            update_sql = "UPDATE transcript SET three_prime_utr_start = %(three_prime_utr_start)s, " \
                         "three_prime_utr_end = %(three_prime_utr_end)s, three_prime_utr_seq = %(three_prime_utr_seq)s, " \
                         "three_prime_utr_checksum = X%(three_prime_utr_checksum)s, five_prime_utr_start = %(five_prime_utr_start)s, " \
                         "five_prime_utr_end = %(five_prime_utr_end)s, five_prime_utr_seq = %(five_prime_utr_seq)s, " \
                         "five_prime_utr_checksum = X%(five_prime_utr_checksum)s WHERE transcript_id = %(transcript_id)s"
            cursor.execute(update_sql, utr_info)
        connection_pool.commit()

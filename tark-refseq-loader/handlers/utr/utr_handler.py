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

    def calculate_utr_info(self, transcript_ids=()):
        """Calculate UTRs for transcripts with a translation in a shared release."""
        select_sql = """SELECT t.transcript_id, tl.translation_id, tl.loc_start as translation_start,
        tl.loc_end as translation_end, t.loc_start as transcript_start, t.loc_end as transcript_end,
        t.loc_strand as transcript_strand, t.stable_id as transcript_stable_id, t.loc_region as transcript_region,
        HEX(t.loc_checksum) as transcript_loc_checksum, a.assembly_name,
        e.exon_id, e.loc_start as exon_start, e.loc_end as exon_end,
        et.exon_order, s.sequence as exon_sequence
    FROM transcript t INNER JOIN translation_transcript tt ON tt.transcript_id = t.transcript_id
    INNER JOIN translation tl on tl.translation_id = tt.translation_id
    LEFT JOIN assembly a ON a.assembly_id = t.assembly_id
    LEFT JOIN exon_transcript et ON et.transcript_id = t.transcript_id
    LEFT JOIN exon e ON e.exon_id = et.exon_id
    LEFT JOIN sequence s ON s.seq_checksum = e.seq_checksum
    WHERE EXISTS (
        SELECT 1 FROM transcript_release_tag trg
        INNER JOIN translation_release_tag tlrg ON tlrg.release_id = trg.release_id
        WHERE trg.feature_id = t.transcript_id AND tlrg.feature_id = tl.translation_id
    )"""
        transcript_ids = tuple(transcript_ids)
        if transcript_ids:
            placeholders = ", ".join(["%s"] * len(transcript_ids))
            select_sql += f" AND t.transcript_id IN ({placeholders})"

        cursor = self.dbc.cursor(dictionary=True)
        try:
            cursor.execute(select_sql, transcript_ids)
            aggregated_results = self.aggregate_results(cursor)
        finally:
            cursor.close()
        return self.map_to_utrtranscipts(aggregated_results)

    def populate_utr_info(self, transcript_ids=()):
        utr_infos = self.calculate_utr_info(transcript_ids)
        self.update_transcripts(utr_infos)
        print(f"Updated UTR information for {len(utr_infos)} transcripts.")

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
            translations = {}
            for row in aggregated_results[transcript_id]:
                translations.setdefault(row["translation_id"], []).append(row)

            utr_info = None
            for translation_id, rows in translations.items():
                try:
                    candidate = UtrTranscript(rows).get_utr_info()
                except ValueError as error:
                    print(f"UTRs unavailable for transcript {transcript_id}, translation {translation_id}: {error}")
                    candidate = {"transcript_id": transcript_id,
                                 **UtrTranscript.utr_fields("five"), **UtrTranscript.utr_fields("three")}

                if utr_info is None:
                    utr_info = candidate
                else:
                    # These columns are shared by all translations of a transcript.
                    # Keep each end only when its coordinates and sequence agree.
                    for end in ("five", "three"):
                        unavailable = UtrTranscript.utr_fields(end)
                        if any(utr_info[field] != candidate[field] for field in unavailable):
                            utr_info.update(unavailable)

            utr_infos.append(utr_info)
        return utr_infos

    def update_transcripts(self, utr_infos):
        """Write the utr info into the transcript table
        :arg utr_infos, a list containing all the utr information for all the transcripts
        """
        update_sql = """UPDATE transcript
            SET three_prime_utr_start = %(three_prime_utr_start)s,
                three_prime_utr_end = %(three_prime_utr_end)s,
                three_prime_utr_seq = %(three_prime_utr_seq)s,
                three_prime_utr_checksum = UNHEX(%(three_prime_utr_checksum)s),
                five_prime_utr_start = %(five_prime_utr_start)s,
                five_prime_utr_end = %(five_prime_utr_end)s,
                five_prime_utr_seq = %(five_prime_utr_seq)s,
                five_prime_utr_checksum = UNHEX(%(five_prime_utr_checksum)s)
            WHERE transcript_id = %(transcript_id)s"""
        cursor = self.dbc.cursor()
        try:
            for utr_info in utr_infos:
                cursor.execute(update_sql, utr_info)
            self.dbc.commit()
        except Exception:
            self.dbc.rollback()
            raise
        finally:
            cursor.close()

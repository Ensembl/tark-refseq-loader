from handlers.utr.utr_transcript import UtrTranscript


class UtrHandler:
    """Handler for calculating and populating UTR information"""
    def __init__(self, dbc):
        self.dbc = dbc

    def populate_utr_info(self):
        """This method is populates the UTR information for ALL transcripts, whether they come from RefSeq, Ensembl or
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
    ORDER BY t.transcript_id""")
        cursor.execute(select_sql)

        # The following loop aggregates query results by transcript id.
        # It would be better to aggregate inside the query, but MySQL 5.6 doesn't offer a way to do that.
        # MySQL 8.0 does have JSON_ARRAYAGG, which we should consider using if we upgrade MySQL
        current_transcript_id = None
        transcript_rows = []
        utr_infos = []
        for i, select_row in enumerate(cursor):
            if i % 10000 == 0:
                print(f"Processed {i} exons")
            if select_row["transcript_id"] == current_transcript_id:
                transcript_rows.append(select_row)
            else:
                if transcript_rows:  # this should only be false for the first select_row
                    self.append_utr_info(current_transcript_id, transcript_rows, utr_infos)
                transcript_rows = [select_row]
                current_transcript_id = select_row["transcript_id"]
        self.append_utr_info(current_transcript_id, transcript_rows, utr_infos)
        self.update_transcripts(utr_infos)

    @staticmethod
    def append_utr_info(transcript_id, transcript_rows, utr_infos):
        try:
            utr_transcript = UtrTranscript(transcript_rows)
            utr_info = utr_transcript.get_utr_info()
            utr_infos.append(utr_info)
        except AssertionError:
            print(
                f"Failed to create UTR information for transcript with id {transcript_id} due to a validation error")

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

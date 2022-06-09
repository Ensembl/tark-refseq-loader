from itertools import groupby


class UtrHandler:
    def __init__(self, transcript_rows):
        # There should be one translation per transcript
        assert len(list(groupby(transcript_rows, lambda t: t["translation_id"]))) == 1
        # Each row should correspond to a unique exon
        assert len([row["exon_id"] for row in transcript_rows]) == len(transcript_rows)

        self.transcript = {"transcript_id": transcript_rows[0]["transcript_id"],
                           "transcript_start": transcript_rows[0]["transcript_start"],
                           "transcript_end": transcript_rows[0]["transcript_end"],
                           "translation_start": transcript_rows[0]["translation_start"],
                           "translation_end": transcript_rows[0]["translation_end"],
                           "loc_strand": transcript_rows[0]["transcript_strand"],
                           "exons": []
                           }
        for exon in transcript_rows:
            self.transcript["exons"].append({"start": exon["exon_start"],
                                             "end": exon["exon_end"],
                                             "order": exon["exon_order"],
                                             "sequence": exon["exon_sequence"]})

    def get_utr_info(self):
        """This method finds 5'/3' UTR start, end, and sequence.  See utr_definition_diagram.png
        for an illustration of how 5'/3' UTRs are calculated from transcript, translation, and exons."""

        translation_start = self.transcript['translation_start']
        translation_end = self.transcript['translation_end']

        if self.transcript["exons"] is not []:
            exons_sorted = sorted(self.transcript["exons"], key=lambda exon: exon["order"])

            first_overlapping_exon = self.get_exon_overlapping_translation(exons_sorted)
            previous_exons = [exon for exon in exons_sorted if exon["order"] < first_overlapping_exon["order"]]
            five_prime_utr_seq = "".join([exon["sequence"] for exon in previous_exons])

            exons_reversed = reversed(exons_sorted)

            last_overlapping_exon = self.get_exon_overlapping_translation(exons_reversed)
            next_exons = [exon for exon in exons_sorted if exon["order"] > last_overlapping_exon["order"]]
            three_prime_utr_seq = "".join([exon["sequence"] for exon in next_exons])

            first_exon, last_exon = exons_sorted[0], exons_sorted[-1]

            if self.transcript['loc_strand'] == -1:
                three_prime_utr_start = translation_start - 1

                five_prime_utr_start = first_exon['end']
                five_prime_utr_end = translation_end + 1

                first_overlapping_exon_utr_len = first_overlapping_exon['end'] - translation_end
                five_prime_utr_seq = five_prime_utr_seq + first_overlapping_exon['sequence'][:first_overlapping_exon_utr_len]

                last_overlapping_exon_utr_len = translation_start - last_overlapping_exon['start']
                if last_overlapping_exon_utr_len > 0:
                    three_prime_utr_seq = last_overlapping_exon['sequence'][-last_overlapping_exon_utr_len:] + three_prime_utr_seq

                if len(three_prime_utr_seq) > 0:
                    three_prime_utr_end = last_exon['start']
                else:
                    three_prime_utr_start = 0
                    three_prime_utr_end = 0
            else:
                three_prime_utr_start = translation_end + 1

                five_prime_utr_start = first_exon['start']
                five_prime_utr_end = translation_start - 1

                first_overlapping_exon_utr_len = translation_start - first_overlapping_exon['start']
                five_prime_utr_seq = five_prime_utr_seq + first_overlapping_exon['sequence'][:first_overlapping_exon_utr_len]

                last_overlapping_exon_utr_len = last_overlapping_exon['end'] - translation_end
                if last_overlapping_exon_utr_len > 0:
                    three_prime_utr_seq = last_overlapping_exon['sequence'][-last_overlapping_exon_utr_len:] + three_prime_utr_seq

                if len(three_prime_utr_seq) > 0:
                    three_prime_utr_end = last_exon['end']
                else:
                    three_prime_utr_start = 0
                    three_prime_utr_end = 0
            return {"three_prime_utr_start": three_prime_utr_start,
                    "three_prime_utr_end": three_prime_utr_end,
                    "three_prime_utr_length": len(three_prime_utr_seq),
                    "three_prime_utr_seq": three_prime_utr_seq,
                    "five_prime_utr_start": five_prime_utr_start,
                    "five_prime_utr_end": five_prime_utr_end,
                    "five_prime_utr_length": len(five_prime_utr_seq),
                    "five_prime_utr_seq": five_prime_utr_seq
                    }

    @staticmethod
    def ranges_overlap(start1, end1, start2, end2):
        x = range(start1, end1)
        y = range(start2, end2)
        xs = set(x)
        overlap = xs.intersection(y)
        return len(overlap)

    def get_exon_overlapping_translation(self, exon_list):
        for exon in exon_list:
            if UtrHandler.ranges_overlap(exon["start"], exon["end"], self.transcript["translation_start"],
                                         self.transcript["translation_end"]) > 0:
                return exon

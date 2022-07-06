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

from itertools import groupby

from handlers.refseq.checksumhandler import ChecksumHandler


class UtrTranscript:
    """Represents a transcript in a form appropriate for getting UTR information.

    Attributes:
        transcript: A dictionary representing a transcript.
    """

    def __init__(self, transcript_rows):
        """Init transcript attribute from transcript_rows, which should be a list of results from a SQL query with one
        element for each exon in the transcript"""

        if len(list(groupby(transcript_rows, lambda t: t["translation_id"]))) != 1:
            raise ValueError(f"The given list of rows does not have a unique translation_id.")

        if len(set([row["exon_id"] for row in transcript_rows])) != len(transcript_rows):
            raise ValueError(f"The given list of rows contains duplicate rows.")

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
        """Finds 5'/3' UTR start, end, and sequence from the transcript attribute.  See utr_definition_diagram.png
        for an illustration of how 5'/3' UTRs are calculated from transcript, translation, and exons.

        :returns: a dictionary containing utr information
        """

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
                five_prime_utr_start = first_exon['end']
                five_prime_utr_end = translation_end + 1

                three_prime_utr_start = translation_start - 1
                three_prime_utr_end = last_exon['start']

                first_overlapping_exon_utr_len = first_overlapping_exon['end'] - translation_end
                if first_overlapping_exon_utr_len > 0:
                    five_prime_utr_seq = five_prime_utr_seq + first_overlapping_exon['sequence'][:first_overlapping_exon_utr_len]

                last_overlapping_exon_utr_len = translation_start - last_overlapping_exon['start']
                if last_overlapping_exon_utr_len > 0:
                    three_prime_utr_seq = last_overlapping_exon['sequence'][-last_overlapping_exon_utr_len:] + three_prime_utr_seq

            else:
                five_prime_utr_start = first_exon['start']
                five_prime_utr_end = translation_start - 1

                three_prime_utr_start = translation_end + 1
                three_prime_utr_end = last_exon['end']

                first_overlapping_exon_utr_len = translation_start - first_overlapping_exon['start']
                if first_overlapping_exon_utr_len > 0:
                    five_prime_utr_seq = five_prime_utr_seq + first_overlapping_exon['sequence'][:first_overlapping_exon_utr_len]

                last_overlapping_exon_utr_len = last_overlapping_exon['end'] - translation_end
                if last_overlapping_exon_utr_len > 0:
                    three_prime_utr_seq = last_overlapping_exon['sequence'][-last_overlapping_exon_utr_len:] + three_prime_utr_seq

            five_prime_utr_checksum = ChecksumHandler.checksum_list([five_prime_utr_seq])
            three_prime_utr_checksum = ChecksumHandler.checksum_list([three_prime_utr_seq])

            if not five_prime_utr_seq:
                five_prime_utr_start = 0
                five_prime_utr_end = 0

            if not three_prime_utr_seq:
                three_prime_utr_start = 0
                three_prime_utr_end = 0

            return {"transcript_id": self.transcript["transcript_id"],
                    "five_prime_utr_start": five_prime_utr_start,
                    "five_prime_utr_end": five_prime_utr_end,
                    "five_prime_utr_seq": five_prime_utr_seq,
                    "five_prime_utr_checksum": five_prime_utr_checksum,
                    "three_prime_utr_start": three_prime_utr_start,
                    "three_prime_utr_end": three_prime_utr_end,
                    "three_prime_utr_seq": three_prime_utr_seq,
                    "three_prime_utr_checksum": three_prime_utr_checksum
                    }

    @staticmethod
    def ranges_overlap(start1, end1, start2, end2):
        return end1 > start2 and end2 > start1

    def get_exon_overlapping_translation(self, exon_list):
        for exon in exon_list:
            if UtrTranscript.ranges_overlap(exon["start"], exon["end"], self.transcript["translation_start"],
                                            self.transcript["translation_end"]):
                return exon

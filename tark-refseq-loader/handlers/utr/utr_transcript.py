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

from handlers.refseq.checksumhandler import ChecksumHandler


class UtrTranscript:
    """Represents a transcript in a form appropriate for getting UTR information.

    Attributes:
        transcript: A dictionary representing a transcript.
    """

    def __init__(self, transcript_rows):
        """Init transcript attribute from transcript_rows, which should be a list of results from a SQL query with one
        element for each exon in the transcript"""

        if len({row["translation_id"] for row in transcript_rows}) != 1:
            raise ValueError("Expected exon rows for one translation")

        if len({row["exon_id"] for row in transcript_rows}) != len(transcript_rows):
            raise ValueError("Duplicate exon IDs")

        row = transcript_rows[0]
        self.transcript = {"transcript_id": row["transcript_id"],
                           "transcript_start": row["transcript_start"],
                           "transcript_end": row["transcript_end"],
                           "translation_start": row["translation_start"],
                           "translation_end": row["translation_end"],
                           "loc_strand": self.get_original_strand(row),
                           "exons": []
                           }
        for exon in transcript_rows:
            self.transcript["exons"].append({"start": exon["exon_start"],
                                             "end": exon["exon_end"],
                                             "order": exon["exon_order"],
                                             "sequence": exon["exon_sequence"]})

    @staticmethod
    def get_original_strand(row):
        strand = row["transcript_strand"]
        if strand != 1 or not row.get("transcript_stable_id", "").startswith(("NM_", "XM_")):
            return strand

        # The RefSeq loader calculated the location checksum before normalizing
        # the insert payload. Older Biopython versions also serialized positions
        # differently, so check the numeric and ExactPosition forms.
        location = {
            "assembly_name": row.get("assembly_name"),
            "loc_region": row.get("transcript_region"),
            "loc_start": row["transcript_start"],
            "loc_end": row["transcript_end"],
            "loc_strand": -1,
        }
        checksum = row.get("transcript_loc_checksum")
        if not checksum or None in location.values():
            return strand

        start, end = location["loc_start"], location["loc_end"]
        loc_attrs = ChecksumHandler.get_loc_attributes()
        for original_start in (start, f"ExactPosition({start})"):
            for original_end in (end, f"ExactPosition({end})"):
                location["loc_start"] = original_start
                location["loc_end"] = original_end
                # Compare the original text only when recognizing a legacy checksum.
                inputs = ChecksumHandler.get_attribute_values(loc_attrs, location)
                if ChecksumHandler.checksum_list(inputs) == checksum:
                    return -1
        return strand

    def get_utr_info(self):
        """Finds 5'/3' UTR start, end, and sequence from the transcript attribute.  See utr_definition_diagram.png
        for an illustration of how 5'/3' UTRs are calculated from transcript, translation, and exons.

        :returns: a dictionary containing utr information
        """

        translation_start = self.transcript['translation_start']
        translation_end = self.transcript['translation_end']
        if translation_start is None or translation_end is None or not 0 < translation_start <= translation_end:
            raise ValueError("Invalid CDS coordinates")

        strand = self.transcript["loc_strand"]
        if strand not in (-1, 1):
            raise ValueError("Invalid transcript strand")

        exons = self.transcript["exons"]
        if any(exon["order"] is None or exon["start"] is None or exon["end"] is None
               or not 0 < exon["start"] <= exon["end"] for exon in exons):
            raise ValueError("Missing or invalid exon coordinates or order")

        exons_sorted = sorted(exons, key=lambda exon: exon["order"])
        if len({exon["order"] for exon in exons_sorted}) != len(exons_sorted):
            raise ValueError("Duplicate exon order")
        starts = [exon["start"] for exon in exons_sorted]
        ends = [exon["end"] for exon in exons_sorted]
        reverse = strand == -1
        if starts != sorted(starts, reverse=reverse) or ends != sorted(ends, reverse=reverse):
            raise ValueError("Exon order disagrees with transcript strand")

        first_coding_exon = self.get_exon_overlapping_translation(exons_sorted)
        last_coding_exon = self.get_exon_overlapping_translation(reversed(exons_sorted))
        cds_five_prime, cds_three_prime = (translation_end, translation_start) if reverse else (
            translation_start, translation_end)
        if (first_coding_exon is None or last_coding_exon is None
                or not first_coding_exon["start"] <= cds_five_prime <= first_coding_exon["end"]
                or not last_coding_exon["start"] <= cds_three_prime <= last_coding_exon["end"]):
            raise ValueError("CDS endpoints must fall within exons")

        first_exon, last_exon = exons_sorted[0], exons_sorted[-1]
        if reverse:
            five_start, five_end = first_exon["end"], cds_five_prime + 1
            three_start, three_end = cds_three_prime - 1, last_exon["start"]
            prefix_length = first_coding_exon["end"] - cds_five_prime
            suffix_length = cds_three_prime - last_coding_exon["start"]
        else:
            five_start, five_end = first_exon["start"], cds_five_prime - 1
            three_start, three_end = cds_three_prime + 1, last_exon["end"]
            prefix_length = cds_five_prime - first_coding_exon["start"]
            suffix_length = last_coding_exon["end"] - cds_three_prime

        previous_exons = [exon for exon in exons_sorted if exon["order"] < first_coding_exon["order"]]
        next_exons = [exon for exon in exons_sorted if exon["order"] > last_coding_exon["order"]]
        five_seq = self.get_utr_sequence(previous_exons, first_coding_exon, prefix_length, five_prime=True)
        three_seq = self.get_utr_sequence(next_exons, last_coding_exon, suffix_length, five_prime=False)

        return {
            "transcript_id": self.transcript["transcript_id"],
            **self.utr_fields("five", five_start, five_end, five_seq),
            **self.utr_fields("three", three_start, three_end, three_seq),
        }

    @staticmethod
    def get_utr_sequence(full_exons, coding_exon, length, five_prime):
        sequences = [exon["sequence"] for exon in full_exons]
        if any(not sequence for sequence in sequences):
            return None

        if not length:
            return "".join(sequences)

        boundary_sequence = coding_exon["sequence"]
        if not boundary_sequence or len(boundary_sequence) < length:
            return None
        if five_prime:
            return "".join(sequences) + boundary_sequence[:length]
        return boundary_sequence[-length:] + "".join(sequences)

    @staticmethod
    def utr_fields(end_name, start=None, end=None, sequence=None):
        if sequence is None:
            start = end = None
        elif sequence == "":
            start = end = 0

        return {
            f"{end_name}_prime_utr_start": start,
            f"{end_name}_prime_utr_end": end,
            f"{end_name}_prime_utr_seq": sequence,
            f"{end_name}_prime_utr_checksum": ChecksumHandler.checksum_list([sequence]),
        }

    @staticmethod
    def ranges_overlap(start1, end1, start2, end2):
        return start1 <= end2 and start2 <= end1

    def get_exon_overlapping_translation(self, exon_list):
        for exon in exon_list:
            if UtrTranscript.ranges_overlap(exon["start"], exon["end"], self.transcript["translation_start"],
                                            self.transcript["translation_end"]):
                return exon

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
import unittest

import pytest

from handlers.refseq.checksumhandler import ChecksumHandler
from handlers.utr.utr_transcript import UtrTranscript


class TestUtrTranscript(unittest.TestCase):
    def test_ranges_overlap(self):
        # exact overlap
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 10, 15)
        self.assertTrue(overlap_score)

        # no overlap
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 20, 25)
        self.assertFalse(overlap_score)

        # first range contains the second
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 11, 14)
        self.assertTrue(overlap_score)

        # second range contains the first
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 8, 18)
        self.assertTrue(overlap_score)

        # ranges intersect but neither contains the other
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 12, 18)
        self.assertTrue(overlap_score)

        # end coord of first range is start coord of the second range
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 15, 20)
        self.assertTrue(overlap_score)

        # start coord of first range is end coord of the second range
        overlap_score = UtrTranscript.ranges_overlap(15, 20, 10, 15)
        self.assertTrue(overlap_score)

    def test_more_than_one_translation_id_throws(self):
        non_unique_translation_rows = [{"transcript_id": 1, "translation_id": 1},
                                       {"transcript_id": 1, "translation_id": 2}]

        transcript = None
        with pytest.raises(ValueError):
            transcript = UtrTranscript(non_unique_translation_rows)
        assert not transcript

    def test_non_unique_exon_throws(self):
        non_unique_exons_rows = [{"transcript_id": 1, "translation_id": 1, "exon_id": 1},
                                 {"transcript_id": 1, "translation_id": 1, "exon_id": 1}]
        transcript = None
        with pytest.raises(ValueError):
            transcript = UtrTranscript(non_unique_exons_rows)
        assert not transcript

    def test_get_utr_info_no_utrs(self):
        transcript_rows = [{"transcript_id": 1, "translation_id": 1, "transcript_start": 1,
                            "transcript_end": 10, "translation_start": 3, "translation_end": 6,
                            "transcript_strand": 1, "exon_id": 1, "exon_start": 3, "exon_end": 4,
                            "exon_order": 1, "exon_sequence": "ACGT"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 1,
                            "transcript_end": 10, "translation_start": 3, "translation_end": 6,
                            "transcript_strand": 1, "exon_id": 2, "exon_start": 5, "exon_end": 6,
                            "exon_order": 2, "exon_sequence": "TGCA"}]
        utr_transcript = UtrTranscript(transcript_rows)
        expected_transcript = {
            "transcript_id": 1,
            "transcript_start": 1,
            "transcript_end": 10,
            "translation_start": 3,
            "translation_end": 6,
            "loc_strand": 1,
            "exons": [
                {"start": 3,
                 "end": 4,
                 "order": 1,
                 "sequence": "ACGT"},
                {"start": 5,
                 "end": 6,
                 "order": 2,
                 "sequence": "TGCA"}
            ]
        }
        self.assertEqual(utr_transcript.transcript, expected_transcript)

        expected_utr_info = {
            "transcript_id": 1,
            "three_prime_utr_start": 0,
            "three_prime_utr_end": 0,
            "three_prime_utr_checksum": "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709",
            "three_prime_utr_seq": "",
            "five_prime_utr_start": 0,
            "five_prime_utr_end": 0,
            "five_prime_utr_checksum": "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709",
            "five_prime_utr_seq": ""
        }
        actual_utr_info = utr_transcript.get_utr_info()
        self.assertEqual(expected_utr_info, actual_utr_info)

    def test_single_base_cds_overlap_on_both_strands(self):
        transcript = dict(transcript_id=1, translation_id=1, transcript_start=10, transcript_end=23,
                          translation_start=13, translation_end=20)
        low_exon = dict(exon_id=1, exon_start=10, exon_end=13, exon_sequence="ACCC")
        high_exon = dict(exon_id=2, exon_start=20, exon_end=23, exon_sequence="GTTA")

        for strand, exons, expected in (
            (1, (low_exon, high_exon), ((10, 12, "ACC"), (21, 23, "TTA"))),
            (-1, (high_exon, low_exon), ((23, 21, "GTT"), (12, 10, "CCC"))),
        ):
            with self.subTest(strand=strand):
                rows = [dict(transcript, **exon, transcript_strand=strand, exon_order=order)
                        for order, exon in enumerate(exons, 1)]
                result = UtrTranscript(rows).get_utr_info()
                for end, (start, stop, sequence) in zip(("five", "three"), expected):
                    self.assertEqual(result[f"{end}_prime_utr_start"], start)
                    self.assertEqual(result[f"{end}_prime_utr_end"], stop)
                    self.assertEqual(result[f"{end}_prime_utr_seq"], sequence)

    def test_legacy_refseq_strand_on_single_exon(self):
        checksum = ChecksumHandler.checksum_list(["GRCh38", "11", "100", "ExactPosition(109)", "-1"])
        row = dict(transcript_id=1, translation_id=1, transcript_start=100, transcript_end=109,
                   translation_start=101, translation_end=106, transcript_strand=1,
                   transcript_stable_id="NM_000001", transcript_loc_checksum=checksum,
                   assembly_name="GRCh38", transcript_region="11", exon_id=1, exon_start=100,
                   exon_end=109, exon_order=1, exon_sequence="AACCCCTTTG")
        result = UtrTranscript([row]).get_utr_info()
        self.assertEqual((result["five_prime_utr_start"], result["five_prime_utr_end"]), (109, 107))
        self.assertEqual(result["five_prime_utr_seq"], "AAC")
        self.assertEqual(result["three_prime_utr_seq"], "G")

    def test_missing_utr_exon_sequence_leaves_only_that_end_unavailable(self):
        transcript = dict(transcript_id=1, translation_id=1, transcript_start=10, transcript_end=23,
                          translation_start=20, translation_end=22, transcript_strand=1)
        rows = [dict(transcript, exon_id=1, exon_start=10, exon_end=13, exon_order=1, exon_sequence=None),
                dict(transcript, exon_id=2, exon_start=20, exon_end=23, exon_order=2, exon_sequence="GTTA")]
        result = UtrTranscript(rows).get_utr_info()
        for field in ("start", "end", "seq", "checksum"):
            self.assertIsNone(result[f"five_prime_utr_{field}"])
        self.assertEqual(result["three_prime_utr_seq"], "A")

    def test_utr_info_forward_strand(self):
        transcript_rows = [{"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": 1, "exon_id": 1, "exon_start": 2, "exon_end": 5,
                            "exon_order": 1, "exon_sequence": "GATT"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": 1, "exon_id": 2, "exon_start": 17, "exon_end": 25,
                            "exon_order": 2, "exon_sequence": "ACATCGC"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": 1, "exon_id": 3, "exon_start": 50, "exon_end": 56,
                            "exon_order": 3, "exon_sequence": "GCATG"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": 1, "exon_id": 4, "exon_start": 88, "exon_end": 92,
                            "exon_order": 4, "exon_sequence": "AGA"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": 1, "exon_id": 5, "exon_start": 94, "exon_end": 100,
                            "exon_order": 5, "exon_sequence": "TTACA"}
                           ]
        utr_transcript = UtrTranscript(transcript_rows)
        expected_utr_info = {
            "transcript_id": 1,
            "three_prime_utr_start": 91,
            "three_prime_utr_end": 100,
            "three_prime_utr_checksum": 'B69DA3CDE08CD9EDD6E32C6DAEBC74F4230A6BFF',
            "three_prime_utr_seq": "GATTACA",
            "five_prime_utr_start": 2,
            "five_prime_utr_end": 19,
            "five_prime_utr_checksum": 'B69DA3CDE08CD9EDD6E32C6DAEBC74F4230A6BFF',
            "five_prime_utr_seq": "GATTACA"
        }
        actual_utr_info = utr_transcript.get_utr_info()
        self.assertEqual(expected_utr_info, actual_utr_info)

    def test_utr_info_reverse_strand(self):
        transcript_rows = [{"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": -1, "exon_id": 1, "exon_start": 2, "exon_end": 5,
                            "exon_order": 5, "exon_sequence": "TACA"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": -1, "exon_id": 2, "exon_start": 17, "exon_end": 25,
                            "exon_order": 4, "exon_sequence": "CGCGAT"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": -1, "exon_id": 3, "exon_start": 50, "exon_end": 56,
                            "exon_order": 3, "exon_sequence": "GCATG"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": -1, "exon_id": 4, "exon_start": 88, "exon_end": 92,
                            "exon_order": 2, "exon_sequence": "CAA"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 10,
                            "transcript_end": 110, "translation_start": 20, "translation_end": 90,
                            "transcript_strand": -1, "exon_id": 5, "exon_start": 94, "exon_end": 100,
                            "exon_order": 1, "exon_sequence": "GATTA"}
                           ]
        utr_transcript = UtrTranscript(transcript_rows)
        expected_utr_info = {
            "transcript_id": 1,
            "three_prime_utr_start": 19,
            "three_prime_utr_end": 2,
            "three_prime_utr_checksum": 'B69DA3CDE08CD9EDD6E32C6DAEBC74F4230A6BFF',
            "three_prime_utr_seq": "GATTACA",
            "five_prime_utr_start": 100,
            "five_prime_utr_end": 91,
            "five_prime_utr_checksum": 'B69DA3CDE08CD9EDD6E32C6DAEBC74F4230A6BFF',
            "five_prime_utr_seq": "GATTACA"
        }
        actual_utr_info = utr_transcript.get_utr_info()
        self.assertEqual(expected_utr_info, actual_utr_info)

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

from handlers.utr.utr_transcript import UtrTranscript


class TestUtrTranscript(unittest.TestCase):
    def test_ranges_overlap(self):
        # full overlap score=5
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 10, 15)
        self.assertEqual(overlap_score, 5)

        # no overlap score=0
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 20, 25)
        self.assertEqual(overlap_score, 0)

        # partial overlap score=3
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 11, 14)
        self.assertEqual(overlap_score, 3)

        # full overlap  score=5
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 8, 18)
        self.assertEqual(overlap_score, 5)

        # partial overlap score=3
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 12, 18)
        self.assertEqual(overlap_score, 3)

        # partial_overlap score = 0 boundary case
        overlap_score = UtrTranscript.ranges_overlap(10, 15, 15, 20)
        self.assertEqual(overlap_score, 0)

        # partial_overlap score = 0 other boundary case
        overlap_score = UtrTranscript.ranges_overlap(15, 20, 10, 15)
        self.assertEqual(overlap_score, 0)

    def test_more_than_one_translation_id_throws(self):
        non_unique_translation_rows = [{"transcript_id": 1, "translation_id": 1},
                                       {"transcript_id": 1, "translation_id": 2}]

        transcript = None
        with pytest.raises(ValueError):
            transcript = UtrTranscript(non_unique_translation_rows)
        assert not transcript

    def test_non_unique_exon_throws(self):
        non_unique_exons_rows = [{"transcript_id": 1, "translation_id": 1, "exon_id": 1},
                                 {"transcript_id": 1, "translation_id": 2, "exon_id": 1}]
        transcript = None
        with pytest.raises(AssertionError):
            transcript = UtrTranscript(non_unique_exons_rows)
        assert not transcript

    def test_get_utr_info_no_utrs(self):
        transcript_rows = [{"transcript_id": 1, "translation_id": 1, "transcript_start": 1,
                            "transcript_end": 10, "translation_start": 2, "translation_end": 9,
                            "transcript_strand": 1, "exon_id": 1, "exon_start": 3, "exon_end": 4,
                            "exon_order": 1, "exon_sequence": "ACGT"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 1,
                            "transcript_end": 10, "translation_start": 2, "translation_end": 9,
                            "transcript_strand": 1, "exon_id": 2, "exon_start": 5, "exon_end": 6,
                            "exon_order": 2, "exon_sequence": "TGCA"}]
        utr_transcript = UtrTranscript(transcript_rows)
        expected_transcript = {
            "transcript_id": 1,
            "transcript_start": 1,
            "transcript_end": 10,
            "translation_start": 2,
            "translation_end": 9,
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
            "three_prime_utr_checksum": None,
            "three_prime_utr_seq": "",
            "five_prime_utr_start": 0,
            "five_prime_utr_end": 0,
            "five_prime_utr_checksum": None,
            "five_prime_utr_seq": ""
        }
        actual_utr_info = utr_transcript.get_utr_info()
        self.assertEqual(expected_utr_info, actual_utr_info)

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
            "three_prime_utr_checksum": '94D561B9AEC0775D6383EB2CB6493E97A1B3C9AB',
            "three_prime_utr_seq": "GATTACA",
            "five_prime_utr_start": 2,
            "five_prime_utr_end": 19,
            "five_prime_utr_checksum": '94D561B9AEC0775D6383EB2CB6493E97A1B3C9AB',
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
            "three_prime_utr_checksum": '94D561B9AEC0775D6383EB2CB6493E97A1B3C9AB',
            "three_prime_utr_seq": "GATTACA",
            "five_prime_utr_start": 100,
            "five_prime_utr_end": 91,
            "five_prime_utr_checksum": '94D561B9AEC0775D6383EB2CB6493E97A1B3C9AB',
            "five_prime_utr_seq": "GATTACA"
        }
        actual_utr_info = utr_transcript.get_utr_info()
        self.assertEqual(expected_utr_info, actual_utr_info)

    # def test_utr_weird_case(self):
    #     transcript_rows =

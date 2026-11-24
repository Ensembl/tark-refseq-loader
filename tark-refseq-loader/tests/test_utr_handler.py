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

from handlers.utr.utr_handler import UtrHandler


class TestUtrHandler(unittest.TestCase):
    def test_aggregate_results(self):
        rows = [{"transcript_id": 1, "exon_id": 1},
                {"transcript_id": 1, "exon_id": 1},
                {"transcript_id": 1, "exon_id": 2},
                {"transcript_id": 2, "exon_id": 1}]

        expected = {1: [{"transcript_id": 1, "exon_id": 1}, {"transcript_id": 1, "exon_id": 2}],
                              2: [{"transcript_id": 2, "exon_id": 1}]}
        actual = UtrHandler.aggregate_results(rows)
        self.assertEqual(actual, expected)

    def test_keeps_utr_end_shared_by_multiple_translations(self):
        transcript = dict(transcript_id=1, transcript_start=10, transcript_end=18, transcript_strand=1,
                          exon_id=1, exon_order=1, exon_start=10, exon_end=18, exon_sequence="AAATGTTTC",
                          translation_end=16)
        rows = [dict(transcript, translation_id=1, translation_start=12),
                dict(transcript, translation_id=2, translation_start=13)]
        result, = UtrHandler.map_to_utrtranscipts({1: rows})
        for field in ("start", "end", "seq", "checksum"):
            self.assertIsNone(result[f"five_prime_utr_{field}"])
        self.assertEqual(result["three_prime_utr_seq"], "TC")

    def test_invalid_cds_is_unavailable_and_does_not_stop_other_transcripts(self):
        transcript = dict(transcript_id=1, translation_id=1, transcript_start=10, transcript_end=18,
                          transcript_strand=1, exon_id=1, exon_order=1, exon_start=10, exon_end=18,
                          exon_sequence="AAATGTTTC", translation_start=0, translation_end=16)
        valid = dict(transcript, transcript_id=2, translation_start=10)
        invalid_info, valid_info = UtrHandler.map_to_utrtranscipts({1: [transcript], 2: [valid]})
        self.assertIsNone(invalid_info["five_prime_utr_start"])
        self.assertIsNone(invalid_info["three_prime_utr_start"])
        self.assertEqual(valid_info["five_prime_utr_start"], 0)
        self.assertEqual(valid_info["three_prime_utr_seq"], "TC")

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

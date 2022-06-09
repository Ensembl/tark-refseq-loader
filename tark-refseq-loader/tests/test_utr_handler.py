import unittest

from handlers.refseq.utrhandler import UtrHandler


class TestUtrHandler(unittest.TestCase):
    def test_ranges_overlap(self):
        # full overlap score=5
        overlap_score = UtrHandler.ranges_overlap(10, 15, 10, 15)
        self.assertEqual(overlap_score, 5)

        # no overlap score=0
        overlap_score = UtrHandler.ranges_overlap(10, 15, 20, 25)
        self.assertEqual(overlap_score, 0)

        # partial overlap score=3
        overlap_score = UtrHandler.ranges_overlap(10, 15, 11, 14)
        self.assertEqual(overlap_score, 3)

        # full overlap  score=5
        overlap_score = UtrHandler.ranges_overlap(10, 15, 8, 18)
        self.assertEqual(overlap_score, 5)

        # partial overlap score=3
        overlap_score = UtrHandler.ranges_overlap(10, 15, 12, 18)
        self.assertEqual(overlap_score, 3)

        # partial_overlap score = 0 boundary case
        overlap_score = UtrHandler.ranges_overlap(10, 15, 15, 20)
        self.assertEqual(overlap_score, 0)

        # partial_overlap score = 0 other boundary case
        overlap_score = UtrHandler.ranges_overlap(15, 20, 10, 15)
        self.assertEqual(overlap_score, 0)

    def test_get_utr_info(self):
        transcript_rows = [{"transcript_id": 1, "translation_id": 1, "transcript_start": 1,
                            "transcript_end": 10, "translation_start": 2, "translation_end": 9,
                            "transcript_strand": 1, "exon_id": 1, "exon_start": 3, "exon_end": 4,
                            "exon_order": 1, "exon_sequence": "ACGT"},
                           {"transcript_id": 1, "translation_id": 1, "transcript_start": 1,
                            "transcript_end": 10, "translation_start": 2, "translation_end": 9,
                            "transcript_strand": 1, "exon_id": 2, "exon_start": 5, "exon_end": 6,
                            "exon_order": 2, "exon_sequence": "TGCA"}]
        utr_handler = UtrHandler(transcript_rows)
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
        self.assertEqual(utr_handler.transcript, expected_transcript)

        expected_utr_info = {
            "three_prime_utr_start": 0,
            "three_prime_utr_end": 0,
            "three_prime_utr_length": 0,
            "three_prime_utr_seq": "",
            "five_prime_utr_start": 0,
            "five_prime_utr_end": 0,
            "five_prime_utr_length": 0,
            "five_prime_utr_seq": ""
        }
        actual_utr_info = utr_handler.get_utr_info()
        self.assertEqual(actual_utr_info, expected_utr_info)

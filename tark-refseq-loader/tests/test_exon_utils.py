import unittest
from handlers.refseq.utils.exon_utils import ExonUtils


class TestExonUtils(unittest.TestCase):

    def test_compute_exon_coordinates(self):

        refseq_exon_list = [{'exon_stable_id_version': 1, 'exon_order': 1, 'exon_end': '6062370', 'exon_strand': '-1',
                             'exon_stable_id': 'id977749', 'exon_start': '6062088'},
                            {'exon_stable_id_version': 1, 'exon_order': 2, 'exon_end': '6026025', 'exon_strand': '-1',
                             'exon_stable_id': 'id977750', 'exon_start': '6025834'},
                            {'exon_stable_id_version': 1, 'exon_order': 3, 'exon_end': '6024354', 'exon_strand': '-1',
                             'exon_stable_id': 'id977751', 'exon_start': '6024244'},
                            {'exon_stable_id_version': 1, 'exon_order': 4, 'exon_end': '6021693', 'exon_strand': '-1',
                             'exon_stable_id': 'id977752', 'exon_start': '6021478'},
                            {'exon_stable_id_version': 1, 'exon_order': 5, 'exon_end': '6019941', 'exon_strand': '-1',
                             'exon_stable_id': 'id977753', 'exon_start': '6019870'},
                            {'exon_stable_id_version': 1, 'exon_order': 6, 'exon_end': '6019499', 'exon_strand': '-1',
                             'exon_stable_id': 'id977754', 'exon_start': '6019428'},
                            {'exon_stable_id_version': 1, 'exon_order': 7, 'exon_end': '6018119', 'exon_strand': '-1',
                             'exon_stable_id': 'id977755', 'exon_start': '6018053'},
                            {'exon_stable_id_version': 1, 'exon_order': 8, 'exon_end': '6012896', 'exon_strand': '-1',
                             'exon_stable_id': 'id977756', 'exon_start': '6010694'}]

        exon_coordinates = ExonUtils.compute_exon_coordinates(refseq_exon_list)

        expected_list = [{'exon_stable_id_version': 1, 'exon_order': 1, 'exon_end': 283, 'exon_strand': '-1',
                          'exon_stable_id': 'id977749', 'exon_start': 1},
                         {'exon_stable_id_version': 1, 'exon_order': 2, 'exon_end': 475, 'exon_strand': '-1',
                          'exon_stable_id': 'id977750', 'exon_start': 284},
                         {'exon_stable_id_version': 1, 'exon_order': 3, 'exon_end': 586, 'exon_strand': '-1',
                          'exon_stable_id': 'id977751', 'exon_start': 476},
                         {'exon_stable_id_version': 1, 'exon_order': 4, 'exon_end': 802, 'exon_strand': '-1',
                          'exon_stable_id': 'id977752', 'exon_start': 587},
                         {'exon_stable_id_version': 1, 'exon_order': 5, 'exon_end': 874, 'exon_strand': '-1',
                          'exon_stable_id': 'id977753', 'exon_start': 803},
                         {'exon_stable_id_version': 1, 'exon_order': 6, 'exon_end': 946, 'exon_strand': '-1',
                          'exon_stable_id': 'id977754', 'exon_start': 875},
                         {'exon_stable_id_version': 1, 'exon_order': 7, 'exon_end': 1013, 'exon_strand': '-1',
                          'exon_stable_id': 'id977755', 'exon_start': 947},
                         {'exon_stable_id_version': 1, 'exon_order': 8, 'exon_end': 3216, 'exon_strand': '-1',
                          'exon_stable_id': 'id977756', 'exon_start': 1014}]

        self.assertListEqual(exon_coordinates, expected_list)

if __name__ == '__main__':
    unittest.main()

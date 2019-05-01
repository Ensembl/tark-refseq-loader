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


class ExonUtils(object):

    @classmethod
    def compute_exon_coordinates(cls, exons):
        updated_exon_list = []
        exon_end = 0
        for original_exon in exons:
            exon = original_exon.copy()
            start = int(exon['exon_start'])
            end = int(exon['exon_end'])

            exon_start = int(exon_end) + 1
            exon_end = int(exon_start) + (end - start)
            exon['exon_start'] = exon_start
            exon['exon_end'] = exon_end
            updated_exon_list.append(exon)

        return updated_exon_list

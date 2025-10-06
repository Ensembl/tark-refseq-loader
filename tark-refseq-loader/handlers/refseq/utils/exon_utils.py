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


import re
from typing import Any, Dict, List


class ExonUtils(object):

    @staticmethod
    def _to_int_pos(v: Any) -> int:
        """
        Normalize a coordinate value to a plain int.

        Accepts:
          - int
          - Biopython Position objects (ExactPosition, BeforePosition, AfterPosition, BetweenPosition),
            which expose `.position`
          - Biopython FeatureLocation ends/starts (they also behave like positions)
          - strings like "10742191" or "ExactPosition(10742191)"
        """
        # Already an int
        if isinstance(v, int):
            return v

        # Biopython Position-like (ExactPosition, etc.) or FeatureLocation boundary
        # Most expose `.position`; FeatureLocation has `.start`/`.end` which in turn have `.position`
        if hasattr(v, "position"):
            try:
                return int(v.position)
            except Exception:
                pass

        # Some callers might pass FeatureLocation directly; take its numeric bounds if present
        if hasattr(v, "start") and hasattr(v.start, "position") and hasattr(v, "end") and hasattr(v.end, "position"):
            # If a full location slipped in, choose one edge (caller should pass a single edge, but be forgiving)
            # Prefer the concrete integer if v is intended to represent one bound, else fall back to int(start.position)
            try:
                return int(v.position)  # in case it mimics a Position
            except Exception:
                return int(v.start.position)

        # Plain string or stringified Biopython repr
        if isinstance(v, str):
            m = re.search(r"\d+", v)
            if m:
                return int(m.group(0))

        raise ValueError(f"Unrecognised coordinate type for exon position: {type(v)} -> {v!r}")

    @classmethod
    def compute_exon_coordinates(cls, exons: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compute relative exon coordinates, preserving the original logic:
        - normalise exon_start/exon_end to ints first
        - exon 1 starts at 1
        - each subsequent exon starts at previous_exon_end + 1
        - exon length = (end - start)
        """
        updated_exon_list: List[Dict[str, Any]] = []
        exon_end = 0

        for original_exon in exons:
            exon = original_exon.copy()

            start = cls._to_int_pos(exon.get('exon_start'))
            end = cls._to_int_pos(exon.get('exon_end'))

            # Defensive checks
            if end < start:
                raise ValueError(f"Exon end < start after normalisation: start={start}, end={end}, exon={original_exon}")

            exon_start = int(exon_end) + 1
            exon_end = int(exon_start) + (end - start)
            exon['exon_start'] = exon_start
            exon['exon_end'] = exon_end
            updated_exon_list.append(exon)

        return updated_exon_list


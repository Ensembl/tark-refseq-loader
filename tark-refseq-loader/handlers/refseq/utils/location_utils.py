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
from operator import index


_POSITION_REPR = re.compile(r"(?:Exact|Uncertain|Before|After)Position\(([+-]?\d+)\)")


def position_as_int(position):
    """Use the numeric boundary, including when a legacy caller passes its text."""
    if isinstance(position, str):
        position = position.strip()
        wrapped = _POSITION_REPR.fullmatch(position)
        if wrapped:
            return int(wrapped.group(1))
        if position.startswith(("<", ">")):
            position = position[1:]
        return int(position)
    if isinstance(position, int):
        return int(position)
    position = getattr(position, "position", position)
    return index(position)


def strand_as_int(strand):
    """Return a signed Biopython strand, retaining unknown or absent values."""
    if strand is None or strand == "None":
        return None
    strand = int(strand) if isinstance(strand, str) else index(strand)
    if strand not in (-1, 0, 1):
        raise ValueError(f"Invalid feature strand: {strand}")
    return strand


def feature_location_values(location, start_offset=0):
    """Read Biopython coordinates before any string conversion or hashing."""
    return (position_as_int(location.start) + start_offset,
            position_as_int(location.end),
            strand_as_int(location.strand))

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

from hashlib import sha1
from types import SimpleNamespace

import pytest

from handlers.refseq.annotationhandler import AnnotationHandler
from handlers.refseq.checksumhandler import ChecksumHandler


class ExactPosition(int):
    def __str__(self):
        return f"ExactPosition({int(self)})"


def digest(*values):
    return sha1(":".join(map(str, values)).encode("utf-8")).hexdigest().upper()


@pytest.mark.parametrize("end", [5227071, ExactPosition(5227071), "ExactPosition(5227071)"])
def test_location_checksum_matches_the_historical_hash(end):
    location = dict(assembly_name="GRCh38", loc_region="11", loc_start=5225464,
                    loc_end=end, loc_strand="-1")
    expected = "8884E6855167648DBB98BEEFFEED96BC4B4748C0"
    assert ChecksumHandler.get_location_checksum(location) == expected


@pytest.mark.parametrize("strand", [1, -1])
def test_feature_checksums_use_numeric_locations_and_keep_biotype(monkeypatch, strand):
    monkeypatch.setattr(AnnotationHandler, "ASSEMBLY_NAME", "GRCh38")
    location = SimpleNamespace(start=ExactPosition(99), end=ExactPosition(113), strand=strand)
    feature = SimpleNamespace(location=location, qualifiers={
        "Dbxref": ["GeneID:123", "HGNC:HGNC:456"],
        "gene_biotype": ["protein_coding"],
        "transcript_id": ["NM_000001.3"],
    })
    cdna = "AACGATGTT"
    sequence_handler = SimpleNamespace(
        get_sequence_by_id=lambda accession: cdna,
        get_seq_record_by_id_location=lambda accession, start, end, strand: cdna[start - 1:end],
        get_fasta_seq_by_id=lambda accession: "MT",
    )

    gene = AnnotationHandler.get_annotated_gene("11", feature)
    assert (gene["loc_start"], gene["loc_end"], gene["loc_strand"]) == (99, 113, strand)
    assert type(gene["loc_end"]) is int
    assert gene["loc_checksum"] == digest("GRCh38", "11", 99, 113, strand)
    gene_fields = (gene["loc_checksum"], "HGNC:456", "123", 1, "protein_coding")
    assert gene["gene_checksum"] == digest(digest(*gene_fields))

    transcript = AnnotationHandler.get_annotated_transcript(sequence_handler, "11", feature)
    assert (transcript["loc_start"], transcript["loc_end"], transcript["loc_strand"]) == (
        100, 113, strand)
    assert type(transcript["loc_end"]) is int
    assert transcript["loc_checksum"] == digest("GRCh38", "11", 100, 113, strand)
    assert transcript["seq_checksum"] == digest(cdna)

    exon_coordinates = [(100, 103), (109, 113)]
    cds_coordinates = [(101, 103), (109, 111)]
    if strand == -1:
        exon_coordinates.reverse()
        cds_coordinates.reverse()
    exon_features = [
        dict(exon_start=ExactPosition(start), exon_end=f"ExactPosition({end})", exon_strand=strand,
             exon_order=order, exon_stable_id=f"exon-{order}", exon_stable_id_version=1)
        for order, (start, end) in enumerate(exon_coordinates, 1)
    ]
    exons = AnnotationHandler.get_annotated_exons(
        sequence_handler, "11", "NM_000001.3", exon_features)
    expected_exon_hashes = []
    offset = 0
    for exon, (start, end) in zip(exons, exon_coordinates):
        length = end - start + 1
        assert (exon["loc_start"], exon["loc_end"], exon["loc_strand"]) == (start, end, strand)
        assert type(exon["loc_end"]) is int
        exon_location = digest("GRCh38", "11", start, end, strand)
        exon_sequence = digest(cdna[offset:offset + length])
        assert exon["exon_checksum"] == digest(exon_location, exon_sequence)
        expected_exon_hashes.append(digest(exon_location, exon_sequence))
        offset += length

    transcript["exon_set_checksum"] = ChecksumHandler.get_exon_set_checksum(exons)
    assert transcript["exon_set_checksum"] == digest(*expected_exon_hashes)
    transcript_fields = (transcript["loc_checksum"], "NM_000001", 3,
                         transcript["exon_set_checksum"], digest(cdna), "protein_coding")
    assert ChecksumHandler.get_transcript_checksum(transcript) == digest(digest(*transcript_fields))

    cds_features = [
        dict(cds_start=ExactPosition(start), cds_end=f"ExactPosition({end})",
             cds_strand=strand, cds_order=order, protein_id="NP_000001.2")
        for order, (start, end) in enumerate(cds_coordinates, 1)
    ]
    translation = AnnotationHandler.get_annotated_cds(
        sequence_handler, "11", "NP_000001.2", cds_features)
    assert (translation["loc_start"], translation["loc_end"], translation["loc_strand"]) == (
        101, 111, strand)
    assert type(translation["loc_end"]) is int
    assert translation["loc_checksum"] == digest("GRCh38", "11", 101, 111, strand)
    assert translation["translation_checksum"] == digest(digest(
        translation["loc_checksum"], "NP_000001", 2, digest("MT")))


def test_missing_exon_sequence_cannot_be_hashed_as_text():
    sequence_handler = SimpleNamespace(get_seq_record_by_id_location=lambda *args: None)
    exon = dict(exon_start=1, exon_end=4, exon_strand=1, exon_order=1)
    with pytest.raises(ValueError, match=r"Missing sequence for exon 1 of NM_000001\.3"):
        AnnotationHandler.get_annotated_exons(sequence_handler, "11", "NM_000001.3", [exon])

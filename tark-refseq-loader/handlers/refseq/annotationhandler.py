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
from handlers.refseq.checksumhandler import ChecksumHandler
from handlers.refseq.confighandler import ConfigHandler

import logging
from handlers.refseq.utils.exon_utils import ExonUtils

# Get an instance of a logger
logger = logging.getLogger(__name__)


class AnnotationHandler(object):

    ASSEMBLY_ID = ConfigHandler().getInstance().get_section_config()["assembly_id"]
    ASSEMBLY_NAME = ConfigHandler().getInstance().get_section_config()["assembly_name"]

    @classmethod
    def get_annotated_gene(cls, chrom, gene_feature):
        gene = {}
        gene['loc_start'] = str(gene_feature.location.start)
        gene['loc_end'] = str(gene_feature.location.end)
        gene['loc_strand'] = str(gene_feature.location.strand)
        gene['loc_region'] = str(chrom)
        gene['stable_id'] = cls.parse_qualifiers(gene_feature.qualifiers, "Dbxref", "GeneID")
        gene['stable_id_version'] = 1
        gene['assembly_id'] = cls.ASSEMBLY_ID
        gene['assembly_name'] = cls.ASSEMBLY_NAME
        # make it none for the moment, otherwise you will get integrity exception
        hgnc_id = cls.parse_qualifiers(gene_feature.qualifiers, "Dbxref", "HGNC:HGNC")
        if hgnc_id is not None:
            hgnc_id = "HGNC:" + hgnc_id
        gene['hgnc_id'] = hgnc_id
        # populate biotype for gene
        if 'gene_biotype' in gene_feature.qualifiers:
            gene['biotype'] = gene_feature.qualifiers['gene_biotype'][0]
        else:
            gene['biotype'] = None
        gene['session_id'] = None
        gene['loc_checksum'] = ChecksumHandler.get_location_checksum(gene)
        gene['gene_checksum'] = ChecksumHandler.get_gene_checksum(gene)
        return gene

    @classmethod
    def get_annotated_transcript(cls, sequence_handler, chrom, mRNA_feature):
        transcript = {}
        # Note we have shifted one base here
        transcript['assembly_id'] = cls.ASSEMBLY_ID
        transcript['assembly_name'] = cls.ASSEMBLY_NAME
        transcript['loc_start'] = str(mRNA_feature.location.start + 1)
        transcript['loc_end'] = str(mRNA_feature.location.end)
        transcript['loc_strand'] = str(mRNA_feature.location.strand)
        transcript['loc_region'] = str(chrom)
        stable_id = mRNA_feature.qualifiers['transcript_id'][0]
        (transcript_stable_id, transcript_stable_id_version) = stable_id.split(".")
        transcript['stable_id'] = transcript_stable_id
        transcript['stable_id_version'] = transcript_stable_id_version
        # populate biotype for transcript
        if transcript_stable_id[0:3] == "NM_":
            transcript['biotype'] = "protein_coding"
        elif transcript_stable_id[0:3] == "NR_":
            transcript['biotype'] = "non_protein_coding"
        elif transcript_stable_id[0:3] == "XM_":
            transcript['biotype'] = "predicted_protein_coding"
        elif transcript_stable_id[0:3] == "XR_":
            transcript['biotype'] = "predicted_non_protein_coding"
        else:
            transcript['biotype'] = None       
        transcript['session_id'] = None
        transcript['transcript_checksum'] = None
        transcript['exon_set_checksum'] = None
        transcript['loc_checksum'] = ChecksumHandler.get_location_checksum(transcript)
        transcript['sequence'] = sequence_handler.get_sequence_by_id(mRNA_feature.qualifiers['transcript_id'][0])
        transcript['seq_checksum'] = ChecksumHandler.get_seq_checksum(transcript, 'sequence')
        return transcript

    @classmethod
    def get_annotated_exons(cls, sequence_handler, seq_region, transcript_identifier, refseq_exon_list):
        exon_sequences = []

        refseq_exon_list_relative_coordinates = ExonUtils.compute_exon_coordinates(refseq_exon_list.copy())
#         '''
#         Ref: BioSeqFeature
#         Note that the start and end location numbering follow Python's scheme,
#         thus a GenBank entry of 123..150 (one based counting) becomes a location
#         of [122:150] (zero based counting).
#         '''

        for exon in refseq_exon_list_relative_coordinates:
            sequence = sequence_handler.get_seq_record_by_id_location(transcript_identifier,
                                                                      exon['exon_start'], exon['exon_end'],
                                                                      int(exon['exon_strand']))
            exon_sequences.append(str(sequence))
        # exon_sequences = sequence_handler.get_exon_sequences_by_identifier(transcript_identifier)
        # print(exon_sequences)
        annotated_exons = []

        if exon_sequences is None:
            return None

        if len(refseq_exon_list) != len(exon_sequences):
            return None

        for exon_feature, exon_sequence in zip(refseq_exon_list, exon_sequences):
            annotated_exons.append(cls.get_annotated_exon(seq_region, exon_feature, exon_sequence))

        return annotated_exons

    @classmethod
    def get_annotated_exon(cls, seq_region, exon_feature, exon_sequence):
        exon = {}
        exon['assembly_id'] = cls.ASSEMBLY_ID
        exon['assembly_name'] = cls.ASSEMBLY_NAME
        exon['loc_start'] = exon_feature["exon_start"]
        exon['loc_end'] = exon_feature["exon_end"]
        exon['loc_strand'] = exon_feature["exon_strand"]
        exon['loc_region'] = str(seq_region)
        exon['loc_checksum'] = ChecksumHandler.get_location_checksum(exon)
        exon['exon_order'] = exon_feature["exon_order"]
        exon['stable_id'] = exon_feature["exon_stable_id"]
        exon['stable_id_version'] = exon_feature["exon_stable_id_version"]
        exon['session_id'] = None
        exon['exon_seq'] = exon_sequence
        exon['seq_checksum'] = ChecksumHandler.get_seq_checksum(exon, 'exon_seq')
        exon['exon_checksum'] = ChecksumHandler.get_exon_checksum(exon)

        return exon

    @classmethod
    def get_annotated_cds(cls, protein_sequence_handler, seq_region, protein_id, cds_list):

        cds_strand = cds_list[0]['cds_strand']
        protein_id = cds_list[0]['protein_id']
        (stable_id, stable_id_version) = protein_id.split(".")

        (translation_start, translation_end) = cls.get_translation_loc(cds_list)
        translation = {}
        translation['assembly_id'] = cls.ASSEMBLY_ID
        translation['assembly_name'] = cls.ASSEMBLY_NAME
        translation['stable_id'] = stable_id
        translation['stable_id_version'] = stable_id_version
        translation['loc_start'] = translation_start
        translation['loc_end'] = translation_end
        translation['loc_strand'] = cds_strand
        translation['loc_region'] = seq_region
        translation['translation_seq'] = protein_sequence_handler.get_fasta_seq_by_id(protein_id)
        translation['seq_checksum'] = ChecksumHandler.get_seq_checksum(translation, 'translation_seq')
        translation['session_id'] = None
        translation['loc_checksum'] = ChecksumHandler.get_location_checksum(translation)

        translation['translation_checksum'] = ChecksumHandler.get_translation_checksum(translation)

        return translation

    @classmethod
    def get_translation_loc(cls, cds_list):
        cds = cds_list[0]
        if(cds['cds_strand'] == '1'):
            cds_start = [cds['cds_start'] for cds in cds_list if cds['cds_order'] == 1]
            cds_end = [cds['cds_end'] for cds in cds_list if cds['cds_order'] == len(cds_list)]
        elif(cds['cds_strand'] == '-1'):
            cds_start = [cds['cds_start'] for cds in cds_list if cds['cds_order'] == len(cds_list)]
            cds_end = [cds['cds_end'] for cds in cds_list if cds['cds_order'] == 1]

        if len(cds_start) > 0 and len(cds_end) > 0:
            return (cds_start[0], cds_end[0])
        else:
            return (0, 0)

    @classmethod
    def parse_qualifiers(cls, qualifiers, key_qualifier, attr=None):
        if key_qualifier in qualifiers:
            cur_qualifiers = qualifiers[key_qualifier]
            for cur_qualifier in cur_qualifiers:
                if attr is not None:
                    my_regex = attr + ":" + "(.*)"
                    matchObj = re.match( my_regex, cur_qualifier, re.M|re.I)  # @IgnorePep8
                    if matchObj and matchObj.group(1):
                        attr_value = matchObj.group(1)
                        return str(attr_value)
        return None

    @classmethod
    def get_seq_region_from_refseq_accession(cls, refseq_accession):
        matchObj = re.match( r'NC_(\d+)\.\d+', refseq_accession, re.M|re.I)  # @IgnorePep8

        if matchObj and matchObj.group(1):
            chrom = int(matchObj.group(1))
            if chrom == 23:
                return "X"
            elif chrom == 24:
                return "Y"
            else:
                return chrom

    @classmethod
    def add_feature_sequence(cls, fasta_handler, feature_locations, feature_id, feature_type='exon'):
        features_with_seq = []
        for feature in feature_locations:
            feature_seq = fasta_handler.get_fasta_seq_by_id(feature_id,
                                                            feature[feature_type + '_start'],
                                                            feature[feature_type + '_end'])
            feature[feature_type + '_seq'] = feature_seq
            features_with_seq.append(feature)
        return features_with_seq

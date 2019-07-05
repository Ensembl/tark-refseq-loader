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

import os
import luigi
from luigi.contrib.lsf import LSFJobTask

from BCBio import GFF

from tark_refseq_loader.task_wrappers.handlers.refseq.annotationhandler import AnnotationHandler
from tark_refseq_loader.task_wrappers.handlers.refseq.databasehandler import DatabaseHandler
from tark_refseq_loader.task_wrappers.handlers.refseq.databasehandler import FeatureHandler
from tark_refseq_loader.task_wrappers.handlers.refseq.checksumhandler import ChecksumHandler
from tark_refseq_loader.task_wrappers.handlers.refseq.fastahandler import FastaHandler
from tark_refseq_loader.task_wrappers.handlers.refseq.confighandler import ConfigHandler


class ParseRecord(LSFJobTask):

    download_dir = luigi.Parameter()
    downloaded_files = luigi.DictParameter()
    seq_region = luigi.Parameter()
    parent_ids = luigi.DictParameter()
    limits = luigi.DictParameter()
    dryrun = luigi.BoolParameter()
    ini_file = luigi.Parameter()
    status_file = None

    def output(self):
        status_dir = self.download_dir + '/' + 'status_logs'
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        status_file = status_dir + '/' + 'status_file_chr' + str(self.seq_region)
        return luigi.LocalTarget(status_file)

    def work(self):

        config = ConfigHandler(ini_file=self.ini_file)
        # mydb_config = config.getInstance().get_section_config(
        #     section_name="DATABASE"
        # )
        dbh = DatabaseHandler(
            ini_file=self.ini_file,
            mypool_name="mypool_" + str(self.seq_region)
        )
        dbc = dbh.get_connection()

        sequence_handler = FastaHandler(
            self.downloaded_files['fasta']
        )

        print("Loading protein.....")
        print(self.downloaded_files['protein'])
        protein_sequence_handler = FastaHandler(self.downloaded_files['protein'])

        print("Working on Seq region limit " + str(self.seq_region))

        gff_handle = open(self.downloaded_files['gff'])

        annotation_handle = AnnotationHandler(self.ini_file)

        # Chromosome seq level
        # for rec in GFF.parse(gff_handle, limit_info=self.limits, target_lines=1000):
        for rec in GFF.parse(gff_handle, limit_info=self.limits):

            for gene_feature in rec.features:

                # skip regions
                if gene_feature.type == "region":
                    continue

                annotated_gene = annotation_handle.get_annotated_gene(
                    self.seq_region,
                    gene_feature
                )

                # gene level
                annotated_transcripts = []
                for mRNA_feature in gene_feature.sub_features:

                    if 'transcript_id' in mRNA_feature.qualifiers:
                        transcript_id = mRNA_feature.qualifiers['transcript_id'][0]
                    else:
                        continue

                    refseq_exon_list = []
                    refseq_exon_order = 1

                    refseq_cds_list = []
                    refseq_cds_order = 1
                    for mRNA_sub_feature in mRNA_feature.sub_features:
                        refseq_exon_dict = {}
                        if 'exon' in mRNA_sub_feature.type:
                            # print("Transcript Has exons" + str(mRNA_sub_feature.id))
                            refseq_exon_dict['exon_stable_id'] = str(mRNA_sub_feature.id)
                            refseq_exon_dict['exon_stable_id_version'] = 1  # dummmy version
                            refseq_exon_dict['exon_order'] = refseq_exon_order
                            # note that we are shifting one base here
                            refseq_exon_dict['exon_start'] = str(mRNA_sub_feature.location.start + 1)
                            refseq_exon_dict['exon_end'] = str(mRNA_sub_feature.location.end)
                            refseq_exon_dict['exon_strand'] = str(mRNA_sub_feature.location.strand)
                            refseq_exon_list.append(refseq_exon_dict)
                            refseq_exon_order += 1

                        refseq_cds_dict = {}
                        if 'CDS' in mRNA_sub_feature.type:

                            refseq_cds_dict['cds_order'] = refseq_cds_order
                            # note that we are shifting one base here
                            refseq_cds_dict['cds_start'] = str(mRNA_sub_feature.location.start + 1)
                            refseq_cds_dict['cds_end'] = str(mRNA_sub_feature.location.end)
                            refseq_cds_dict['cds_strand'] = str(mRNA_sub_feature.location.strand)
                            refseq_cds_dict['cds_id'] = str(mRNA_sub_feature.id)
                            refseq_cds_dict['protein_id'] = str(mRNA_sub_feature.qualifiers['protein_id'][0])  # @IgnorePep8
                            refseq_cds_list.append(refseq_cds_dict)
                            refseq_cds_order += 1

                    annotated_transcript = annotation_handle.get_annotated_transcript(
                        sequence_handler,
                        self.seq_region,
                        mRNA_feature
                    )

                    # add sequence and other annotations
                    annotated_exons = []
                    if len(refseq_exon_list) > 0:
                        annotated_exons = annotation_handle.get_annotated_exons(
                            sequence_handler,
                            self.seq_region,
                            transcript_id,
                            refseq_exon_list
                        )

                        if annotated_exons is not None and len(annotated_exons) > 0:

                            exon_set_checksum = ChecksumHandler.get_exon_set_checksum(annotated_exons)
                            annotated_transcript['exon_set_checksum'] = exon_set_checksum
                            annotated_transcript['exons'] = annotated_exons
                        else:
                            annotated_transcript['exons'] = []

                    annotated_translation = []
                    if len(refseq_cds_list) > 0:
                        protein_id = refseq_cds_list[0]['protein_id']
                        annotated_translation = annotation_handle.get_annotated_cds(
                            protein_sequence_handler,
                            self.seq_region,
                            protein_id,
                            refseq_cds_list
                        )
                        annotated_transcript['translation'] = annotated_translation
                    else:
                        annotated_transcript['translation'] = []

                    annotated_transcript['transcript_checksum'] = ChecksumHandler.get_transcript_checksum(annotated_transcript)  # @IgnorePep8
                    annotated_transcripts.append(annotated_transcript)

                annotated_gene['transcripts'] = annotated_transcripts
                feature_object_to_save = {}
                feature_object_to_save["gene"] = annotated_gene

                if (
                        not self.dryrun
                        and annotated_gene is not None
                        and annotated_gene['stable_id'] is not None
                ):
                    print("About to load gene => " + str(annotated_gene['stable_id']))
                    feature_handler = FeatureHandler(parent_ids=self.parent_ids, dbc=dbc)
                    feature_handler.save_features_to_database(feature_object_to_save)

        dbc.close()
        gff_handle.close()

        print("About to write to the status file")
        status_dir = self.download_dir + '/' + 'status_logs'
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        self.status_file = status_dir + '/' + 'status_file_chr' + str(self.seq_region)
        status_handle = open(self.status_file, "w")
        status_handle.write("Done")
        status_handle.close()

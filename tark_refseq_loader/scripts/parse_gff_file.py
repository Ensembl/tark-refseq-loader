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
import sys
import argparse
import re

import luigi
from luigi.contrib.lsf import LSFJobTask


from BCBio import GFF

from handlers.refseq.annotationhandler import AnnotationHandler
from handlers.refseq.databasehandler import DatabaseHandler, FeatureHandler
from handlers.refseq.checksumhandler import ChecksumHandler
from handlers.refseq.fastahandler import FastaHandler
from handlers.refseq.confighandler import ConfigHandler

SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=4096"
MEMORY_FLAG_ALIGNMENT = "4096"
RESOURCE_FLAG_MERGE = "mem=4096"
MEMORY_FLAG_MERGE = "4096"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False


class ParseRecord(LSFJobTask):

    download_dir = luigi.Parameter()
    downloaded_files = luigi.DictParameter()
    seq_region = luigi.Parameter()
    parent_ids = luigi.DictParameter()
    limits = luigi.TupleParameter()
    dryrun = luigi.BoolParameter()
    status_file = None

    def output(self):
        status_dir = self.download_dir + '/' + 'status_logs'
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        status_file = status_dir + '/' + 'status_file_chr' + str(self.seq_region)
        return luigi.LocalTarget(status_file)

    def work(self):

        mydb_config = ConfigHandler().getInstance().get_section_config(section_name="DATABASE")
        dbh = DatabaseHandler(
            db_config=mydb_config,
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

        # Chromosome seq level
        for rec in GFF.parse(gff_handle, limit_info=self.limits, target_lines=1000):

            for gene_feature in rec.features:

                # skip regions
                if gene_feature.type == "region":
                    continue

                annotated_gene = AnnotationHandler.get_annotated_gene(self.seq_region, gene_feature)

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

                    annotated_transcript = AnnotationHandler.get_annotated_transcript(
                        sequence_handler,
                        self.seq_region,
                        mRNA_feature
                    )

                    # add sequence and other annotations
                    annotated_exons = []
                    if len(refseq_exon_list) > 0:
                        annotated_exons = AnnotationHandler.get_annotated_exons(
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
                        annotated_translation = AnnotationHandler.get_annotated_cds(
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


class ParseGffFileWrapper(luigi.Task):
    """
    Wrapper Task to parse gff file
    """

    download_dir = luigi.Parameter()
    dryrun = luigi.BoolParameter()
    limit_chr = luigi.Parameter()
    user_python_path = luigi.Parameter()

    gff_file = 'GCF_000001405.38_GRCh38.p12_genomic.gff'
    fasta_file = 'GCF_000001405.38_GRCh38.p12_rna.fna'
    protein_file = 'GCF_000001405.38_GRCh38.p12_protein.faa'

    def output(self):
        """
        """

    def run(self):
        downloaded_files = {}
        downloaded_files['gff'] = os.path.join(self.download_dir, self.gff_file)
        downloaded_files['fasta'] = os.path.join(self.download_dir, self.fasta_file)
        downloaded_files['protein'] = os.path.join(self.download_dir, self.protein_file)

        # Examine for available regions
        # examiner = GFF.GFFExaminer()

        # load the parent tables
        parent_ids = None
        # use for debugging only

        if not self.dryrun:
            mydb_config = ConfigHandler().getInstance().get_section_config(section_name="DATABASE")
            dbh = DatabaseHandler(
                db_config=mydb_config,
                mypool_name="mypool_parentids"
            )

            print(dbh)
            feature_handler = FeatureHandler(dbc=dbh.get_connection())
            parent_ids = feature_handler.populate_parent_tables()

        print(downloaded_files['gff'])

        # You could examine the file to get the possible chr, initialising it to save some time
        #         with open(downloaded_files['gff']) as gff_handle_examiner:
        #             possible_limits = examiner.available_limits(gff_handle_examiner)
        #             chromosomes = sorted(possible_limits["gff_id"].keys())
        chromosomes = [
            ('NC_000001.11',),
            ('NC_000002.12',),
            ('NC_000003.12',),
            ('NC_000004.12',),
            ('NC_000005.10',),
            ('NC_000006.12',),
            ('NC_000007.14',),
            ('NC_000008.11',),
            ('NC_000009.12',),
            ('NC_000010.11',),
            ('NC_000011.10',),
            ('NC_000012.12',),
            ('NC_000013.11',),
            ('NC_000014.9',),
            ('NC_000015.10',),
            ('NC_000016.10',),
            ('NC_000017.11',),
            ('NC_000018.10',),
            ('NC_000019.10',),
            ('NC_000020.11',),
            ('NC_000021.9',),
            ('NC_000022.11',),
            ('NC_000023.11',),
            ('NC_000024.10',),
            ('NC_012920.1',)
        ]
        limits = dict()
        # for testing
        filter_regions = None
        parse_jobs = []
        for chrom_tuple in chromosomes:
            chrom = chrom_tuple[0]
            if not chrom.startswith("NC_"):
                continue
            print(chrom_tuple)

            seq_region = self.get_seq_region_from_refseq_accession(chrom)

            # Restrict only for filter_region
            if self.limit_chr is not None:
                if ',' in self.limit_chr:
                    filter_regions = self.limit_chr.split(',')
                else:
                    filter_regions = [self.limit_chr]

                if str(seq_region) not in filter_regions:
                    print(" Skipping " + str(seq_region))
                    continue

            limits["gff_id"] = chrom_tuple

            parse_job = ParseRecord(
                download_dir=self.download_dir,
                downloaded_files=downloaded_files,
                seq_region=str(seq_region),
                parent_ids=parent_ids,
                limits=limits,
                dryrun=self.dryrun
            )
            parse_jobs.append(parse_job)

        yield parse_jobs

    def get_seq_region_from_refseq_accession(self, refseq_accession):
        matchObj = re.match( r'NC_(\d+)\.\d+', refseq_accession, re.M|re.I)  # @IgnorePep8

        if matchObj and matchObj.group(1):
            chrom = int(matchObj.group(1))
            if chrom == 23:
                return "X"
            elif chrom == 24:
                return "Y"
            elif chrom == 12920:
                return "MT"
            else:
                return chrom

if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(
        description="RefSeq Loader Pipeline Wrapper")
    PARSER.add_argument("--download_dir", default="/tmp", help="Path to where the downloaded files should be saved")
    PARSER.add_argument("--dryrun", default=".", help="Load to db or not")
    PARSER.add_argument("--workers", default="4", help="Workers")
    PARSER.add_argument("--limit_chr", default=None, help="Limit the chr")
    PARSER.add_argument("--python_path", default=sys.executable, help="")
    PARSER.add_argument("--shared_tmp_dir", help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()
    print(ARGS)

    luigi.build(
        [
            ParseGffFileWrapper(
                download_dir=ARGS.download_dir,
                dryrun=ARGS.dryrun,
                limit_chr=ARGS.limit_chr,
                user_python_path=ARGS.python_path
            )
        ],
        workers=ARGS.workers, local_scheduler=True, parallel_scheduling=True, no_lock=True)

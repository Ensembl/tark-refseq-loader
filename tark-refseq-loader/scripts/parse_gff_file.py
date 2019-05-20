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
import luigi
import os
import wget
from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.lsf import LocalLSFJobTask
import subprocess
from BCBio import GFF
import argparse
import re
from handlers.refseq.annotationhandler import AnnotationHandler
from handlers.refseq.databasehandler import DatabaseHandler
from handlers.refseq.checksumhandler import ChecksumHandler
from handlers.refseq.genbankhandler import GenBankHandler
from handlers.refseq.fastahandler import FastaHandler
from handlers.refseq.confighandler import ConfigHandler
from luigi.contrib.lsf import LSFJobTask
import sys

SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=16384"
MEMORY_FLAG_ALIGNMENT = "16384"
RESOURCE_FLAG_MERGE = "mem=16384"
MEMORY_FLAG_MERGE = "16384"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False


#class ParseRecord(luigi.Task):
class ParseRecord(LSFJobTask):
    download_dir = luigi.Parameter()
    downloaded_files = luigi.DictParameter()
    seq_region = luigi.Parameter()
    parent_ids = luigi.DictParameter()
    limits = luigi.TupleParameter()
    dryrun = luigi.BoolParameter()
    status_file = None



    # task_namespace = 'ParseGffFile'
#
#     def requires(self):
#         return DownloadRefSeqSourceFile(self.download_dir, self.file_to_download, self.ftp_root)

    def output(self):
        status_dir = self.download_dir + '/' + 'status_logs'
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        status_file = status_dir + '/' + 'status_file_chr' + str(self.seq_region)
        return luigi.LocalTarget(status_file)

    def work(self):

        print("Loading gbff.....")
        print(self.downloaded_files['gbff'])

        #sequence_handler = GenBankHandler(self.downloaded_files['gbff'])
        sequence_handler = FastaHandler(self.downloaded_files['fasta'])

        print("Loading protein.....")
        print(self.downloaded_files['protein'])
        protein_sequence_handler = FastaHandler(self.downloaded_files['protein'])

        print("Working on Seq region limit " + str(self.seq_region))

        gff_handle = open(self.downloaded_files['gff'])
        #with open(self.downloaded_files['gff']) as gff_handle:

            # Chromosome seq level
        for rec in GFF.parse(gff_handle, limit_info=self.limits):

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

#                         #if transcript_id != "NM_000417.2":
#                             continue

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

                    annotated_transcript = AnnotationHandler.get_annotated_transcript(sequence_handler,
                                                                                      self.seq_region,
                                                                                      mRNA_feature)

                    # add sequence and other annotations
                    annotated_exons = []
                    if len(refseq_exon_list) > 0:
                        annotated_exons = AnnotationHandler.get_annotated_exons(sequence_handler, self.seq_region,
                                                                                transcript_id,
                                                                                refseq_exon_list)

                        if annotated_exons is not None and len(annotated_exons) > 0:

                            exon_set_checksum = ChecksumHandler.get_exon_set_checksum(annotated_exons)
                            annotated_transcript['exon_set_checksum'] = exon_set_checksum
                            annotated_transcript['exons'] = annotated_exons
                        else:
                            annotated_transcript['exons'] = []

                    annotated_translation = []
                    if len(refseq_cds_list) > 0:
                        protein_id = refseq_cds_list[0]['protein_id']
                        annotated_translation = AnnotationHandler.get_annotated_cds(protein_sequence_handler,
                                                                                    self.seq_region,
                                                                      protein_id,
                                                                      refseq_cds_list)
                        annotated_transcript['translation'] = annotated_translation
                    else:
                        annotated_transcript['translation'] = []

                    annotated_transcript['transcript_checksum'] = ChecksumHandler.get_transcript_checksum(annotated_transcript)  # @IgnorePep8
                    annotated_transcripts.append(annotated_transcript)

                annotated_gene['transcripts'] = annotated_transcripts
                feature_object_to_save = {}
                feature_object_to_save["gene"] = annotated_gene

                # remove later
                #if len(annotated_gene['transcripts']) > 0:
                    #print(feature_object_to_save)

                if not self.dryrun:
                    mydb_config = ConfigHandler().getInstance().get_section_config(section_name="DATABASE")
                    dbh = DatabaseHandler(db_config=mydb_config,
                                             mypool_name="mypool_" + str(self.seq_region))
                    dbh.save_features_to_database(feature_object_to_save, self.parent_ids)
                    dbh.cnxpool.close()
#                     if status is None:
#                         print("====Feature not saved for " + str(self.parent_ids))

        gff_handle.close()

        print("About to write to the status file")
        status_dir = self.download_dir + '/' + 'status_logs'
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        self.status_file = status_dir + '/' + 'status_file_chr' + str(self.seq_region)
        status_handle = open(self.status_file, "w")
        status_handle.write("Done")
        status_handle.close()


# time PYTHONPATH='.' python scripts/parse_gff_file.py --download_dir='/Users/prem/workspace/software/tmp/refseq_download_dir'
# time PYTHONPATH='.' python scripts/parse_gff_file.py --download_dir='/hps/nobackup2/production/ensembl/prem/refseq_download' --python_path='/homes/prem/workspace/software/tark-refseq-loader/tark-refseq-loader'
class ParseGffFileWrapper(luigi.WrapperTask):
    """
    Wrapper Task to parse gff file
    """

    download_dir = luigi.Parameter()
    gff_file = 'GCF_000001405.38_GRCh38.p12_genomic.gff'
    genbank_file = 'GCF_000001405.38_GRCh38.p12_rna.gbff'
    fasta_file = 'GCF_000001405.38_GRCh38.p12_rna.fna'
    protein_file = 'GCF_000001405.38_GRCh38.p12_protein.faa'
    # user_python_path = luigi.Parameter()

    def requires(self):
        downloaded_files = {}
        downloaded_files['gff'] = self.download_dir + "/" + self.gff_file
        downloaded_files['fasta'] = self.download_dir + "/" + self.fasta_file
        downloaded_files['protein'] = self.download_dir + "/" + self.protein_file
        downloaded_files['gbff'] = self.download_dir + "/" + self.genbank_file

        # Examine for available regions
        examiner = GFF.GFFExaminer()

        # load the parent tables
        parent_ids = None
        # use for debugging only
        dryrun = False

        if not dryrun:
            mydb_config = ConfigHandler().getInstance().get_section_config(section_name="DATABASE")
            parent_ids = DatabaseHandler(db_config=mydb_config,
                                         mypool_name="mypool_parentids").populate_parent_tables()

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
        # filter_regions = None
        filter_regions = ['1', '2']
        # filter_regions = ['21', '22']
        for chrom_tuple in chromosomes:
            chrom = chrom_tuple[0]
            if not chrom.startswith("NC_"):
                continue
            print(chrom_tuple)

            seq_region = self.get_seq_region_from_refseq_accession(chrom)

            # Restrict only for filter_region
            if filter_regions is not None:
                if str(seq_region) not in filter_regions:
                    print(" Skipping " + str(seq_region))
                    continue

            limits["gff_id"] = chrom_tuple

            # resource_flag = (defaults to mem=8192)
            # memory_flag = (defaults to 8192)
            # queue_flag = (defaults to queue_name)
            # runtime_flag = (defaults to 60)
            # job_name_flag = (defaults to )

            yield ParseRecord(
                   download_dir=self.download_dir,
                   downloaded_files=downloaded_files,
                   seq_region=str(seq_region),
                   parent_ids=parent_ids,
                   limits=limits,
                   dryrun=dryrun,
                   queue_flag=QUEUE_FLAG,
                   save_job_info=SAVE_JOB_INFO,
                   resource_flag=RESOURCE_FLAG_MERGE,
                   memory_flag=MEMORY_FLAG_MERGE,
                   shared_tmp_dir=self.tmp_dir

                )

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
    PARSER.add_argument("--tmp_dir", default="/tmp", help="TMP dir")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    luigi.build(
        [
            ParseGffFileWrapper(
                download_dir=ARGS.download_dir,
                tmp_dir=ARGS.tmp_dir
            )
        ],
        workers=25, local_scheduler=True)

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
import subprocess
from BCBio import GFF
import argparse
import re
from handlers.refseq.annotationhandler import AnnotationHandler
from handlers.refseq.databasehandler import DatabaseHandler
from handlers.refseq.checksumhandler import ChecksumHandler
from handlers.refseq.genbankhandler import GenBankHandler
from handlers.refseq.fastahandler import FastaHandler


class ParseRecord(luigi.Task):
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

    def run(self):

        print("Loading gbff.....")
        print(self.downloaded_files['gbff'])
        sequence_handler = GenBankHandler(self.downloaded_files['gbff'])

        print("Loading protein.....")
        print(self.downloaded_files['protein'])
        protein_sequence_handler = FastaHandler(self.downloaded_files['protein'])

        print(" Seq region limit " + str(self.seq_region))
        with open(self.downloaded_files['gff']) as gff_handle:

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
                            print("Has transcript id " + str(transcript_id))
                        else:
                            continue

                        if transcript_id != "NM_194255.3":
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

                        annotated_transcript = AnnotationHandler.get_annotated_transcript(sequence_handler, self.seq_region,
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
                    if len(annotated_gene['transcripts']) > 0:
                        print(feature_object_to_save)

                    if not self.dryrun:
                        status = DatabaseHandler.getInstance().save_features_to_database(feature_object_to_save,
                                                                                         self.parent_ids)
                        if status is None:
                            print("====Feature not save for " + str(self.parent_ids))

        print("About to write to the status file")
        status_dir = self.download_dir + '/' + 'status_logs'
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        self.status_file = status_dir + '/' + 'status_file_chr' + str(self.seq_region)
        status_handle = open(self.status_file, "w")
        status_handle.write("Done")
        status_handle.close()

# 
# class CheckStatus(luigi.Task):
#     def output(self):
#         status_file = self.download_dir + '/' + 'status_file.txt'
#         return luigi.LocalTarget(status_file)
# 
#     def run(self):
#         print("About to write to the status file")
#         status_handle = open(self.download_dir + '/status_file.txt', "w")
#         status_handle.write("Done")
#         status_handle.close()

#time PYTHONPATH='.' python scripts/parse_gff_file.py --download_dir='/Users/prem/workspace/software/tmp/refseq_download_dir'
class ParseGffFileWrapper(luigi.WrapperTask):
    """
    Wrapper Task to parse gff file
    """

    download_dir = luigi.Parameter()
    gff_file = 'GCF_000001405.38_GRCh38.p12_genomic.gff'
    genbank_file = 'GCF_000001405.38_GRCh38.p12_rna.gbff'
    fasta_file = 'GCF_000001405.38_GRCh38.p12_rna.fna'
    protein_file = 'GCF_000001405.38_GRCh38.p12_protein.faa'

    #task_namespace = 'ParseGffFileWrapper'
#     def complete(self):
#         complete_list = []
#         for file_ in self.files_to_download:
#             base = os.path.basename(file_)
#             downloaded_file_url_zipped = self.download_dir + '/' + file_
#             downloaded_file_url_unzipped = self.download_dir + '/' + os.path.splitext(base)[0]
# 
#             if os.path.exists(downloaded_file_url_zipped) and os.path.exists(downloaded_file_url_unzipped):
#                 complete_list.append(True)
# 
#         if len(complete_list) == len(self.files_to_download):
#             return True
#         else:
#             return False
    def requires(self):
        downloaded_files = {}
        downloaded_files['gff'] = self.download_dir + "/" + self.gff_file
        downloaded_files['fasta'] = self.download_dir + "/" + self.fasta_file
        downloaded_files['protein'] = self.download_dir + "/" + self.protein_file
        downloaded_files['gbff'] = self.download_dir + "/" + self.genbank_file
# 
#         sequence_handler = GenBankHandler(downloaded_files['gbff'])
#         protein_sequence_handler = FastaHandler(downloaded_files['protein'])

        # Examine for available regions
        examiner = GFF.GFFExaminer()

        # load the parent tables
        parent_ids = None
        dryrun = False
        if not dryrun:
            parent_ids = DatabaseHandler.getInstance().populate_parent_tables()

        print(downloaded_files['gff'])
        with open(downloaded_files['gff']) as gff_handle_examiner:
            possible_limits = examiner.available_limits(gff_handle_examiner)
            chromosomes = sorted(possible_limits["gff_id"].keys())


            limits = dict()
            # for testing
            filter_regions = ['21']
            for chrom_tuple in chromosomes:
                chrom = chrom_tuple[0]
                if not chrom.startswith("NC_"):
                    continue


                seq_region = self.get_seq_region_from_refseq_accession(chrom)

                print(" Seq region " + str(seq_region))

                # Restrict only for filter_region
                if filter_regions is not None:
                    if str(seq_region) not in filter_regions:
                        print(" Skipping " + str(seq_region))
                        continue

                print(" Seq region limit " + str(seq_region))
                limits["gff_id"] = chrom_tuple

                yield ParseRecord(
                       self.download_dir,
                       downloaded_files,
                       str(seq_region),
                       parent_ids,
                       limits,
                       True
                    )

#         yield CheckStatus()


    def get_seq_region_from_refseq_accession(self, refseq_accession):
        matchObj = re.match( r'NC_(\d+)\.\d+', refseq_accession, re.M|re.I)  # @IgnorePep8

        if matchObj and matchObj.group(1):
            chrom = int(matchObj.group(1))
            if chrom == 23:
                return "X"
            elif chrom == 24:
                return "Y"
            else:
                return chrom

if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(
        description="RefSeq Loader Pipeline Wrapper")
    PARSER.add_argument("--download_dir", default="/tmp", help="Path to where the downloaded files should be saved")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    luigi.build(
        [
            ParseGffFileWrapper(
                download_dir=ARGS.download_dir,
            )
        ],
        workers=25, local_scheduler=True)
